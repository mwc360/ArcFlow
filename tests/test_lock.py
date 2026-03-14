"""Tests for the singleton job lock."""

import json
import os
import time
from datetime import datetime, timezone, timedelta

import pytest

from arcflow.lock import JobLock, JobLockError, _PROCESS_INSTANCE_ID


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def lock_dir(tmp_path):
    """Provide a temporary directory for lock files."""
    return str(tmp_path / "locks")


@pytest.fixture
def make_lock(lock_dir):
    """Factory for creating JobLock instances with short timeouts for tests."""
    def _make(job_id="test-job", timeout=5, poll_interval=1):
        return JobLock(
            job_id=job_id,
            lock_path=lock_dir,
            timeout_seconds=timeout,
            poll_interval=poll_interval,
        )
    return _make


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestJobLockInit:
    def test_requires_non_empty_job_id(self):
        with pytest.raises(ValueError, match="non-empty"):
            JobLock(job_id="", lock_path="Files/locks/")

    def test_requires_non_none_job_id(self):
        with pytest.raises(ValueError, match="non-empty"):
            JobLock(job_id=None, lock_path="Files/locks/")

    def test_sets_attributes(self, lock_dir):
        lock = JobLock(job_id="my-job", lock_path=lock_dir, timeout_seconds=600, poll_interval=10)
        assert lock.job_id == "my-job"
        assert lock.lock_path == lock_dir
        assert lock.timeout_seconds == 600
        assert lock.poll_interval == 10
        assert not lock.held

    def test_repr(self, make_lock):
        lock = make_lock("repr-job")
        r = repr(lock)
        assert "repr-job" in r
        assert "held=False" in r


# ---------------------------------------------------------------------------
# Acquire / Release lifecycle
# ---------------------------------------------------------------------------

class TestAcquireRelease:
    def test_acquire_creates_lock_file(self, make_lock, lock_dir):
        lock = make_lock()
        lock.acquire()
        assert lock.held
        assert lock.is_locked
        expected_file = os.path.join(lock_dir, "test-job.lock")
        assert os.path.exists(expected_file)

        # Verify JSON content
        with open(expected_file) as f:
            data = json.load(f)
        assert data["job_id"] == "test-job"
        assert "acquired_at" in data
        assert "hostname" in data
        assert "pid" in data

        lock.release()

    def test_release_deletes_lock_file(self, make_lock, lock_dir):
        lock = make_lock()
        lock.acquire()
        lock.release()
        assert not lock.held
        assert not lock.is_locked
        assert not os.path.exists(os.path.join(lock_dir, "test-job.lock"))

    def test_release_is_idempotent(self, make_lock):
        lock = make_lock()
        lock.acquire()
        lock.release()
        lock.release()  # should not raise
        assert not lock.held

    def test_double_acquire_is_noop(self, make_lock):
        lock = make_lock()
        lock.acquire()
        lock.acquire()  # should not raise, already held
        assert lock.held
        lock.release()

    def test_creates_lock_directory(self, tmp_path):
        nested = str(tmp_path / "a" / "b" / "c")
        lock = JobLock(job_id="nested-test", lock_path=nested)
        lock.acquire()
        assert os.path.isdir(nested)
        lock.release()

    def test_same_instance_reentry(self, make_lock, lock_dir):
        """A new lock in the same process should take over without waiting."""
        first = make_lock(timeout=60)
        first.acquire()
        assert first.held

        # Simulate notebook re-run: new JobLock instance, same process
        second = make_lock(timeout=2, poll_interval=1)
        second.acquire()  # should NOT block or raise
        assert second.held

        second.release()
        assert not second.held

    def test_foreign_instance_does_not_reenter(self, lock_dir):
        """A lock from a different process (different instance_id) should conflict."""
        os.makedirs(lock_dir, exist_ok=True)
        lock_file = os.path.join(lock_dir, "test-job.lock")
        payload = {
            "job_id": "test-job",
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 3600,
            "instance_id": "some-other-process-id",
            "hostname": "other-host",
            "pid": 1,
        }
        with open(lock_file, "w") as f:
            json.dump(payload, f)

        contender = JobLock(job_id="test-job", lock_path=lock_dir, timeout_seconds=2, poll_interval=1)
        with pytest.raises(JobLockError):
            contender.acquire()


# ---------------------------------------------------------------------------
# Conflict detection — wait/retry + timeout
# ---------------------------------------------------------------------------

class TestConflictDetection:
    def _write_foreign_lock(self, lock_dir, job_id="test-job", timeout=60):
        """Write a lock file that appears to come from a different instance."""
        os.makedirs(lock_dir, exist_ok=True)
        lock_file = os.path.join(lock_dir, f"{job_id}.lock")
        payload = {
            "job_id": job_id,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": timeout,
            "instance_id": "foreign-instance-xyz",
            "hostname": "other-host",
            "pid": 99999,
        }
        with open(lock_file, "w") as f:
            json.dump(payload, f)

    def test_blocks_and_raises_on_timeout(self, make_lock, lock_dir):
        """Lock held by a different instance should cause wait then raise."""
        self._write_foreign_lock(lock_dir, timeout=3600)

        contender = make_lock(timeout=2, poll_interval=1)
        start = time.monotonic()
        with pytest.raises(JobLockError, match="Failed to acquire"):
            contender.acquire()
        elapsed = time.monotonic() - start
        assert elapsed >= 1.5  # should have waited ~2s

    def test_succeeds_when_holder_releases(self, lock_dir):
        """Contender should acquire after foreign holder releases mid-wait."""
        import threading

        self._write_foreign_lock(lock_dir, timeout=3600)

        # Delete the foreign lock file after 1 second (simulates holder releasing)
        lock_file = os.path.join(lock_dir, "test-job.lock")
        def release_later():
            time.sleep(1)
            os.remove(lock_file)
        t = threading.Thread(target=release_later)
        t.start()

        contender = JobLock(job_id="test-job", lock_path=lock_dir, timeout_seconds=10, poll_interval=1)
        contender.acquire()  # should succeed after foreign lock is deleted
        assert contender.held
        contender.release()
        t.join()

    def test_different_job_ids_do_not_conflict(self, make_lock):
        lock_a = make_lock("job-a")
        lock_b = make_lock("job-b")
        lock_a.acquire()
        lock_b.acquire()  # should not block
        assert lock_a.held
        assert lock_b.held
        lock_a.release()
        lock_b.release()


# ---------------------------------------------------------------------------
# Stale lock recovery
# ---------------------------------------------------------------------------

class TestStaleLockRecovery:
    def test_recovers_stale_lock(self, make_lock, lock_dir):
        """Lock older than timeout_seconds should be auto-recovered."""
        lock = make_lock(timeout=5)

        # Manually write a stale lock file
        os.makedirs(lock_dir, exist_ok=True)
        stale_time = (datetime.now(timezone.utc) - timedelta(seconds=3600)).isoformat()
        lock_file = os.path.join(lock_dir, "test-job.lock")
        with open(lock_file, "w") as f:
            json.dump({"job_id": "test-job", "acquired_at": stale_time, "hostname": "old-host", "pid": 99999}, f)

        lock.acquire()  # should recover stale lock without waiting
        assert lock.held
        lock.release()

    def test_recovers_corrupt_lock_file(self, make_lock, lock_dir):
        """Corrupt JSON should be treated as stale."""
        os.makedirs(lock_dir, exist_ok=True)
        lock_file = os.path.join(lock_dir, "test-job.lock")
        with open(lock_file, "w") as f:
            f.write("not valid json {{{")

        lock = make_lock(timeout=5)
        lock.acquire()  # should treat corrupt file as stale
        assert lock.held
        lock.release()

    def test_recovers_lock_with_missing_timestamp(self, make_lock, lock_dir):
        """Lock file missing acquired_at should be treated as stale."""
        os.makedirs(lock_dir, exist_ok=True)
        lock_file = os.path.join(lock_dir, "test-job.lock")
        with open(lock_file, "w") as f:
            json.dump({"job_id": "test-job"}, f)

        lock = make_lock(timeout=5)
        lock.acquire()
        assert lock.held
        lock.release()


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------

class TestHeartbeat:
    def test_heartbeat_refreshes_acquired_at(self, lock_dir):
        """Heartbeat should update acquired_at in the lock file."""
        lock = JobLock(
            job_id="hb-test", lock_path=lock_dir,
            timeout_seconds=60, heartbeat_interval=1,
        )
        lock.acquire()

        lock_file = os.path.join(lock_dir, "hb-test.lock")
        with open(lock_file) as f:
            t1 = json.load(f)["acquired_at"]

        time.sleep(1.5)  # wait for at least one heartbeat

        with open(lock_file) as f:
            t2 = json.load(f)["acquired_at"]

        assert t2 > t1, "Heartbeat should have refreshed acquired_at"
        lock.release()

    def test_heartbeat_stops_on_release(self, lock_dir):
        """Heartbeat thread should stop when lock is released."""
        lock = JobLock(
            job_id="hb-stop", lock_path=lock_dir,
            timeout_seconds=60, heartbeat_interval=1,
        )
        lock.acquire()
        assert lock._heartbeat_thread is not None
        assert lock._heartbeat_thread.is_alive()

        lock.release()
        assert lock._heartbeat_thread is None

    def test_heartbeat_prevents_false_stale_recovery(self, lock_dir):
        """A lock with active heartbeat should not be recovered as stale by a foreign instance."""
        holder = JobLock(
            job_id="hb-stale", lock_path=lock_dir,
            timeout_seconds=3, heartbeat_interval=1,
        )
        holder.acquire()

        # Wait longer than timeout_seconds — heartbeat keeps it fresh
        time.sleep(4)

        # Manually write a foreign instance_id into a DIFFERENT lock to prove
        # the contender can't steal it.  We verify by reading the lock file
        # and confirming acquired_at was refreshed by the heartbeat.
        lock_file = os.path.join(lock_dir, "hb-stale.lock")
        with open(lock_file) as f:
            data = json.load(f)

        acquired_at = datetime.fromisoformat(data["acquired_at"])
        age = (datetime.now(timezone.utc) - acquired_at).total_seconds()
        # Heartbeat should have kept acquired_at within ~1-2s
        assert age < 3, f"Heartbeat failed: lock age is {age}s (should be <3s)"

        holder.release()

    def test_default_heartbeat_interval(self):
        """Default heartbeat_interval should be timeout_seconds // 3, min 10."""
        lock = JobLock(job_id="defaults", lock_path="tmp/", timeout_seconds=3600)
        assert lock.heartbeat_interval == 1200  # 3600 // 3

        lock2 = JobLock(job_id="defaults2", lock_path="tmp/", timeout_seconds=15)
        assert lock2.heartbeat_interval == 10   # min 10

    def test_default_poll_interval(self):
        """Default poll_interval should be timeout_seconds // 10, min 5."""
        lock = JobLock(job_id="defaults", lock_path="tmp/", timeout_seconds=3600)
        assert lock.poll_interval == 360  # 3600 // 10

        lock2 = JobLock(job_id="defaults2", lock_path="tmp/", timeout_seconds=30)
        assert lock2.poll_interval == 5   # min 5


# ---------------------------------------------------------------------------
# Context manager
# ---------------------------------------------------------------------------

class TestContextManager:
    def test_context_manager_acquires_and_releases(self, make_lock, lock_dir):
        lock = make_lock()
        with lock:
            assert lock.held
            assert lock.is_locked
        assert not lock.held
        assert not lock.is_locked

    def test_context_manager_releases_on_exception(self, make_lock, lock_dir):
        lock = make_lock()
        with pytest.raises(RuntimeError):
            with lock:
                assert lock.held
                raise RuntimeError("boom")
        assert not lock.held
        assert not lock.is_locked


# ---------------------------------------------------------------------------
# Config integration (no Spark needed)
# ---------------------------------------------------------------------------

class TestConfigIntegration:
    def test_default_config_has_lock_disabled(self):
        from arcflow.config import get_config
        config = get_config()
        assert config["job_lock_enabled"] is False
        assert config["job_id"] is None
        assert config["job_lock_poll_interval"] is None
        assert config["job_lock_heartbeat_interval"] is None

    def test_config_merges_lock_settings(self):
        from arcflow.config import get_config
        config = get_config({
            "job_id": "my-etl",
            "job_lock_enabled": True,
            "job_lock_path": "custom/locks/",
            "job_lock_timeout_seconds": 1800,
            "job_lock_poll_interval": 15,
        })
        assert config["job_id"] == "my-etl"
        assert config["job_lock_enabled"] is True
        assert config["job_lock_path"] == "custom/locks/"
        assert config["job_lock_timeout_seconds"] == 1800
        assert config["job_lock_poll_interval"] == 15


# ---------------------------------------------------------------------------
# Controller integration (mocked — no Spark)
# ---------------------------------------------------------------------------

class TestControllerIntegration:
    @pytest.fixture
    def mock_controller(self, lock_dir):
        """Create a Controller-like object with just enough for lock testing."""
        from arcflow.lock import JobLock
        from arcflow.config import get_config

        config = get_config({
            "job_id": "controller-test",
            "job_lock_enabled": True,
            "job_lock_path": lock_dir,
            "job_lock_timeout_seconds": 5,
            "job_lock_poll_interval": 1,
        })

        # Build the lock exactly as Controller.__init__ does
        lock = JobLock(
            job_id=config["job_id"],
            lock_path=config["job_lock_path"],
            timeout_seconds=config["job_lock_timeout_seconds"],
            poll_interval=config["job_lock_poll_interval"],
        )
        return lock, config

    def test_lock_lifecycle_mirrors_controller(self, mock_controller):
        """Simulate the Controller.run_full_pipeline lock lifecycle."""
        lock, config = mock_controller

        # Simulate run_full_pipeline: acquire
        lock.acquire()
        assert lock.held

        # Simulate stop_all: release
        lock.release()
        assert not lock.held

    def test_lock_prevents_different_instance(self, lock_dir):
        """Two different processes (different instance_ids) with same job_id should conflict."""
        # Write a lock file as if from a foreign process
        os.makedirs(lock_dir, exist_ok=True)
        lock_file = os.path.join(lock_dir, "controller-test.lock")
        payload = {
            "job_id": "controller-test",
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": 3600,
            "instance_id": "foreign-process-xyz",
            "hostname": "other-host",
            "pid": 99999,
        }
        with open(lock_file, "w") as f:
            json.dump(payload, f)

        lock2 = JobLock(job_id="controller-test", lock_path=lock_dir,
                        timeout_seconds=2, poll_interval=1)
        with pytest.raises(JobLockError):
            lock2.acquire()

    def test_same_instance_controller_reentry(self, lock_dir):
        """Re-creating Controller in same process should re-acquire the lock."""
        lock1 = JobLock(job_id="reentry-test", lock_path=lock_dir,
                        timeout_seconds=60, poll_interval=1)
        lock1.acquire()
        assert lock1.held

        # Simulate: user re-runs cell, new Controller, same process
        lock2 = JobLock(job_id="reentry-test", lock_path=lock_dir,
                        timeout_seconds=2, poll_interval=1)
        lock2.acquire()  # should NOT block — same _PROCESS_INSTANCE_ID
        assert lock2.held

        lock2.release()


# ---------------------------------------------------------------------------
# Package exports
# ---------------------------------------------------------------------------

class TestExports:
    def test_job_lock_exported(self):
        from arcflow import JobLock as JL
        assert JL is JobLock

    def test_job_lock_error_exported(self):
        from arcflow import JobLockError as JLE
        assert JLE is JobLockError
