"""
Singleton job lock for ArcFlow pipelines.

Prevents duplicate concurrent runs of the same pipeline job using a
file-based marker lock. The lock file contains JSON metadata for
debugging stale locks. A background heartbeat thread keeps the lock
file fresh to prevent false stale-recovery on long-running jobs.

Re-entry is automatic within the same Python process.  A module-level
instance ID (generated once at import time) is written into every lock
file.  When a new ``JobLock`` sees a lock file with the same ID it
re-acquires silently — this covers notebook re-runs where a previous
Controller was not cleaned up.  A different Spark job (separate
process) gets a different ID and will block as expected.
"""
import json
import logging
import os
import platform
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

# Unique per-process — generated once at import time.
# Same notebook kernel  → same _PROCESS_INSTANCE_ID → re-entry allowed.
# Different Spark job   → different _PROCESS_INSTANCE_ID → lock conflict.
_PROCESS_INSTANCE_ID: str = uuid.uuid4().hex[:12]

logger = logging.getLogger(__name__)


class JobLockError(Exception):
    """Raised when a job lock cannot be acquired within the timeout period."""


class JobLock:
    """
    File-based singleton lock to prevent duplicate pipeline runs.

    Writes a JSON marker file to ``<lock_path>/<job_id>.lock``.  If the
    file already exists the caller retries every *poll_interval* seconds
    until *timeout_seconds* is reached, then raises ``JobLockError``.

    Stale locks (older than *timeout_seconds*) are automatically recovered.

    While the lock is held a daemon heartbeat thread refreshes the
    ``acquired_at`` timestamp every ``heartbeat_interval`` seconds
    (default: ``timeout_seconds // 3``) so that long-running jobs are
    never mistaken for stale.

    **Instance re-entry:** If the environment variable
    ``ARCFLOW_INSTANCE_ID`` is set and the existing lock file contains the
    same value, the lock is silently re-acquired.  This allows a notebook
    cell to re-create a ``Controller`` without being blocked by the
    previous instance's lock.

    Supports context-manager usage::

        with JobLock(job_id="my-job", lock_path="Files/locks/"):
            controller.run_full_pipeline()
    """

    def __init__(
        self,
        job_id: str,
        lock_path: str = "Files/locks/",
        timeout_seconds: int = 60,
        poll_interval: Optional[int] = None,
        heartbeat_interval: Optional[int] = None,
    ):
        if not job_id:
            raise ValueError("job_id must be a non-empty string")

        self.job_id = job_id
        self.lock_path = lock_path
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval or max(timeout_seconds // 10, 5)
        self.heartbeat_interval = heartbeat_interval or max(timeout_seconds // 3, 10)
        self._lock_file = os.path.join(lock_path, f"{job_id}.lock")
        self._held = False
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: Optional[threading.Thread] = None

    # ── public API ──────────────────────────────────────────────────

    @staticmethod
    def get_instance_id() -> str:
        """Return the process-level instance ID."""
        return _PROCESS_INSTANCE_ID

    def acquire(self) -> None:
        """Acquire the lock, waiting/retrying if already held.

        Raises:
            JobLockError: If the lock cannot be acquired within *timeout_seconds*.
        """
        if self._held:
            logger.debug(f"Lock already held for job '{self.job_id}', skipping acquire")
            return

        deadline = time.monotonic() + self.timeout_seconds
        first_attempt = True

        while True:
            existing = self._read_lock_file()

            if existing is None:
                self._write_lock_file()
                self._held = True
                self._start_heartbeat()
                logger.info(f"Job lock acquired: {self.job_id} ({self._lock_file})")
                return

            # Same-instance re-entry: the env var ARCFLOW_INSTANCE_ID matches
            # the lock file.  This covers notebook re-runs where a previous
            # Controller was not cleaned up.
            if self._is_same_instance(existing):
                logger.info(
                    f"Re-acquiring lock for job '{self.job_id}' "
                    f"(same instance_id={self.get_instance_id()!r})"
                )
                self._write_lock_file()
                self._held = True
                self._start_heartbeat()
                return

            # Stale lock recovery
            if self._is_stale(existing):
                logger.warning(
                    f"Recovering stale lock for job '{self.job_id}' "
                    f"(acquired_at={existing.get('acquired_at')}, "
                    f"instance_id={existing.get('instance_id')}, "
                    f"hostname={existing.get('hostname')})"
                )
                self._delete_lock_file()
                self._write_lock_file()
                self._held = True
                self._start_heartbeat()
                logger.info(f"Job lock acquired (stale recovery): {self.job_id}")
                return

            if first_attempt:
                logger.warning(
                    f"Job '{self.job_id}' is already locked "
                    f"(holder: instance_id={existing.get('instance_id')}, "
                    f"hostname={existing.get('hostname')}, "
                    f"acquired_at={existing.get('acquired_at')}). "
                    f"Waiting up to {self.timeout_seconds}s for release..."
                )
                first_attempt = False

            if time.monotonic() >= deadline:
                raise JobLockError(
                    f"Failed to acquire lock for job '{self.job_id}' after "
                    f"{self.timeout_seconds}s. Lock held by "
                    f"instance_id={existing.get('instance_id')}, "
                    f"hostname={existing.get('hostname')}, "
                    f"acquired_at={existing.get('acquired_at')}. "
                    f"Lock file: {self._lock_file}"
                )

            time.sleep(self.poll_interval)

    def release(self) -> None:
        """Release the lock by deleting the lock file. Idempotent."""
        if not self._held:
            return
        self._stop_heartbeat()
        self._delete_lock_file()
        self._held = False
        logger.info(f"Job lock released: {self.job_id}")

    @property
    def is_locked(self) -> bool:
        """Check whether the lock file currently exists on disk."""
        return os.path.exists(self._lock_file)

    @property
    def held(self) -> bool:
        """Whether this instance currently holds the lock."""
        return self._held

    # ── context manager ─────────────────────────────────────────────

    def __enter__(self) -> "JobLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.release()

    # ── internals ───────────────────────────────────────────────────

    def _write_lock_file(self) -> None:
        os.makedirs(self.lock_path, exist_ok=True)
        payload = {
            "job_id": self.job_id,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "instance_id": self.get_instance_id(),
            "hostname": platform.node(),
            "pid": os.getpid(),
        }
        with open(self._lock_file, "w") as f:
            json.dump(payload, f, indent=2)

    def _read_lock_file(self) -> Optional[dict]:
        try:
            with open(self._lock_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Corrupt lock file '{self._lock_file}': {e}. Treating as stale.")
            return {"acquired_at": "1970-01-01T00:00:00+00:00"}

    def _delete_lock_file(self) -> None:
        try:
            os.remove(self._lock_file)
        except FileNotFoundError:
            pass

    def _start_heartbeat(self) -> None:
        """Start a daemon thread that refreshes the lock file timestamp."""
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"arcflow-lock-heartbeat-{self.job_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()
        logger.debug(f"Heartbeat started for job '{self.job_id}' (interval={self.heartbeat_interval}s)")

    def _stop_heartbeat(self) -> None:
        """Signal the heartbeat thread to stop and wait for it."""
        if self._heartbeat_thread is None:
            return
        self._heartbeat_stop.set()
        self._heartbeat_thread.join(timeout=5)
        self._heartbeat_thread = None
        logger.debug(f"Heartbeat stopped for job '{self.job_id}'")

    def _heartbeat_loop(self) -> None:
        """Background loop that rewrites the lock file to refresh acquired_at."""
        while not self._heartbeat_stop.wait(timeout=self.heartbeat_interval):
            try:
                self._write_lock_file()
                logger.debug(f"Heartbeat: refreshed lock file for job '{self.job_id}'")
            except OSError as e:
                logger.warning(f"Heartbeat: failed to refresh lock file for job '{self.job_id}': {e}")

    def _is_same_instance(self, lock_data: dict) -> bool:
        """Check if the lock was written by this same logical instance."""
        return lock_data.get("instance_id") == self.get_instance_id()

    def _is_stale(self, lock_data: dict) -> bool:
        acquired_str = lock_data.get("acquired_at")
        if not acquired_str:
            return True
        try:
            acquired_at = datetime.fromisoformat(acquired_str)
            age_seconds = (datetime.now(timezone.utc) - acquired_at).total_seconds()
            # Use holder's timeout if recorded, otherwise fall back to our own
            holder_timeout = lock_data.get("timeout_seconds", self.timeout_seconds)
            return age_seconds > holder_timeout
        except (ValueError, TypeError):
            return True

    def __repr__(self) -> str:
        return (
            f"JobLock(job_id={self.job_id!r}, lock_path={self.lock_path!r}, "
            f"held={self._held}, timeout={self.timeout_seconds}s)"
        )
