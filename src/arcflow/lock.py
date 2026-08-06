"""
Singleton job lock for ArcFlow pipelines.

Prevents duplicate concurrent runs of the same pipeline job. Azure-backed
locks use a finite renewable blob lease on one persistent marker file.
Local locks use an exclusive marker plus an owner-specific heartbeat.

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
import posixpath
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import fsspec
from fsspec.asyn import sync

try:
    from azure.core.exceptions import HttpResponseError, ResourceExistsError
    _EXCLUSIVE_CREATE_CONFLICTS = (FileExistsError, ResourceExistsError)
    _AZURE_HTTP_ERRORS = (HttpResponseError,)
except ImportError:
    _EXCLUSIVE_CREATE_CONFLICTS = (FileExistsError,)
    _AZURE_HTTP_ERRORS = ()

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

    Azure-backed paths use one persistent JSON marker protected by a finite
    blob lease. The lease is renewed in the background and naturally expires
    if the process dies. Local paths use atomic marker creation and a
    heartbeat sidecar with stale-lock recovery.

    **Local instance re-entry:** If the existing lock file contains the same
    process-level instance ID, the lock is silently re-acquired. This allows
    a notebook cell to re-create a ``Controller`` without being blocked by
    the previous instance's lock.

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
        lease_duration_seconds: int = 60,
        lease_renew_interval: Optional[int] = None,
        filesystem: Optional[Any] = None,
    ):
        if not job_id:
            raise ValueError("job_id must be a non-empty string")

        self.job_id = job_id
        self.lock_path = lock_path
        self.timeout_seconds = timeout_seconds
        self.poll_interval = poll_interval or max(timeout_seconds // 10, 5)
        self.heartbeat_interval = heartbeat_interval or max(timeout_seconds // 3, 10)
        self.lease_duration_seconds = lease_duration_seconds
        self.lease_renew_interval = (
            lease_renew_interval or max(lease_duration_seconds // 3, 5)
        )
        try:
            if filesystem is None:
                self._fs, fs_lock_path = fsspec.core.url_to_fs(lock_path)
            else:
                self._fs = filesystem
                fs_lock_path = filesystem._strip_protocol(lock_path)
        except (ImportError, ValueError) as e:
            raise ValueError(
                f"Unsupported job lock path '{lock_path}': {e}"
            ) from e

        self._fs_lock_path = fs_lock_path.rstrip("/")
        self._fs_lock_file = posixpath.join(
            self._fs_lock_path, f"{job_id}.lock"
        )
        protocol = self._fs.protocol
        if isinstance(protocol, (tuple, list)):
            protocol = protocol[0]
        self._protocol = protocol
        self._uses_lease = protocol in ("abfs", "abfss", "az")
        if self._uses_lease:
            if not 15 <= lease_duration_seconds <= 60:
                raise ValueError(
                    "lease_duration_seconds must be between 15 and 60"
                )
            if not 0 < self.lease_renew_interval < lease_duration_seconds:
                raise ValueError(
                    "lease_renew_interval must be greater than 0 and less "
                    "than lease_duration_seconds"
                )
        if protocol in ("file", "local"):
            self._lock_file = os.path.normpath(self._fs_lock_file)
        else:
            self._lock_file = f"{lock_path.rstrip('/')}/{job_id}.lock"
        self._held = False
        self._lease = None
        self._blob_client = None
        if self._uses_lease:
            try:
                container, blob, _ = self._fs.split_path(self._fs_lock_file)
                self._blob_client = self._fs.service_client.get_blob_client(
                    container=container,
                    blob=blob,
                )
            except (AttributeError, TypeError, ValueError) as e:
                raise ValueError(
                    f"Azure filesystem backend '{self._protocol}' does not "
                    "expose the blob lease API"
                ) from e
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

        if self._uses_lease:
            self._acquire_lease()
            return

        deadline = time.monotonic() + self.timeout_seconds
        first_attempt = True

        while True:
            if self._create_lock_file_exclusive():
                self._held = True
                self._write_heartbeat_file()
                self._start_heartbeat()
                logger.info(
                    f"Job lock acquired: {self.job_id} "
                    f"({self._lock_file}, backend={self._protocol})"
                )
                return

            existing = self._read_lock_file()
            if existing is None:
                continue

            # Same-instance re-entry covers notebook re-runs where a previous
            # Controller was not cleaned up.
            if self._is_same_instance(existing):
                logger.info(
                    f"Re-acquiring lock for job '{self.job_id}' "
                    f"(same instance_id={self.get_instance_id()!r})"
                )
                self._held = True
                self._write_heartbeat_file()
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
                if self._delete_stale_lock(existing):
                    continue

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
        """Release the lease or delete the local lock marker. Idempotent."""
        if not self._held:
            return
        if self._uses_lease:
            self._release_lease()
            return
        self._stop_heartbeat()
        existing = self._read_lock_file()
        if existing is not None and self._is_same_instance(existing):
            self._delete_lock_file()
        elif existing is not None:
            logger.warning(
                f"Job lock ownership changed for '{self.job_id}'; "
                f"not deleting lock owned by {existing.get('instance_id')!r}"
            )
        self._delete_heartbeat_file(self.get_instance_id())
        self._held = False
        logger.info(f"Job lock released: {self.job_id}")

    @property
    def is_locked(self) -> bool:
        """Check whether this instance holds a lease or a local marker exists."""
        if self._uses_lease:
            return self._held
        return self._fs.exists(self._fs_lock_file)

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

    def _lock_payload(self) -> dict:
        """Build immutable ownership metadata for a new lock."""
        return {
            "job_id": self.job_id,
            "acquired_at": datetime.now(timezone.utc).isoformat(),
            "timeout_seconds": self.timeout_seconds,
            "instance_id": self.get_instance_id(),
            "hostname": platform.node(),
            "pid": os.getpid(),
        }

    def _acquire_lease(self) -> None:
        """Acquire a finite Azure blob lease, retrying until timeout."""
        self._ensure_lease_marker()
        deadline = time.monotonic() + self.timeout_seconds
        first_attempt = True

        while True:
            try:
                lease = sync(
                    self._fs.loop,
                    self._blob_client.acquire_lease,
                    lease_duration=self.lease_duration_seconds,
                )
            except _AZURE_HTTP_ERRORS as e:
                if not self._is_lease_conflict(e):
                    raise JobLockError(
                        f"Failed to acquire Azure lease for job "
                        f"'{self.job_id}': {e}"
                    ) from e

                if first_attempt:
                    logger.warning(
                        f"Job '{self.job_id}' is already leased. Waiting up "
                        f"to {self.timeout_seconds}s for release..."
                    )
                    first_attempt = False

                if time.monotonic() >= deadline:
                    raise JobLockError(
                        f"Failed to acquire lock for job '{self.job_id}' "
                        f"after {self.timeout_seconds}s because the OneLake "
                        f"lease remained held. Lock file: {self._lock_file}"
                    ) from e

                time.sleep(self.poll_interval)
                continue

            self._lease = lease
            try:
                self._write_leased_payload()
            except (OSError,) + _AZURE_HTTP_ERRORS as e:
                try:
                    sync(self._fs.loop, lease.release)
                finally:
                    self._lease = None
                raise JobLockError(
                    f"Lease acquired but ownership metadata could not be "
                    f"written for job '{self.job_id}': {e}"
                ) from e

            self._held = True
            self._cleanup_legacy_heartbeat_files()
            self._start_heartbeat()
            logger.info(
                f"Job lock acquired: {self.job_id} "
                f"({self._lock_file}, backend={self._protocol}, "
                f"lease={self.lease_duration_seconds}s)"
            )
            return

    def _ensure_lease_marker(self) -> None:
        """Create the persistent lease marker once."""
        self._fs.makedirs(self._fs_lock_path, exist_ok=True)
        try:
            self._fs.pipe_file(
                self._fs_lock_file,
                b"{}",
                overwrite=False,
            )
        except _EXCLUSIVE_CREATE_CONFLICTS:
            pass

    @staticmethod
    def _is_lease_conflict(error: Exception) -> bool:
        error_code = str(getattr(error, "error_code", ""))
        return (
            getattr(error, "status_code", None) in (409, 412)
            or error_code in {
                "LeaseAlreadyPresent",
                "LeaseIsBreakingAndCannotBeAcquired",
                "LeaseIdMismatchWithLeaseOperation",
            }
        )

    def _write_leased_payload(self) -> None:
        payload = json.dumps(self._lock_payload(), indent=2).encode("utf-8")
        sync(
            self._fs.loop,
            self._blob_client.upload_blob,
            data=payload,
            overwrite=True,
            lease=self._lease,
        )

    def _release_lease(self) -> None:
        self._stop_heartbeat()
        lease = self._lease
        self._lease = None
        try:
            if lease is not None:
                sync(self._fs.loop, lease.release)
        except _AZURE_HTTP_ERRORS as e:
            logger.warning(
                f"Failed to release Azure lease for job '{self.job_id}': {e}"
            )
        finally:
            self._held = False
        logger.info(f"Job lock released: {self.job_id}")

    def _cleanup_legacy_heartbeat_files(self) -> None:
        try:
            for path in self._fs.glob(f"{self._fs_lock_file}.*.heartbeat"):
                self._fs.rm(path)
        except OSError as e:
            logger.warning(
                f"Could not remove legacy heartbeat files for "
                f"'{self.job_id}': {e}"
            )

    def _create_lock_file_exclusive(self) -> bool:
        """Atomically create the ownership marker if it does not exist."""
        self._fs.makedirs(self._fs_lock_path, exist_ok=True)
        payload = json.dumps(self._lock_payload(), indent=2).encode("utf-8")
        try:
            with self._fs.open(self._fs_lock_file, "xb") as f:
                f.write(payload)
            return True
        except _EXCLUSIVE_CREATE_CONFLICTS:
            return False
        except NotImplementedError as e:
            if self._protocol not in ("abfs", "abfss", "az"):
                raise JobLockError(
                    f"Filesystem backend '{self._protocol}' does not support "
                    f"exclusive lock creation with mode 'xb'"
                ) from e
            try:
                self._fs.pipe_file(
                    self._fs_lock_file,
                    payload,
                    overwrite=False,
                )
                return True
            except _EXCLUSIVE_CREATE_CONFLICTS:
                return False
            except TypeError as pipe_error:
                raise JobLockError(
                    f"Azure filesystem backend '{self._protocol}' supports "
                    "neither mode 'xb' nor atomic pipe_file(overwrite=False)"
                ) from pipe_error

    def _heartbeat_file(self, instance_id: str) -> str:
        return f"{self._fs_lock_file}.{instance_id}.heartbeat"

    def _write_heartbeat_file(self) -> None:
        payload = {
            "instance_id": self.get_instance_id(),
            "heartbeat_at": datetime.now(timezone.utc).isoformat(),
        }
        heartbeat_file = self._heartbeat_file(self.get_instance_id())
        with self._fs.open(heartbeat_file, "wb") as f:
            f.write(json.dumps(payload).encode("utf-8"))

    def _read_lock_file(self) -> Optional[dict]:
        try:
            with self._fs.open(self._fs_lock_file, "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Corrupt lock file '{self._lock_file}': {e}. Treating as stale.")
            return {"acquired_at": "1970-01-01T00:00:00+00:00"}

    def _read_heartbeat_file(self, instance_id: Optional[str]) -> Optional[dict]:
        if not instance_id:
            return None
        try:
            with self._fs.open(self._heartbeat_file(instance_id), "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(
                f"Corrupt heartbeat file for job '{self.job_id}': {e}. "
                "Falling back to acquired_at."
            )
            return None

    def _delete_lock_file(self) -> None:
        try:
            self._fs.rm(self._fs_lock_file)
        except FileNotFoundError:
            pass

    def _delete_heartbeat_file(self, instance_id: Optional[str]) -> None:
        if not instance_id:
            return
        try:
            self._fs.rm(self._heartbeat_file(instance_id))
        except FileNotFoundError:
            pass

    def _delete_stale_lock(self, observed: dict) -> bool:
        """Delete only the stale owner that was observed by the caller."""
        current = self._read_lock_file()
        if current is None:
            return True
        if (
            current.get("instance_id") != observed.get("instance_id")
            or current.get("acquired_at") != observed.get("acquired_at")
            or not self._is_stale(current)
        ):
            return False
        self._delete_lock_file()
        self._delete_heartbeat_file(current.get("instance_id"))
        return True

    def _start_heartbeat(self) -> None:
        """Start a daemon thread that renews lease or local heartbeat."""
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            name=f"arcflow-lock-renewal-{self.job_id}",
            daemon=True,
        )
        self._heartbeat_thread.start()
        interval = (
            self.lease_renew_interval
            if self._uses_lease
            else self.heartbeat_interval
        )
        logger.debug(
            f"Lock renewal started for job '{self.job_id}' "
            f"(interval={interval}s)"
        )

    def _stop_heartbeat(self) -> None:
        """Signal the heartbeat thread to stop and wait for it."""
        if self._heartbeat_thread is None:
            return
        self._heartbeat_stop.set()
        self._heartbeat_thread.join(timeout=5)
        self._heartbeat_thread = None
        logger.debug(f"Heartbeat stopped for job '{self.job_id}'")

    def _heartbeat_loop(self) -> None:
        """Refresh the owner heartbeat while ownership remains unchanged."""
        interval = (
            self.lease_renew_interval
            if self._uses_lease
            else self.heartbeat_interval
        )
        while not self._heartbeat_stop.wait(timeout=interval):
            try:
                if self._uses_lease:
                    sync(self._fs.loop, self._lease.renew)
                    logger.debug(
                        f"Lease renewed for job '{self.job_id}'"
                    )
                    continue
                existing = self._read_lock_file()
                if existing is None or not self._is_same_instance(existing):
                    logger.warning(
                        f"Heartbeat stopped for job '{self.job_id}' because "
                        "lock ownership was lost"
                    )
                    self._held = False
                    return
                self._write_heartbeat_file()
                logger.debug(f"Heartbeat: refreshed job '{self.job_id}'")
            except (OSError,) + _AZURE_HTTP_ERRORS as e:
                self._held = False
                logger.error(
                    f"Job lock renewal failed for '{self.job_id}'; "
                    f"ownership is no longer guaranteed: {e}"
                )
                return

    def _is_same_instance(self, lock_data: dict) -> bool:
        """Check if the lock was written by this same logical instance."""
        return lock_data.get("instance_id") == self.get_instance_id()

    def _is_stale(self, lock_data: dict) -> bool:
        timestamp = lock_data.get("acquired_at")
        heartbeat = self._read_heartbeat_file(lock_data.get("instance_id"))
        if heartbeat is not None:
            timestamp = heartbeat.get("heartbeat_at", timestamp)
        if not timestamp:
            return True
        try:
            last_seen = datetime.fromisoformat(timestamp)
            age_seconds = (datetime.now(timezone.utc) - last_seen).total_seconds()
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
