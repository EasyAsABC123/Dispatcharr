"""Exclusive-by-default migration leases and opt-in Source/Native functional lanes.

Original Dispatcharr and dependency authors retain application/algorithm credit.
This module starts no work. Owners must retain each lease through actual waits,
pipe EOF, process-group retirement and independently verified fixture cleanup.
Opt-in still requires real namespace/resource qualification by the batch owner.
"""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import tempfile
import uuid

PAIR_MODE = "source-native-pair-v1"
BACKENDS = ("Original", "Native")
MIN_AVAILABLE_BYTES = 12 * 1024**3
MIN_ALLOWED_CPUS = 8


def functional_backend(plan):
    """Return a single requested functional lane; absent/false stays exclusive."""
    mode = plan.get("functional_parallel")
    if mode is None or mode is False:
        return None
    if mode != PAIR_MODE:
        raise ValueError("unknown functional concurrency mode")
    contexts = [plan.get(key, []) for key in ("source_contexts", "native_contexts")]
    if any(not isinstance(value, list) for value in contexts):
        raise ValueError("explicit backend context lists required")
    selected = [backend for backend, values in zip(BACKENDS, contexts) if values]
    binding = plan.get("base_binding", {})
    if len(selected) != 1 or not isinstance(binding, dict) or binding.get("performance") is not False:
        raise ValueError("one strictly functional backend per paired service required")
    return selected[0]


def uncertainty_path(path, backend):
    """Each backend retains its pending/uncertain owner without overwriting peers."""
    if backend not in BACKENDS:
        raise ValueError("unknown backend lane")
    path = Path(path)
    return path.with_name(path.name + ".functional-" + backend + ".cleanup-required.json")


def _lane_path(path, backend):
    return path.with_name(path.name + ".functional-" + backend)


def _generation():
    return {"pid": os.getpid(),
            "starttime": Path("/proc/self/stat").read_text().rsplit(")", 1)[1].split()[19],
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip()}


def _generation_live(owner):
    try:
        if type(owner["pid"]) is not int or owner["pid"] <= 0:
            return False
        if owner["boot_id"] != Path("/proc/sys/kernel/random/boot_id").read_text().strip():
            return False
        fields = (Path("/proc") / str(owner["pid"]) / "stat").read_text().rsplit(")", 1)[1].split()
        return fields[19] == owner["starttime"] and fields[0] not in ("Z", "X")
    except (KeyError, TypeError, ValueError, IndexError, OSError):
        return False


def _fsync_directory(path):
    directory = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _write_marker(path, record):
    marker = uncertainty_path(path, record["backend"])
    fd, temporary_name = tempfile.mkstemp(prefix=marker.name + ".", suffix=".tmp", dir=marker.parent)
    temporary = Path(temporary_name)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(record, stream)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, marker)
        _fsync_directory(marker.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _pending_owner_holds_lane(path, backend, record):
    """A PID alone is insufficient: a matching live pending owner still has a lane."""
    try:
        if record["backend"] != backend or record["primary_lock"] != str(path.resolve()):
            return False
        if record["state"] != "pending" or record["cleanup_verified"] is not False:
            return False
        if not isinstance(record["lease_id"], str) or not record["lease_id"]:
            return False
        if not _generation_live(record["owner_generation"]):
            return False
        with _lane_path(path, backend).open("a") as probe:
            stat = os.fstat(probe.fileno())
            if record["lane_device"] != stat.st_dev or record["lane_inode"] != stat.st_ino:
                return False
            try:
                fcntl.flock(probe, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(probe, fcntl.LOCK_UN)
            return False
    except (KeyError, TypeError, ValueError, OSError):
        return False


def _check_markers(path, requested_backend):
    # An earlier uncertainty marker is never evidence of a live pending lease.
    if path.with_name(path.name + ".functional-cleanup-required.json").exists():
        raise RuntimeError("Prior paired physical cleanup must be independently established")
    for backend in BACKENDS:
        marker = uncertainty_path(path, backend)
        if not marker.exists():
            continue
        # Own lane has already been acquired: any old marker in that lane is
        # stale. A legitimate live same-backend owner was refused by flock.
        if requested_backend is None or requested_backend == backend:
            raise RuntimeError("Prior paired physical cleanup must be independently established")
        try:
            record = json.loads(marker.read_text())
        except (OSError, ValueError):
            raise RuntimeError("Prior paired physical cleanup must be independently established") from None
        if not _pending_owner_holds_lane(path, backend, record):
            raise RuntimeError("Prior paired physical cleanup must be independently established")


def _remove_owned_pending(path, pending):
    marker = uncertainty_path(path, pending["backend"])
    try:
        observed = json.loads(marker.read_text())
    except (OSError, ValueError):
        raise RuntimeError("Owned pending lease marker missing or unreadable; independent cleanup required") from None
    if observed != pending:
        raise RuntimeError("Owned pending lease marker changed; independent cleanup required")
    marker.unlink()
    _fsync_directory(marker.parent)


def _resource_headroom():
    available = next(int(row.split()[1]) * 1024
                     for row in Path("/proc/meminfo").read_text().splitlines()
                     if row.startswith("MemAvailable:"))
    if available < MIN_AVAILABLE_BYTES or len(os.sched_getaffinity(0)) < MIN_ALLOWED_CPUS:
        raise RuntimeError("paired functional headroom requires12GiB available and8 allowed CPUs")


@contextlib.contextmanager
def fixture_lease(path, backend=None):
    """Hold EX by default, or SH plus one exclusive functional backend slot.

    A paired owner fsyncs a pending identity BEFORE control reaches its body.
    Other backends may join only a matching live pending owner still holding
    its lane. Dead/stale pending or explicit uncertain records block shared and
    exclusive admissions, including after SIGKILL or os._exit bypasses finally.

    Set the yielded cleanup_verified to literal True only after actual child/
    group/fixture cleanup. That removes only this exact own pending record and
    fsyncs the directory before releasing locks. No automatic stale clearing is
    offered; independent cleanup receipts are still required for recovery.

    Root and support processes need separate nested lock paths. This helper
    starts no applications and grants no performance or isolation acceptance.
    """
    path = Path(path)
    if backend is not None and backend not in BACKENDS:
        raise ValueError("unknown backend lane")
    with path.open("a") as primary:
        mode = fcntl.LOCK_EX if backend is None else fcntl.LOCK_SH
        fcntl.flock(primary, mode | fcntl.LOCK_NB)
        if backend is None:
            _check_markers(path, None)
            yield None
            return
        with _lane_path(path, backend).open("a") as lane:
            fcntl.flock(lane, fcntl.LOCK_EX | fcntl.LOCK_NB)
            _check_markers(path, backend)
            _resource_headroom()
            owner = _generation()
            stat = os.fstat(lane.fileno())
            pending = {"state": "pending", "backend": backend,
                       "primary_lock": str(path.resolve()), "owner_generation": owner,
                       "lease_id": uuid.uuid4().hex, "lane_device": stat.st_dev,
                       "lane_inode": stat.st_ino, "cleanup_verified": False}
            _write_marker(path, pending)
            admission = {"cleanup_verified": False, "backend": backend,
                         "owner_generation": dict(owner), "lease_id": pending["lease_id"]}
            try:
                yield admission
            finally:
                if admission["cleanup_verified"] is True:
                    _remove_owned_pending(path, pending)
                else:
                    _write_marker(path, {**pending, "state": "uncertain",
                                         "reason": "Actual paired child/group/fixture cleanup not established"})
