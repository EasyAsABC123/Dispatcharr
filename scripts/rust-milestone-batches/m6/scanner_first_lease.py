"""Physical fence for the exact existing first-scanner fixture."""

from contextlib import contextmanager
import fcntl
from hashlib import file_digest
import os
from pathlib import Path

MARKER = ".dispatcharr-recording-cohort-cleanup-required.json"
PENDING = ".scanner-first-cleanup-required.json"


def need(predicate, message):
    if not predicate:
        raise ValueError(message)


def identity(path):
    stat = path.stat()
    return [stat.st_dev, stat.st_ino]


def verify(coordinator, checksum):
    need(coordinator.is_absolute() and coordinator.resolve() == coordinator
         and coordinator.is_file() and coordinator.name == "coordinator.py"
         and coordinator.parent.name == "rust-milestone-batches"
         and coordinator.parent.parent.name == "scripts", "exact tracked dispatcher path required")
    with coordinator.open("rb") as stream:
        need(file_digest(stream, "sha256").hexdigest() == checksum, "exact bound dispatcher digest required")
    lock = coordinator.with_name(".dispatcher.lock")
    need(not lock.is_symlink() and lock.resolve() == lock, "dispatcher lease path cannot be redirected")
    return lock


def first_fence(artifacts, lock):
    legacy_root = artifacts.parents[1]
    legacy = legacy_root / "scripts/rust-milestone-batches"
    markers = [artifacts / PENDING, artifacts / ".api202-positive-fifo-cleanup-required.json",
               legacy_root / MARKER, legacy / MARKER,
               legacy / ".cleanup-required.json", lock.parent / ".cleanup-required.json"]
    markers.extend(legacy / (".dispatcher.lock.functional-" + backend + ".cleanup-required.json")
                   for backend in ("Original", "Native"))
    need(not any(os.path.lexists(marker) for marker in markers), "prior physical scanner retirement unresolved")
    return legacy / ".dispatcher.lock"


@contextmanager
def physical_fence(artifacts, lock):
    legacy = first_fence(artifacts, lock)
    need(legacy.resolve() == legacy and legacy.is_file(), "existing physical lease inode required")
    if identity(legacy) == identity(lock):
        yield
        return
    with legacy.open("rb") as stream:
        stat = os.fstat(stream.fileno())
        need([stat.st_dev, stat.st_ino] == identity(legacy), "stable physical lease inode required")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("physical scanner resources already leased") from None
        first_fence(artifacts, lock)
        yield
