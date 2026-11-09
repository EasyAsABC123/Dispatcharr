"""Capture the exact fixture/probe bytes executed during a Compose trial.

Dispatcharr's original authors retain full application credit. Snapshotting is
local and reversible; it prevents parallel repository edits changing a running
trial's fixtures after their hashes have been recorded.
"""
import hashlib
from pathlib import Path
import stat

DIRECTORIES=('scripts/rust-integration','scripts/rust-account-contracts',
             'scripts/rust-playback-baseline','scripts/rust-baseline')
COMPOSE=('docker/compose.rust-playback.yml','docker/compose.rust-candidate.yml')


def snapshot_sources(root,destination):
    root=Path(root).resolve();destination=Path(destination)
    destination.mkdir(parents=True,exist_ok=False)
    paths=[]
    for name in DIRECTORIES:
        folder=root/name
        paths.extend(path for path in folder.rglob('*') if path.is_file()
                     and not set(path.relative_to(folder).parts)&{'__pycache__','.pytest_cache'}
                     and path.suffix!='.pyc')
    paths.extend(root/name for name in COMPOSE)
    hashes={}
    for path in sorted(paths):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError('Fixture snapshots require repository regular files')
        relative=path.relative_to(root);content=path.read_bytes()
        target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(content);target.chmod(stat.S_IMODE(path.stat().st_mode))
        hashes[str(relative)]=hashlib.sha256(content).hexdigest()
    return hashes
