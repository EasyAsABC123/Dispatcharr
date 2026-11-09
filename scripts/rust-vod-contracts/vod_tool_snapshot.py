"""Freeze executed contract tooling; original Dispatcharr application credit retained."""
import hashlib
from pathlib import Path
import stat

DIRECTORIES=('scripts/rust-output-contracts','scripts/rust-vod-contracts','scripts/rust-timeshift-contracts',
             'scripts/rust-integration','scripts/rust-account-contracts',
             'scripts/rust-playback-baseline','scripts/rust-baseline',
             'scripts/rust-ingest-parity')
COMPOSE=('docker/compose.rust-playback.yml','docker/compose.rust-candidate.yml')


def snapshot_sources(root,destination):
    root=Path(root).resolve();destination=Path(destination)
    destination.mkdir(parents=True,exist_ok=False)
    hashes={}
    files=[]
    for directory in DIRECTORIES:
        folder=root/directory
        files.extend(path for path in folder.rglob('*') if path.is_file()
            and not set(path.relative_to(folder).parts)&{'__pycache__','.pytest_cache'}
            and path.suffix!='.pyc')
    files.extend(root/path for path in COMPOSE)
    for path in sorted(files):
        if path.is_symlink() or not path.resolve().is_relative_to(root):
            raise ValueError('Contract snapshots require repository regular files')
        relative=path.relative_to(root);data=path.read_bytes()
        target=destination/relative;target.parent.mkdir(parents=True,exist_ok=True)
        target.write_bytes(data);target.chmod(stat.S_IMODE(path.stat().st_mode))
        hashes[str(relative)]=hashlib.sha256(data).hexdigest()
    return hashes
