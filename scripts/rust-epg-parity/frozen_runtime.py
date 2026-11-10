"""Reuse one captured runtime for independent disposable component regressions.
Original Dispatcharr authors retain application credit. Capture is not acceptance.
"""
import hashlib
import json
from pathlib import Path, PurePosixPath
import tarfile


def read_archive(path):
    result = {}
    total = 0
    with tarfile.open(path, 'r:') as archive:
        for member in archive:
            relative = PurePosixPath(member.name)
            if (not member.isfile() or relative.is_absolute() or '..' in relative.parts
                    or not member.name.startswith('rust/') or str(relative) != member.name
                    or member.name in result or len(result) >= 4096):
                raise ValueError('Unsafe or duplicate runtime member')
            total += member.size
            if member.size < 0 or total > 128 * 1024 * 1024:
                raise ValueError('Runtime snapshot capacity')
            with archive.extractfile(member) as source:
                data = source.read()
            if len(data) != member.size:
                raise ValueError('Truncated runtime member')
            result[member.name] = data
    if not result or 'rust/Cargo.toml' not in result or 'rust/Cargo.lock' not in result:
        raise ValueError('Incomplete runtime snapshot')
    return result


def create_manifest(archive, destination):
    archive = Path(archive).resolve()
    captured = read_archive(archive)
    manifest = {'format_version': 1, 'captured': True, 'acceptance_verified': False,
                'archive': str(archive),
                'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(),
                'source_sha256': {name: hashlib.sha256(data).hexdigest()
                                  for name, data in sorted(captured.items())}}
    Path(destination).write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


def load_manifest(path):
    path = Path(path).resolve()
    manifest = json.loads(path.read_text())
    if manifest.get('format_version') != 1 or manifest.get('captured') is not True:
        raise ValueError('Unsupported runtime snapshot manifest')
    archive = Path(manifest['archive']).resolve()
    if hashlib.sha256(archive.read_bytes()).hexdigest() != manifest['archive_sha256']:
        raise ValueError('Runtime archive digest mismatch')
    captured = read_archive(archive)
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in captured.items()}
    if not hashes or hashes != manifest['source_sha256']:
        raise ValueError('Runtime source map mismatch')
    return captured, hashes, archive, hashlib.sha256(path.read_bytes()).hexdigest()
