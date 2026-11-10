#!/usr/bin/env python3
"""Derive component context from exact accepted runtime bytes plus test packaging.
All original Dispatcharr application authors retain full credit.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import tarfile
ROOT = Path(__file__).resolve().parents[2]
ADDITIONS = ('rust/Dockerfile.ingest-epg-tests', 'rust/runtime/ingest_epg_test.sh', 'LICENSE', 'ATTRIBUTION.md')


def sha(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-evidence', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    base_bytes = args.source_evidence.read_bytes()
    base = json.loads(base_bytes)
    if not base['passed']:
        raise ValueError('Requires accepted Source/native component evidence')
    archive = args.source_evidence.parent / 'rust-source.tar'
    if sha(archive.read_bytes()) != base['rust_source_archive_sha256']:
        raise ValueError('Accepted runtime archive differs')
    files = {}
    with tarfile.open(archive) as source:
        for member in source.getmembers():
            if not member.isfile() or not member.name.startswith('rust/') or '..' in Path(member.name).parts or member.name in files:
                raise ValueError('Accepted archive requires unique regular Rust files')
            files[member.name] = source.extractfile(member).read()
    hashes = {name: sha(data) for name, data in files.items()}
    if hashes != base['rust_source_sha256']:
        raise ValueError('Accepted runtime map differs from archive bytes')
    additions = {}
    for name in ADDITIONS:
        data = (ROOT / name).read_bytes()
        if name in files and files[name] != data:
            raise ValueError('Component packaging must not replace accepted runtime bytes')
        files[name] = data
        additions[name] = sha(data)
    source_map = {name: sha(data) for name, data in files.items()}
    tree = sha(json.dumps(source_map, sort_keys=True, separators=(',', ':')).encode())
    with tarfile.open(output / 'source.tar', 'w') as target:
        for name, data in sorted(files.items()):
            item = tarfile.TarInfo(name)
            item.size = len(data)
            item.mode = 0o644
            item.mtime = 0
            target.addfile(item, io.BytesIO(data))
    record = {'format_version': 1, 'scope': 'Component source archive derived from accepted import46 runtime; not an application build',
        'passed': True, 'source_sha256': source_map, 'source_tree_sha256': tree,
        'source_archive_sha256': sha((output / 'source.tar').read_bytes()),
        'base_evidence_sha256': sha(base_bytes), 'base_runtime_archive_sha256': base['rust_source_archive_sha256'],
        'base_runtime_sha256': hashes, 'packaging_additions_sha256': additions,
        'tool_sha256': sha(Path(__file__).read_bytes()), 'application_image_verified': False,
        'full_refresh_job_verified': False, 'performance_verified': False}
    (output / 'build.json').write_text(json.dumps(record, indent=2) + '\n')
    print(json.dumps({'passed': True, 'runtime_files_preserved': len(hashes), 'packaging_files': len(additions), 'source_tree_sha256': tree}))


if __name__ == '__main__':
    main()
