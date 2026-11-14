#!/usr/bin/env python3
"""Verify immutable image61 build evidence; optionally reconstruct its inputs.

This performs no Docker, Cargo, network or application execution.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import subprocess
import tarfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify(repo, output=None):
    packet = Path(__file__).resolve().parent
    build = json.loads((packet / 'build.json').read_text())
    review = json.loads((packet / 'Root-build-review.json').read_text())
    probe = json.loads((packet / 'Root-credit-frontend-probe.json').read_text())
    revision = 'e96aac33399600df347c799e8ac5135d49f10d85'
    assert build['passed'] is True and build['exit_code'] == 0
    assert build['git_revision'] == revision
    assert build['image_id'] == 'sha256:9e463647fd7cb822b3b96822408b203f54029d7de7f2546d97f4ff1d83e6004c'
    raw = subprocess.check_output(
        ['git', '-C', str(repo), 'archive', revision, '--',
         'LICENSE', 'ATTRIBUTION.md', 'rust', 'frontend'])
    inputs = {}
    with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            path = PurePosixPath(member.name)
            assert member.isfile() and not path.is_absolute() and '..' not in path.parts
            assert member.name not in inputs
            inputs[member.name] = archive.extractfile(member).read()
    hashes = {name: digest(data) for name, data in sorted(inputs.items())}
    assert hashes == build['source_sha256']
    assert len(hashes) == 1370
    assert sum(name.startswith('rust/') for name in hashes) == 873
    assert sum(name.startswith('frontend/') for name in hashes) == 495
    tree_hash = digest(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode())
    assert tree_hash == build['source_tree_sha256'] == review['source_tree_sha256']
    normalized = io.BytesIO()
    with tarfile.open(fileobj=normalized, mode='w') as archive:
        for name, data in sorted(inputs.items()):
            entry = tarfile.TarInfo(name)
            entry.size, entry.mode, entry.mtime = len(data), 0o644, 0
            archive.addfile(entry, io.BytesIO(data))
    assert digest(normalized.getvalue()) == build['source_archive_sha256']
    assert digest((packet / 'runtime-packages.txt').read_bytes()) == build['runtime_packages_sha256']
    builder = json.loads((packet / 'builder-inputs.json').read_text())
    assert builder['revision'] == revision
    assert set(builder['files']) == {'scripts/rust-integration/build.py', 'docker/compose.rust-candidate.yml'}
    support = {}
    for name, expected in builder['files'].items():
        support[name] = subprocess.check_output(['git', '-C', str(repo), 'show', revision + ':' + name])
        assert digest(support[name]) == expected
    assert digest(support['docker/compose.rust-candidate.yml']) == build['candidate_overlay_template_sha256']
    assert probe['passed'] is True and probe['actual_exit'] == 0
    assert probe['stdout'] == (packet / 'image57-installed-inventory.txt').read_text()
    lines = probe['stdout'].splitlines()
    assert lines[0] == '65532' and len(lines[1:]) == 114
    assert probe['installed_hashed_files'] == 114
    assert len(probe['fresh_exact_cleanup']) == 2
    assert all(q['actual_exit'] == 0 and q['stdout'] == '' for q in probe['fresh_exact_cleanup'])
    assert review['passed'] is True and review['all1370archive_bytes_exact_Git'] is True
    assert review['new_Main_or_performance_acceptance'] is False
    negative = json.loads((packet / 'Root-probe-environment-negative.json').read_text())
    assert negative['actual_exit'] == 1 and negative['business_or_probe_containers_started'] == 0
    if output is not None:
        output.mkdir(parents=True, exist_ok=False)
        (output / 'source.tar').write_bytes(normalized.getvalue())
        for name, data in {**inputs, **support}.items():
            target = output / 'tree' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
    return {'passed': True, 'application_inputs': 1370, 'builder_inputs': 2,
            'archive_sha256': build['source_archive_sha256'],
            'installed_files': 114, 'application_or_performance_replayed': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    print(json.dumps(verify(args.repo, args.output), indent=2))
