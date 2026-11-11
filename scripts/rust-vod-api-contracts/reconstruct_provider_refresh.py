#!/usr/bin/env python3
"""Reconstruct reviewed union70 only from immutable Git objects, never worktree files.
Original Dispatcharr authors retain application and algorithm credit.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def reconstruct(repo, output, manifest):
    assert hashlib.sha256(manifest.read_bytes()).hexdigest() == '2025301f8c50e274c7c703cb6c772254cf1271a40bd0108e169701277193b20a'
    spec = json.loads(manifest.read_text())
    assert spec['schema'] == 1 and spec['rust_input_count'] == 734
    output.mkdir(parents=True, exist_ok=False)
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args])
    # Explicit repository root avoids git archive's nested-working-directory subtree behavior.
    archive = git('archive', spec['base_commit'])
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(output, filter='data')
    for name, expected in (spec['files'] | spec['tool_files']).items():
        path = Path(name)
        assert path.parts[0] in {'rust', 'scripts', 'docker'} and '..' not in path.parts
        body = git('cat-file', 'blob', expected['git_blob'])
        assert hashlib.sha256(body).hexdigest() == expected['sha256'], name
        target = output / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(body)
    actual = {str(p.relative_to(output)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (output / 'rust').rglob('*') if p.is_file()}
    assert actual == {p: v['sha256'] for p, v in spec['files'].items()}
    proof = {'passed': True, 'scope': spec['scope'], 'base_commit': spec['base_commit'],
             'rust_inputs': len(actual), 'tool_files': len(spec['tool_files']), 'source_manifest_sha256': hashlib.sha256(manifest.read_bytes()).hexdigest(),
             'source_inputs_sha256': spec['inputs_sha256'],
             'base_archive_sha256': hashlib.sha256(archive).hexdigest(),
             'authority': 'Only pinned Git archive and explicitly named Git blobs; no mutable/private fallback'}
    (output / 'provider-refresh-reconstruction.json').write_text(json.dumps(proof, indent=2) + '\n')
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = Path(__file__).with_name('provider-refresh-union70-inputs.json')
    print(json.dumps(reconstruct(args.repo.resolve(), args.output.resolve(), manifest)))


if __name__ == '__main__':
    main()
