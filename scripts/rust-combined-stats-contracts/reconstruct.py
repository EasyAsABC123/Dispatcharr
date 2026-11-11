#!/usr/bin/env python3
"""Exact current746 Stats integration from immutable Git objects only.
Original Dispatcharr authors retain application and algorithm credit.
There is no worktree, ignored-artifact, binary or private-data fallback.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

PIN = 'cd27269f9e996eb032d6c4365f579afd61337675031313fe8c6e4e8610d032e5'


def reconstruct(repo, output, manifest):
    raw = manifest.read_bytes()
    if hashlib.sha256(raw).hexdigest() != PIN:
        raise ValueError('Pinned Stats746 manifest hash differs')
    spec = json.loads(raw)
    if (spec['schema'] != 1 or spec['rust_input_count'] != 746 or
            len(spec['files']) != 746 or len(spec['tool_files']) != 42 or
            spec['base_commit'] != '1a749cbca10f32e61f24f1ade1644bff905cb7f3'):
        raise ValueError('Stats746 shape/base differs')

    def git(*arguments):
        return subprocess.check_output(['git', '-C', str(repo), *arguments])

    # Verify every requested object exists before extracting anything. A private
    # file with matching bytes never substitutes for an unavailable Git blob.
    for name, row in (spec['files'] | spec['tool_files']).items():
        path = Path(name)
        if path.is_absolute() or '..' in path.parts or path.parts[0] not in {'rust', 'scripts'}:
            raise ValueError('Unsafe manifest path')
        body = git('cat-file', 'blob', row['git_blob'])
        if hashlib.sha256(body).hexdigest() != row['sha256']:
            raise ValueError('Stats746 Git blob differs: '+name)
    output.mkdir(parents=True, exist_ok=False)
    archive = git('archive', spec['base_commit'])
    with tarfile.open(fileobj=io.BytesIO(archive)) as source:
        source.extractall(output, filter='data')
    for name, row in (spec['files'] | spec['tool_files']).items():
        target = output/name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(git('cat-file', 'blob', row['git_blob']))
    actual = {str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
              for path in sorted((output/'rust').rglob('*')) if path.is_file()}
    if actual != {name: row['sha256'] for name, row in spec['files'].items()}:
        raise ValueError('Reconstructed Rust closure differs')
    proof = dict(passed=True, base_commit=spec['base_commit'], rust_inputs=len(actual),
        replay_tool_files=len(spec['tool_files']), source_manifest_sha256=PIN,
        source_inputs_sha256=spec['rust_map_sha256'],
        base_archive_sha256=hashlib.sha256(archive).hexdigest(),
        authority='Only pinned Git archive and explicitly named Git blobs; no mutable/private fallback')
    (output/'combined-stats-reconstruction.json').write_text(json.dumps(proof, indent=2)+'\n')
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    manifest = Path(__file__).with_name('combined-stats-746-inputs.json')
    print(json.dumps(reconstruct(args.repo.resolve(), args.output.resolve(), manifest)))


if __name__ == '__main__':
    main()
