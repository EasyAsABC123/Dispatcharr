#!/usr/bin/env python3
"""Verify the qualified Git snapshot; explicit revision/index, no live fallback."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def verify(repo, revision, index):
    here = Path(__file__).resolve().parent
    raw = (here / 'source-inputs.json').read_bytes()
    expected = json.loads(raw)
    assert hashlib.sha256(raw).hexdigest() == 'ab572032d8b2b1550c454d40e1051d80f01fea1594d3f54a01b6722d73252328'
    assert len(expected) == 892 and sum(p.startswith('rust/') for p in expected) == 887
    if index:
        inputs = {p: subprocess.check_output(['git', '-C', str(repo), 'show', ':' + p]) for p in expected}
    else:
        revision = subprocess.check_output(['git', '-C', str(repo), 'rev-parse', '--verify', revision + '^{commit}'], text=True).strip()
        archive = subprocess.check_output(['git', '-C', str(repo), 'archive', revision, '--', 'rust', *[p for p in expected if not p.startswith('rust/')]])
        inputs = {}
        with tarfile.open(fileobj=io.BytesIO(archive)) as source:
            for member in source:
                if member.isdir():
                    continue
                assert member.isfile() and member.name not in inputs
                inputs[member.name] = source.extractfile(member).read()
    assert {p: hashlib.sha256(b).hexdigest() for p, b in inputs.items()} == expected
    receipts = json.loads((here / 'actual-check-receipts.json').read_text())
    assert len(receipts) == 8 and all(r['actual_wait_exit'] == 0 and r['source_inputs_unchanged'] for r in receipts)
    proof = json.loads((here / 'outside-proof.json').read_text())
    assert proof['source_count'] == 892 and proof['actual_test_counts']['workspace'] == {'passed': 860, 'failed': 0, 'ignored': 751, 'blocks': 172}
    return {'passed': True, 'source_inputs': 892, 'index': index, 'revision': revision, 'application_or_performance_accepted': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--revision')
    mode.add_argument('--index', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.repo, args.revision, args.index)))
