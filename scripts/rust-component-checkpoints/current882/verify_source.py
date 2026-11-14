#!/usr/bin/env python3
"""Verify the exact current882 Git inputs and the earlier default-test inputs.

Choose an explicit revision or the staged index. No checkout fallback, compiler,
application, Docker or network execution occurs.
"""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile


def digest(data):
    return hashlib.sha256(data).hexdigest()


def verify(repo, revision, index):
    here = Path(__file__).resolve().parent
    current_bytes = (here / 'source-inputs.json').read_bytes()
    expected = json.loads(current_bytes)
    assert len(expected) == 882
    assert sum(p.startswith('rust/') for p in expected) == 877
    assert digest(current_bytes) == 'b304327485c8480b6e9d75e870ed31e52de898f7e1babef7c79f8758bf6758d6'
    if index:
        inputs = {p: subprocess.check_output(['git', '-C', str(repo), 'show', ':' + p]) for p in expected}
    else:
        resolved = subprocess.check_output(
            ['git', '-C', str(repo), 'rev-parse', '--verify', revision + '^{commit}'], text=True).strip()
        raw = subprocess.check_output(['git', '-C', str(repo), 'archive', resolved, '--', 'rust',
                                       *[p for p in expected if not p.startswith('rust/')]])
        inputs = {}
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            for member in archive.getmembers():
                if member.isdir():
                    continue
                assert member.isfile() and member.name not in inputs
                inputs[member.name] = archive.extractfile(member).read()
    assert {p: digest(b) for p, b in inputs.items()} == expected
    prior = json.loads((here / 'default-source-inputs.json').read_bytes())
    changed = {p for p in expected if expected[p] != prior[p]}
    assert changed == {'rust/crates/store/src/plugin_repository_fetch.rs',
                       'rust/crates/store/src/plugin_repository_fetch_peers.rs'}
    earlier = dict(inputs)
    for path in changed:
        earlier[path] = (here / 'prior-test-inputs' / Path(path).name).read_bytes()
    assert {p: digest(b) for p, b in earlier.items()} == prior
    summary = json.loads((here / 'QA-summary.json').read_text())
    assert (summary['passed'], summary['ignored'], summary['result_blocks']) == (823, 746, 172)
    assert summary['actual_workspace_exit'] == 0
    assert summary['source_map_sha256'] == digest((here / 'default-source-inputs.json').read_bytes())
    assert summary['current_source_map_sha256'] == digest(current_bytes)
    assert summary['full_defaults_repeated_after_testonly_delta'] is False
    for filename, stages in [('default-receipts.json', {'fmt', 'check', 'strict', 'defaults', 'store-no-run', 'job-no-run'}),
                             ('current-receipts.json', {'fmt', 'strict-store', 'fetch-focused', 'store-no-run'})]:
        receipt = json.loads((here / filename).read_text())
        commands = receipt['commands']
        assert len(commands) == len(stages) and {v['stage'] for v in commands} == stages
        assert all(type(v['actual_wait_exit']) is int and v['actual_wait_exit'] == 0
                   and v['source_inputs_unchanged'] is True for v in commands)
    selected = json.loads((here / 'selected-runtime-inputs.json').read_text())
    assert len(selected['files']) == 7
    for item in selected['files']:
        assert expected[item['destination']] == item['sha256']
        assert len(inputs[item['destination']]) == item['bytes']
    return {'passed': True, 'Rust_inputs': 877, 'coupled_inputs': 5,
            'exact_current_and_prior_Git_inputs': True,
            'compiler_or_application_replayed': False,
            'full_milestone_or_performance_acceptance': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=Path.cwd())
    choice = parser.add_mutually_exclusive_group(required=True)
    choice.add_argument('--revision')
    choice.add_argument('--index', action='store_true')
    args = parser.parse_args()
    print(json.dumps(verify(args.repo, args.revision, args.index), indent=2))
