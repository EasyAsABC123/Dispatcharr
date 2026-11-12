#!/usr/bin/env python3
"""Explicit supported Original-only fixture entrypoints; no candidate or sampler.
Historical-only fixtures are not freshly qualified by merely shipping this entry.
"""
import argparse
import base64
import re
import signal
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from urllib.parse import parse_qs, urlsplit

ROOT = Path(__file__).resolve().parents[2]


def prepare(root, destination, fixture):
    for key in ['seed', 'oracle']:
        reference = fixture[key]
        path = root / reference['path']
        if hashlib.sha256(path.read_bytes()).hexdigest() != reference['sha256']:
            raise ValueError('FixtureCodeChanged:' + key)
    for directory in ['scripts', 'docker', 'docs']:
        shutil.copytree(root / directory, destination / directory,
                        ignore=shutil.ignore_patterns('__pycache__', 'node_modules'))
    substitution = fixture['execution']['shadow_oracle_substitution']
    if substitution:
        source = destination / substitution['authority']
        target = destination / substitution['target']
        target.write_bytes(source.read_bytes())
    return [sys.executable, str(destination / fixture['execution']['supported_runner']),
            *fixture['execution']['runner_flags']]


def assert_fresh(value, fixture_id, output):
    names = {'provider150': 'provider-actions-source.json', 'cached90': 'cached-provider-info-source.json',
             'images240': 'vod-images-source.json', 'unified154': 'unified-source.json'}
    capture_path = output / 'http' / names[fixture_id]
    capture = json.loads(capture_path.read_text())
    assert capture['passed'] is True
    if fixture_id == 'unified154':
        assert capture['target'] == 'Original' and capture['exact_owned_rows_restored'] is True
        assert capture['fingerprints_before'] == capture['fingerprints_after']
    elif fixture_id == 'provider150':
        assert capture['fullmicro18table_before'] == capture['fullmicro18table_after']
    elif fixture_id == 'cached90':
        assert capture['full18_exact_microseconds_before'] == capture['full18_exact_microseconds_after']
    else:
        assert capture['full18_exact_before'] == capture['full18_exact_after']
        assert capture['owned_local_file_removed'] is True
    owned_nonce = None
    if fixture_id == 'images240':
        nonces = {parse_qs(urlsplit(r['path']).query).get('fixture', [None])[0] for r in capture['origin_requests']}
        assert len(nonces) == 1
        owned_nonce = nonces.pop()
        assert re.fullmatch('[a-f0-9]{32}', owned_nonce or '')
    checked = 0
    for contract in value['contracts']:
        if contract['fixture_id'] != fixture_id:
            continue
        for case in contract['cases']:
            row = capture['records'][case['source_index']]
            assert all(row[k] == case[k] for k in ['path', 'method', 'role'])
            actual = row['expected']
            assert actual['status'] == case['response']['status']
            headers = actual.get('headers', {'Content-Type': actual.get('content_type'), **actual.get('selected_headers', {})})
            for name, expected in case['response']['headers'].items():
                if name == 'Content-Length':
                    continue
                got = headers.get(name)
                if name == 'Content-Disposition' and expected and 'rust-probe-vod-image-' in expected:
                    old_nonce = re.search(r'rust-probe-vod-image-([a-f0-9]{32})', expected).group(1)
                    assert got == expected.replace(old_nonce, owned_nonce)
                else:
                    assert got == expected, name
            if 'body_base64' in actual:
                body = base64.b64decode(actual['body_base64'], validate=True)
            elif 'body_text' in actual:
                body = actual['body_text'].encode()
            else:
                # Some unified public/capture records intentionally retain only
                # a body hash; never fabricate bytes or assert a new raw decode.
                body = None
            if body is not None:
                assert hashlib.sha256(body).hexdigest() == actual['body_sha256']
                assert len(body) <= 5 * 1024 * 1024
                if 'body_bytes' in actual:
                    assert len(body) == actual['body_bytes']
                if headers.get('Content-Length') is not None:
                    assert headers['Content-Length'] == str(len(body))
                if (headers.get('Content-Type') or '').startswith('application/json'):
                    assert isinstance(json.loads(body), (dict, list))
            checked += 1
    result = {'passed': True, 'fixture': fixture_id, 'Source_cases_checked': checked,
              'kind': 'fresh scoped status/stable-header/available-wire-integrity assertions',
              'captured_historical_body_hashes_not_reused_for_dynamic_UUID_clock_origin_values': True,
              'parameterized_fields': ['Content-Length from current bytes', 'owned image filename32hexnonce'],
              'capture_sha256': hashlib.sha256(capture_path.read_bytes()).hexdigest(),
              'Native_verified': False, 'whole_endpoint_verified': False}
    (output / 'fresh-Source-contract-assertions.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', choices=['provider150', 'cached90', 'images240', 'unified154'], required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    if output.exists():
        parser.error('fresh output required')
    value = json.loads((ROOT / 'scripts/rust-contracts/executable-source-bindings.json').read_text())
    fixture = value['fixtures'][args.fixture]
    git_directory = subprocess.check_output(['git', 'rev-parse', '--absolute-git-dir'], cwd=ROOT, text=True).strip()
    with tempfile.TemporaryDirectory(prefix='dispatcharr-source-contract-') as directory:
        shadow = Path(directory) / 'support'
        command = prepare(ROOT, shadow, fixture) + ['--output', str(output)]
        env = dict(os.environ, GIT_DIR=git_directory, GIT_WORK_TREE=str(shadow))
        # Underlying qualified runner creates output and propagates actual failure.
        child = subprocess.Popen(command, cwd=shadow, env=env, start_new_session=True)
        try:
            returncode = child.wait(timeout=900)
        except BaseException:
            # Retain the support tree and process ownership through the original
            # controller's cleanup. Never kill/drop its physical cleanup on a
            # wrapper observation timeout or caller interruption.
            if child.poll() is None:
                os.killpg(child.pid, signal.SIGINT)
            child.wait()
            raise
        result = subprocess.CompletedProcess(command, returncode)
        if result.returncode == 0:
            assert_fresh(value, args.fixture, output)
        if output.exists():
            record = {'fixture': args.fixture, 'command': command, 'exit_code': result.returncode,
                      'shadow_oracle_substitution': fixture['execution']['shadow_oracle_substitution'],
                      'source_only': True, 'resource_sampler': False,
                      'fresh_predicate_replay': 'provider_gets/run.py provides the separately qualified78 assertion entry; other fixtures capture their declared Original oracle'}
            (output / 'Source-fixture-entry.json').write_text(json.dumps(record, indent=2) + '\n')
        return result.returncode


if __name__ == '__main__':
    raise SystemExit(main())
