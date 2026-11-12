"""Executable scoped Original contracts; full endpoint/Native coverage stays open."""
import ast
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def sha(data):
    return hashlib.sha256(data).hexdigest()


def canonical(value):
    return sha(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())


def load(root=ROOT):
    root = Path(root)
    spec = json.loads((root / 'scripts/rust-contracts/provider_gets/contracts.json').read_text())
    binding_bytes = (root / 'scripts/rust-contracts/case-bindings.json').read_bytes()
    attest_bytes = (root / 'scripts/rust-contracts/datasets/image44-scoped-cases.json').read_bytes()
    assert sha(binding_bytes) == spec['public_binding_sha256'], 'binding authority'
    assert sha(attest_bytes) == spec['public_attestation_sha256'], 'attestation authority'
    bindings = json.loads(binding_bytes)
    attestation = json.loads(attest_bytes)
    for name, digest in spec['fixture_code_sha256'].items():
        assert sha((root / name).read_bytes()) == digest, 'fixture code: ' + name
    # Read the literal fixture role definitions without importing Django or source.
    tree = ast.parse((root / 'scripts/rust-vod-api-contracts/source.py').read_text())
    role_loop = next(n for n in tree.body if isinstance(n, ast.For)
                     and isinstance(n.target, ast.Tuple)
                     and [x.id for x in n.target.elts] == ['role', 'level', 'active', 'props'])
    roles = {name: dict(user_level=level, is_active=active, custom_properties=props)
             for name, level, active, props in ast.literal_eval(role_loop.iter)}
    roles['anonymous'] = None
    provider = {b['contract_id']: b for b in bindings['bindings']
                if b.get('evidence_dataset_id') == 'provider150'}
    assert len(spec['contracts']) == len(provider) == 3
    seen = set()
    for item in spec['contracts']:
        b = provider[item['contract_id']]
        assert item['contract_id'] not in seen
        seen.add(item['contract_id'])
        assert item['method'] == b['method'] == 'GET'
        assert item['source_handler'] == b['source_handler']
        assert item['pattern_exact'] == b['pattern_exact']
        assert item['owning_milestone'] == 'M5'
        assert item['fully_verified'] is False and item['complete_role_matrix'] is False
        assert item['setup_fixture']['role_specs'] == {r: roles[r] for r in item['roles_captured']}
        assert item['roles_captured'] == sorted({c['role'] or 'anonymous' for c in b['cases']})
        assert len(item['cases']) == len(b['cases'])
        assert item['setup_fixture']['argv'] == ['--provider-actions-source', '--mode', 'http', '--output', '<new-empty-output>']
        assert item['setup_fixture']['runner'] == 'scripts/rust-vod-api-contracts/run_provider_actions.py'
        for case, original in zip(item['cases'], b['cases']):
            row = next(r for r in attestation['case_projection']
                       if r['dataset_id'] == 'provider150' and r['source_index'] == original['source_index'])
            assert {k: case[k] for k in ['source_index', 'path', 'method', 'role']} == {
                k: original[k] for k in ['source_index', 'path', 'method', 'role']}
            assert case['label'] == row['label'] and case['accept'] == row['accept']
            assert case['status'] == row['source_response']['status']
            assert case['static_selected_headers'] == {
                k: v for k, v in row['source_response']['headers'].items() if k != 'Content-Length'}
            assert case['historical_exact_response_sha256'] == canonical(row['source_response'])
            assert case['historical_response_authority'] == row['source_authority']
            assert len(case['fresh_assertions']) == 4
    assert sum(len(c['cases']) for c in spec['contracts']) == 78
    assert spec['fully_verified_operations'] == 0 and spec['Native_implementation_or_completeness_claim'] is False
    return spec


def assert_capture(spec, capture, run):
    assert capture['passed'] is True and capture['mode'] == 'http'
    assert len(capture['records']) == 150 and run['provider_action_cases'] == 150
    assert capture['owned_metadata_restored'] is True
    assert len(capture['fullmicro18table_before']) == 18
    assert capture['fullmicro18table_before'] == capture['fullmicro18table_after']
    assert capture['provider_info_or_refresh_executed'] is False
    assert run['passed'] is True and run['resources_retained'] is False
    assert run['cleanup'] == 'owned Compose project removed'
    assert run['provider_trap_attempts'] == 0 and run['provider_refresh_rows_unchanged'] is True
    result = []
    for item in spec['contracts']:
        observations = []
        for case in item['cases']:
            actual = capture['records'][case['source_index']]
            for key in ['path', 'role', 'method', 'label', 'accept']:
                assert actual[key] == case[key], 'request tuple: ' + key
            response = actual['expected']
            assert actual['observation'] == 'actual Original TCP'
            assert response['status'] == case['status'], 'status'
            assert {k: response['headers'].get(k) for k in case['static_selected_headers']} == case['static_selected_headers'], 'headers'
            body = response['body_text'].encode('utf-8')
            assert len(body) == response['body_bytes'] <= 4 * 1024 * 1024
            assert sha(body) == response['body_sha256'], 'body hash'
            length = response['headers']['Content-Length']
            assert length is None or length == str(len(body)), 'Content-Length'
            if response['headers']['Content-Type'].startswith('application/json'):
                decoded = json.loads(body)
                assert decoded == response['json']
                assert isinstance(decoded, list if response['status'] == 200 else dict), 'JSON grammar'
            observations.append({k: case[k] for k in ['source_index', 'path', 'role', 'method', 'label', 'status']})
        result.append({'contract_id': item['contract_id'], 'cases_passed': len(observations),
                       'observations': observations, 'complete_endpoint': False, 'Native_verified': False})
    return {'passed': True, 'mode': 'fresh actual Original TCP scoped assertions',
            'contracts': result, 'GET_assertions': 78, 'owned_18_tables_restored': True,
            'whole_endpoint_verified': False, 'Native_verified': False}
