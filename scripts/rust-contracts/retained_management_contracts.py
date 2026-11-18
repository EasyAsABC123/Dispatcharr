"""Exact retained Source12/guide25 assignments; no Source or Native execution.
All application/model/signal/task credit remains with original Dispatcharr authors.
"""
import hashlib
import json
import re
from pathlib import Path

KIND = 'retained_original_management12_channelguide25_HTTP'
PATH = 'scripts/rust-contracts/retained-management/bindings.json'
SHA256 = '13a0efd078bf4c8a7540101250ca11320453016ba6a64967e2dd597757a92215'


def dependencies():
    return {PATH}


def validate(root, value, bindings=None):
    raw = (Path(root) / PATH).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == SHA256, 'reviewed retained Source packet'
    assert value == json.loads(raw), 'exact retained Source definitions'
    assert value['binding_kind'] == KIND and value['unique_Source_cases'] == 37
    assert value['operation_count'] == len(value['contracts']) == 12
    assert value['fully_verified_operations'] == value['new_Source_executions'] == 0
    assert value['Native_coverage_increased'] is False
    seen = set()
    groups = {}
    for contract in value['contracts']:
        assert contract['fully_verified'] is False
        assert contract['observed_roles'] == ['admin']
        fixture = value['fixtures'][contract['fixture_id']]
        assert fixture['baseline_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
        pattern = contract['pattern_exact'].replace('/^', '/')
        pattern = re.sub(r'<int:[^>]+>', r'[0-9]+', pattern)
        for case in contract['cases']:
            identity = (case['fixture_id'], case['label'])
            assert identity not in seen, 'one retained Source leaf per assignment'
            seen.add(identity)
            groups[identity[0]] = groups.get(identity[0], 0) + 1
            assert case['contract_id'] == contract['contract_id']
            assert re.fullmatch(pattern, case['path'].split('?', 1)[0].lstrip('/'))
            assert case['method'] in contract['observed_methods']
            assert contract['method'] is None or case['method'] == contract['method']
            assert case['role'] == 'admin' and case['Source_positive_leaf_qualified']
            assert case['HTTP_status'] in [200, 201, 204]
            assert len(case['response_body_sha256']) == 64
        assert contract['result']['Source_cases_passed'] == len(contract['cases'])
        assert contract['result']['new_Source_cases_this_task'] == 0
        assert contract['result']['Native_coverage_increased'] is False
    assert groups == {'management-first4': 4, 'management-second8': 8, 'channel-guide25': 25}
    assert value['fixtures']['management-first4']['actual_controller_exit'] == 1
    assert value['fixtures']['management-second8']['actual_controller_exit'] == 0
    return {'Source_positive_leaves': 37, 'distinct_operation_roots': 12,
            'new_Source_executions': 0, 'fully_verified_added': 0}


def verify_private_custody(root, value):
    """Optional offline raw replay; never emits credential-bearing capture data."""
    import base64
    root = Path(root)
    validate(root, value)
    checked = set()

    def exact(reference):
        path = Path(reference['path'])
        path = path if path.is_absolute() else root / path
        assert path.is_file() and not path.is_symlink(), 'retained regular capture'
        raw = path.read_bytes()
        assert hashlib.sha256(raw).hexdigest() == reference['sha256'], 'retained capture custody'
        checked.add(str(path))
        return json.loads(raw)

    for group in ['management-first4', 'management-second8', 'channel-guide25']:
        fixture = value['fixtures'][group]
        qualification = exact(fixture['qualification'])
        assert qualification['passed'] is True
        exact(fixture['report'])
        refs = qualification.get('refs', qualification.get('evidence', {}))
        if isinstance(refs, dict):
            refs = [{'path': path, 'sha256': item['sha256'] if isinstance(item, dict) else item}
                    for path, item in refs.items()]
        for reference in refs:
            # Journals are JSONL, so check their bytes without decoding as JSON.
            path = Path(reference['path'])
            path = path if path.is_absolute() else root / path
            assert path.is_file() and not path.is_symlink()
            assert hashlib.sha256(path.read_bytes()).hexdigest() == reference['sha256']
            checked.add(str(path))
    for contract in value['contracts']:
        for case in contract['cases']:
            request = exact(case['request_custody'])
            wire = exact(case['wire_custody'])
            assert wire['request'] == request
            assert all(request[k] == case[k] for k in ['method', 'path', 'role'])
            response = wire['response']
            assert not response['body_truncated'] and response['status'] == case['HTTP_status']
            body = base64.b64decode(response['body_base64'], validate=True)
            assert len(body) == case['response_body_bytes']
            assert hashlib.sha256(body).hexdigest() == case['response_body_sha256']
    return {'private_custody_files_verified': len(checked), 'Source_HTTP_leaves': 37,
            'Source_or_Native_applications_started': 0, 'raw_credentials_published': False}


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--private-custody', action='store_true')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    value = json.loads((root / PATH).read_text())
    print(json.dumps(verify_private_custody(root, value) if args.private_custody
                     else validate(root, value)))
