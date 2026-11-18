"""Six exact retained CoreSettings Source32 framework assignments; no app calls.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
"""
import hashlib
import importlib.util
import json
import re
import sys
from pathlib import Path

KIND = 'retained_original_core_settings_Source32_framework'
BASE = 'scripts/rust-contracts/core-settings-source32/'
INPUT = 'scripts/rust-contracts/core-settings-source32-assignments/bindings.json'
INVENTORY_SHA = '846b118db1a7497e98cc617b815f3a90a8ba9a68a3e7246017d0d2b30902a3f2'
# Only these six observed, unformatted operations. Collection GET is not inferred.
OPERATIONS = {
    ('POST', 'create'): ('route-4e613bb9583a369dde8a:POST', 'api/core/^settings/$', 'coresettings-list', 8),
    ('GET', 'retrieve'): ('route-2560f46761708905bcfa:GET', 'api/core/^settings/(?P<pk>[^/.]+)/$', 'coresettings-detail', 1),
    ('PATCH', 'partial_update'): ('route-2560f46761708905bcfa:PATCH', 'api/core/^settings/(?P<pk>[^/.]+)/$', 'coresettings-detail', 11),
    ('PUT', 'update'): ('route-2560f46761708905bcfa:PUT', 'api/core/^settings/(?P<pk>[^/.]+)/$', 'coresettings-detail', 2),
    ('DELETE', 'destroy'): ('route-2560f46761708905bcfa:DELETE', 'api/core/^settings/(?P<pk>[^/.]+)/$', 'coresettings-detail', 5),
    ('POST', 'check'): ('route-162671b3862403093cd4:POST', 'api/core/^settings/check/$', 'coresettings-check', 5),
}
_VALIDATED = set()


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def inventory(root):
    base = Path(root) / BASE
    path = base / 'packet-inventory.json'
    assert sha(path) == INVENTORY_SHA, 'exact already-reviewed Source32 public packet'
    value = json.loads(path.read_bytes())
    assert len(value) == 155
    return value


def dependencies():
    root = Path(__file__).resolve().parents[2]
    return {INPUT, BASE + 'packet-inventory.json', *(BASE + name for name in inventory(root))}


def authority(root):
    base = Path(root) / BASE
    bound = inventory(root)
    for name, expected in bound.items():
        path = base / name
        assert path.is_file() and not path.is_symlink() and sha(path) == expected, 'exact Source32 authority file'
    # Run the existing complete privacy-adapted 32-case validator once per exact
    # immutable public packet. Every call still checks all 155 bound file hashes.
    spec = importlib.util.spec_from_file_location('retained_core32_public_verifier', base / 'verify.py')
    verifier = importlib.util.module_from_spec(spec)
    previous_path = list(sys.path)
    try:
        spec.loader.exec_module(verifier)
    finally:
        sys.path[:] = previous_path
    if INVENTORY_SHA not in _VALIDATED:
        result = verifier.validate()
        assert result['passed'] is True and result['projection_executed'] is False
        assert result['Source_TCP_Native_Main_performance'] is False
        _VALIDATED.add(INVENTORY_SHA)
    return verifier.observations(), json.loads((base / 'reference.json').read_bytes()), json.loads((base / 'executed-executor/definition/plan.json').read_bytes())


def derive(root):
    root = Path(root)
    observations, reference, plan = authority(root)
    assert len(observations['requests']) == 32
    fixtures = {}
    contracts = []
    seen = set()
    for (method, action), (identifier, pattern, url_name, count) in OPERATIONS.items():
        cases = []
        for index, actual in enumerate(observations['requests']):
            resolver = actual['resolver']
            if actual['method'] != method or resolver['action'] != action:
                continue
            assert resolver['class'] == 'core.api_views.CoreSettingsViewSet'
            assert resolver['url_name'] == url_name
            assert re.fullmatch(pattern.replace('/^', '/'), actual['path'].lstrip('/'))
            assert index not in seen
            seen.add(index)
            cases.append({'Source_index': index, 'label': actual['name'], 'role': actual['role'],
                          'method': method, 'path': actual['path'], 'actual_resolver': resolver,
                          'status': actual['status'], 'response_body_bytes': actual['response_body_bytes'],
                          'response_body_sha256': actual['response_body_sha256'],
                          'public_observation_json_pointer': '/requests/' + str(index),
                          'privacy_projected_observation_sha256': digest(actual),
                          'case_definition_sha256': digest(plan['cases'][index]),
                          'projection_executed': False})
        assert len(cases) == count
        roles = sorted({case['role'] for case in cases})
        fid = 'retained-Source32:' + identifier
        fixtures[fid] = {'id': fid, 'baseline_revision': reference['Original_revision'],
                         'baseline_image': plan['Original_image'],
                         'original_executed_executor_seal_sha256': reference['Source_executor_seal_sha256'],
                         'original_private_capture_sha256': reference['original_raw_sha256'],
                         'executed_definition_path': BASE + 'executed-executor/definition/plan.json',
                         'executed_definition_sha256': reference['plan_sha256'],
                         'public_packet_inventory': {'path': BASE + 'packet-inventory.json', 'sha256': INVENTORY_SHA},
                         'setup': {'owned_seed_and_API_key_roles': plan['seed'], 'auth': plan['auth'],
                                   'target_policy': plan['target_policy'], 'ownership': plan['ownership'],
                                   'named_case_definition_indices': [case['Source_index'] for case in cases]},
                         'actual_controller_exit': 0, 'raw_recorder_passed': False,
                         'projected_observations_executed': False,
                         'transport_scope': 'Actual Original URL resolver/DRF APIClient with persisted API keys; no TCP/JWT, Native/Main/default-worker or performance authority',
                         'privacy_scope': 'Existing Root-reviewed projection keeps exact response bytes/resolver/roles/settings/effects. Credential/derivation bytes, accounts-user serialization and binary Redis/receiver identities remain private-hash evidence; adapted predicates do not publicly recompute them.'}
        contracts.append({'contract_id': identifier, 'source_handler': 'core.api_views.CoreSettingsViewSet',
                          'pattern_exact': pattern, 'method': method, 'source_action': action,
                          'owning_milestone': 'M5', 'fixture_id': fid, 'observed_roles': roles,
                          'source_role_specs': {role: {'Source_role_definition': plan['auth']['roles'][role],
                                                      'authentication': 'anonymous' if role == 'anonymous' else 'real persisted Source API key; private bytes retained by hash',
                                                      'TCP_or_JWT_qualified': False} for role in roles},
                          'cases': cases, 'fully_verified': False,
                          'assertions': ['Exact observed resolver class/action/URL-name and unformatted path bind every case to one pinned discovery operation; no format aliases or collection GET inferred',
                                         'Existing public verifier checks all 32 privacy-adapted semantic predicates, exact case/setup definitions, full mandatory snapshot coverage and physically settled lifecycle',
                                         'Raw recorder passed:false, actual controller exit0 and unexecuted public privacy projection remain distinct; no public redecoding of omitted private bytes'],
                          'result': {'state': 'partial_retained_Original_framework_API_key_Source32', 'Source_cases_passed': count,
                                     'new_Source_cases_this_task': 0, 'Native_coverage_increased': False,
                                     'complete_role_matrix': False, 'fully_verified': False, 'performance_verified': False,
                                     'raw_recorder_passed': False, 'projection_executed': False},
                          'remaining': ['Only recorded roles/input/error cases; no complete endpoint role/grammar matrix',
                                        'APIClient/API-key authority only; no Source TCP/JWT, Native, packaged Main or browser verification',
                                        'Privacy-adapted public predicates preserve opaque evidence for omitted credential/SQL/binary Redis bytes, without claiming their public redecoding']})
    assert seen == set(range(32))
    return {'format_version': 1, 'binding_kind': KIND,
            'credit': 'Original Dispatcharr and dependency authors retain all application/model/signal/algorithm credit and licenses.',
            'contracts': contracts, 'fixtures': fixtures, 'Source_observations': 32, 'operation_count': 6,
            'new_Source_executions': 0, 'Native_coverage_increased': False, 'fully_verified_operations': 0}


def validate(root, value, bindings=None):
    assert value == derive(root), 'exact six Source32 setup/resolver/role/response/assertion definitions'
    return {'Source_observations': 32, 'operation_assignments': 6, 'new_Source_executions': 0,
            'Native_coverage_increased': False, 'fully_verified_added': 0}


if __name__ == '__main__':
    root = Path(__file__).resolve().parents[2]
    value = derive(root)
    target = root / INPUT
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, indent=2, ensure_ascii=False) + '\n')
    print(json.dumps(validate(root, value)))
