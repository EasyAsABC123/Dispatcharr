"""Attach stock Original dispatch metadata without promoting route behaviors."""
from collections import Counter
from hashlib import sha256
import json

INPUT = 'scripts/rust-contracts/runtime-methods-probe-322.json'
INPUT_SHA256 = 'd0b8c89d21408a1216825f7454f6f9da589b9d19a8f9d7052facaa76857b1831'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
VERBS = {'GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD', 'OPTIONS', 'TRACE'}
KIND = 'stock_class_dispatch_metadata'


def attach(root, report):
    path = root / INPUT
    if not path.exists():
        return report
    blob = path.read_bytes()
    assert sha256(blob).hexdigest() == INPUT_SHA256
    packet = json.loads(blob)
    assert packet['pinned_original_revision'] == report['baseline_ref'] == ORIGINAL
    assert packet['historical_roster_sha256'] == report['input_sha256']['docs/rust-runtime-contracts.json']
    assert packet['owned_container_retired'] is packet['empty_owned_project_inventories'] is True
    assert packet['actual_exits'] == dict.fromkeys(('preflight','run','inspect','evaluate','cleanup','outer'), 0)
    assert packet['actual_runtime_process_exit'] == packet['actual_routed_calls'] == packet['actual_database_attempts'] == 0
    assert packet['inferred_roles'] == packet['inferred_HTTP_results'] == 0
    assert packet['formal_M0_accepted'] is False
    assert len(report['runtime_routes']) == packet['full_resolver_roster_size'] == 592
    placeholders = {r['resolver_ordinal']: r for r in report['runtime_routes'] if not r['declared_methods_resolved']}
    observations = packet['observations']
    observed_ordinals = [item['ordinal'] for item in observations]
    assert len(observations) == len(placeholders) == 322 and observed_ordinals == sorted(placeholders)
    counts = Counter(item['classification'] for item in observations)
    assert counts == packet['classification_counts'] and counts[KIND] == 139
    for item in observations:
        route = placeholders[item['ordinal']]
        operation, = route['operations']
        assert (item['route_id'], item['pattern'], item['handler']) == (route['route_id'], route['pattern_exact'], route['source_handler'])
        assert item['method_placeholder'] == operation['contract_id'] and operation['method'] is None
        assert item['operation_method_promoted'] is False
        if item['classification'] != KIND:
            continue
        selector = item['framework_dispatch']
        assert selector in ('django.views.generic.base.View.dispatch','rest_framework.views.APIView.dispatch')
        assert item['framework_handler_return_observed'] is False
        class_names = item['http_method_names']
        class_methods = item['class_handlers']
        methods = item['framework_dispatch_methods']
        alias = item['implicit_HEAD_through_View_setup']
        assert type(alias) is bool and set(class_methods) <= set(class_names) <= VERBS
        assert set(methods) == set(class_methods) | ({'HEAD'} if alias else set())
        assert all(list(sorted(set(seq))) == seq for seq in (class_names,class_methods,methods))
        route['Original_runtime_class_dispatch'] = {
            'framework': selector,
            'methods': methods,
            'implicit_HEAD_through_View_setup': alias,
            'dispatched_HTTP_or_roles_verified': False,
            'attestation': {'path': INPUT,'sha256': INPUT_SHA256},
        }
    report['input_sha256'][INPUT] = INPUT_SHA256
    report['Original_runtime_dispatch_inventory'] = {
        'attestation': {'path': INPUT,'sha256': INPUT_SHA256},
        'scope': 'stock Original callback metadata without requests or database connections',
        'method_placeholders_observed': 322,
        'class_metadata_attachments': counts[KIND],
        'remaining_without_class_metadata': 322 - counts[KIND],
        'classifications': dict(sorted(counts.items())),
        'new_declared_operation_ids': 0,
        'fully_verified': False,
    }
    return report
