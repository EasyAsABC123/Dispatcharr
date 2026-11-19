"""Bind the retained Original cross-replica live TS channel stop."""
import ast
import hashlib
from uuid import UUID

from source_contracts import checked, digest

KIND = 'retained_original_Live_TS_global_stop_HTTP'
INPUT = 'scripts/rust-contracts/retained-live-stop/bindings.json'
INPUT_SHA = '019fadce530a36b430982ab8c6ab133fde2a42f6b6030a39320bdf4906c2ef2f'
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
BASE = 'scripts/rust-milestone-batches/m235/profile-source-media/'
SUMMARY = BASE + 'publication-summary.json'
CLEANUP = BASE + 'fresh-cleanup.json'
PROBE = BASE + 'shared_ts.py'
MANIFEST = BASE + 'manifest.json'
SUMMARY_SHA = '2be48a2d9f0f7670fdaef83e0d30bfe79c660dc8ea15fe35305debaf5994cc44'
CLEANUP_SHA = '84474730de56e56f79cfe66677358d98c2c7d63514e5b46ef2d09947111e3ef5'
PROBE_SHA = 'edbc2050c3bb2fba7bc694150e607f17e82c3b9bd216e2aaf9c41cad930806b3'
RAW_SHA = '4a8b38d7996ac0a05e247683de886f82773e58bbdff8f327135a0f1c2c7250fa'
SEED_SHA = 'e9324a549bfc8f29ff53dea3f98edffb7025d3aab6f1d6b4205b1a6d8ae0d490'
IDENTIFIER = 'route-085345eee60898a2f608:METHOD_UNRESOLVED'
ROUTE = 'proxy/ts/stop/<str:channel_id>'
ROUTE_NAME = 'proxy:live_proxy:stop_channel'
ORDINAL = 389
HANDLER = 'apps.proxy.live_proxy.views.WrappedAPIView'
FIXTURE = 'Original-two-replica-TS-owned-admin-stop'


def dependencies():
    return {INPUT, SUMMARY, CLEANUP, PROBE, MANIFEST}


def load(root):
    return checked(root, {'path': INPUT, 'sha256': INPUT_SHA})


def expected_response(channel):
    return {'message': 'Channel stop request sent', 'channel_id': channel, 'previous_state': {'state': 'active'}}


def verify_driver(root):
    raw = (root / PROBE).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == PROBE_SHA
    syntax = ast.parse(raw)
    [match] = [node for node in ast.walk(syntax) if isinstance(node, ast.Call)
               and isinstance(node.func, ast.Name) and node.func.id == 'observe'
               and node.args and isinstance(node.args[0], ast.Constant)
               and node.args[0].value == 'stop_response']
    assert ast.unparse(match.args[1]) == "lambda: http(base_b + '/proxy/ts/stop/' + channel, method='POST', token=admin)"
    assert any(isinstance(node, ast.Assign) and ast.unparse(node) == "admin = accounts['admin']['access']"
               for node in ast.walk(syntax))
    assert b"service_a,service_b,port='baseline',('python-replica'if args.source_topology=='two-replicas'else'baseline'),5656" in raw


def verify_public(root, fixture):
    authority = fixture['public_artifacts']
    assert authority == [{'path': p, 'sha256': h} for p, h in ((SUMMARY, SUMMARY_SHA), (CLEANUP, CLEANUP_SHA))]
    public, cleanup = (checked(root, item) for item in authority)
    assert public['Original_git'] == fixture['source_revision'] == BASELINE
    assert public['Original_image'] == fixture['source_image'] == IMAGE
    assert public['raw_result_sha256'] == fixture['private_capture_sha256'] == RAW_SHA
    assert public['controller_actual_exit'] == fixture['actual_source_controller_exit'] == 0
    assert public['capture_complete'] is public['capture_valid'] is True and public['raw_passed_literal'] is False
    assert public['performance_verified'] is public['full_M3_passed'] is False
    assert fixture['raw_controller_passed'] is fixture['native_execution_claimed'] is False
    assert fixture['exact_source_compose_cleanup'] is True and fixture['independent_absence_checks'] == 5
    assert fixture['host_generation_absence_verified'] is False
    assert cleanup['passed'] and len(cleanup['checks']) == 5
    assert all(row['passed'] and row['exit'] == 0 and not row['stdout'] and not row['stderr'] for row in cleanup['checks'])
    [stop] = [item for item in public['cases'] if item['case'] == 'stop']
    assert stop['stop_response_status'] == 200 and stop['shared_fixture_clean_for_next_context'] is True
    assert stop['actual_input_process_count'] == stop['upstream_connection_delta'] == stop['active_upstreams'] == 1
    assert stop['shared']['clients'] == 2 and stop['shared']['provider_slots'] == 1
    assert stop['origin_active_after'] == stop['process_count_after'] == stop['after']['clients'] == stop['after']['provider_slots'] == 0
    assert all(c['http_status'] == 200 and c['ended'] for c in stop['readers'])
    manifest = checked(root, fixture['public_tool_manifest'])
    assert fixture['public_tool_manifest']['path'] == MANIFEST
    assert manifest['files']['publication-summary.json']['sha256'] == SUMMARY_SHA
    assert manifest['files']['fresh-cleanup.json']['sha256'] == CLEANUP_SHA
    assert manifest['files']['shared_ts.py']['sha256'] == fixture['request_driver_sha256'] == PROBE_SHA
    verify_driver(root)


def validate(root, packet):
    assert packet == load(root)
    assert packet['binding_kind'] == KIND and packet['operation_count'] == len(packet['contracts']) == 1
    assert packet['new_Source_executions'] == packet['fully_verified_operations'] == 0
    assert packet['Native_coverage_increased'] is packet['performance_verified'] is False
    fixture = packet['fixtures'][FIXTURE]
    verify_public(root, fixture)
    assert fixture['private_seed_sha256'] == SEED_SHA
    [definition] = packet['contracts']
    assert definition['contract_id'] == IDENTIFIER and definition['owning_milestone'] == 'M3'
    assert definition['pattern_exact'] == ROUTE and definition['source_handler'] == HANDLER
    assert definition['source_action'] is definition['method'] is None and definition['fully_verified'] is False
    assert definition['fixture_id'] == FIXTURE
    assert (definition['resolver_ordinal'], definition['resolver_name']) == (ORDINAL, ROUTE_NAME)
    assert definition['observed_roles'] == ['admin'] and definition['observed_methods'] == ['POST']
    assert definition['source_role_specs'] == {'admin': {'fixture_role_or_supplied_credential': 'admin', 'complete': False}}
    assert definition['result'] == {'Source_case_observations': 1, 'new_Source_cases_this_task': 0,
                                    'Native_coverage_increased': False}
    [observation] = definition['cases']
    channel = observation['owned_channel_uuid']
    assert str(UUID(channel)) == channel
    assert (observation['method'], observation['role'], observation['path'], observation['status']) == (
        'POST', 'admin', '/proxy/ts/stop/' + channel, 200)
    assert observation['path_authority'] == 'exact_executed_driver_and_seeded_channel_matching_wire_response'
    assert observation['capture_pointer'] == '/cases/1/stop_response'
    assert observation['json_response_sha256'] == digest(expected_response(channel))
    assert observation['both_readers_and_upstream_retired'] is True


def restore_pre_live_stop(root, rows):
    value = load(root)
    [operation] = [op for route in rows for op in route['operations'] if op['contract_id'] == IDENTIFIER]
    assert operation.pop('executable_Source_contract') == value['contracts'][0] and not operation['related_evidence']
    operation.update(setup_fixture=None, assertions=[], result=None, coverage_state='not_yet_mapped')
    operation['role_matrix'] = {'state': 'unmapped', 'required_roles': []}
