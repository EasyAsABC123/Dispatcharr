"""Bind exact retained Original TS detail and targeted stop HTTP."""
import ast
from uuid import UUID

from source_contracts import checked

KIND = 'retained_original_Live_TS_detail_and_targeted_stop_HTTP'
INPUT = 'scripts/rust-contracts/retained-ts-controls/bindings.json'
INPUT_SHA = '385a31ad93a053d81934b6dfd62b17dbcbf05042a70074795596f5bc5184a73e'
BASE = 'scripts/rust-milestone-batches/m235/ts-controls/'
PUBLIC = BASE + 'Original.json'
DRIVER = BASE + 'request-driver.py'
PUBLIC_SHA = '4e648d737c509e19edbf0783a7aa193cb6a7ee4934f5f356da43bf25185fecae'
DRIVER_SHA = 'f77ef1d5577db801b2bb5c3dd9a9263c9a824538dd455d54476303cf0aafd7a9'
FIXTURE = 'Original-two-replica-TS-owned-detail-and-targeted-stop'
COORDINATES = {
    'detail': ('route-c5355f0ef3ea403e2a2f:METHOD_UNRESOLVED',388,'channel_status_detail','status','GET'),
    'target': ('route-d11609e20f7f2969ef50:METHOD_UNRESOLVED',390,'stop_client','stop_client','POST'),
}
EXPECTED = {
    'detail_anonymous':('GET','status','anonymous',401),
    'detail_standard':('GET','status','standard',403),
    'detail_missing':('GET','status','admin',404),
    'detail_active':('GET','status','admin',200),
    'stop_standard':('POST','stop_client','standard',403),
    'stop_target':('POST','stop_client','admin',200),
    'detail_survivor':('GET','status','admin',200),
}


def dependencies():
    return {INPUT, PUBLIC, DRIVER}


def load(root):
    return checked(root, {'path':INPUT,'sha256':INPUT_SHA})


def source_assertions(receipt):
    assert receipt['source_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert receipt['source_image']=='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
    assert receipt['request_driver_sha256']==DRIVER_SHA
    assert receipt['actual_launcher_exit']==0
    assert all(receipt[k] is True for k in ('stdout_EOF','stderr_EOF','process_group_settled','capture_valid','exact_cleanup'))
    assert all(receipt[k] is False for k in ('Native_verified','performance_verified','next_stream_verified',
               'full_role_matrix','full_M3_passed','cleanup_fallback_used','raw_characterization_passed'))
    assert sorted(receipt['role_cleanup'])==['baseline','origin','python-replica']
    assert all(v==dict(exact_container_absence=True,exact_host_generation_absence=True) for v in receipt['role_cleanup'].values())
    assert receipt['remaining_project_resources']==dict(container=0,network=0,volume=0)
    assert [(v['attempt'],v['gate'],v['actual_launcher_exit'],v['Compose_started']) for v in receipt['prior_pre_Compose_failures']]==[
        (1,'output_path_preflight',1,False),(2,'coordinator_dependency_import',1,False)]
    proof = receipt['scope_observations']
    assert proof['two_distinct_replica_clients'] and proof['one_upstream_connection_and_provider_slot']
    assert proof['starting_client_count']==2 and proof['final_client_count']==1
    assert proof['target_actual_eof'] and proof['peer_non_padding_bytes_after_target_eof']>=300000
    assert proof['denied_stop_preserved_both_viewers'] and not proof['peer_stop_marker_present']
    assert proof['scope_teardown_and_both_decoders_passed']
    channel = receipt['channel']
    assert str(UUID(channel))==channel
    for row,(name,(method,segment,role,status)) in zip(receipt['observations'],EXPECTED.items(),strict=True):
        assert (row['case'],row['method'],row['role'],row['status'])==(name,method,role,status)
        prefix = f'/proxy/ts/{segment}/'
        coord = row['path'].removeprefix(prefix)
        assert row['path']==prefix+coord and str(UUID(coord))==coord
        assert (coord!=channel)==(name=='detail_missing')
        assert row['content_type']=='application/json'
    by_name = {row['case']:row for row in receipt['observations']}
    active = by_name['detail_active']['body_projection']
    survivor = by_name['detail_survivor']['body_projection']
    owner, follower = (active['clients'][name] for name in ('owner','follower'))
    assert owner['client_id']!=follower['client_id'] and owner['worker_sha256']!=follower['worker_sha256']
    assert active['channel_id']==survivor['channel_id']==channel and active['state']==survivor['state']=='active'
    assert active['client_count']==2 and survivor['client_count']==1 and survivor['clients']=={'owner':owner}
    assert by_name['stop_target']['body']==dict(message='Client stop request processed',channel_id=channel,
                                               client_id=follower['client_id'],locally_processed=False)


def generate(receipt):
    source_assertions(receipt)
    observed = {'detail':['anonymous','standard','admin'],'target':['standard','admin']}
    assertions = {
        'detail': ['exact anonymous 401, standard 403 and admin unknown channel 404',
                   'admin 200 sees two replica clients before the targeted stop and the exact owned survivor afterward'],
        'target': ['standard POST 403 preserves both viewers and their accounting',
                   'admin POST 200 targets the other replica, which reaches actual EOF while the same peer continues media',
                   'one unchanged upstream/provider reservation and exact final scope retirement'],
    }
    fixture = {
        'id':FIXTURE,'source_revision':receipt['source_revision'],'source_image':receipt['source_image'],
        'request_driver_sha256':DRIVER_SHA,'private_capture_sha256':receipt['raw_fingerprints']['capture/shared-ts.json'],
        'actual_source_controller_exit':0,'raw_controller_passed':False,'native_execution_claimed':False,
        'exact_source_compose_cleanup':True,'host_generation_absence_verified':True,
        'prior_pre_Compose_failures':2,'public_artifacts':[{'path':PUBLIC,'sha256':PUBLIC_SHA}],
    }
    contracts = []
    for key,(identifier,ordinal,name,segment,method) in COORDINATES.items():
        route = 'proxy/ts/'+segment+'/<str:channel_id>'
        assert receipt['source_route_authority'][key]['route']==route
        assert receipt['source_route_authority'][key]['method']==method
        cases = [row for row in receipt['observations'] if row['method']==method]
        contracts.append({
            'contract_id':identifier,'source_handler':'apps.proxy.live_proxy.views.WrappedAPIView',
            'pattern_exact':route,'resolver_ordinal':ordinal,'resolver_name':'proxy:live_proxy:'+name,
            'method':None,'source_action':None,'owning_milestone':'M3','fixture_id':FIXTURE,
            'observed_roles':observed[key],'observed_methods':[method],
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in observed[key]},
            'assertions':assertions[key],
            'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,'Native_coverage_increased':False},
            'fully_verified':False,'cases':cases,
        })
    return {
        'binding_kind':KIND,'operation_count':2,'new_Source_executions':0,
        'credit':'Original Dispatcharr authors and contributors retain full application and behavior credit.',
        'fully_verified_operations':0,'Native_coverage_increased':False,'performance_verified':False,
        'fixtures':{FIXTURE:fixture},'contracts':contracts,
    }


def validate(root, packet):
    import hashlib
    evidence = checked(root, {'path':PUBLIC,'sha256':PUBLIC_SHA})
    driver = (root/DRIVER).read_bytes()
    assert hashlib.sha256(driver).hexdigest()==DRIVER_SHA
    tree = ast.parse(driver)
    [capture] = [node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='capture']
    assignments = {ast.unparse(node) for node in capture.body if isinstance(node,ast.Assign)}
    assert {"detail = '/proxy/ts/status/' + channel","stop = '/proxy/ts/stop_client/' + channel"} <= assignments
    assert packet==load(root)==generate(evidence)


def restore_pre_ts_controls(root, rows):
    contracts = {contract['contract_id']:contract for contract in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier = operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
