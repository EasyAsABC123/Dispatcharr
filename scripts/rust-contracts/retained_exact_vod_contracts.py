"""Bind exact retained Original VOD playback and stats GET rows."""
import ast
import hashlib

from source_contracts import checked

KIND = 'retained_original_exact_VOD_split_HTTP'
INPUT = 'scripts/rust-contracts/retained-exact-vod/bindings.json'
INPUT_SHA = 'c6b4deaf8aa0e070265bf6bc91a4abd718433c8396d648682a15099ae26bfde3'
ROOT = 'scripts/rust-integration/exact-vod-original-c98/'
RECEIPT = ROOT+'receipt.json'
DRIVER = ROOT+'request-driver.py'
RECEIPT_SHA = '709aa1ddd26e3910a28094d0ac15adc9bbc248848af9630a2d17d873bbe1bc0c'
DRIVER_SHA = 'c275eb63f5234044961fdbed735246c3203cfe8b4f2ecd3b59be2b8b4c06ec06'
FIXTURE = 'Original-C98-split-exact-VOD-HTTP'
ROWS = {
    'vod-session-with-profile':('route-96391910b341255d0479:METHOD_UNRESOLVED',399,
        'proxy/vod/<str:content_type>/<uuid:content_id>/<str:session_id>/<int:profile_id>/',
        'proxy:vod_proxy:vod_stream_with_session_and_profile','/proxy/vod/movie/{movie_uuid}/{private_session}/{profile_id}/'),
    'vod-stats':('route-3f97315e0a8b4d4e4e58:METHOD_UNRESOLVED',402,
        'proxy/vod/stats/','proxy:vod_proxy:vod_stats','/proxy/vod/stats/'),
    'xc-movie-ext':('route-003ffce2bb702836db0a:METHOD_UNRESOLVED',413,
        'movie/<str:username>/<str:password>/<str:stream_id>.<str:extension>',
        'stream_xc_movie','/movie/{private_xc_username}/{private_xc_password}/{movie_pk}.mp4'),
    'xc-series-ext':('route-607c7709efce75e5591a:METHOD_UNRESOLVED',414,
        'series/<str:username>/<str:password>/<str:stream_id>.<str:extension>',
        'stream_xc_episode','/series/{private_xc_username}/{private_xc_password}/{episode_pk}.mp4'),
}
EXPECTED = [
    ('vod-stats','admin',200,404),('vod-stats','viewer',403,404),('vod-stats','anonymous',401,404),
    ('vod-session-with-profile','anonymous',206,206),
    ('xc-movie-ext','xc-disposable-user',301,200),('xc-series-ext','xc-disposable-user',301,200),
]
CLEAN = {'compose_down_passed':True,'remaining_owned':{'containers':0,'volumes':0,'networks':0}}


def dependencies():
    return {INPUT,RECEIPT,DRIVER}


def load(root):
    return checked(root,{'path':INPUT,'sha256':INPUT_SHA})


def assertions(receipt):
    assert receipt['exact_inputs']['original_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert receipt['request_driver_sha256']==DRIVER_SHA
    assert receipt['actual_exits']=={'original_pair':1,'native_retry':0}
    assert receipt['retained_original_pair_failure']=={'phase':'native-compose-bootstrap','type':'KeyError'}
    assert all(receipt[k] is False for k in ('Native_http_present_in_initial_pair','Native_retry_resampled_Original_HTTP',
                                             'Native_verified','host_generation_absence_claimed','full_role_matrix',
                                             'performance_verified','full_M4_accepted'))
    assert receipt['completed_Original_HTTP_capture_before_later_launcher_failure']
    assert receipt['exact_owned_project_cleanup_verified'] and receipt['cleanup']==dict(original_pair=CLEAN,native_retry=CLEAN)
    assert receipt['primary_status_and_selected_body_match']==1 and receipt['primary_status_differences']==5
    rows = receipt['routes']
    assert [(row['operation'],row['role'],row['original']['status'],row['native']['status']) for row in rows]==EXPECTED
    stats,_,_,profile,movie,series = rows
    assert stats['original']['value']==dict(vod_connections=[],total_connections=0,finite_timestamp=True)
    assert stats['original']['idle_stats_shape_matches'] and not stats['original']['body_truncated']
    prefix = receipt['exact_inputs']['origin']['prefix_sha256']
    assert profile['original']==profile['native']
    for ranged in (profile['original'],movie['original_owned_follow']):
        assert ranged['status']==206 and ranged['owned_prefix_matched'] and not ranged['body_truncated']
        assert ranged['response_bytes']==65536 and ranged['response_sha256']==prefix
        assert ranged['content_range']==f"bytes 0-65535/{receipt['exact_inputs']['origin']['bytes']}"
    for row in (movie,series):
        assert row['original']['redirect_kind']=='same XC path with session_id query'
        assert row['original']['location_present'] and row['original']['response_bytes']==0
        assert row['native']['content_type']=='text/html; charset=utf-8' and row['native']['response_bytes']==858
    assert series['original_owned_follow'] is None and movie['native_owned_follow'] is None
    assert set(receipt['exact_source_route_authorities'])==set(ROWS)


def generate(receipt):
    assertions(receipt)
    matches = {
        'vod-session-with-profile':['anonymous movie range reaches the exact session plus profile route and returns 65,536 owned MP4 bytes'],
        'vod-stats':['idle admin GET returns Original VOD stats JSON', 'standard viewer and anonymous receive the separately observed denials'],
        'xc-movie-ext':['owned XC Movie PK and literal .mp4 GET receive a same-XC-path 301 with an opaque session query',
                        'one validated same-service movie follow returns the owned 64 KiB range'],
        'xc-series-ext':['owned XC Episode PK and literal .mp4 GET receive a same-XC-path 301 with an opaque session query',
                         'redirect is observed without requesting series media'],
    }
    raw = receipt['exact_inputs']['record_sha256']
    fixture = {
        'id':FIXTURE,'source_revision':receipt['exact_inputs']['original_revision'],'source_image':receipt['pinned_original_image_ref'],
        'private_capture_sha256':raw['original'],'private_Original_phase_launcher_exit_sha256':raw['original_exit'],
        'request_driver_sha256':DRIVER_SHA,'public_artifacts':[{'path':RECEIPT,'sha256':RECEIPT_SHA}],
        'Original_observations_completed':True,'actual_initial_paired_launcher_exit':1,
        'initial_pair_later_failure_phase':'native-compose-bootstrap','physical_Original_namespace_cleanup':True,
        'actual_native_retry_launcher_exit':0,'candidate98_scoped_primary_status_mismatches':5,
        'native_execution_claimed':True,'Native_parity_claimed':False,'performance_verified':False,
    }
    contracts = []
    for operation,(ident,ordinal,pattern,name,path) in ROWS.items():
        authority = receipt['exact_source_route_authorities'][operation]
        assert (authority['resolver_ordinal'],authority['pattern_exact'],authority['resolver_name'],authority['observed_method'])==(
            ordinal,pattern,name,'GET')
        cases = []
        for row in receipt['routes']:
            if row['operation']!=operation:
                continue
            observed = dict(role=row['role'],method='GET',path_template=path,
                            path_authority='exact executed frozen request with secret segments preserved privately',**row['original'])
            if row['original_owned_follow'] is not None:
                observed['same_service_movie_follow']=row['original_owned_follow']
            cases.append(observed)
        roles = [row['role'] for row in cases]
        contracts.append({
            'contract_id':ident,'fixture_id':FIXTURE,'pattern_exact':pattern,'source_handler':'apps.proxy.vod_proxy.views.WrappedAPIView',
            'resolver_ordinal':ordinal,'resolver_name':name,'owning_milestone':'M4','method':None,'source_action':None,
            'observed_methods':['GET'],'observed_roles':roles,
            'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
            'assertions':matches[operation],
            'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,'Native_coverage_increased':False},
            'fully_verified':False,'cases':cases,
        })
    return {'binding_kind':KIND,'operation_count':4,'new_Source_executions':0,
            'credit':'Original Dispatcharr authors and contributors retain full application and behavior credit.',
            'fully_verified_operations':0,'Native_coverage_increased':False,'performance_verified':False,
            'fixtures':{FIXTURE:fixture},'contracts':contracts}


def validate(root,packet):
    receipt = checked(root,{'path':RECEIPT,'sha256':RECEIPT_SHA})
    raw_driver = (root/DRIVER).read_bytes()
    assert hashlib.sha256(raw_driver).hexdigest()==DRIVER_SHA
    tree = ast.parse(raw_driver)
    [capture] = [node for node in tree.body if isinstance(node,ast.FunctionDef) and node.name=='capture']
    texts = [node.value for node in ast.walk(capture) if isinstance(node,ast.Constant) and isinstance(node.value,str)]
    assert {'/proxy/vod/stats/','/proxy/vod/movie/','.mp4','vod-session-with-profile',
            'xc-disposable-user','bytes=0-65535'}<=set(texts)
    assert packet==load(root)==generate(receipt)


def restore_pre_vod_split(root,rows):
    contracts = {contract['contract_id']:contract for contract in load(root)['contracts']}
    for route in rows:
        for operation in route['operations']:
            identifier = operation['contract_id']
            if identifier in contracts:
                assert operation.pop('executable_Source_contract')==contracts[identifier] and not operation['related_evidence']
                operation.update(setup_fixture=None,assertions=[],result=None,coverage_state='not_yet_mapped')
                operation['role_matrix']={'state':'unmapped','required_roles':[]}
