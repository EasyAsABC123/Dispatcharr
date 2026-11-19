"""Create a scoped M0 projection from retained repository, TS and socket evidence."""
import argparse
import ast
import collections
import gzip
import hashlib
import json
from pathlib import Path
import re

KIND = 'retained_original_repository_socket_switch'
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
PACKAGE = Path(__file__).resolve().parent
ROOT = PACKAGE.parents[2]
TRANSPORT = 'scripts/rust-integration/plugin-repository-transport-source1/'
METADATA = 'scripts/rust-pluginrepo-contracts/metadata-component/'
SOCKET = 'scripts/rust-realtime-contracts/'
SWITCH = 'scripts/rust-playback-baseline/'
REPOS = 'route-e906572127d879858b21:METHOD_UNRESOLVED'
DETAIL = 'route-2893312784054a3f5856:METHOD_UNRESOLVED'
PREVIEW = 'route-6341bfe67df8f5e35bd9:METHOD_UNRESOLVED'
REFRESH = 'route-3401d1e3590d5d19427d:METHOD_UNRESOLVED'
CHANGE = 'route-211afdb7c19d0ea91286:METHOD_UNRESOLVED'
WS = 'route-1156f2f1488d795754d5:METHOD_UNRESOLVED'


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(record):
    return sha(json.dumps(record, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode())


def evidence(relative):
    raw = (ROOT/relative).read_bytes()
    reference = {'path':relative,'sha256':sha(raw)}
    if relative.endswith('.gz'):
        raw = gzip.decompress(raw)
    return raw,reference


def load(relative):
    raw,reference=evidence(relative)
    return json.loads(raw),reference


def repo_id(path):
    if path=='/api/plugins/repos/':
        return REPOS
    if path=='/api/plugins/repos/preview/':
        return PREVIEW
    if re.fullmatch(r'/api/plugins/repos/[0-9]+/refresh/',path):
        return REFRESH
    assert re.fullmatch(r'/api/plugins/repos/[0-9]+/',path),path
    return DETAIL


def observation(index,label,path,method,role,status,projection,transport,**notes):
    assert status is not None and 100 <= status <= 599
    assert re.fullmatch(r'[0-9a-f]{64}',projection)
    return {'record_index':index,'label':label,'path':path,'method':method,
            'role':role,'HTTP_status':status,'response_projection_sha256':projection,
            'transport':transport,**notes}


def metadata():
    groups={}
    fixtures={}
    captured,a=load(TRANSPORT+'captured-report.json')
    qualified,b=load(TRANSPORT+'qualification.json')
    clean,c=load(TRANSPORT+'Root-independent-cleanup.json')
    assert qualified['Source_metadata_HTTPS_DNS_capture_qualified'] and clean['passed']
    assert qualified['actual_controller_exit']==0 and qualified['actual_Source_requests']==12
    assert len(captured['observations'])==12
    assert captured['Native_qualified'] is False
    for index,row in enumerate(captured['observations']):
        assert row['observations_complete']
        stage=row['stage']
        request,_=load(TRANSPORT+f'HTTP/{stage}-request.json')
        response,_=load(TRANSPORT+f'HTTP/{stage}-wire.json')
        assert (row['path'],row['method'],row['status'])==(request['path'],request['method'],response['status'])
        groups.setdefault(repo_id(row['path']),[]).append(
            observation(index,stage,row['path'],row['method'],row['credential_role'] or 'anonymous',
                        row['status'],row['wire']['sha256'],'Original_TCP_HTTP',
                        projection_scope='exact retained private response-record digest',
                        fixture_case_source='HTTPS12'))
    fixtures['HTTPS12']={'references':[a,b,c],'actual_controller_exit':0,'retained_responses':12}
    exceptions=[]
    for epoch,count in (('Source24',24),('Source2',2)):
        projected,projection=load(METADATA+f'data/{epoch}-projection.json.gz')
        execution,terminal=load(METADATA+f'lifecycle/{epoch}/execution-report.json.gz')
        assert projected['capture_complete'] and projected['Source_framework_APIClient_only']
        assert not projected['Source_TCP_or_Native'] and not projected['force_authenticate_used']
        assert projected['actual_request_count']==projected['completed_capture_count']==len(projected['requests'])==count
        assert execution['Source_requests']==count and execution['Source_framework_not_TCP']
        assert execution['actual_controller_exit']==0 and execution['all_children_waited_and_pipes_settled']
        fixtures[epoch]={'references':[projection,terminal],'actual_controller_exit':0,
                         'retained_responses':count-int(epoch=='Source24')}
        for row in projected['requests']:
            assert row['capture_complete'] and row['actual_request_invoked']
            identifier=repo_id(row['path'])
            if row['transport']=='framework_exception':
                assert epoch=='Source24' and row['index']==24 and 'status' not in row
                error=row['framework_exception']
                assert error['complete'] and error['type']=='builtins.AttributeError'
                exceptions.append({'fixture_case_source':epoch,'record_index':24,'path':row['path'],
                                   'method':row['method'],'role':row['actor'],
                                   'framework_exception_type':error['type'],
                                   'HTTP_response_returned':False})
                continue
            assert row['transport']=='http_response'
            status=row['status']
            if epoch=='Source2':
                assert status==500 and row['response_exc_info']
            groups.setdefault(identifier,[]).append(
                observation(row['index'],row['name'],row['path'],row['method'],row['actor'],
                            status,row['response_body_sha256'],'Original_Django_APIClient',
                            projection_scope='returned response body',
                            fixture_case_source=epoch,
                            resolved_handler=row['resolver']['class']))
    assert len(exceptions)==1
    assert sum(map(len,groups.values()))==37
    return groups,fixtures,exceptions


def switch():
    report,a=load(SWITCH+'verified-source-application-stream-change-1.json')
    policy,b=load(SWITCH+'stream-change-policy.json')
    driver,c=evidence(SWITCH+'stream_change.py')
    assert a and b['sha256']==report['policy_sha256']
    assert policy['Source']==BASELINE and policy['contract']['route_id']==CHANGE
    assert report['passed'] and not report['fully_verified']
    assert not any(report['cleanup'][key] for key in ('containers','networks','volumes'))
    assert b"'m0-owned-switch-admin',user_level=10," in driver
    assert b"'/proxy/ts/change_stream/'+channels[0]['channel_uuid']" in driver
    names=('method_probe','anonymous_probe','switch')
    facts=[('GET','admin',405),('POST','anonymous',401),('POST','admin',200)]
    leaves=[]
    for index,(key,(method,role,status)) in enumerate(zip(names,facts)):
        row=report[key]
        assert (row['method'],row['authenticated'],row['status'])==(method,role=='admin',status)
        leaves.append(observation(index,key,'/proxy/ts/change_stream/<owned-channel>',
                     method,role,status,row['body_sha256'],'Original_TCP_HTTP',
                     projection_scope='returned body',fixture_case_source='TS-change'))
    return leaves,{'references':[a,b,c],'closed_own_run':True,
                   'path_normalization':'The owned channel identifier is omitted.'}


def socket():
    report,a=load(SOCKET+'verified-source-wire-1.json')
    inside,b=evidence(SOCKET+'source_inside.py')
    opener,c=evidence(str((PACKAGE/'historical_ws_client.py').relative_to(ROOT)))
    assert report['baseline_commit']==BASELINE and report['wire_contracts_passed']
    assert len(report['cases'])==report['case_count']==55
    assert b['sha256']==report['tool_sha256'][b['path']]
    assert c['sha256']==report['tool_sha256'][SOCKET+'ws_client.py']
    assert b"'GET '+path+' HTTP/1.1'" in opener and b"'Upgrade: websocket'" in opener
    assert b"'/ws/?'+urlencode(query)" in inside
    calls=[
        ('auth-absent','anonymous'),('auth-invalid','invalid_credential'),
        ('auth-refresh','admin_refresh_token'),('auth-inactive','inactive'),
        ('auth-missing-user','missing_user'),('auth-apikey-query','admin_apikey_query'),
        ('auth-header-only','admin_header_only'),('auth-wrong-case','admin_wrong_query_key'),
        ('auth-invalid-first','admin_duplicate_token_invalid_first'),
        ('auth-blank-first','admin_duplicate_token_blank_first'),
        ('auth-valid-first','admin_duplicate_token_valid_first'),
        ('role-admin','admin'),('role-standard','standard'),('role-viewer','viewer'),
        ('origin','standard'),('origin','standard'),('origin','standard'),
    ]
    assert b"for role in ['admin','standard','viewer']:" in inside
    tree=ast.parse(inside)
    auth=next(node for node in ast.walk(tree) if isinstance(node,ast.Assign) and
              isinstance(node.targets[0],ast.Name) and node.targets[0].id=='auth_cases')
    definitions=[(el.elts[0].value,el.elts[3].value) for el in auth.value.elts]
    assert definitions==[(label.removeprefix('auth-'), 403 if i<9 else 101)
                         for i,(label,_) in enumerate(calls[:11])]
    leaves=[]
    for index,(label,role) in enumerate(calls):
        row=report['cases'][index]
        assert row['case']==label and row['status']==(403 if index<9 else 101)
        leaves.append(observation(index,label,'/ws/','GET',role,row['status'],
                      digest({'case':label,'status':row['status']}),'Original_Daphne_WebSocket_handshake',
                      projection_scope='handshake status and case only',fixture_case_source='Daphne55'))
    return leaves,{'references':[a,b,c],'retained_handshake_requests':17,
                   'excluded':'Frames, post-handshake state and 38 other top-level observations.',
                   'cleanup_scope':report['cleanup'],'Source_exception_cleanup_passed':False}


def produce(register_path):
    raw=register_path.read_bytes()
    register=json.loads(raw)
    assert register['executable_Source_mapping']['operations']==264
    ids={o['contract_id']:(r,o) for r in register['runtime_routes'] for o in r['operations']}
    grouped,repo_fixture,exceptions=metadata()
    grouped[CHANGE],switch_fixture=switch()
    grouped[WS],socket_fixture=socket()
    assert len(ids)==798 and len(grouped)==6 and sum(map(len,grouped.values()))==57
    fixtures={
        'repository':{'scope':'Returned Original HTTP and APIClient metadata responses','epochs':repo_fixture},
        'switch':{'scope':'Actual Original admin TS switch and auth/method probes',**switch_fixture},
        'socket':{'scope':'Actual Original HTTP upgrade handshakes',**socket_fixture},
    }
    contracts=[]
    for key,leaves in sorted(grouped.items()):
        route,op=ids[key]
        assert not op.get('executable_Source_contract') and op['method'] is None and op['source_action'] is None
        pattern=route['pattern_exact']
        regex=re.sub(r'<int:[^>]+>',r'[0-9]+',re.sub(r'<str:[^>]+>',r'[^/]+',pattern))
        for row in leaves:
            assert re.fullmatch(regex,row['path'].lstrip('/'))
            if 'resolved_handler' in row:
                assert row['resolved_handler']==route['source_handler']
        roles=sorted({row['role'] for row in leaves})
        contracts.append({'contract_id':key,'source_handler':route['source_handler'],'pattern_exact':pattern,
                          'method':None,'source_action':None,'owning_milestone':op['owning_milestone'],
                          'fixture_id':{'Daphne55':'socket','TS-change':'switch'}.get(leaves[0]['fixture_case_source'],'repository'),
                          'observed_methods':sorted({row['method'] for row in leaves}),'observed_roles':roles,
                          'source_role_specs':{role:{'fixture_actor_or_credential_case':role,'complete':False} for role in roles},
                          'assertions':['Bind retained observed route, request method, fixture actor and returned status',
                                        'Keep framework outcomes separate from actual TCP and WebSocket handshakes'],
                          'cases':leaves,'fully_verified':False,
                          'result':{'Source_case_observations':len(leaves),'new_Source_cases_this_task':0,
                                    'Native_coverage_increased':False}})
    return {'binding_kind':KIND,'scope':'Retained returned Original statuses only',
            'Original_author_credit':'Original Dispatcharr authors retain application and algorithm credit.',
            'based_on_register_sha256':sha(raw),'operation_count':6,'unique_Source_cases':57,
            'new_Source_executions':0,'fully_verified_operations':0,'Native_coverage_increased':False,
            'fixtures':fixtures,'excluded_non_HTTP_outcome':exceptions,'contracts':contracts}


if __name__=='__main__':
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--register',type=Path,required=True)
    cli.add_argument('--output',type=Path,required=True)
    args=cli.parse_args()
    args.output.write_text(json.dumps(produce(args.register),indent=2)+'\n')
