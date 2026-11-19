"""Project exact retained Original DVR and XC requests without fixture replay."""
import argparse
import collections
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import quote

BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
KIND='retained_original_DVR_XC_playback_HTTP'
DVR='scripts/rust-milestone-batches/m478/batch8-recording/attempt6/'
COMSKIP='scripts/rust-integration/comskip-positive-lifecycle-source1/'
XC='scripts/rust-xc-contracts/'
XC_REPORT='artifacts/rust/m7-xc-live-native-2/xc-source.json'
XC_SHA='d6e4d94766f3588c7a00d0d7afe9147c4cf223d82aee1233eec0660dfe2edfde'
DVR_HLS='route-0b392188d7e4e96171af:GET'
DVR_DETAIL='route-b406c782172baf90cd51:GET'
COMSKIP_POST='route-f3532fc9c5a320a9d392:POST'
XC_LONG='route-9075d4e584abb111bb98:METHOD_UNRESOLVED'
XC_SHORT='route-203e37771a6f3e3d4056:METHOD_UNRESOLVED'


def sha(data):
    return hashlib.sha256(data).hexdigest()


def digest(value):
    return sha(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())


def load(root,path):
    raw=(root/path).read_bytes()
    return json.loads(raw),{'path':path,'sha256':sha(raw)}


def request(index,label,method,path,role,status,projection,content_scope,**details):
    assert type(status) is int and 100<=status<=599
    return {'source_index':index,'label':label,'path':path,'method':method,'role':role,
            'HTTP_status':status,'response_projection_sha256':projection,
            'response_hash_scope':content_scope,**details}


def dvr(root,private):
    execution,exit_ref=load(root,DVR+'execution.json')
    report,report_ref=load(root,DVR+'report.json')
    cleanup,cleanup_ref=load(root,DVR+'Original/cleanup.json')
    auth,auth_ref=load(root,DVR+'Original/authorization.json')
    playback,playback_ref=load(root,DVR+'Original/active-playback.json')
    assert execution['actual_exit']==1 and execution['failure'] is None
    assert all(execution[field] is True for field in ('process_group_settled','stdout_EOF','stderr_EOF'))
    assert report['cleanup'][0]==cleanup
    assert not cleanup['errors'] and cleanup['exact_owned_absence']
    assert cleanup['all_original_process_generations_absent'] and cleanup['host_CLI_groups_settled']
    assert report['backends'][0]['status']=='failed' and not report['scoped_workflow_passed']
    assert auth['passed'] and playback['executed_slice_passed'] and not playback['full_playback_passed']
    plan='artifacts/rust/m3-normal-recording-http-driver-1/cohort/playback_cases.py'
    dispatch_path='artifacts/rust/run-recording-whole-batch6.py'
    binding_path='artifacts/rust/normal-recording-whole-batch-1/binding5.json'
    dispatch=(private/dispatch_path).read_bytes()
    assert sha(dispatch)=='e176be60e145e4c68b260ad5a6705d6cca2ba419be666a8b77453da7e334ff62'
    assert binding_path.encode() in dispatch and b'/results6' in dispatch
    binding,binding_ref=load(private,binding_path)
    assert binding_ref['sha256']=='dbcec78b823046156f2b4c4eb86fab3f1ce9e2634ab4350ae43f6e1b70dd33d9'
    driver=(private/plan).read_bytes()
    driver_reference,=[row for row in binding['inputs'] if row['path'].endswith('/'+plan)]
    assert sha(driver)==driver_reference['sha256']=='b1f538d1cd23bcf28ed88ce58d658eee1ad967063b4603b0b0bde3666b53f3df'
    assert b"query={'token':fixture.admin}if token else None" in driver
    assert b"playlist_request('hls-query-token',token=True)" in driver
    hls=[]
    for i,row in enumerate(playback['actual_cells']):
        if not re.fullmatch(r'/api/channels/recordings/1/hls/.+',row.get('path','')):
            continue
        assert row['method']=='GET' and row['status']=='passed'
        kind= 'admin_query_token' if row['query_credentials_present'] else 'admin_authorization'
        if row['cell']=='hls-query-token':
            assert row['query_credentials_present'] and row['actual_query_propagation']
            assert row['credential_role'] is None and not row['authorization_present']
        else:
            assert row['credential_role']=='admin' and row['authorization_present']
        assert not row['api_key_present']
        hls.append(request(i,row['cell'],row['method'],row['path'],'admin',row['actual_status'],
                           row['body_sha256'],'actual body digest',credential_mode=kind))
    assert len(hls)==8
    i=8
    row=auth['actual'][i]
    assert (row['method'],row['path'],row['credential_role'],row['status'],row['expected_status'])==(
        'GET','/api/channels/recordings/2/','manager',404,404)
    detail=[request(i,'manager-hidden-recording',row['method'],row['path'],row['credential_role'],
                    row['status'],digest(row['body']),'actual returned JSON projection')]
    fixtures={
        'DVR-HLS':{'references':[exit_ref,report_ref,cleanup_ref,playback_ref,binding_ref,{'path':dispatch_path,'sha256':sha(dispatch)},
                                  {'path':plan,'sha256':sha(driver)}],
                   'outer_controller_exit':1,'exact_Original_cleanup':True,
                   'selected_HLS_probe_reported_pass':True},
        'DVR-authorization':{'references':[exit_ref,report_ref,cleanup_ref,auth_ref],
                             'outer_controller_exit':1,'exact_Original_cleanup':True,
                             'selected_authorization_probe_reported_pass':True},
    }
    return {DVR_HLS:('DVR-HLS',hls),DVR_DETAIL:('DVR-authorization',detail)},fixtures


def comskip(root):
    index,index_ref=load(root,COMSKIP+'index.json')
    proof,proof_ref=load(root,COMSKIP+'qualification.json')
    assert index['Source_actual_business_leaves']==proof['actual_Source_business_leaves']==6
    assert index['literal_controller_exits']==proof['literal_controller_exits']==[1,1,1,1,0]
    assert proof['manual_mark_repeat_cut_repeat_qualified']
    assert not proof['manual_four_global_producer_graph_qualified']
    retained={entry['path']:entry['sha256'] for entry in index['files']}
    leaves=[]
    references=[index_ref,proof_ref]
    for i,label in enumerate(('positive-mark','mark-repeat','positive-cut','cut-repeat')):
        file=f'cells/{label}/HTTP.json'
        row,ref=load(root,COMSKIP+file)
        assert ref['sha256']==retained[file]
        assert row['method']=='POST' and row['credential_role']=='admin' and row['status']==200
        assert row['path']==f'/api/channels/recordings/{1+(i>=2)}/comskip/'
        references.append(ref)
        leaves.append(request(i,label,row['method'],row['path'],row['credential_role'],row['status'],
                              digest(row['body']),'actual returned JSON projection'))
    return {COMSKIP_POST:('Original-manual-Comskip',leaves)},{
        'Original-manual-Comskip':{'references':references,'narrow_manual_business_leaves':True,
                                   'literal_controller_exits':[1,1,1],'global_producer_graph_qualified':False}}


def xc(root,private):
    receipt,receipt_ref=load(root,XC+'verified-xc-live-host-1.json')
    capture,capture_ref=load(private,XC_REPORT)
    assert receipt['artifact']==XC_REPORT and capture_ref['sha256']==XC_SHA
    assert receipt['passed'] and capture['passed'] and not capture['resources_retained']
    assert receipt['http_cases']==len(capture['cases'])==len(capture['actual'])==36
    assert receipt['baseline_commit']==capture['baseline_commit']==BASE
    assert receipt['baseline_image']==capture['baseline_image']==IMAGE
    assert capture['cleanup']==receipt['cleanup']=='isolated Compose project removed'
    assert capture['differences']==[] and receipt['differences']==0
    assert capture['native_exit_code']==receipt['native_exit_code']==0
    tool=XC+'live_source.py'
    tool_digest=sha((root/tool).read_bytes())
    assert tool_digest==capture['tool_sha256'][tool]
    leaves=collections.defaultdict(list)
    for i,row in enumerate(capture['cases']):
        assert row==capture['actual'][i]
        assert isinstance(row['channel_input'],str) and len(row['channel_input'])<120
        assert row['path_shape'] in ('/live/<user>/<pass>/<id>','/<user>/<pass>/<id>')
        identifier=XC_LONG if row['path_shape'].startswith('/live/') else XC_SHORT
        path=row['path_shape'].replace('<id>',quote(row['channel_input'],safe=''))
        leaves[identifier].append(request(i,row['case'],row['method'],path,row['role'],row['status'],
                                    digest(row),'retained response projection with normalized Allow order',
                                    fixture_role_is_auth_success=False,
                                    credential_path_components_omitted=True))
    assert tuple(map(len,(leaves[XC_LONG],leaves[XC_SHORT])))==(34,2)
    return {key:('XC-live-entry-paired',rows) for key,rows in leaves.items()},{
        'XC-live-entry-paired':{'references':[receipt_ref,{'path':tool,'sha256':tool_digest},capture_ref],
                               'private_capture_path':XC_REPORT,'retained_Original_HTTP_rows':36,
                               'Source_cleanup_verified':True,'positive_media_qualified':False}}


def produce(root,private,register):
    raw=register.read_bytes()
    prior=json.loads(raw)
    assert prior['executable_Source_mapping']['operations']==373
    routes={op['contract_id']:(route,op) for route in prior['runtime_routes'] for op in route['operations']}
    packets=[dvr(root,private),comskip(root),xc(root,private)]
    cohorts={}
    contracts=[]
    for packet,fixtures in packets:
        cohorts.update(fixtures)
        for identifier,(fixture,cases) in packet.items():
            route,op=routes[identifier]
            assert not op.get('executable_Source_contract') and not op['fully_verified']
            pattern=route['pattern_exact'].replace('/^','/')
            regex=re.sub(r'<(?:int|str|path):[^>]+>',r'[^/]+',pattern)
            # Django's path converter covers nested HLS file components.
            if identifier==DVR_HLS:
                regex=re.sub(r'<path:[^>]+>',r'.+',pattern)
                regex=re.sub(r'<(?:int|str):[^>]+>',r'[^/]+',regex)
            for row in cases:
                assert re.fullmatch(regex,row['path'].lstrip('/'))
                assert op['method'] is None or op['method']==row['method']
            roles=sorted({row['role'] for row in cases})
            contracts.append({
                'contract_id':identifier,'pattern_exact':route['pattern_exact'],
                'source_handler':route['source_handler'],
                **{name:op[name] for name in ('method','source_action','owning_milestone')},
                'fixture_id':fixture,'observed_roles':roles,
                'observed_methods':sorted({row['method'] for row in cases}),
                'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
                'fully_verified':False,'cases':cases,
                'assertions':['Match literal retained target family, method, fixture role and actual response',
                              'Keep viewer behavior and controller settlement evidence separate'],
                'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                          'Native_coverage_increased':False},
            })
    assert len(contracts)==5 and sum(len(item['cases']) for item in contracts)==49
    return {'binding_kind':KIND,'scope':'Retained Original DVR and XC HTTP observations',
            'Origin_credit':'All application and algorithm credit remains with original Dispatcharr authors.',
            'based_on_register_sha256':sha(raw),'operation_count':5,'unique_Source_cases':49,
            'new_Source_executions':0,'fully_verified_operations':0,'Native_coverage_increased':False,
            'fixtures':cohorts,'contracts':sorted(contracts,key=lambda entry:entry['contract_id'])}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo',type=Path,required=True)
    parser.add_argument('--private-root',type=Path,required=True)
    parser.add_argument('--register',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    result=produce(args.repo,args.private_root,args.register)
    args.output.write_text(json.dumps(result,indent=2)+'\n')
