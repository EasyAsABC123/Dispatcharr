"""Validate physically retained, exact literal Original residual HTTP assignments."""
import hashlib
import json
import re

KIND='retained_original_residual_frozen_HTTP'
FOLDER='scripts/rust-contracts/retained-residual-source/'
MANIFEST='scripts/rust-contracts/executable-source-inputs.json'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REACT='m0-original-packaged-react-root-1'
HISTORY={'m0-original-admin-history-1','m0-original-admin-history-2'}
REVIEWED_WAIT={
    'm0-drf-api-roots-source-1':('drf-api-roots','7b28444fc2885468d239a81c82609bde7bc8ce6b314b6b5cdb633ee3644ca8b1'),
    'm0-channels-streams-format-source-1':('channels-streams-original','7540eac78d1b529ecb41acb30b8d04d2ace72564d062d8b5a682e800e0256a47'),
}
CREDIT='Original Dispatcharr, Django, Django REST Framework, and dependency authors retain full application and behavior credit.'
METHODS={'GET','POST','PUT','PATCH','DELETE'}
SCOPES={
    'm0-drf-api-roots-source-1':'Exact API roots and owned role-selected links',
    'm0-channels-streams-format-source-1':'Formatted channels and streams role responses and scoped SQL, schedules and queues',
    'm0-original-direct-redirects-1':'Exact Original direct redirect status and Location with no redirect follow',
    'm0-original-admin-history-1':'Exact Django admin history, real persisted entry, empty and missing object controls',
    'm0-original-admin-history-2':'Exact Django admin history, visible persisted entry, empty and missing object controls',
    'm0-original-direct-redirects-2':'Exact Original direct redirect and separately verified unnamed resolver view',
    'm0-admin-documentation-reads-source-2':'Exact remaining Django admin catch-all and GET logout role guards',
    'm0-channel-group-profile-source-1':'Selected Original channel group and profile reads and scoped mutations',
    'm0-owned-dvr-detail-source-1':'Selected Recording and recurring DVR detail reads and scoped mutations',
    'm0-connect-format-residual-source-1':'Formatted Connect response and scoped SQL, schedules and queues',
    'm0-core-settings-format-source-1':'Formatted Core setting result and scoped cache/SQL changes',
    'm0-accounts-format-residual-source-1':'Selected Accounts formatted actions and owned state effects',
    'm0-provider-guide-residual-source-1':'Selected provider/guide methods, owned SQL and actual broker scope',
    'm0-local-operation-residue-source-1':'Selected Plugin, series and VOD operations with unchanged owned state',
    'm0-admin-documentation-reads-source-1':'Selected Django admin and schema reads with literal responses',
    REACT:'Packaged default bare React index and exact shipped asset responses',
    'm0-next-stream-idle-source-1':'Owned idle channel whose start/stop and upstream state stays untouched',
}


def dependencies(root):
    manifest=json.loads((root/MANIFEST).read_bytes())
    result={item['path'] for item in manifest['inputs'] if item['kind']==KIND}
    assert all(re.fullmatch(FOLDER+r'capture-[a-f0-9]{64}\.json',path) for path in result)
    return result


def hex64(value):
    return isinstance(value,str) and re.fullmatch('[a-f0-9]{64}',value) is not None


def path_matches(template,path):
    assert isinstance(template,str) and len(template)<=512 and template.startswith('/')
    assert isinstance(path,str) and len(path)<=512 and path.startswith('/')
    pattern=re.escape(template)
    for field in set(re.findall(r'{([a-z_]+)}',template)):
        replacements={'channel_uuid':r'[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}'}
        kind=replacements.get(field,r'[1-9][0-9]*')
        pattern=pattern.replace(re.escape('{'+field+'}'),kind)
    return re.fullmatch(pattern,path) is not None


def checked_case(row,role,status,method,path):
    assert row['method']==method and row['path']==path and row['role']==role
    assert row['actual_status']==status and type(row['actual_status']) is int
    assert 100<=status<=599 and type(row['actual_body_bytes']) is int and row['actual_body_bytes']>=0
    assert hex64(row['actual_body_sha256']) and isinstance(row['actual_content_type'],str)
    assert len(row['actual_content_type'])<=128
    if row['actual_body_bytes']==0:
        assert row['actual_body_sha256']==hashlib.sha256(b'').hexdigest()
    if status==500:
        assert row['actual_content_type'].startswith('text/html')
    if 'actual_location' in row:
        assert row['actual_location'] is None or (isinstance(row['actual_location'],str) and
                  len(row['actual_location'])<=512 and '\r' not in row['actual_location'] and '\n' not in row['actual_location'])


def materialize(captures):
    """Derive register rows exclusively from the minimal reviewed sanitization."""
    fixtures={}
    contracts=[]
    selected=set()
    for capture in captures:
        stage=capture['stage']
        assert stage in SCOPES and (capture['original_revision'],capture['immutable_source_image'])==(ORIGINAL,IMAGE)
        assert capture['source_archive_sha256']==(None if stage==REACT else ARCHIVE)
        assert hex64(capture['freeze_sha256'])
        if 'reviewed_outer_wait' in capture:
            assert stage in REVIEWED_WAIT and 'outer_execution_sha256' not in capture
            label, controller_sha = REVIEWED_WAIT[stage]
            custody = capture['reviewed_outer_wait']
            assert set(custody)=={'mode','stage_label','actual_exit','controller_sha256',
                                  'controller_state_sha256','separate_outer_EOF_process_group_witness'}
            assert (custody['mode'],custody['stage_label'],custody['controller_sha256'])==(
                'reviewed_hardcoded_subprocess.run',label,controller_sha)
            assert type(custody['actual_exit']) is int and custody['actual_exit']==capture['whole_launcher_exit']==0
            assert hex64(custody['controller_state_sha256']) and custody['separate_outer_EOF_process_group_witness'] is False
            outer_identity=custody['controller_state_sha256']
        else:
            assert hex64(capture['outer_execution_sha256'])
            outer_identity=capture['outer_execution_sha256']
        assert re.fullmatch('[a-f0-9]{40}',capture['scope_base'])
        assert capture['exact_owned_cleanup'] is True
        assert capture['retired_owned_roles']==(['original'] if stage==REACT else ['baseline','origin'])
        states=capture['complete_terminal_states']
        assert type(states) is dict and states and set(states.values())<={'passed','failed','blocked','not_run'}
        assert capture['whole_launcher_exit']==(0 if all(value=='passed' for value in states.values()) else 1)
        assert list(capture['phase_exits'])==['startup','probe','cleanup']
        assert all(value=={'actual_exit':capture['whole_launcher_exit'] if name=='probe' else 0,
                          'EOF_and_process_group_settled':True} for name,value in capture['phase_exits'].items())
        files={'receipt.json','probe.stdout','project-before.json','independent-cleanup.json',
               'owned-role.json' if stage==REACT else 'owned-roles.json',
               'startup-execution.json','probe-execution.json','cleanup-execution.json'}
        assert set(capture['raw_fingerprints'])==files and all(hex64(x) for x in capture['raw_fingerprints'].values())
        operations=capture['operations']
        omitted=set(capture['already_mapped_passed_operations'])
        mapped={op['name'] for op in operations}
        assert operations and len(operations)==len(mapped) and not mapped & omitted
        assert mapped | omitted=={name for name,state in states.items() if state=='passed'}
        fixture='Original-residual-'+stage+'-'+outer_identity
        assert fixture not in fixtures
        fixtures[fixture]={'id':fixture,'description':SCOPES[stage],
            **{key:value for key,value in capture.items() if key!='operations'}}
        for op in operations:
            expected=op['expected_cases']
            roles=list(expected)
            role_set={'anonymous'} if stage==REACT else {'anonymous','viewer','standard','admin'}
            if stage in HISTORY:
                role_set.add('restricted_staff')
            assert set(roles)==role_set and len(roles)==len(op['cases'])
            assert roles==[row['role'] for row in op['cases']]
            method=op['observed_method']
            declared=op['declared_method']
            assert method in METHODS and declared in (None,method)
            assert path_matches(op['path_template'],op['actual_path'])
            resolver=op['resolver']
            assert set(resolver)=={'ordinal','pattern','handler','action','view_name'} and type(resolver['ordinal']) is int
            if declared is None:
                assert resolver['action'] is None
            for role,row in zip(roles,op['cases'],strict=True):
                checked_case(row,role,expected[role],method,op['actual_path'])
            effects=op['owned_effects']
            controls=op['additional_controls']
            if stage==REACT:
                assert method=='GET' and op['actual_path']=='/' and list(expected.values())==[200]
                assert hex64(effects['stock_index_sha256']) and effects['stock_index_sha256']==op['cases'][0]['actual_body_sha256']
                assets=effects['asset_matches']
                assert 1<=len(assets)<=8 and any(asset['kind']=='module' for asset in assets)
                assert all(asset['same_stock_file'] is True and asset['actual_status']==200 and hex64(asset['actual_sha256'])
                           for asset in assets)
            else:
                assert hex64(effects['complete_raw_proof_sha256'])
            assert set(controls)==({'empty_existing','missing'} if stage in HISTORY else set())
            for label,status in (('empty_existing',200),('missing',302)):
                if label in controls:
                    check=controls[label]
                    assert path_matches(op['path_template'],check['path'])
                    checked_case(check,'admin',status,method,check['path'])
            key=op['contract_id']
            assert key not in selected and re.fullmatch(r'route-[a-f0-9]{20}:(?:GET|POST|PUT|PATCH|DELETE|METHOD_UNRESOLVED)',key)
            selected.add(key)
            assert (op['owning_milestone'] in {'M'+str(i) for i in range(12)})
            contracts.append({'contract_id':key,'fixture_id':fixture,'resolver_ordinal':resolver['ordinal'],
                'resolver_name':resolver['view_name'],'pattern_exact':resolver['pattern'],'source_handler':resolver['handler'],
                'owning_milestone':op['owning_milestone'],'method':declared,'source_action':resolver['action'],
                'observed_methods':[method],'observed_roles':roles,
                'source_role_specs':{role:{'fixture_role_or_supplied_credential':role,'complete':False} for role in roles},
                'assertions':[SCOPES[stage],'Independent scoped assertions completed against the sealed Original fixture.',
                              'Owned namespace, exact container identity and process generation retired.'],
                'result':{'state':'partial_exact_Original_observations','Source_case_observations':len(roles),
                          'new_Source_cases_this_task':0,'Native_coverage_increased':False,
                          'known_Original_legacy_error_cells':list(expected.values()).count(500)},
                'fully_verified':False,'cases':op['cases'],'additional_Original_controls':controls,
                'owned_state':effects,'literal_path_template':op['path_template']})
    assert len(contracts)==len(selected) and contracts
    return fixtures,contracts


def build(captures):
    fixtures,contracts=materialize(captures)
    return {'format_version':1,'binding_kind':KIND,'credit':CREDIT,'captures':captures,
            'fixtures':fixtures,'contracts':contracts,'operation_count':len(contracts),
            'fully_verified_operations':0,'Native_coverage_increased':False,'performance_verified':False}


def validate(root,value):
    assert value==build(value['captures']),'M0 physically retained Original residual projection mismatch'
