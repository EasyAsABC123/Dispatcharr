"""Join three retained Original HTTP cohorts to existing route operations.

Original Dispatcharr and dependency authors retain application and algorithm credit.
"""
import ast
import collections
import hashlib
import json
import re
from pathlib import Path
from urllib.parse import urlencode, urlsplit

from source_contracts import checked, digest

KIND = 'retained_original_provider_channel_vod_HTTP'
INPUT = 'scripts/rust-contracts/retained-provider-channel-vod/bindings.json'
INPUT_SHA256 = '1fb9dae1ab5ec1b6a90226491f071947dad34119013c703fdfc9d44456bcbb9a'
M3U = 'Original-Image22-provider-reads'
CHANNEL = 'Original-channel-producer-TCP-98'
VOD = 'Original-VOD-wire-2-suffix'
IMAGE22 = 'scripts/rust-integration/verified-integration-m5-auth-image22-1.json'
IMAGE22_SHA256 = 'e7cb41ab0e72d66c705d254ebd416f3098dd2c4ed797305dcd012883fd0fc4e8'
M3U_RAW = 'artifacts/rust/m5-auth-full-integration-image22-2/integration.json'
M3U_RAW_SHA256 = 'bc1b852bd233b03bff581d52349a9ee1d7e27e96664885f86e2be4b3c96c99ae'
CHANNEL_ROOT = 'scripts/rust-milestone-batches/m6/channel-http-source-v1/'
CHANNEL_SUMMARY = CHANNEL_ROOT + 'summary.json'
CHANNEL_SUMMARY_SHA256 = '26ad5e53e8324d700f453f064d0dcff0b0f534955a3903174f113cf43ab58889'
CHANNEL_RAW = CHANNEL_ROOT + 'actual/source-report.json'
CHANNEL_RAW_SHA256 = '703ce237661a04f19409c24dd059810f13b764a58ad673408b3e381522389b55'
CHANNEL_PATHS = CHANNEL_ROOT + 'driver/tools/cases.py'
CHANNEL_PATHS_SHA256 = '3a7102875195403dcc01c6b64db32270f109fc86e99d6b75617ce28d6de40011'
CHANNEL_REQUEST = CHANNEL_ROOT + 'driver/tools/guest.py'
CHANNEL_REQUEST_SHA256 = 'c8cd4145bcab89b1f27dbf5845287d0791a2a01669886ada61f1d8f88a30b3e8'
VOD_RAW = 'scripts/rust-vod-api-contracts/source-vod-http-wire-2.json'
VOD_RAW_SHA256 = '82b35c4e6777aa26b6211f36639a516a3dbb42d776c3d1ba6501bb743a3bde33'
VOD_INDICES = (23,24,66,67,109,110)
PROVIDER_ROLES = ('admin','standard','viewer','inactive','None')


def receipt(root, name, wanted):
    return checked(Path(root), {'path':name, 'sha256':wanted})


def dependencies():
    return {INPUT,IMAGE22,CHANNEL_SUMMARY,CHANNEL_RAW,CHANNEL_PATHS,CHANNEL_REQUEST,VOD_RAW}


def source_references():
    return {M3U:{'path':IMAGE22,'sha256':IMAGE22_SHA256},
            CHANNEL:{'path':CHANNEL_RAW,'sha256':CHANNEL_RAW_SHA256},
            VOD:{'path':VOD_RAW,'sha256':VOD_RAW_SHA256}}


def check_bytes(root, path, wanted):
    raw = (Path(root)/path).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == wanted
    return raw


def channel_routes(root):
    tree=ast.parse(check_bytes(root, CHANNEL_PATHS, CHANNEL_PATHS_SHA256))
    routes=next(ast.literal_eval(node.value) for node in tree.body
                if isinstance(node,ast.Assign) and any(isinstance(term,ast.Name) and term.id=='ROUTES'
                                                       for term in node.targets))
    driver=check_bytes(root, CHANNEL_REQUEST, CHANNEL_REQUEST_SHA256)
    assert b"obs.request('/api/channels/channels/'+ROUTES[spec['route']][0]" in driver
    return routes


def provider_cases(run):
    # Frozen ordered roots: the first two admin account detail IDs identify
    # the main provider and the separate XC account used in wrong-parent cases.
    capture = run['checks']
    prefixes = [capture[i]['name'] for i in (656,657,670,686)]
    assert [value.rsplit(':',1)[0] for value in prefixes] == [
        'provider-read:accounts:admin:detail','provider-read:accounts:admin:detail',
        'provider-read:profiles:admin:detail','provider-read:filters:admin:detail']
    provider, xc, first_profile, first_filter = [value.rsplit(':',1)[1] for value in prefixes]
    assert all(value.isascii() and value.isdecimal() for value in (provider,xc,first_profile,first_filter))
    roots = {'accounts':'/api/m3u/accounts/',
             'profiles':f'/api/m3u/accounts/{provider}/profiles/',
             'filters':f'/api/m3u/accounts/{provider}/filters/',
             'server-groups':'/api/m3u/server-groups/'}
    for index in [*range(655,711),*range(739,745)]:
        observed = capture[index]
        prefix, family, label=observed['name'].split(':',2)
        assert prefix=='provider-read' and family in roots and observed['passed']
        role, path = 'admin', roots[family]
        if label in PROVIDER_ROLES:
            role='anonymous' if label=='None' else label
        elif ':detail:' in label:
            role, suffix=label.split(':detail:')
            assert role in ('admin','standard') and suffix.isascii() and suffix.isdecimal()
            path += suffix + '/'
        elif label in ('pagination-ignored','filters-ignored'):
            params = {'page':'bad','page_size':'1'} if label=='pagination-ignored' else {
                'search':'no-such-row','name':'missing','is_active':'false','ordering':'-id','epg_source':'2147483647'}
            path += '?' + urlencode(params)
        elif label == 'missing-detail':
            path += '2147483647/'
        elif label == 'big-integer-missing-detail':
            path += '9'*100 + '/'
        elif label == 'malformed-detail':
            path += 'not-a-number/'
        elif label in ('missing-parent-list','big-integer-parent-list'):
            parent = '2147483647' if label=='missing-parent-list' else '9'*100
            assert family in ('profiles','filters')
            path = f'/api/m3u/accounts/{parent}/{family}/'
        else:
            assert label=='wrong-parent-detail' and family in ('profiles','filters')
            first_id = first_profile if family=='profiles' else first_filter
            path=f'/api/m3u/accounts/{xc}/{family}/{first_id}/'
        fields=observed['details']
        assert fields['python_status']==fields['rust_status'] and fields['python_status'] in (200,401,403,404)
        assert fields['python_body_sha256']==fields['rust_body_sha256']
        yield {'source_index':index,'label':observed['name'],'path':path,'method':'GET','role':role,
               'HTTP_status':fields['python_status'],'response_sha256':fields['python_body_sha256'],
               'record_sha256':digest(observed)}


def channel_cases(root, source):
    routes=channel_routes(root)
    assert source['passed'] and source['Source_API_calls']==source['actual_TCP_HTTP_attempts']==98
    assert source['Source_APIClient_calls']==source['Source_task_body_calls']==0
    for index, row in enumerate(source['cases']):
        spec = row['definition']
        key = spec['route']
        if spec['method']!='POST' or key=='logos':
            continue
        assert key in ('names','tvg','bulk')
        assert row['credential_role']==spec['credential_role']
        assert row['status']==spec['expected_status'] and row['absolute_HTTP']['timer_joined']
        physical=row['actual_physical_HTTP_close']
        assert physical['fd_after']==-1 and physical['original_inode_absent']
        assert len(row['new_family_frames'])==spec['expected_new_family_frames']
        yield {'source_index':index,'label':spec['name'],
               'path':'/api/channels/channels/'+routes[key][0],'method':'POST',
               'role':row['credential_role'],'HTTP_status':row['status'],'response_sha256':row['raw_body_sha256'],
               'published_family_frame_count':len(row['new_family_frames']),'record_sha256':digest(row)}


def vod_cases(source):
    assert source['additional_cases']==len(source['cases'])==129
    assert source['baseline_cases']==463 and source['provider_refresh_rows_unchanged']
    assert not source['provider_trap_attempts']
    for index in VOD_INDICES:
        row=source['cases'][index]
        assert row['method']=='GET' and row['role']=='standard' and row['expected']['status']==200
        assert re.fullmatch(r'/api/vod/(movies|series|episodes)\.json/?',row['path'])
        yield {'source_index':index,'label':row['path'],'path':row['path'],'method':'GET',
               'role':row['role'],'HTTP_status':row['expected']['status'],
               'response_sha256':row['expected']['body_sha256'],'record_sha256':digest(row)}


def validate(root, value):
    root=Path(root)
    assert value==receipt(root,INPUT,INPUT_SHA256)
    assert value['binding_kind']==KIND and len(value['contracts'])==value['operation_count']==14
    assert value['new_Source_executions']==value['fully_verified_operations']==0
    assert value['Native_coverage_increased'] is False
    observed=collections.defaultdict(dict)
    counts=collections.Counter()
    for binding in value['contracts']:
        assert binding['fully_verified'] is False
        name=binding['fixture_id']
        counts[name]+=1
        route=re.compile(binding['pattern_exact'].replace('/^','/'))
        result=binding['result']
        assert result['Source_case_observations']==len(binding['cases'])
        assert result['new_Source_cases_this_task']==0
        assert result['Native_coverage_increased'] is result['complete_role_matrix'] is False
        assert binding['observed_roles']==sorted({case['role'] for case in binding['cases']})
        assert binding['source_role_specs']=={role:value['fixtures'][name]['roles'][role]
                                              for role in binding['observed_roles']}
        for row in binding['cases']:
            assert row['method']==binding['method']
            assert route.fullmatch(urlsplit(row['path']).path.lstrip('/'))
            assert binding['contract_id'].endswith(':'+row['method'])
            assert row['source_index'] not in observed[name]
            observed[name][row['source_index']]=row
    assert counts=={M3U:8,CHANNEL:3,VOD:3}
    assert {k:len(v) for k,v in observed.items()}=={M3U:62,CHANNEL:64,VOD:6}
    assert sum(len(r) for r in observed.values())==value['unique_Source_cases']==132
    image=receipt(root,IMAGE22,IMAGE22_SHA256)
    matched,=(run for run in image['runs'] if run['report']==M3U_RAW)
    assert image['passed'] and matched['sha256']==M3U_RAW_SHA256 and 'provider_reads' in matched['contract_suites']
    assert image['runs'][0]['cleanup']=='project removed'
    assert set(observed[M3U])==set([*range(655,711),*range(739,745)])
    summary=receipt(root,CHANNEL_SUMMARY,CHANNEL_SUMMARY_SHA256)
    assert summary['passed'] and summary['source_report_sha256']==CHANNEL_RAW_SHA256
    assert summary['Source_confirmed_TCP_HTTP_calls']==98 and summary['Source_task_body_calls']==0
    assert summary['owner_actual_exit']==summary['web_role_normal_exit']==0
    assert observed[CHANNEL]=={row['source_index']:row for row in channel_cases(root,receipt(root,CHANNEL_RAW,CHANNEL_RAW_SHA256))}
    assert observed[VOD]=={row['source_index']:row for row in vod_cases(receipt(root,VOD_RAW,VOD_RAW_SHA256))}
    for fixture_id, ref in source_references().items():
        assert value['fixtures'][fixture_id]['source_reference']==ref


def restore_pre_provider_channel_vod(root, rows):
    """Remove only this cohort when verifying older register snapshots."""
    value=receipt(root,INPUT,INPUT_SHA256)
    ops={op['contract_id']:op for route in rows for op in route['operations']}
    for contract in value['contracts']:
        op=ops[contract['contract_id']]
        assert op.pop('executable_Source_contract')==contract
        op.update(setup_fixture=None, assertions=[], result=None,
                  coverage_state='scoped_evidence_needs_case_mapping' if op['related_evidence'] else 'not_yet_mapped')
        for key in ('observed_fixture_definitions','complete','observed_roles'):
            op['role_matrix'].pop(key)
        op['role_matrix']['state']='unmapped'


def verify_private(root):
    root=Path(root)
    binding=receipt(root,INPUT,INPUT_SHA256)
    validate(root,binding)
    run=receipt(root,M3U_RAW,M3U_RAW_SHA256)
    assert run['passed'] and run['cleanup']=='project removed' and not run['resources_retained']
    source=binding['fixtures'][M3U]
    for path, expected in source['tool_sha256'].items():
        assert run['tool_source_sha256'][path]==expected
        check_bytes(root,source['tool_source']+'/'+path,expected)
    assert list(provider_cases(run)) == sorted(
        [row for definition in binding['contracts'] if definition['fixture_id']==M3U for row in definition['cases']],
        key=lambda row:row['source_index'])
    return {'Original_HTTP_cases_verified':binding['unique_Source_cases'],'new_application_runs':0}


if __name__=='__main__':
    print(json.dumps(verify_private(Path(__file__).resolve().parents[2])))
