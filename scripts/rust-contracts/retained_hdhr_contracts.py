"""Bind retained Source HDHomeRun paths to existing register operations.

Original Dispatcharr and dependency authors retain application and algorithm credit.
"""
import ast
import collections
import json
import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

from source_contracts import checked, digest

KIND = 'retained_original_HDHR_Image27_TCP'
INPUT = 'scripts/rust-contracts/retained-hdhr/bindings.json'
INPUT_SHA256 = '9c77f36ed60e1ab57c33e27794223215cb956fe09196184acda1be7833806ac4'
PUBLIC = 'scripts/rust-hdhr-contracts/verified-hdhr-image27-1.json'
PUBLIC_SHA256 = '182c397aa39f5262697a98bb14f7a2950e1bfd7010526bda866c528e6281d1de'
RAW = 'artifacts/rust/m7-hdhr-image27-1/hdhr-image.json'
RAW_SHA256 = '2e25ff267dccc33e7c211496238732fe3696941d93e2fe01645440652baa12a5'
PROBE = 'scripts/rust-hdhr-contracts/probe.py'
PROBE_SHA256 = '111963af9cfac17ba4803ca9efc8eabf8858d3dde05d1bb0f4e051f26502b002'
FIXTURE = 'retained-Original-Image27-HDHomeRun'


def dependencies():
    return {INPUT, PUBLIC, PROBE}


def read(root, path, sha):
    return checked(Path(root), {'path':path,'sha256':sha})


def route_regex(pattern):
    pattern=pattern.replace('/^','/').removeprefix('^')
    pattern=re.sub(r'<str:[^>]+>',r'[^/]+',pattern)
    return re.compile(re.sub(r'<int:[^>]+>',r'[0-9]+',pattern))


def execute_request_plan(source, probe_bytes):
    """Recover fixture actor labels from the exact captured request driver."""
    expected=source['expected']
    def path_param(index, position):
        return unquote(urlsplit(expected[index]['path']).path.split('/')[position])
    profiles=[path_param(i,2) for i in (12,15,18,21,24,27)]
    assert profiles[-1]=='unknown'
    seed={'profile':profiles[0], 'aliases':profiles[1:-1],
          'active_output':int(path_param(30,3)), 'inactive_output':int(path_param(32,3))}
    class Control:
        def call(self, payload):
            return seed if payload['op']=='seed' else None
    roles=[]
    def observe(name,path,method,role,headers):
        row=expected[len(roles)]
        assert (row['name'],row['path'],row['method'])==(name,path,method)
        if 'Authorization' in (headers or {}):
            assert role is None and headers['Authorization']=='Bearer invalid'
            role='invalid_bearer'
        if 'X-API-Key' in (headers or {}):
            assert role is None and headers['X-API-Key']=='invalid'
            role='invalid_API_key'
        roles.append(role or 'anonymous')
        return row
    plan=next(node for node in ast.parse(probe_bytes).body
              if isinstance(node,ast.FunctionDef) and node.name=='exercise')
    request=next(node for node in plan.body if isinstance(node,ast.FunctionDef) and node.name=='request')
    request.body=ast.parse('return observe(name, path, method, role, headers)').body
    code=compile(ast.fix_missing_locations(ast.Module(body=[plan],type_ignores=[])),PROBE,'exec')
    namespace={'observe':observe}
    exec(code,namespace)
    namespace['exercise']('unused',Control())
    assert len(roles)==len(expected)==127
    return roles


def project(source, roles):
    assert len(source['expected'])==len(source['actual'])==len(roles)==127
    for index,row in enumerate(source['expected']):
        assert type(row['status']) is int and 100 <= row['status'] <= 599
        yield {'source_index':index,'record_json_pointer':f'/expected/{index}',
               'path':row['path'],'method':row['method'],'role':roles[index],
               'HTTP_status':row['status'],'response_projection_sha256':digest(row)}


def validate(root, value):
    root=Path(root)
    assert value==read(root,INPUT,INPUT_SHA256)
    assert value['binding_kind']==KIND and value['new_Source_executions']==0
    assert value['Native_coverage_increased'] is False and value['fully_verified_operations']==0
    fixture=value['fixtures'][FIXTURE]
    published=read(root,PUBLIC,PUBLIC_SHA256)
    assert published['passed'] and published['cases']==127 and not published['differences']
    assert published['evidence']==RAW and published['evidence_sha256']==RAW_SHA256
    assert fixture['public_historical_receipt']=={'path':PUBLIC,'sha256':PUBLIC_SHA256}
    assert fixture['private_historical_capture']=={'path':RAW,'sha256':RAW_SHA256}
    assert fixture['frozen_request_plan']=={'path':PROBE,'sha256':PROBE_SHA256}
    assert fixture['historical_cleanup']==published['cleanup']=='isolated Compose project removed'
    assert len(value['contracts'])==value['operation_count']==25
    seen=set()
    unresolved=0
    for definition in value['contracts']:
        assert definition['fixture_id']==FIXTURE and definition['fully_verified'] is False
        assert definition['method'] is None or definition['method'] in {'GET','POST','PUT','PATCH','DELETE'}
        unresolved+=definition['method'] is None
        if definition['method'] is None:
            assert definition['contract_id'].endswith(':METHOD_UNRESOLVED')
            assert definition['source_action'] is None
        cases=definition['cases']
        assert definition['observed_methods']==sorted({case['method'] for case in cases})
        assert definition['observed_roles']==sorted({case['role'] for case in cases})
        assert definition['source_role_specs']=={role:fixture['roles'][role] for role in definition['observed_roles']}
        assert definition['result']['Source_case_observations']==len(cases)
        assert definition['result']['new_Source_cases_this_task']==0
        assert definition['result']['Native_coverage_increased'] is False
        regex=route_regex(definition['pattern_exact'])
        for row in cases:
            method=definition['method']
            assert method is None or row['method']==method
            assert regex.fullmatch(unquote(urlsplit(row['path']).path).lstrip('/'))
            i=row['source_index']
            assert i not in seen and row['record_json_pointer']==f'/expected/{i}'
            assert re.fullmatch('[0-9a-f]{64}',row['response_projection_sha256'])
            seen.add(i)
    assert unresolved==16 and len(value['contracts'])-unresolved==9
    assert len(seen)==value['unique_Source_cases']==120
    excluded=set(value['outside_existing_declared_operation_indices'])
    assert len(excluded)==7 and seen | excluded == set(range(127))


def restore_pre_hdhr(root, rows):
    """Remove only this cohort when verifying older register snapshots."""
    value=read(root,INPUT,INPUT_SHA256)
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
    value=read(root,INPUT,INPUT_SHA256)
    validate(root,value)
    source=read(root,RAW,RAW_SHA256)
    import hashlib
    probe=(root/PROBE).read_bytes()
    assert hashlib.sha256(probe).hexdigest()==source['tool_sha256'][PROBE]==PROBE_SHA256
    assert source['passed'] and source['cleanup']=='isolated Compose project removed'
    assert source['baseline_commit']==value['fixtures'][FIXTURE]['baseline_revision']
    assert source['baseline_archive_sha256']==value['fixtures'][FIXTURE]['baseline_archive_sha256']
    assert not source['differences'] and not source['resources_retained']
    observed={case['source_index']:case for case in project(source,execute_request_plan(source,probe))}
    for contract in value['contracts']:
        assert all(case==observed[case['source_index']] for case in contract['cases'])
    return {'exact_path_method_actor_matches':len(observed),'bound_Source_cases':value['unique_Source_cases'],
            'new_application_runs':0}


if __name__ == '__main__':
    print(json.dumps(verify_private(Path(__file__).resolve().parents[2])))
