"""Read terminal Original admin wire/custody, never emit secrets or fixture HTML."""
import argparse
import ast
import base64
import collections
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit
import zlib

KIND='retained_original_admin_model_TCP'
CASE='m0-admin-model-callbacks'
RECIPE_PATH='artifacts/rust/m0-admin-model-source-driver-1/cohort/recipes.py'
SUFFIXES={
    'list':('changelist','GET','list-superuser','superuser'),
    'add-form':('add','GET','actual-add','superuser'),
    'actual-add':('add','POST','actual-add','superuser'),
    'change-form':('change','GET','actual-change','superuser'),
    'actual-change':('change','POST','actual-change','superuser'),
    'delete-confirm':('delete','GET','actual-delete-confirmed','superuser'),
    'actual-delete':('delete','POST','actual-delete-confirmed','superuser'),
    'no-perm-list':('changelist','GET','staff-no-model-permission-list','staff_without_model_permission'),
    'no-perm-add':('add','GET','staff-no-model-permission-add','staff_without_model_permission'),
    'no-perm-change':('change','GET','staff-no-model-permission-change','staff_without_model_permission'),
    'no-perm-delete':('delete','GET','staff-no-model-permission-delete','staff_without_model_permission'),
}

def digest(data):
    return hashlib.sha256(data).hexdigest()

def load(path):
    raw=path.read_bytes()
    return json.loads(raw),digest(raw)

def raw_body(item):
    return base64.b64decode(item['body_base64'],validate=True)

def literal_recipes(path):
    tree=ast.parse(path.read_bytes())
    assignment=next(node for node in tree.body if isinstance(node,ast.Assign)
                    and ast.unparse(node.targets[0])=='RECIPES')
    return {value[0] for value in ast.literal_eval(assignment.value)}

def session_data(encoded):
    data,_,_=encoded.rsplit(':',2)
    compressed=data.startswith('.')
    payload=base64.urlsafe_b64decode(data.removeprefix('.')+'='*((-len(data.removeprefix('.')))%4))
    if compressed:
        reader=zlib.decompressobj()
        payload=reader.decompress(payload,4097)
        assert reader.eof and not reader.unused_data
    assert len(payload)<=4096
    return json.loads(payload)

def cookie_value(item,key):
    values=[cookie['value'] for cookie in item if cookie['name']==key]
    assert len(values)<=1
    return values[0] if values else None

def role_from_snapshot(before,token,user):
    tables=before['tables']
    rows=[item for item in tables['django_session'] if item['session_key']==token]
    assert len(rows)==1
    session=session_data(rows[0]['session_data'])
    assert str(session['_auth_user_id'])==str(user)
    assert session['_auth_user_backend']=='django.contrib.auth.backends.ModelBackend'
    actor,=[item for item in tables['accounts_user'] if item['id']==user]
    assert actor['is_staff'] is True and actor['is_active'] is True
    for name in ('accounts_user_groups','accounts_user_user_permissions'):
        assert not any(row.get('user_id')==user for row in tables[name])
    return 'superuser' if actor['is_superuser'] else 'staff_without_model_permission'

def terminal(root,path):
    execution,controller=load(path/'dispatch/execution.json')
    assert execution['actual_exit'] in (0,1) and execution['failure'] is None
    assert execution['remaining_group_members']==[]
    assert all(execution[k] is True for k in ('process_group_settled','stdout_EOF','stderr_EOF'))
    report,summary=load(path/'results/report.json')
    assert report['expected_backend_order']==['Original']
    assert report['exact_cleanup_passed'] is True and report['expected_cleanup_count']==1
    assert not report['full_milestone_passed'] and not report['performance_verified']
    fixture,=sorted((path/'fixtures').glob('Original-*'))
    cleanup,cleanup_hash=load(fixture/'independent-cleanup.json')
    assert not cleanup['errors']
    assert all(cleanup[k] is True for k in ('exact_owned_absence','all_original_process_generations_absent',
                                          'host_CLI_groups_settled'))
    assert cleanup==report['cleanup'][0]
    manifest,manifest_hash=load(fixture/CASE/'report.json')
    assert manifest['status'] in ('PARTIAL_ACTUAL_OBSERVATIONS_WITH_EXPLICIT_BLOCKERS',
                                  'OBSERVATIONS_COMPLETE_REQUIRES_INDEPENDENT_SOURCE_REVIEW')
    assert manifest['actual_Source'] and not manifest['actual_Native']
    assert manifest['single_actual_admin_session'] and manifest['raw5203_NOT_RUN']
    assert not manifest['Source_business_or_auth51_replayed']
    assert 0<manifest['actual_HTTP_calls']<=297
    return fixture,manifest,{
        'actual_controller_exit':execution['actual_exit'],
        'controller_receipt_sha256':controller,'aggregate_report_sha256':summary,
        'independent_cleanup_receipt_sha256':cleanup_hash,
        'actual_context_report_sha256':manifest_hash,
        'exact_owned_cleanup_verified':True,
        'selected_application_cells_passed':report['selected_application_cells_passed'],
        'fixture_report_path':str((fixture/CASE/'report.json').relative_to(root)),
    }

def terminal_case(raw, context_report, logical_path, custody):
    request,response=raw['request'],raw['response']
    assert not response['body_truncated']
    assert 'incomplete_read' not in response
    assert 100<=response['status']<=599
    assert not any(name.casefold()=='authorization' for name in request['headers'])
    return {'path':logical_path,'method':request['method'],'status':response['status'],
            'response_bytes':len(raw_body(response)),'response_body_sha256':digest(raw_body(response)),
            'private_wire_sha256':custody,
            'semantic_outcome':context_report['status']}

def decode_label(label,models):
    model,suffix=label.split('-',1)
    assert model in models and suffix in SUFFIXES
    return model,suffix

def graph_target(model,action,graphs):
    literal='/admin/'+model.replace('.','/')+'/'
    if action=='changelist':
        return literal
    if action=='add':
        return literal+'add/'
    owned=graphs['read' if action=='change' else 'delete'][model]
    assert type(owned) is int and owned>0
    return f'{literal}{owned}/{action}/'

def read_wire(wires,filename):
    match=re.fullmatch(r'([0-9]{4})-(.+)-wire\.json',filename)
    assert match,filename
    idx,label=int(match[1]),match[2]
    wire,sha=load(wires/filename)
    pre,pre_sha=load(wires/(filename.replace('-wire.json','-SQL-before.json')))
    post,post_sha=load(wires/(filename.replace('-wire.json','-SQL-after.json')))
    request,_=load(wires/(filename.replace('-wire.json','-request.json')))
    assert request==wire['request']
    return idx,label,wire,pre,post,sha,pre_sha,post_sha

def successful_report_witness(context,suffix,wire_sha):
    if context['status']!='observed':
        return False
    fact=context['fact']
    if suffix.startswith('no-perm-'):
        fact=fact['actual_same_session_permission_denial']
    elif suffix.startswith('actual-'):
        fact=fact['HTTP']
    elif suffix!='list':
        return False
    reference=fact['private_raw']
    assert reference['sha256']==wire_sha
    return True

def extract(root, run, registry_path):
    fixture,report,completion=terminal(root,run)
    private=fixture/CASE/'private'
    wire_root=private/'wire'
    startup,_=load(private/'queue-drain.json')
    assert startup=={'passed':True,'queues':['celery','dvr']}
    seed,seed_sha=load(private/'declared-owned-graphs.json')
    models=literal_recipes(root/RECIPE_PATH)
    assert len(models)==len(seed['registry'])==27 and models==set(seed['registry'])
    actor=seed['actor']
    assert actor['is_superuser'] and actor['is_staff']
    cut_off,perm_off_sha=load(private/'permission-transition-off.json')
    restored,perm_on_sha=load(private/'permission-transition-restored.json')
    for transition,prior in ((cut_off,True),(restored,False)):
        assert transition['actor']==actor['id'] and transition['declared_only_superuser_transition']
        assert transition['before']=={'is_superuser':prior,'groups':[],'direct_permissions':[]}
        assert transition['after']=={'is_superuser':not prior}
        assert not transition['session_forged_or_login_replayed']
    registry,registry_sha=load(registry_path)
    assert registry['executable_Source_mapping']['operations']==270
    by_name={row['name']:row for row in registry['runtime_routes'] if row['name'] and row['name']!='admin:'}
    recipes={(row['model'],row['operation']):row for row in report['models']}
    assert len(recipes)==len(report['models'])
    files=sorted(file.name for file in wire_root.glob('*-wire.json'))
    assert len(files)==report['actual_HTTP_calls']
    linked=collections.defaultdict(list)
    existing=collections.Counter()
    session_token=None
    starts=['one-admin-CSRF','one-admin-session-login','actual-protected-admin-site']
    control_outcomes=[]
    for index,filename in enumerate(files,1):
        idx,label,wire,before,after,w_sha,b_sha,a_sha=read_wire(wire_root,filename)
        assert idx==index
        req,resp=wire['request'],wire['response']
        request_token=cookie_value(req['cookies_before'],'sessionid')
        response_token=cookie_value(resp['cookies_after'],'sessionid')
        if idx<=3:
            assert label==starts[idx-1] and resp['status']==[200,302,200][idx-1]
            assert req['method']==['GET','POST','GET'][idx-1]
            assert req['path']==(['/admin/login/?next=/admin/']*2+['/admin/'])[idx-1]
            assert index!=2 or request_token is None and response_token is not None
            if index==2:
                session_token=response_token
                assert role_from_snapshot(after,session_token,actor['id'])=='superuser'
            control_outcomes.append({'label':label,'method':req['method'],'status':resp['status'],
                                     'path':req['path'],'admitted_as_new_operation':False})
            continue
        assert session_token is not None and request_token==session_token and response_token==session_token
        model,suffix=decode_label(label,models)
        action,method,operation,role=SUFFIXES[suffix]
        assert req['method']==method
        path=graph_target(model,action,seed['graphs'])
        assert req['path']==path and urlsplit(path).query==''
        assert bool(raw_body(req))==(method=='POST')
        assert role_from_snapshot(before,session_token,actor['id'])==role
        assert role_from_snapshot(after,session_token,actor['id'])==role
        resolver_name='admin:'+model.replace('.','_')+'_'+action
        route=by_name[resolver_name]
        old,=route['operations']
        assert old['method'] is None and old['source_action'] is None and not old['fully_verified']
        regex=re.sub(r'<path:object_id>',r'[0-9]+',route['pattern_exact'])
        assert re.fullmatch(regex,path.lstrip('/'))
        context=recipes[model,operation]
        item=terminal_case(wire,context,path,w_sha)
        item.update(label=label,model=model,admin_callback=action,source_index=idx,role=role,
                    role_binding='retained actual session and per-request database actor state',
                    SQL_before_sha256=b_sha,SQL_after_sha256=a_sha,
                    semantic_workflow_witness=successful_report_witness(context,suffix,w_sha))
        if 'executable_Source_contract' in old:
            existing[old['contract_id']]+=1
        else:
            linked[old['contract_id']].append((route,old,item))
    assert sum(map(len,linked.values()))+sum(existing.values())+len(control_outcomes)==len(files)
    contracts=[]
    for identifier,items in sorted(linked.items()):
        route,op,_=items[0]
        cases=[item[2] for item in items]
        roles=sorted({row['role'] for row in cases})
        contracts.append({
            'contract_id':identifier,'pattern_exact':route['pattern_exact'],'source_handler':route['source_handler'],
            'route_name':route['name'],'method':None,'source_action':None,'owning_milestone':op['owning_milestone'],
            'fixture_id':'Original-admin-model-callbacks-actual',
            'observed_methods':sorted({row['method'] for row in cases}),'observed_roles':roles,
            'source_role_specs':{role:{'fixture_actor_role':role,'complete':False} for role in roles},
            'fully_verified':False,'cases':cases,
            'assertions':['Exact named admin initiative, actual request path and returned HTTP status',
                          'Same persisted Django session and bound actor mode in both SQL snapshots',
                          'Distinguish selected successful postconditions from blocked or non-mutating leaves'],
            'result':{'Source_case_observations':len(cases),'new_Source_cases_this_task':0,
                      'Native_coverage_increased':False},
        })
    status=collections.Counter(row['status'] for row in report['models'])
    observed=collections.Counter((row['method'],row['role'],row['status']) for rows in linked.values() for _,_,row in rows)
    sources={'seed_graph_identity_sha256':seed_sha,'permission_off_sha256':perm_off_sha,
             'permission_restored_sha256':perm_on_sha,**completion}
    return {
        'binding_kind':KIND,'scope':'Partial terminal Original admin request evidence',
        'operation_count':len(contracts),'unique_Source_cases':sum(map(len,linked.values())),
        'fully_verified_operations':0,
        'fixtures':{'Original-admin-model-callbacks-actual':{
            'baseline_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430',
            'scope':'Original admin TCP on dedicated backend listener',
            'actual_Terminal_cleanup_verification':sources,
            'actual_source_request_attempts':len(files),
            'actor_roles':['superuser','staff_without_model_permission'],
            'single_admin_cookie_session_actual':True,
            'selected_actor_role_scope_only':True,
        }},
        'Original_author_credit':'Original Dispatcharr authors retain all application and algorithm credit.',
        'register_sha256':registry_sha,'retained_new_operation_count':len(contracts),
        'retained_new_operation_wires':sum(map(len,linked.values())),
        'existing_assignment_wires':dict(sorted(existing.items())),'login_controls_already_scoped':control_outcomes,
        'actual_HTTP_attempts':len(files),'actual_model_outcome_counts':dict(status),
        'actual_blocker_count':len(report['blockers']),
        'model_role_method_status_counts':[{'method':key[0],'role':key[1],'HTTP_status':key[2],'count':value}
                                           for key,value in sorted(observed.items())],
        'new_Source_executions':0,'Native_coverage_increased':False,
        'contracts':contracts,
    }

def main():
    cli=argparse.ArgumentParser(description=__doc__)
    cli.add_argument('--repo',type=Path,required=True)
    cli.add_argument('--run',type=Path,required=True)
    cli.add_argument('--register',type=Path,required=True)
    cli.add_argument('--output',type=Path,required=True)
    opt=cli.parse_args()
    value=extract(opt.repo.resolve(),opt.run.resolve(),opt.register)
    opt.output.write_text(json.dumps(value,indent=2)+'\n')
    print(json.dumps({key:value[key] for key in
                      ('retained_new_operation_count','retained_new_operation_wires','actual_HTTP_attempts',
                       'actual_model_outcome_counts','actual_blocker_count','existing_assignment_wires')},
                     indent=2))

if __name__=='__main__':
    main()
