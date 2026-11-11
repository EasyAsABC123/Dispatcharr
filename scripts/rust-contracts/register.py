#!/usr/bin/env python3
"""Build a traceable contract work register without inventing parity coverage.
Original Dispatcharr authors retain all application/algorithm credit and AGPL.
"""
import argparse,collections,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
HTTP={'get','post','put','patch','delete','head','options','trace'}

def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()).hexdigest()
def milestone(view,pattern,action):
    if 'vod_proxy' in view or 'timeshift' in view:return 'M4'
    if 'live_proxy' in view:return 'M3'
    if any(x in view for x in ['apps.output','apps.hdhr','consumers']):return 'M7'
    if any(x in view for x in ['apps.plugins','apps.backups','django.contrib.admin','django_celery_beat']):return 'M8'
    if any(x in view for x in ['apps.epg','apps.m3u']):return 'M6' if any(x in str(action) for x in ['refresh','import','parse','match','fetch']) else 'M5'
    if any(x in view for x in ['apps.accounts','apps.channels','apps.vod','apps.dashboard','apps.connect','core.']):return 'M5'
    if 'spectacular' in view or 'schema' in pattern:return 'M0'
    return 'M8'
def related(view,action,method):
    if view in {'apps.vod.api_views.MovieViewSet','apps.vod.api_views.SeriesViewSet','apps.vod.api_views.EpisodeViewSet','apps.vod.api_views.VODCategoryViewSet'} and action in {'list','retrieve'} and method in {'GET','HEAD','OPTIONS'}:
        return [{'report':'scripts/rust-integration/verified-checkpoint-image35-1.json','scope':'Captured ordinary/category HTTP and scoped React only; does not prove every role/input/action','authority':'related scoped evidence; row-specific case mapping still required'}]
    return []
# Captured evidence adapters preserve discovery IDs and incomplete coverage.
# Original Dispatcharr authors retain application/algorithm credit and AGPL.
def checked_json(reference):
    path=ROOT/reference['path']
    assert hashlib.sha256(path.read_bytes()).hexdigest()==reference['sha256']
    return json.loads(path.read_text())

def response_wire(value):
    import base64
    data=value['body_text'].encode()if'body_text'in value else base64.b64decode(value['body_base64'],validate=True)
    assert hashlib.sha256(data).hexdigest()==value['body_sha256']
    if'body_bytes'in value:assert len(data)==value['body_bytes']
    return {'status':value['status'],'body_bytes':len(data),'body_sha256':value['body_sha256'],'headers':value['headers']}

def deep_checkpoint_dataset(dataset):
    # Deliberately consumes the real checkpoint schema; never fabricates .runtime.
    summary=checked_json(dataset['image44_public_summary'])
    runtime=dataset['runtime'];manifest=checked_json({'path':runtime['manifest_path'],'sha256':runtime['manifest_sha256']})
    assert summary['passed']is True and manifest['passed']is True
    assert summary['image_id']==manifest['image_id']==runtime['image_id']
    assert summary['source_tree_sha256']==manifest['source_tree_sha256']==runtime['source_tree_sha256']
    assert summary['build_manifest']=={'path':runtime['manifest_path'],'sha256':runtime['manifest_sha256']}
    assert runtime['git_revision']==manifest['git_revision']and runtime['tag']==manifest['tag']
    # This draft is scoped to the reviewed687-input image44, not arbitrary counts.
    assert sum(name.startswith('rust/')for name in manifest['source_sha256'])==687 and summary['Rust_inputs']==687
    assert summary[dataset['summary_case_field']]==dataset['case_count']
    assert summary['whole_application_verified']is False and summary['full_milestone_complete']is False
    gates=[gate for gate in summary['gates']if gate.get('evidence')==dataset['source_run']['path']]
    assert len(gates)==1 and gates[0]['passed']is True
    assert gates[0]['evidence_sha256']==dataset['source_run']['sha256']
    run=checked_json(dataset['source_run']);comparison=checked_json(dataset['comparison_report']);source=checked_json(dataset['source_capture'])
    assert run['passed']is True and run['application_image_verified']is True and run['resources_retained']is False
    assert run['build_manifest_sha256']==runtime['manifest_sha256']
    assert all(run[key]==value for key,value in dataset['source_run_required'].items())
    assert run['candidate_binding']=={'image_id':runtime['image_id'],'source_tree_sha256':runtime['source_tree_sha256'],'manifest_verified':True}
    assert comparison['passed']is True and source['passed']is True
    assert len(comparison['records'])==len(source['records'])==dataset['case_count']
    assert dict(collections.Counter(row['method']for row in comparison['records']))==dataset['method_counts']
    return comparison,source

def captured_vod_route(rows,case):
    import re
    path=case['path'].split('?',1)[0].lstrip('/')
    # This corpus has no encoded path segments. Do not guess how a new encoded
    # or ambiguous route would resolve; require a new exact resolver witness.
    assert '%'not in path
    matches=[]
    for route in rows:
        if not route['pattern_exact'].startswith('api/vod/'):continue
        pattern=route['pattern_exact'].replace('/^','/')
        pattern=re.sub(r'<int:[^>]+>',r'[0-9]+',pattern)
        if re.fullmatch(pattern,path):matches.append(route)
    assert len(matches)==1
    return matches[0]['route_id']

def deep_checkpoint_case(case,dataset,loaded,route,rows):
    assert case['dataset_id']==dataset['dataset_id']
    assert captured_vod_route(rows,case)==route['route_id']
    comparison,source=loaded;index=case['source_index'];row=comparison['records'][index]
    assert case['record_json_pointer']==f'/records/{index}'and case['record_sha256']==digest(row)
    assert case['expected_response_sha256']==digest(row['expected'])
    if dataset['dataset_id']=='images240':
        assert case['accept']==row.get('accept')and case['label']==row.get('label')
        assert case['request_extra_sha256']==digest(row.get('request_extra',{}))
    assert all(case[key]==row.get(key)for key in ['path','method','role'])
    assert row.get('differences',[])==[]
    expected=response_wire(row['expected']);actual=response_wire(row.get('actual',row.get('actual_native')))
    assert actual==expected
    assert case['status']==expected['status']and case['body_bytes']==expected['body_bytes']and case['body_sha256']==expected['body_sha256']
    assert case['selected_headers_sha256']==digest(expected['headers'])
    fresh={item['index']:item for item in comparison.get('predeclared_fresh_references',[])}
    assert case['fresh_predeclared_Source_tie_authority']==(index in fresh)
    if dataset['dataset_id']=='provider150':assert len(fresh)==25
    authority=fresh[index]['fresh_actual_Source']if index in fresh else source['records'][index]['expected']
    if index in fresh:assert fresh[index]['no_intervening_state_change']is True
    assert response_wire(authority)==expected
    assert all(source['records'][index].get(key)==row.get(key)for key in ['path','method','role'])
    assert case['result']=='passed_exact_Source_native_comparison'


def checkpoint_dataset(dataset,deep_evidence=False):
    attestation=checked_json(dataset['attestation'])
    assert attestation['fully_verified_route_operations']==0
    projection=attestation['case_projection'];projection_sha=digest(projection)
    assert projection_sha==attestation['validation_authority']['case_projection_sha256']==dataset['attestation']['case_projection_sha256']
    assert attestation['validation_authority']['public_summary']==dataset['image44_public_summary']
    envelopes=[item for item in attestation['datasets']if item['dataset_id']==dataset['dataset_id']]
    assert len(envelopes)==1;envelope=envelopes[0]
    assert envelope['runtime']==dataset['runtime']and envelope['fully_verified']is False
    assert envelope['case_count']==dataset['case_count']and envelope['method_counts']==dataset['method_counts']
    assert envelope['summary_case_field']==dataset['summary_case_field']and envelope['image44_public_summary']==dataset['image44_public_summary']
    assert all(envelope['raw_evidence'][key]==dataset[key]for key in ['comparison_report','source_capture','source_run'])
    source=envelope['source'];assert source['baseline_commit']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
    assert source['baseline_image']=='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
    summary=checked_json(dataset['image44_public_summary']);runtime=dataset['runtime']
    assert summary['passed']is True and summary['image_id']==runtime['image_id']and summary['source_tree_sha256']==runtime['source_tree_sha256']
    assert summary['build_manifest']=={'path':runtime['manifest_path'],'sha256':runtime['manifest_sha256']}
    assert summary['Rust_inputs']==687 and summary[dataset['summary_case_field']]==dataset['case_count']
    assert summary['whole_application_verified']is False and summary['full_milestone_complete']is False
    gates=[gate for gate in summary['gates']if gate.get('evidence')==dataset['source_run']['path']]
    assert len(gates)==1 and gates[0]['passed']is True and gates[0]['evidence_sha256']==dataset['source_run']['sha256']
    rows=[row for row in projection if row['dataset_id']==dataset['dataset_id']]
    assert len(rows)==dataset['case_count']and [row['source_index']for row in rows]==list(range(dataset['case_count']))
    assert dict(collections.Counter(row['method']for row in rows))==dataset['method_counts']
    assert all(row['source_response']==row['candidate_response']for row in rows)
    assert sum(row['source_authority']=='predeclared_fresh_same_state'for row in rows)==(25 if dataset['dataset_id']=='provider150'else 0)
    assert all(row['fresh_no_intervening_state_change']is True for row in rows if row['source_authority']=='predeclared_fresh_same_state')
    return rows,deep_checkpoint_dataset(dataset)if deep_evidence else None

def checkpoint_case(case,dataset,loaded,route,rows):
    attested,deep=loaded;row=attested[case['source_index']]
    assert captured_vod_route(rows,case)==route['route_id']and case['dataset_id']==dataset['dataset_id']
    assert all(case[key]==row.get(key)for key in ['source_index','path','method','role'])
    assert case['record_json_pointer']==f"/records/{case['source_index']}"and case['record_sha256']==row['raw_record_sha256']
    wire=row['source_response'];assert case['status']==wire['status']and case['body_bytes']==wire['body_bytes']and case['body_sha256']==wire['body_sha256']
    assert case['selected_headers_sha256']==digest(wire['headers'])
    assert case['fresh_predeclared_Source_tie_authority']==(row['source_authority']=='predeclared_fresh_same_state')
    if dataset['dataset_id']=='images240':
        assert case['accept']==row['accept']and case['label']==row['label']and case['request_extra_sha256']==row['request_extra_sha256']
    assert case['result']=='passed_exact_Source_native_comparison'
    if deep is not None:deep_checkpoint_case(case,dataset,deep,route,rows)


def build(bindings_path=None,deep_evidence=False):
    runtime_path=ROOT/'docs/rust-runtime-contracts.json';static_path=ROOT/'docs/rust-contract-inventory.json'
    runtime=json.loads(runtime_path.read_text());static=json.loads(static_path.read_text());rows=[];seen=collections.Counter()
    methods_path=ROOT/'docs/rust-runtime-methods.json';dispatch=json.loads(methods_path.read_text())if methods_path.exists()else None
    if dispatch is not None:
        identity=lambda r:(r['pattern'],r.get('name'),r['view'])
        assert [identity(r)for r in dispatch['routes']]==[identity(r)for r in runtime['routes']]
        assert dispatch['actual_method_or_permission_acceptance']is False
    for ordinal,route in enumerate(runtime['routes']):
        identity={key:route.get(key)for key in ['name','pattern','view','methods']};base=digest(identity);occurrence=seen[base];seen[base]+=1
        root_id='route-'+base[:20]+('-'+str(occurrence)if occurrence else '')
        methods=route.get('methods');operations=[]
        for method,action in (methods.items()if isinstance(methods,dict)and methods else [(None,None)]):
            name=method.upper()if method else None;reports=related(route['view'],action,name)
            operations.append({'contract_id':root_id+':'+(name or 'METHOD_UNRESOLVED'),'method':name,'source_action':action,
                'owning_milestone':milestone(route['view'],route['pattern'],action),'milestone_assignment':'proposed work ownership, not acceptance',
                'role_matrix':{'state':'unmapped','required_roles':[]},'setup_fixture':None,'assertions':[],
                'coverage_state':'scoped_evidence_needs_case_mapping'if reports else 'not_yet_mapped',
                'related_evidence':reports,'result':None,'fully_verified':False})
        candidates=dispatch['routes'][ordinal]if dispatch is not None else None
        view=route['view'];parts=view.split('.');source_file='/'.join(parts[:-1])+'.py'if view.startswith(('apps.','core.','dispatcharr.'))else None
        prefix='/'.join(parts[:2])+'/'if view.startswith('apps.')else'core/'if view.startswith('core.')else None
        tests=[p for p in static['test_files']if prefix and p.startswith(prefix)]
        rows.append({'route_id':root_id,'resolver_ordinal':ordinal,'pattern_exact':route['pattern'],'name':route.get('name'),'source_handler':view,
            'source_module_candidate':source_file,'source_test_candidates':tests,'test_association':'module only; not evidence these tests cover this route',
            'declared_methods_resolved':isinstance(methods,dict)and bool(methods),
            'runtime_handler_candidates':candidates['handler_method_candidates']if candidates else None,
            'default_permission_candidates':candidates['default_permission_candidates']if candidates else None,
            'dynamic_permission_method':candidates['dynamic_permission_method']if candidates else None,
            'candidate_scope':'Static runtime attributes only; never executed HTTP or role acceptance',
            'operations':operations})
    bindings_path=Path(bindings_path)if bindings_path is not None else ROOT/'scripts/rust-contracts/case-bindings.json'
    case_bindings=json.loads(bindings_path.read_text())if bindings_path.exists()else None
    if case_bindings is not None:
        indexed={op['contract_id']:(route,op)for route in rows for op in route['operations']}
        bound=set()
        datasets={item['dataset_id']:item for item in case_bindings.get('evidence_datasets',[])}
        assert len(datasets)==len(case_bindings.get('evidence_datasets',[]))
        loaded_datasets={}
        seen_cases=set()
        def bind_case(case,dataset,loaded,route):
            key=(dataset['dataset_id'],case['source_index'])
            assert key not in seen_cases
            checkpoint_case(case,dataset,loaded,route,rows)
            seen_cases.add(key)
        def dataset_for(item):
            key=item['evidence_dataset_id'];dataset=datasets[key]
            if key not in loaded_datasets:loaded_datasets[key]=checkpoint_dataset(dataset,deep_evidence)
            return dataset,loaded_datasets[key]
        for binding in case_bindings['bindings']:
            contract=binding['contract_id'];assert contract not in bound;bound.add(contract)
            route,op=indexed[contract]
            assert binding['pattern_exact']==route['pattern_exact']and binding['source_handler']==route['source_handler']
            assert binding['method']==op['method']and binding['fully_verified']is False
            if binding.get('packaged_report_schema')=='checkpoint_image_v1':
                dataset,loaded=dataset_for(binding)
                assert binding['runtime']==dataset['runtime']
                assert binding['packaged_report']==dataset['image44_public_summary']['path']and binding['packaged_report_sha256']==dataset['image44_public_summary']['sha256']
                assert binding['evidence_dataset_case_count']==dataset['case_count']and binding['evidence_dataset_methods']==dataset['method_counts']
                assert binding['comparison_report']==dataset['comparison_report']and binding['source_capture']==dataset['source_capture']and binding['source_run']==dataset['source_run']
                for case in binding['cases']:bind_case(case,dataset,loaded,route)
            else:
                report_path=ROOT/binding['packaged_report'];report=json.loads(report_path.read_text())
                assert hashlib.sha256(report_path.read_bytes()).hexdigest()==binding['packaged_report_sha256']
                assert report['passed']is True and report['runtime']==binding['runtime']
            assert binding['cases']and all(case['method']==op['method']and case['result']=='passed_exact_Source_native_comparison'for case in binding['cases'])
            op['case_bindings']=binding
            op['coverage_state']='partial_captured_cases_mapped'
            op['role_matrix']={'state':'partial_captured_cases_only','observed_roles':sorted({case['role']or'anonymous'for case in binding['cases']}),'required_roles':[],'remaining':'Required role/input matrix is still incomplete.'}
        route_index={route['route_id']:route for route in rows}
        for observation in case_bindings.get('observations_for_methods_not_declared_in_discovery',[]):
            if 'evidence_dataset_id'not in observation:continue # preserve historical observations
            route=route_index[observation['route_id']]
            assert observation['pattern_exact']==route['pattern_exact']and observation['source_handler']==route['source_handler']
            assert observation['fully_verified']is False and observation['contract_id']is None
            assert observation['method']not in {op['method']for op in route['operations']}
            assert observation['case']['method']==observation['method']
            dataset,loaded=dataset_for(observation);bind_case(observation['case'],dataset,loaded,route)
        assert seen_cases=={(key,index)for key,dataset in datasets.items()for index in range(dataset['case_count'])}
    attestations=sorted({item['attestation']['path']:item['attestation']for item in(case_bindings or{}).get('evidence_datasets',[])}.values(),key=lambda item:item['path'])
    attestation_paths=[ROOT/item['path']for item in attestations]
    operations=[]
    for path,item in sorted(runtime['openapi']['paths'].items()):
        for method,op in sorted(item.items()):
            if method not in HTTP:continue
            operations.append({'schema_contract_id':'schema-'+digest([path,method])[:20],'method':method.upper(),'path_exact':path,
                'operation_id':op.get('operationId'),'tags':op.get('tags',[]),'authentication_schema':op.get('security'),
                'authentication_is_not_role_authorization':True,'runtime_case_mapping':'unmapped','coverage_state':'discovery_only','result':None,'fully_verified':False})
    non_http=[]
    non_http_kinds={'migration','model_candidate','task_candidate','management_command','websocket_consumer_candidate','websocket_route'}
    for item in static['candidates']:
        kind=item.get('kind','unknown')
        if kind not in non_http_kinds:continue
        location={k:item.get(k)for k in ['kind','path','line','name','callable']if k in item}
        non_http.append({'candidate_id':'candidate-'+digest(location)[:20],'source':location,'state':'static_candidate_requires_runtime_contract','result':None,'fully_verified':False})
    candidate_ids=[x['candidate_id']for x in non_http];assert len(candidate_ids)==len(set(candidate_ids))
    ids=[op['contract_id']for r in rows for op in r['operations']];assert len(ids)==len(set(ids))
    assert len(rows)==len(runtime['routes']) and len(operations)==sum(k in HTTP for p in runtime['openapi']['paths'].values()for k in p)
    assert all(op['result']is None and not op['fully_verified']for r in rows for op in r['operations'])
    return {'format_version':1,'scope':'Contract work register linking runtime discovery and explicit mapping gaps; not executable parity acceptance',
        'credit':'All application/algorithm credit belongs to original Dispatcharr authors and contributors; AGPL retained.',
        'baseline_ref':static['baseline_ref'],'input_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in [runtime_path,static_path,*([methods_path]if dispatch is not None else[]),*([bindings_path]if case_bindings is not None else[]),*attestation_paths]},
        'runtime_discovery_metadata':runtime['metadata'],'case_attestation_bindings':attestations,'case_binding_validation':{'mode':'deep_private_raw_revalidation'if deep_evidence else'committed_sanitized_attestation','is_new_HTTP_replay':False},'limits':['Method/action is unresolved where original runtime discovery returned null; no GET/POST guesses.',
            'Schema authentication is not a role/permission proof; every role matrix still requires case mapping.',
            'Module-associated tests and related scoped reports never mark an entire route fully verified.',
            'Runtime resolver order and exact raw patterns, including format variants, are retained.',
            'Non-HTTP entries are static candidates, not exhaustive environment/command/task/plugin/admin coverage.'],
        'counts':{'runtime_routes':len(rows),'routes_with_unresolved_methods':sum(not r['declared_methods_resolved']for r in rows),
            'methodless_routes_with_runtime_handler_candidates':sum(not r['declared_methods_resolved']and r['runtime_handler_candidates']is not None for r in rows),
            'runtime_operation_work_items':len(ids),'OpenAPI_operations':len(operations),'static_non_HTTP_candidates':len(non_http),
            'fully_verified_route_operations':0,'scoped_evidence_requiring_case_mapping':sum(bool(op['related_evidence'])for r in rows for op in r['operations']),
            'operations_with_partial_case_bindings':sum('case_bindings'in op for r in rows for op in r['operations']),
            'mapped_packaged_case_observations':sum(len(op['case_bindings']['cases'])for r in rows for op in r['operations']if'case_bindings'in op)},
        'observations_for_methods_not_declared_in_discovery':case_bindings.get('observations_for_methods_not_declared_in_discovery',[])if case_bindings is not None else[],
        'unrepresented_contract_families':['Environment defaults and precedence','Periodic schedule trigger/crash semantics','Plugin context/capability APIs','Admin workflows and CSRF/session behavior','Signals and background startup/shutdown hooks'],
        'runtime_routes':rows,'openapi_operations':operations,'non_HTTP_candidates':non_http}
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,default=ROOT/'docs/rust-contract-register.json');parser.add_argument('--bindings',type=Path,default=ROOT/'scripts/rust-contracts/case-bindings.json');parser.add_argument('--deep-evidence',action='store_true',help='Revalidate private raw captures/build matrix; default validates committed sanitized attestations');args=parser.parse_args();value=build(args.bindings,args.deep_evidence);data=(json.dumps(value,indent=2,ensure_ascii=False)+'\n').encode();args.output.write_bytes(data)
    print(json.dumps({'written':str(args.output),'counts':value['counts'],'sha256':hashlib.sha256(data).hexdigest(),'parity_acceptance':False}))
if __name__=='__main__':main()
