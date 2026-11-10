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
def build():
    runtime_path=ROOT/'docs/rust-runtime-contracts.json';static_path=ROOT/'docs/rust-contract-inventory.json'
    runtime=json.loads(runtime_path.read_text());static=json.loads(static_path.read_text());rows=[];seen=collections.Counter()
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
        view=route['view'];parts=view.split('.');source_file='/'.join(parts[:-1])+'.py'if view.startswith(('apps.','core.','dispatcharr.'))else None
        prefix='/'.join(parts[:2])+'/'if view.startswith('apps.')else'core/'if view.startswith('core.')else None
        tests=[p for p in static['test_files']if prefix and p.startswith(prefix)]
        rows.append({'route_id':root_id,'resolver_ordinal':ordinal,'pattern_exact':route['pattern'],'name':route.get('name'),'source_handler':view,
            'source_module_candidate':source_file,'source_test_candidates':tests,'test_association':'module only; not evidence these tests cover this route',
            'declared_methods_resolved':isinstance(methods,dict)and bool(methods),'operations':operations})
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
        'baseline_ref':static['baseline_ref'],'input_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in [runtime_path,static_path]},
        'runtime_discovery_metadata':runtime['metadata'],'limits':['Method/action is unresolved where original runtime discovery returned null; no GET/POST guesses.',
            'Schema authentication is not a role/permission proof; every role matrix still requires case mapping.',
            'Module-associated tests and related scoped reports never mark an entire route fully verified.',
            'Runtime resolver order and exact raw patterns, including format variants, are retained.',
            'Non-HTTP entries are static candidates, not exhaustive environment/command/task/plugin/admin coverage.'],
        'counts':{'runtime_routes':len(rows),'routes_with_unresolved_methods':sum(not r['declared_methods_resolved']for r in rows),
            'runtime_operation_work_items':len(ids),'OpenAPI_operations':len(operations),'static_non_HTTP_candidates':len(non_http),
            'fully_verified_route_operations':0,'scoped_evidence_requiring_case_mapping':sum(bool(op['related_evidence'])for r in rows for op in r['operations'])},
        'unrepresented_contract_families':['Environment defaults and precedence','Periodic schedule trigger/crash semantics','Plugin context/capability APIs','Admin workflows and CSRF/session behavior','Signals and background startup/shutdown hooks'],
        'runtime_routes':rows,'openapi_operations':operations,'non_HTTP_candidates':non_http}
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,default=ROOT/'docs/rust-contract-register.json');args=parser.parse_args();value=build();data=(json.dumps(value,indent=2,ensure_ascii=False)+'\n').encode();args.output.write_bytes(data)
    print(json.dumps({'written':str(args.output),'counts':value['counts'],'sha256':hashlib.sha256(data).hexdigest(),'parity_acceptance':False}))
if __name__=='__main__':main()
