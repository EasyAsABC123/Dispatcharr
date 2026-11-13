"""Offline evidence integrity and finite Main checks; never dispatch applications."""
import contextlib,copy,hashlib,importlib,importlib.util,json,math,re,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
AUTHORITY_SHA='7fe67cc43e15c57b683c33e54601a694d7c9acfe84e0bdcefebae4de36c81bca'
IMAGE='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a'
APP='d77f25fa94ee128c79ade9176d8eb4a9d23bf498'
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):
 p=Path(p);need(p.is_file()and not p.is_symlink()and p.stat().st_size<=16777216,'BoundedRegularFile');return json.loads(p.read_bytes())
def canonical(v):return hashlib.sha256(json.dumps(v,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()
def load(p,name):
 s=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
@contextlib.contextmanager
def tools(name):
 names=['qualify','identity','origin_coverage','source_reference'];old=list(sys.path);saved={k:sys.modules.get(k)for k in names}
 for k in names:sys.modules.pop(k,None)
 sys.path.insert(0,str(HERE/'tools'/name/'blocked/tools'))
 try:yield importlib.import_module('qualify')
 finally:
  sys.path[:]=old
  for k in names:
   sys.modules.pop(k,None)
   if saved[k]is not None:sys.modules[k]=saved[k]
def cleanup(value,projects):
 need(value.get('passed')is True and value.get('inventories')=={'containers':[],'networks':[],'volumes':[]},'EmptyAllResourceKinds')
 need(set(projects)<=set(value.get('projects',[]))and projects,'ExplicitOwnedProjects')
 for project in projects:
  rows=[r for r in value.get('queries',[])if r.get('project')==project];need(len(rows)==3 and{r.get('kind')for r in rows}=={'containers','networks','volumes'},'ThreeIndependentOwnedQueries')
  for row in rows:need(row.get('exit')==0 and row.get('stdout')==row.get('stderr')==''and 'label=com.docker.compose.project='+project in row.get('argv',[]),'ActualScopedQuerySuccess')
def matrix(value,authority):
 plan=read(HERE/'package-definition/plan.json');expected={g['name']:g for g in plan['groups']};rows=value.get('rows')
 need(len(expected)==28 and type(rows)is list and len(rows)==28 and{r.get('required_node')for r in rows}==set(expected),'Mandatory28UniqueNodes')
 need(value.get('worker_graph_activation')is False and value.get('performance_claim')is False,'NoGraphOrPerformanceTransfer')
 intervals=[];controllers={}
 for row in rows:
  n=row['required_node'];c=read(HERE/row['controller']);v=read(HERE/row['review'])
  need(canonical(c)==authority['controllers'][n],'ExactActualControllerTimeArgsAuthority')
  need(c.get('name')==v.get('name')==n and c['argv']==expected[n]['argv']and c['cwd']==plan['support_context'],'RequiredNodeActualMapping')
  need(c['returncode']==0 and c['process_settled']is True and v.get('passed')is True and v.get('semantic_review_complete')is True,'ActualWaitSemanticReview')
  a,b=c['started_epoch'],c['completed_epoch'];need(all(type(t)in[int,float]and math.isfinite(t)for t in [a,b])and 1e9<a<b<2e9,'FiniteActualDispatchTimes')
  if n=='strict-typed154':need(v['cleanup']=={'scope':'offline-no-resources'}and row['cleanup']is None,'PureNodeNoFixture')
  else:
   need(v['cleanup']['verified']is True,'ReviewedPhysicalCleanup');cleanup(read(HERE/row['cleanup']),v['cleanup']['project_ids'])
  need(type(row.get('reports'))is list and len(row['reports'])>=1,'NonemptyActualReportCoverage')
  for ref in row['reports']:need(ref in authority['files'],'PinnedActualProjection')
  controllers[n]=c;intervals.append((a,b,n))
 for g in plan['groups']:
  for dep in g.get('requires',[]):need(controllers[dep]['completed_epoch']<=controllers[g['name']]['started_epoch'],'DependenciesActuallySettled')
 for a,b,n in intervals:
  need(sum(c<=a<d for c,d,_ in intervals)<=3,'Maximum3ActualChildren')
  if n in ['sharedTS18','sharedTS6-all-down','retention100']:need(not any(m!=n and c<b and a<d for c,d,m in intervals),'HeavyActuallySolo')
 need(value['actual_controller_attempts']==value['actual_children']==value['actual_exit0_children']==28 and value['application_fixture_children']==27 and value['offline_controller_children']==1,'Exactly28ActualControllersNoInventedRetries')
 history=read(HERE/'Root-reported-tooling-history.json');need(history['raw_transcript_available']is False and history['application_attempts_added']==0 and history['fixture_retries']is False and history['raw_application_results_changed']is False,'ToolingHistoryNeverFabricatedAsAppFailures')
 return {'nodes':28,'actual_controller_attempts':28,'application_fixture_children':27,'offline_controller_children':1,'tooling_history_separately_reported_without_raw_receipt':True,'observed_maximum_package_children':max(sum(c<=a<d for c,d,_ in intervals)for a,_,_ in intervals),'three_heavy_nodes_solo_within_package_trace':True}

def header(phase,summary,identity):
 with tools('header')as q:
  policy=read(HERE/'tools/header/blocked/policy.json');derived=q.phase(phase,policy,'native');need(derived==phase.get('qualification'),'ExactDerivedHeaderQualification')
  ref=HERE/'tools/header/blocked/retained-source';reference=importlib.import_module('source_reference').validate(read(ref/'worker-header-source-private.json'),read(ref/'summary-private.json'),read(ref/'auth-identity-projection.json'),(ref/'source-web.stderr').read_text(),policy)
  importlib.import_module('origin_coverage').validate(summary['origin_terminal'],summary['fixture_nonce'],terminal=True)
 need(summary.get('passed')is True and summary.get('native')==phase and summary['candidate']['image_id']==IMAGE and summary['Source_VOD_requests_rerun']is False and summary['retained_Source6_offline_reference']==reference,'NativeHeaderOnlyAndExactSource6')
 need(identity['auth_http_status']==200 and identity['identity']=={'id':identity['expected_real_SQL_user_id'],'user_level':10,'stream_limit':0},'RealSQLBearerIdentityProjection')
 need(identity['physical_close']['fd_after']==-1 and identity['physical_close']['original_inode_absent']is True and identity['absolute_observation']['timer_joined']is True,'AuthSocketAndTimerPhysicallyClosed')
 audit=load(HERE/'main4s/validate.py','header_default_audit');audit.audit(summary['default_before'],True);audit.audit(summary['default_after'],True);need(summary['default_before']==summary['default_after']and summary['default_named_tables_sequences_unchanged']is True,'HeaderDefaultNineUnchanged')
 need(summary['full_M4_or_header_parity']is False and derived['Source_invalid_HEAD_observed']is False and derived['Native_invalid_HEAD']==405,'HeaderDiagnosticScope')
 root=read(HERE/'evidence/Main/header/Root-review.json');need(root['passed']is True and root['actual_host_exit']==0 and root['recomputed_qualification']==derived and root['default_named9_structural_equal']is True,'RootHeaderScopedProof');fresh_queries(root['fresh_owned_cleanup_queries'],[summary['project']]);return derived
def notifications(phase,summary):
 projected=copy.deepcopy(phase);auth=projected.get('actual_auth_requests');need(type(auth)is dict and set(auth)=={'admin','standard'},'BothCredentialRoles')
 for role,row in auth.items():
  identity=row['identity_projection'];need(identity=={'id':phase[role+'_user_id'],'user_level':10 if role=='admin'else 1,'stream_limit':0},'MeasuredRoleSQLIdentity')
  # Explicit synthetic adapter for the inherited validator's ID extraction,
  # never published as actual response bytes. Actual full DTO stays hash-bound.
  row['body_hex']=json.dumps({'id':identity['id']}).encode().hex()
 with tools('notifications')as q:
  derived=q.validate(projected,read(HERE/'tools/notifications/blocked/policy.json'));q.audit(summary['default_before']);q.audit(summary['default_after'])
 need(phase['qualification']==derived and summary['passed']is True and summary['candidate']['image_id']==summary['actual_Main_image_id']==IMAGE and summary['actual_Main_Config_User']=='65532:65532','NotificationNormalMainAndDerivedResult')
 need(summary['default_before']==summary['default_after']and summary['default_eleven_named_tables_sequences_unchanged']is True,'NotificationDefaultElevenExact')
 need(summary['Source_notification_requests_executed']is False and summary['default_workers_activated']is False and summary['resource_sampler']is False and summary['full_endpoint_or_performance_acceptance']is False,'NotificationHonestScope')
 root=read(HERE/'evidence/Main/notifications/Root-independent-review.json');need(root['passed']is True and root['recomputed_qualification']==derived and root['default_named11_structural_equality']is True,'RootNotificationScopedProof');fresh_queries(root['fresh_owned_cleanup_queries'],[summary['project']]);return derived

def fresh_queries(rows,projects):
 need(type(rows)is list and len(rows)==3*len(projects),'ThreeFreshQueriesPerProject')
 for project in projects:
  found=[r for r in rows if 'label=com.docker.compose.project='+project in r.get('argv',[])]
  need(len(found)==3,'ExactOwnedProjectQueries')
  need({r['argv'][1]for r in found}=={'container','network','volume'},'AllResourceKinds')
  for r in found:need(r.get('actual_exit',r.get('exit'))==0 and r.get('stdout')=='' and r.get('stderr','')=='','FreshQueryExitAndEmpty')
def catalogue(v):
 need(v['capture_complete']is True and v['observer_valid']is True and v['owned_SQL_unchanged']is True and v['body_status_content_type_parity']is True,'Actual26CaptureHTTPAndSQL')
 rows=v['cases'];need(len(rows)==26 and len({r['name']for r in rows})==26,'Exactly26UniqueCases')
 need([r['status']for r in rows]==[200]*24+[401]*2,'Actual26Statuses')
 for r in rows:
  need(r['method']=='GET' and r['before']==r['after'] and r['before_named11']==r['after_named11'],'ReadOnlyActualSQL')
  need(sha_bytes(bytes.fromhex(r['body_hex']))==r['body_sha256'],'ActualBodySHA')
  need(all(x is True for x in r['parity'].values()),'EveryMeasuredParityPredicate')
 need(v['actual_production_Main']is True and v['full_M7_or_performance']is False and v['info_actions_or_fixed_clock_hook']is False,'ProductionListOnlyScope')
 return {'actual_HTTP_cases':26,'HTTP_qualification':True,'original_controller_host_exit':1,'original_post_Main_backend_observer':'UNMEASURED','full_M7_or_performance':False}
def sha_bytes(v):return hashlib.sha256(v).hexdigest()
def sdk(review,source,selected,copied,host,after,exit_receipt):
 need(review['passed']is True and review['ref']==APP and review['actual_host_wait_exit']==0 and review['actual_compose_exits']==[0,0],'SDKActualExits')
 need(len(source)==12176 and len(selected)==10957 and len(copied)==3015 and len(host)==2 and source==after,'SDKExactFullSelectedCopyHostDistinct')
 for group in [selected,copied,host]:need(all(source.get(n)==h for n,h in group.items()),'SDKInputSubsetExact')
 need(not(set(copied)&set(host)),'SDKHostControlsOutsideCOPY')
 need(review['Rust']=={'passed':698,'failed':0,'ignored':732,'result_blocks':184}and review['Python']['passed']==361 and review['Python']['skipped']==1 and review['TS_helper_byte_exact_cases']==1806,'SDKActualCounts')
 need(exit_receipt['returncode']==0 and exit_receipt['wait_completed']is True,'SDKHostWaitSettled')
 return {'Rust_passed':698,'Rust_ignored':732,'Python_passed':361,'Python_optional_skip':1,'TS_cases':1806,'full_Git_inputs':12176,'selected':10957,'COPY':3015,'host_only':2,'actual_host_exit':0,'performance_transfer':False}
def verify():
 need(sha(HERE/'authority.json')==AUTHORITY_SHA,'ExactSealedPublicationAuthority');a=read(HERE/'authority.json')
 for n,h in a['files'].items():
  rel=Path(n);need(not rel.is_absolute()and '..'not in rel.parts,'RelativePayload');need(sha(HERE/rel)==h,'ExactSelectedPayload:'+n)
 app=read(HERE/'application.json');need(app['image_id']==IMAGE and app['git_revision']==APP and len(app['source_sha256'])==1336 and sum(n.startswith('rust/')for n in app['source_sha256'])==839,'Exact57Application839Plus495Plus2')
 scope=read(HERE/'scope.json');need(scope['Original_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','ExactOriginal');need(scope['performance_gain_transfer']is False and scope['worker_graph_activation']is False and scope['current_callback_union_runtime_transfer']is False,'NoBroaderTransfer')
 for n,m in read(HERE/'tool-closures.json').items():
  root=HERE/'tools'/n;need(sha(root/'selection.json')==m['selection_sha256'],'ExactToolSelection');rows=read(root/'selection.json')['files'];need(len(rows)==m['payload_count'],'ToolCount')
  for r in rows:need(sha(root/'blocked'/r['path'])==r['sha256'],'ExactBlockedTool')
  seal=read(root/'reviewed-seal57.json')
  for rel,h in seal['local_inputs'].items():need(sha(root/'reviewed-inventory'/rel)==h,'FullReviewedBindingInventory')
 sd=sdk(*[read(HERE/('evidence/SDK/'+n))for n in ['offline-exact-review-corrected-label.json','all-source-inputs.json','verifier-selected-inputs.json','SDK-copy-closure.json','host-only-controls.json','all-source-after.json','actual-process-exit.json']])
 m=matrix(read(HERE/'matrix.json'),a)
 n=notifications(read(HERE/'evidence/Main/notifications/notification-Main-private.json'),read(HERE/'evidence/Main/notifications/summary-private.json'))
 v=load(HERE/'main4s/validate.py','main57_retained');data=read(HERE/'main4s/retained.json');four=v.validate(data,read(HERE/'main4s/independent-cleanup-projection.json'),read(HERE/'main4s/coverage-authority.json'))
 root=read(HERE/'evidence/Main/main4s/Root-review.json');need(root['passed']is True and root['actual_host_exit']==0 and all(c['default_named9_tables9sequences_structural_equality']is True for c in root['cases']),'RootMain4sScope')
 fresh_queries(root['fresh_owned_cleanup_queries'],[c['project']for c in data['summary']['cases']])
 h=header(read(HERE/'evidence/Main/header/worker-header-native-private.json'),read(HERE/'evidence/Main/header/summary-private.json'),read(HERE/'header-auth-identity.json'))
 c=catalogue(read(HERE/'evidence/Main/catalogue26/Main26-private.json'))
 summary=read(HERE/'evidence/Main/catalogue26/summary-private.json');need(summary['passed']is False and summary['actual_controller_exit']==1 and summary['default_full6259_unchanged']is True,'OriginalCatalogueHost1Literal')
 original=read(HERE/'evidence/Main/catalogue26/Root-independent-HTTP-and-cleanup-review.json');need(original['passed']is True and original['actual_host_exit']==1 and original['old_observer_gate_promoted']is False,'SeparateHTTPQualificationNeverRelabelsController')
 observer=read(HERE/'evidence/Main/backend-observer/summary-private.json');need(observer['passed']is True and observer['actual_controller_exit']==0 and observer['owned_SQL_backends_absent_after_Main_exit']is True and observer['default_full6259_unchanged']is True,'SeparateObserverNormalStopBackendAbsence')
 backend=read(HERE/'evidence/Main/backend-observer/backends-after-Main-exit.json');need(len(backend)==1 and backend[0]['actual_backends']==[],'ActualAfterExitBackendEmpty')
 return {'passed':True,'scope':'Offline retained evidence; no app execution','SDK':sd,'matrix':m,'notifications_Main9':n,'Main4s':four,'Native_header7':h,'catalogue26':c,'separate_normal_stop_backend_observer_exit':0,'performance_claim':False,'current_callback_union_transfer':False}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
