"""Offline evidence integrity and finite Main checks; never dispatch applications."""
import contextlib,copy,hashlib,importlib,importlib.util,json,math,re,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
AUTHORITY_SHA='ea1b25079f1c69237c75f2b4471bb817239697529fa2b7cd8f7a457e3eee89b1'
IMAGE='sha256:120df2f930a7f4d2738a04fbd793c16b230b7a80114f4d6b03f670244940a014'
APP='aca6d900ad13700ab86b2898134a61e4dba57bc1'
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
  need(v['cleanup']['verified']is True,'ReviewedPhysicalCleanup')
  if n=='strict-typed154':need(v['cleanup']['scope']=='offline-no-resources','PureNodeNoFixture')
  else:cleanup(read(HERE/row['cleanup']),v['cleanup']['project_ids'])
  need(type(row.get('reports'))is list and len(row['reports'])>=1,'NonemptyActualReportCoverage')
  for ref in row['reports']:need(ref in authority['files'],'PinnedActualProjection')
  controllers[n]=c;intervals.append((a,b,n))
 for g in plan['groups']:
  for dep in g.get('requires',[]):need(controllers[dep]['completed_epoch']<=controllers[g['name']]['started_epoch'],'DependenciesActuallySettled')
 for a,b,n in intervals:
  need(sum(c<=a<d for c,d,_ in intervals)<=3,'Maximum3ActualChildren')
  if n in ['sharedTS18','sharedTS6-all-down','retention100']:need(not any(m!=n and c<b and a<d for c,d,m in intervals),'HeavyActuallySolo')
 need(value['all_actual_fixture_attempts']==value['actual_children']==value['actual_exit0_children']==28,'Exactly28ActualControllersNoInventedRetries')
 history=read(HERE/'Root-reported-tooling-history.json');need(history['raw_transcript_available']is False and history['application_attempts_added']==0 and history['fixture_retries']is False and history['raw_application_results_changed']is False,'ToolingHistoryNeverFabricatedAsAppFailures')
 return {'nodes':28,'actual_fixture_attempts':28,'tooling_history_separately_reported_without_raw_receipt':True,'observed_maximum_package_children':max(sum(c<=a<d for c,d,_ in intervals)for a,_,_ in intervals),'three_heavy_nodes_solo_within_package_trace':True}

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
 cleanup(read(HERE/'evidence/Main/header/independent-cleanup.json'),[summary['project']]);return derived
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
 root=read(HERE/'evidence/Main/notifications/Root-review.json');need(root['passed']is True and root['actual_host_exit']==0 and root['normal_Main9_recomputed']==derived and root['statuses']==[201,200,200,200,400,403,400,200,204]and root['independent_owned_inventories_empty']is True,'RootIndependentMain9');return derived
def verify():
 need(sha(HERE/'authority.json')==AUTHORITY_SHA,'ExactSealedPublicationAuthority');authority=read(HERE/'authority.json')
 for name,h in authority['files'].items():
  rel=Path(name);need(not rel.is_absolute()and'..'not in rel.parts,'RegularRelativePayload');need(sha(HERE/rel)==h,'ExactSelectedPayload:'+name)
 closures=read(HERE/'tool-closures.json')
 for name,meta in closures.items():
  root=HERE/'tools'/name;need(sha(root/'selection.json')==meta['selection_sha256'],'ExactMainToolSelection');rows=read(root/'selection.json')['files'];need(len(rows)==meta['payload_count'],'ExactMainToolCount')
  for row in rows:need(sha(root/'blocked'/row['path'])==row['sha256'],'ExactMainToolBytes:'+name+':'+row['path'])
 app=read(HERE/'application.json');need(app['image_id']==IMAGE and app['git_revision']==APP and len(app['source_sha256'])==1291 and sum(p.startswith('rust/')for p in app['source_sha256'])==794,'Exact55App794')
 support=read(HERE/'package-definition/support-inputs.json');need(support['revision']=='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'and support['file_count']==len(support['files'])==6148,'ExactHistoricalSupport6148')
 scope=read(HERE/'scope.json');need(scope['Original_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and scope['worker_graph_activation']is False and scope['performance_gain_transfer']is False and scope['full_M2_M3_M4_M5_M6_M7_M8_M9_acceptance']is False,'OriginalAndNoBroaderAcceptance')
 m=matrix(read(HERE/'matrix.json'),authority)
 n=notifications(read(HERE/'evidence/Main/notifications/notification-Main-private.json'),read(HERE/'evidence/Main/notifications/summary-private.json'))
 return {'passed':True,'scope':'PARTIAL offline packet stage: package28 and notificationMain9 only; Main4s/header pending, no publication-complete claim','matrix':m,'notifications_Main9':n,'image':IMAGE,'Main4s':scope['Main4s_status'],'header':scope['header_Main_status'],'performance_claim':False,'full_milestone_acceptance':False}
if __name__=='__main__':print(json.dumps(verify(),indent=2))
