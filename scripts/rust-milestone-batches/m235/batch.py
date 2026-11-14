"""M2/M3/M5 registry and executable adapter for ONE Root/global dispatcher.
No local parallel pool, application start on import, per-helper cue or compiler
workspace. Global dispatcher supplies actual leases/runner and one batch binding.
"""
import argparse,hashlib,json,re,time
from pathlib import Path
HERE=Path(__file__).resolve().parent
SHA=re.compile('[a-f0-9]{64}')
def need(v,label):
 if not v:raise ValueError(label)
def load_matrix():
 m=json.loads((HERE/'matrix.json').read_text());ids=[n['id']for n in m['nodes']];need(len(ids)==len(set(ids)),'UniqueGlobalIDs');need(len([x for x in ids if x.startswith('package/')])==28,'Exact28SharedIDs')
 for n in m['nodes']:need(all(x in ids or x in m.get('cross_owner_dependencies',[])or x.startswith('external/')for x in n['depends_on']),'KnownDependency')
 return m
def checked_file(value,repo):
 need(type(value)is dict and set(value)>={'path','sha256'},'BoundFileShape');p=Path(value['path']);p=p if p.is_absolute()else repo/p;need(p.is_file()and not p.is_symlink(),'RegularBoundFile');need(SHA.fullmatch(value['sha256'])and hashlib.sha256(p.read_bytes()).hexdigest()==value['sha256'],'ExactBoundFile');return p.resolve()
def candidate_ready(bindings,repo):
 c=bindings.get('candidate');need(type(c)is dict,'NewCandidateUnbound')
 manifest=checked_file(c['manifest'],repo);archive=checked_file(c['archive'],repo);runtime=checked_file(c['runtime_map'],repo);checked_file(c['credit_frontend_UID_review'],repo)
 build=json.loads(manifest.read_text());mapping=json.loads(runtime.read_text())
 need(build.get('passed')is True and build.get('exit_code')==0,'ActualBuildExit0')
 need(build.get('image_id')==c['image_id']and build.get('git_revision')==bindings['integration_git'],'ExactCandidateImageAndRevision')
 need(build.get('source_archive_sha256')==c['archive']['sha256'],'ActualArchiveBinding')
 expected={k:v for k,v in mapping.items()if k.startswith('rust/')};actual={k:v for k,v in build['source_sha256'].items()if k.startswith('rust/')}
 need(actual==expected and bool(expected),'AllRustInputsNotHistoricalMainTransfer')
 need(c.get('image_id','').startswith('sha256:')and SHA.fullmatch(c['image_id'][7:]),'ExactImageID')
 need(c['runtime_map']['sha256']==bindings['integration_source_map_sha256'],'SameIntegratedCandidateNotOldImageOrComponent')
 need(c.get('production_binary_attested')is True,'ActualProductionBinaryRequiredNotFeatureAcceptance')
 # The global runner performs actual Docker image/UID/input proof before dispatch;
 # a component ELF or metadata flag alone does not attest that physical preflight.
 return manifest

def package_argv(node,bindings,repo):
 c=bindings['candidate'];support=Path(bindings['support']['root']);out=Path(bindings['output_root']);need(out.is_absolute(),'ExplicitBatchOutputRoot')
 old_support='/home/justin/github/Dispatcharr/artifacts/rust/image54-package-plan-1/support';old_output='/home/justin/github/Dispatcharr/artifacts/rust/image57-packaged-matrix-1';argv=list(node['template_argv'])
 for i,token in enumerate(argv):
  if token.startswith(old_support+'/'):argv[i]=str(support/token[len(old_support)+1:])
  elif token.startswith(old_output+'/'):argv[i]=str(out/'package'/token[len(old_output)+1:])
 for flag in ['--candidate-build-manifest','--build-manifest','--manifest']:
  if flag in argv:argv[argv.index(flag)+1]=str(checked_file(c['manifest'],repo))
 if '--candidate-image'in argv:argv[argv.index('--candidate-image')+1]=c['image_id']
 if '--source-cases'in argv and bindings.get('retained_sources',{}).get('operator17'):argv[argv.index('--source-cases')+1]=str(checked_file(bindings['retained_sources']['operator17']['corpus'],repo))
 need(not any('image57-'in x for x in argv),'NoOldExecutionPathOrCueInheritance')
 need(Path(argv[1]).is_file(),'ExistingFrozenRunnerRequired')
 return argv

def resolve_node(node_id,bindings,repo=None):
 repo=Path(repo or bindings.get('repo','.')).resolve();matrix=load_matrix();node=next(n for n in matrix['nodes']if n['id']==node_id);result={'id':node_id,'status':'blocked','reasons':[],'depends_on':node['depends_on'],'fixture_group':node['fixture_group'],'exclusive_heavy':node['exclusive_heavy'],'implementation':node['implementation'],'entrypoint':node['entrypoint']}
 try:
  need(matrix['resource_measurement_authorized']is False,'NoAutomaticResources')
  if node_id.startswith('performance/'):
   phase=bindings.get('resource_phase',{});need(phase.get('authorized')is True,'QuietPhaseNotAuthorizedNoAutomaticBenchmark')
   functional=json.loads(checked_file(phase['functional_acceptance'],repo).read_text());need(functional.get('full_batch_functional_passed')is True,'AllRequiredFunctionalCellsBeforeResources')
  if node_id=='package/fresh-Original17capture'and bindings.get('retained_sources',{}).get('operator17'):
   ref=bindings['retained_sources']['operator17'];checked_file(ref['corpus'],repo);checked_file(ref['qualification'],repo)
   need(ref['Original_git']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and ref['selectors_and_support_equal']is True,'ExactRetainedSource17Inputs')
   result.update(ready=False,status='retained_source_authority',satisfies_dependency=True,reasons=[],retained_source=ref)
   return result
  if node['runner']=='existing-package':
   candidate_ready(bindings,repo);support=bindings['support'];checked_file(support['inputs'],repo);checked_file(support['Git_environment'],repo)
   # Per-node review uses the same integrated candidate, keeping strict old
   # guards intact until their explicit new map binding has been reviewed.
   guard=bindings.get('package_bindings',{}).get(node['template_group']);need(guard is not None,'StrictPackageInputRebindingPending');guard_path=checked_file(guard,repo)
   guard_value=json.loads(guard_path.read_text())
   need(guard_value['candidate_image_id']==bindings['candidate']['image_id']and guard_value['runtime_map_sha256']==bindings['integration_source_map_sha256']and guard_value['workload_unchanged']is True,'ActualNewStrictInputBinding')
   argv=package_argv(node,bindings,repo)
   need(guard_value['argv']==argv,'ExactReviewedNodeArgv')
   env=json.loads(checked_file(support['Git_environment'],repo).read_text());need(all(k in env for k in ['GIT_DIR','GIT_WORK_TREE','GIT_INDEX_FILE','DOCKER_HOST','DOCKER_CONFIG','PATH']),'BoundGitAndToolingEnvironment')
   result.update(argv=argv,cwd=str(repo),environment_overlay=env,strip_inherited_prefixes=['GIT_','DOCKER_','COMPOSE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','DATABASE_','FIXTURE_','RUST_'],status='not_run',ready=True)
   if node_id=='package/complete-refresh':result['same_global_lease_as']='package/complete-refresh-original';result['reference_max_age_seconds']=900
   if node_id=='package/guide-grid-packaged':result['UTC_minute_window']=[0,35]
  else:
   workflow=bindings.get('workflows',{}).get(node_id);need(workflow is not None,'WorkflowAdapterOrFixtureBindingPending')
   if node.get('performance')or node['entrypoint'].startswith('production')or node['entrypoint'].startswith('unchanged'):candidate_ready(bindings,repo)
   else:
    component=bindings.get('component');need(component is not None or node_id.startswith('source/'),'CompiledCurrentComponentUnbound')
    if component:checked_file(component['proof'],repo);need(component['runtime_map_sha256']==bindings['integration_source_map_sha256'],'NoHistoricalELFTransfer')
   argv=workflow['argv'];need(type(argv)is list and len(argv)>=2 and all(type(x)is str and x for x in argv),'ExactStructuredArgv')
   checked_file(workflow['controller'],repo);checked_file(workflow['fixture_inputs'],repo);need(str(Path(argv[1]).resolve())==str(checked_file(workflow['controller'],repo)),'ActualBoundEntrypoint')
   need(workflow.get('cleanup_contract')is not None and workflow.get('max_seconds',0)>0,'FiniteCleanupAndBudget')
   result.update(argv=argv,cwd=str(repo),status='not_run',ready=True)
   if node['implementation']in ['501-unimplemented','not-assembled']:
    need(workflow.get('collect_known_failure_statuses')is True,'ProbeKnownMissingRequiredRouteInsteadOfSkip')
    result['expected_current_failure']=True
  result['performance']=node.get('performance',False)
  result['global_max_functional_fixtures']=3;result['global_dispatcher_only']=True
 except (ValueError,KeyError,StopIteration,TypeError,FileNotFoundError)as e:result.update(ready=False);result['reasons'].append(str(e))
 return result

def readiness(bindings,repo=None):
 nodes=[resolve_node(n['id'],bindings,repo)for n in load_matrix()['nodes']]
 return {'batch':load_matrix()['batch'],'nodes':nodes,'ready':sum(n['ready']for n in nodes),'blocked':sum(n['status']=='blocked'for n in nodes),'retained_source_dependencies':sum(n['status']=='retained_source_authority'for n in nodes),'milestone_passed':False,'global_dispatcher_only':True}

def run_node(node_id,bindings,global_context,runner,repo=None):
 """Called by Root's single global scheduler, not by another thread pool.
 runner(job) owns bounded subprocess waits, physical halt/cleanup and raw journal.
 """
 job=resolve_node(node_id,bindings,repo);need(job.get('ready')is True,'NodeNotReady')
 need(global_context['batch']==load_matrix()['batch']and global_context['node_id']==node_id,'OneGlobalBatchLeaseIdentity')
 need(global_context['active_functional_fixtures']<=3 and global_context['active_functional_fixtures']>=1,'GlobalThreeNotThreePerOwner')
 if job.get('performance'):need(global_context.get('resource_quiet')is True and global_context['active_functional_fixtures']==1,'ActualGlobalQuietResourceLease')
 if job['exclusive_heavy']:need(global_context['active_functional_fixtures']==1 and global_context['heavy_exclusive']is True,'HeavyActuallySolo')
 need(global_context.get('candidate_image_id')==(bindings.get('candidate')or{}).get('image_id')or node_id.startswith('source/'),'SingleCandidateIdentity')
 # Global dispatcher persists an attempted record before invoking this function,
 # enforces exactly one attempt and continues independent nodes after clean fail.
 need(global_context['attempt']==1 and global_context['claim_persisted']is True,'SingleBatchAttemptJournalRequired')
 if '@GLOBAL_CONTEXT@'in job.get('argv',[]):
  need(global_context.get('context_path')is not None,'GlobalAttemptContextFileRequired')
  job['argv']=[str(global_context['context_path'])if v=='@GLOBAL_CONTEXT@'else v for v in job['argv']]
 result=runner(job);need(result['status']in ['passed','failed','blocked','not_run'],'NoSkipAsPass')
 if result['status']=='passed':need(result.get('actual_exit')==0 and result.get('cleanup_verified')is True and result.get('semantic_assertions_passed')is True,'ExitAloneNotAcceptance')
 return result

def aggregate(results):
 """A retained/component result cannot fill a current production-Main cell."""
 m=load_matrix();index={r['id']:r for r in results};need(len(index)==len(results),'NoDuplicateResultIDs');out={}
 spec={n['id']:n for n in m['nodes']}
 for id,row in index.items():
  need(id in spec,'UnknownResultID')
  if row.get('status')=='passed':
   need(row.get('actual_exit')==0 and row.get('cleanup_verified')is True and row.get('semantic_assertions_passed')is True,'ActualResultPredicates')
   need(row.get('entrypoint')==spec[id]['entrypoint'],'NoComponentAsMainAcceptance')
   if 'Main'in spec[id]['entrypoint']or'unchanged'in spec[id]['entrypoint']:need(row.get('candidate_image_id','').startswith('sha256:'),'ActualMainCandidateBinding')
   need(row.get('known_unimplemented_required_cells',[])==[],'Known501OrUnimplementedCannotPass')
 for milestone,ids in m['criteria'].items():
  missing=[i for i in ids if i not in index or index[i].get('status')!='passed'];out[milestone]={'passed':not missing,'unpassed_required':missing}
 return {'milestones':out,'full_batch_passed':all(v['passed']for v in out.values()),'actual_results':len(results),'unexecuted_or_blocked_cannot_pass':True}
def main():
 p=argparse.ArgumentParser();p.add_argument('--bindings',type=Path);p.add_argument('--repo',type=Path,default=Path.cwd());p.add_argument('--matrix',action='store_true');a=p.parse_args()
 if a.matrix:print(json.dumps(load_matrix(),indent=2));return
 b={}if a.bindings is None else json.loads(a.bindings.read_text());print(json.dumps(readiness(b,a.repo),indent=2))
if __name__=='__main__':main()
