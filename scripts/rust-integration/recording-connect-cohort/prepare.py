#!/usr/bin/env python3
"""Portable Source-first recording Connect cohort preparation and bounded launch.
Original Dispatcharr and dependency authors retain application/model/task credit.
No application starts unless the explicit `run` command is requested.
"""
import argparse,fcntl,hashlib,importlib.util,io,json,os,shutil,signal,subprocess,sys,tarfile,time
from pathlib import Path
sys.dont_write_bytecode=True
os.environ['PYTHONDONTWRITEBYTECODE']='1'
HERE=Path(__file__).resolve().parent
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE_SHA='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
EVENT='recording-plugin-characterization'
NATIVE=[EVENT,'recording-connect-boundaries','m8-actions','normal-backup-restore','normal-backup-restore-partial-admission','grouped-m6','nonempty-restart']

def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def load(p):return json.loads(Path(p).read_text())
def save(p,v):
 p=Path(p);p.parent.mkdir(parents=True,exist_ok=True);temporary=p.with_suffix(p.suffix+'.tmp');temporary.write_text(json.dumps(v,indent=2,sort_keys=True)+'\n');temporary.chmod(0o600);temporary.replace(p)
def module(p,name):
 sys.dont_write_bytecode=True;os.environ['PYTHONDONTWRITEBYTECODE']='1'
 spec=importlib.util.spec_from_file_location(name,p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def support_inputs(repo):
 files=[]
 for name in ['scripts/rust-playback-baseline','scripts/rust-baseline']:
  files.extend(p for p in(repo/name).rglob('*')if p.is_file()and'__pycache__'not in p.parts)
 files.extend(repo/p for p in ['scripts/rust-integration/fixture_ports.py','scripts/rust-integration/seed_accounts.py','docker/compose.rust-playback.yml','scripts/rust-milestone-batches/coordinator.py'])
 return files

def prepare_catalogue(args,repo,out,build,archive,source):
 fourteen=args.workflow=='catalogue-fourteen';thirteen=args.workflow in('catalogue-thirteen','catalogue-fourteen');twelve=args.workflow in('catalogue-twelve','catalogue-thirteen','catalogue-fourteen');nine=args.workflow in('catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen');eight=args.workflow in('catalogue-eight','catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen');seven=args.workflow in('catalogue-seven','catalogue-eight','catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen');six=args.workflow in('catalogue-six','catalogue-seven','catalogue-eight','catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen')
 tools=repo/('scripts/rust-integration/catalogue-settings-cohort'if fourteen else'scripts/rust-integration/catalogue-language-cohort'if thirteen else'scripts/rust-integration/catalogue-domains-cohort'if twelve else'scripts/rust-integration/catalogue-identity-cohort'if nine else'scripts/rust-integration/catalogue-shared-cohort'if eight else'scripts/rust-integration/catalogue-metadata-cohort'if seven else'scripts/rust-integration/catalogue-extended-cohort'if six else'scripts/rust-integration/catalogue-comskip-cohort')
 manifest=load(tools/'payloads.json')
 if fourteen:
  assert manifest['selected_runtime_payloads']==len(manifest['files'])==42 and manifest['parent_qualified_public_runtime_payloads']==40 and manifest['parent_frozen_driver_total_payloads']==182 and manifest['validation_mode']=='DIRECT_CLEAN_PUBLIC14_NO_PRIVATE14_RUN'
 else:
  assert manifest['selected_runtime_payloads']==(40 if thirteen else 38 if twelve else 35 if nine else 33 if eight else 31 if seven else 29 if six else 26)and manifest['frozen_driver_total_payloads']==(182 if thirteen else 180 if twelve else 177 if nine else 175 if eight else 173 if seven else 171 if six else 168)
 for row in manifest['files']:assert sha(tools/row['path'])==row['sha256'],('FrozenPublicHelperChanged',row['path'])
 retained=load(tools/'retained-Source-refs.json');bound={};authority=[]
 for group,rows in retained.items():
  bound[group]={}
  for name,ref in rows.items():
   path=repo/ref['path'];assert path.is_file()and sha(path)==ref['sha256'],('ExactPublicSourceAuthorityRequired',group,name)
   bound[group][name]={'path':str(path.resolve()),'sha256':ref['sha256']};authority.append(path)
 assert bound['Comskip']['input']['sha256']=='ebcc185d00fb24ab467c0605639415061f3001ff09e1381a70420fd1aa72fd3d'
 assert Path(bound['Comskip']['input']['path']).stat().st_size==719267
 e=load(bound['Source58']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert load(bound['Source58']['report']['path'])['status']=='passed'and load(bound['Source58']['cleanup']['path'])['all_owned_resources_absent']
 e=load(bound['VODRepeat']['execution']['path']);assert e['actual_exit']==1 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert load(bound['VODRepeat']['report']['path'])['status']=='failed'and load(bound['VODRepeat']['offline_correction']['path'])['offline_corrected_capture_qualified']is True
 e=load(bound['Comskip']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert load(bound['Comskip']['report']['path'])['status']=='passed'and load(bound['Comskip']['cleanup']['path'])['exact_owned_absence']
 e=load(bound['Series']['execution']['path']);assert e['actual_exit']==1 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert load(bound['Series']['report']['path'])['status']=='passed'and load(bound['Series']['qualification']['path'])['Source_Series_context_capture_qualified']is True and load(bound['Series']['Root_cleanup']['path'])['passed']
 if six:
  proof=load(bound['ProviderInfo5']['authority']['path']);assert proof['capture_passed']and proof['restored18']and proof['sequences_restored']and proof['excerpt_sha256']==bound['ProviderInfo5']['cases']['sha256']
  assert len(proof['cases'])==5 and all(row['historical_native_HTTP_control_passed']for row in proof['cases'])
  e=load(bound['Advanced']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
  q=load(bound['Advanced']['qualification']['path']);assert q['Source_advanced_Series_context_capture_qualified']and q['actual_Source_cases_passed']==4 and q['actual_controller_exit']==0
  assert load(bound['Advanced']['report']['path'])['status']=='passed'and load(bound['Advanced']['cleanup']['path'])['exact_owned_absence']and load(bound['Advanced']['Root_cleanup']['path'])['passed']
  authority.append(repo/'scripts/rust-integration/provider-info-source5/redaction-provenance.json')
 public=[p for p in tools.rglob('*')if p.is_file()and'__pycache__'not in p.parts]
 inputs={str(p.resolve()):sha(p)for p in [HERE/'prepare.py',*public,*support_inputs(repo),*[repo/name for name in build['source_sha256']if name.startswith('rust/')],archive,args.build.resolve(),*authority]}
 binding={'status':'prepared_not_run','performance':False,'full_milestone_accepted':False,'candidate_image_id':build['image_id'],'candidate_git_revision':build['git_revision'],'candidate_source_tree_sha256':build['source_tree_sha256'],'candidate_build':str(args.build.resolve()),'docker':args.docker,'support_root':str(repo),'functional_pids_limit':1600,'functional_budget_scope':'Functional four-context cohort only; no performance-equivalence claim','fixture_output':str(out/'fixtures'),'Original_archive':str(archive),'Original_source_root':str(out/'Original-source'),'Original_source_sha256':source,'adapter_sha256':sha(tools/'fixture_adapter.py'),'instrumented_fixture_adapter':str(tools/'fixture_adapter.py'),'retained_Source_playback':str(out/'unmeasured-exceptional-playback.json'),'retained_source_contexts':{},'retained_native_contexts':{},'restore_callbacks':{},'retained_source_vod_ingestion':bound['Source58'],'retained_source_vod_repeat':bound['VODRepeat'],'retained_source_comskip':bound['Comskip'],'retained_source_vod_series':bound['Series'],'inputs':[{'path':p,'sha256':h}for p,h in sorted(inputs.items())]}
 if six:
  binding['retained_source_provider_info']=bound['ProviderInfo5']
  binding['retained_source_vod_series_advanced']=bound['Advanced']
  binding['functional_budget_scope']='Functional six-context cohort only; no performance-equivalence claim'
 if seven:
  q=load(bound['DenseList']['qualification']['path']);assert q['Source_dense_list_context_capture_qualified']and q['actual_Source_cases_passed']==6 and q['actual_controller_exit']==0
  e=load(bound['DenseList']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
  assert load(bound['DenseList']['cleanup']['path'])['exact_owned_absence']and load(bound['DenseList']['Root_cleanup']['path'])['passed']
  binding['retained_source_vod_dense_list']=bound['DenseList']
  labels=('fresh-dense','identical-repeat','nonblank-metadata-update','sparse-blank-and-logo-clear','duplicate-representative-and-reuse','last-owner-prune')
  binding['retained_source_vod_dense_list_SQL']={label:{end:bound['DenseListSQL'][label+'_'+end]for end in('before','after')}for label in labels}
  binding['retained_source_vod_dense_list_http_origin']=bound['DenseListOrigin']['proof']
  binding['functional_budget_scope']='Functional seven-context cohort only; no performance-equivalence claim'
 if eight:
  q=load(bound['Shared']['qualification']['path']);assert q['Source_shared_metadata_context_capture_qualified']and q['actual_Source_cases_passed']==4 and q['actual_controller_exit']==0
  e=load(bound['Shared']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
  assert load(bound['Shared']['cleanup']['path'])['exact_owned_absence']and load(bound['Shared']['Root_cleanup']['path'])['passed']
  binding['retained_source_vod_shared_metadata']=bound['Shared']
  labels=('shared-A-initial','shared-B-update','shared-A-prune','shared-B-prune','declared-after-A-seed')
  binding['retained_source_vod_shared_metadata_SQL']={label:{end:bound['SharedSQL'][label+'_'+end]for end in('before','after')}for label in labels}
  binding['retained_source_vod_shared_metadata_http_origin']=bound['SharedOrigin']
  binding['functional_budget_scope']='Functional eight-context cohort only; no performance-equivalence claim'
 if nine:
  q=load(bound['ExternalIdentity']['qualification']['path']);assert q['Source_external_identity_context_capture_qualified']and q['actual_Source_cases_passed']==9 and q['actual_controller_exit']==0
  e=load(bound['ExternalIdentity']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
  assert load(bound['ExternalIdentity']['cleanup']['path'])['exact_owned_absence']and load(bound['ExternalIdentity']['Root_cleanup']['path'])['passed']
  binding['retained_source_vod_external_identity']=bound['ExternalIdentity']
  phases=('fresh-priority','duplicate-ID-occurrences','same-provider-ID-repeat','external-ID-name-year-change','alias-and-zero-precedence','changed-primary-existing-target','sparse-ID-omission','secondary-ID-recheck','duplicate-secondary-conflict')
  labels=phases+tuple(label+'-setup'for label in phases)
  binding['retained_source_vod_external_identity_SQL']={label:{end:bound['ExternalIdentitySQL'][label+'_'+end]for end in('before','after')}for label in labels}
  binding['retained_source_vod_external_identity_http_origin']=bound['ExternalIdentityOrigin']
  binding['functional_budget_scope']='Functional nine-context cohort only; no performance-equivalence claim'
 if twelve:
  q=load(bound['CatalogueDomains17']['qualification']['path']);assert q['Source_three_domains_component_capture_qualified']and q['aggregate_actual_Source_cases_passed']==17 and q['single_controller_all17_passed']is False and q['literal_grouped_controller_exit']==1 and q['literal_scale_controller_exit']==0 and q['known12_Source_roots_replayed']is False
  source_folder=Path(bound['CatalogueDomains17']['index']['path']).parent
  for controller,code in [('grouped-attempt1',1),('scale-only',0)]:
   folder=source_folder/'controllers'/controller;e=load(folder/'execution.json');assert e['actual_exit']==code and e['failure']is None and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']and load(folder/'Root-independent-cleanup.json')['passed']
  binding['retained_source_vod_catalogue_domains']=bound['CatalogueDomains17']
  binding['functional_budget_scope']='Functional twelve-context cohort only; no performance-equivalence claim'
 if thirteen:
  q=load(bound['CategoryLanguage']['qualification']['path']);assert q['Source_category_language_capture_qualified']and q['actual_Source_cases_passed']==11 and q['actual_controller_exit']==0
  e=load(bound['CategoryLanguage']['execution']['path']);assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']and load(bound['CategoryLanguage']['Root_cleanup']['path'])['passed']
  binding['retained_source_vod_category_language']=bound['CategoryLanguage']
  binding['retained_source_vod_category_language_tag_receivers']=bound['CategoryLanguageReceivers']['proof']
  binding['functional_budget_scope']='Functional thirteen-context cohort only; no performance-equivalence claim'
 if fourteen:
  q=load(bound['LiveSettings12']['qualification']['path']);assert q['Source_twelve_manual_cells_qualified']and q['single_whole_twelve_cell_controller_exit0']is False and q['first_controller_exit']==1 and q['remaining_controller_exit']==0 and q['remaining_new_manual_cells_passed']==11 and q['known_combined_business_replayed']is False
  positive=bound['PositiveComskip6'];assert set(positive)=={'qualification','business','content','assets','assets_index'}
  pq=load(positive['qualification']['path']);assert pq['actual_Source_business_leaves']==6 and pq['manual_mark_repeat_cut_repeat_qualified']and pq['natural_CUT_actual_controller_exit']==0 and not pq['whole_six_cell_controller_pass_exists']
  source_root=Path(positive['qualification']['path']).parent
  # Every public business/content leaf and the portable exact assets are bound,
  # not merely their top-level module names. Source private captures are never read.
  for root,index_path in[(source_root,source_root/'index.json'),(Path(positive['assets_index']['path']).parent,Path(positive['assets_index']['path']))]:
   public_index=load(index_path);inputs[str(index_path.resolve())]=sha(index_path)
   for row in public_index['files']:
    path=(root/row['path']).resolve();assert path.is_relative_to(root.resolve())and path.is_file()and path.stat().st_size==row['bytes']and sha(path)==row['sha256'],('ExactPositiveComskipPublicFile',row['path'])
    inputs[str(path)]=row['sha256']
  binding['retained_source_positive_comskip']=positive
  binding['inputs']=[{'path':p,'sha256':h}for p,h in sorted(inputs.items())]
  binding['retained_source_live_vod_settings']=bound['LiveSettings12'];binding['positive_comskip_epochs']=True;binding['functional_budget_scope']='Functional fourteen-context public cohort with retained negative3 plus manualpositive4 and naturalautomatic2 Comskip epochs, twelve contiguous Native settings calls, no Source replay/private14 duplicate/performance claim'
 plan={'state':'UNQUALIFIED_PUBLIC_EXPORT_PREPARED','workflow':args.workflow,'repo':str(repo),'cohort_root':str(tools),'output':str(out),'base_binding':binding,'source_contexts':[],'native_contexts':['vod-repeat','vod-series','comskip-manual','vlc-live']+(['vod-series-info','vod-series-advanced']if six else[])+(['vod-dense-list']if seven else[])+(['vod-shared-metadata']if eight else[])+(['vod-external-identity']if nine else[])+(['vod-international-catalogue','vod-ordered-title-identity','vod-catalogue-scale']if twelve else[])+(['vod-category-language-lifecycle']if thirteen else[])+(['live-vod-group-settings']if fourteen else[]),'Docker_host':args.docker_host,'source_seconds':1200,'native_seconds':3300 if fourteen else 1800 if six else 1200,'cleanup_seconds':90,'qualification':('42 runtime payloads; unchanged old13 plus Source12 settings and six retained positive Comskip leaves across declared Main epochs; direct clean public14 required, no private duplicate. 'if fourteen else'40 portable payloads; unchanged Source language oracle plus actual receiver receipts; clean public13 execution required. 'if thirteen else'38 portable payloads; Source17 gzip authority; clean public12 execution required. 'if twelve else'35 portable payloads; strict safe public external-ID Source projection; clean public9 execution required. 'if nine else'33 portable payloads; strict safe public shared Source projection; clean public8 execution required. 'if eight else'31 portable payloads; strict safe public Source projection; clean public7 execution required. 'if seven else'Exact29 frozen payload subset; correctedprivate6 +cleanpublic6 required. 'if six else'Exact26 frozen payload subset; ')+'public closure still requires actual clean-checkout validation. Retained Source captures are reused, including literal failed VOD controller/offline correction.','credit':'Original Dispatcharr and dependencies retain all application/model/task/protocol/algorithm credit.'}
 save(out/'plan.json',plan);print(json.dumps({'plan':str(out/'plan.json'),'state':plan['state'],'applications_started':0,'native_contexts':plan['native_contexts']}))

def prepare(args):
 repo=args.repo.resolve();out=args.output.resolve();assert repo==HERE.parents[2],'Run the tools from their matching repository checkout'
 assert not out.exists(),'FreshOutputRequired';out.mkdir(parents=True,mode=0o700)
 build=load(args.build);assert build['passed']and build['exit_code']==0 and build['image_id'].startswith('sha256:')and len(build['image_id'])==71
 # Preserve image/code identity without requiring another developer's private SDK directory.
 for name,h in build['source_sha256'].items():
  if name.startswith('rust/'):assert sha(repo/name)==h,('RustSourceDoesNotMatchImage',name)
 archive=out/'Original.tar'
 if args.source_archive:
  assert sha(args.source_archive)==ARCHIVE_SHA;shutil.copyfile(args.source_archive,archive)
 else:
  with archive.open('xb')as stream:subprocess.run(['git','archive','--format=tar',ORIGINAL],cwd=repo,stdout=stream,check=True,timeout=60)
 assert sha(archive)==ARCHIVE_SHA,'PinnedOriginalArchiveMismatch; provide --source-archive from the pinned Original commit'
 source={}
 with tarfile.open(archive)as reader:
  for item in reader:
   if item.isfile():
    path=Path(item.name);assert not path.is_absolute()and'..'not in path.parts;source[item.name]=hashlib.sha256(reader.extractfile(item).read()).hexdigest()
 if args.workflow in('catalogue-series-comskip-vlc','catalogue-six','catalogue-seven','catalogue-eight','catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen'):
  assert not args.all_native,'all-native applies only to the recording Connect workflow'
  prepare_catalogue(args,repo,out,build,archive,source);return
 public=[p for p in HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts]
 inputs={str(p):sha(p)for p in [*public,*support_inputs(repo),*[repo/name for name in build['source_sha256']if name.startswith('rust/')],archive,args.build.resolve(),repo/'rust/crates/job-transport/datasets/source-default-stats-wire.json']}
 binding={'status':'prepared_not_run','performance':False,'full_milestone_accepted':False,'candidate_image_id':build['image_id'],'candidate_git_revision':build['git_revision'],'candidate_source_tree_sha256':build['source_tree_sha256'],'candidate_build':str(args.build.resolve()),'docker':args.docker,'support_root':str(repo),'functional_pids_limit':1600,'functional_budget_scope':'Functional cohort only; no performance-equivalence claim','fixture_output':str(out/'fixtures'),'Original_archive':str(archive),'Original_source_root':str(out/'Original-source'),'Original_source_sha256':source,'adapter_sha256':sha(HERE/'fixture_adapter.py'),'instrumented_fixture_adapter':str(HERE/'fixture_adapter.py'),'m6_adapter':str(HERE/'m6_adapter.py'),'m6_adapter_sha256':sha(HERE/'m6_adapter.py'),'normal_backup_case':str(HERE/'cohort/normal_backup_case.py'),'normal_backup_case_sha256':sha(HERE/'cohort/normal_backup_case.py'),'backup_physical_case_sha256':sha(HERE/'cohort/backup_physical_case.py'),'retained_Source_playback':str(out/'unmeasured-exceptional-playback.json'),'retained_source_contexts':{},'retained_native_contexts':{},'restore_callbacks':{},'inputs':[{'path':p,'sha256':h}for p,h in sorted(inputs.items())]}
 for context,name,backends in [('normal-backup-restore','backup_restore_cases.py',['Original','Native']),('normal-backup-restore-partial-admission','backup_restore_admission_case.py',['Native'])]:
  p=HERE/'cohort'/name;binding['restore_callbacks'][context]={'path':str(p),'sha256':sha(p),'backends':backends}
 contexts=NATIVE if args.all_native else[EVENT]
 plan={'state':'UNQUALIFIED_PUBLIC_EXPORT_PREPARED','repo':str(repo),'output':str(out),'base_binding':binding,'source_contexts':[EVENT],'native_contexts':contexts,'Docker_host':args.docker_host,'source_seconds':1200,'native_seconds':3600 if args.all_native else 600,'cleanup_seconds':90,'qualification':'Portable export still requires actual affected Source/Native validation. This is not byte-exact reproduction of historical private helpers.','credit':'Original Dispatcharr and dependencies retain application/model/task/protocol/algorithm credit.'}
 save(out/'plan.json',plan);print(json.dumps({'plan':str(out/'plan.json'),'state':plan['state'],'applications_started':0,'native_contexts':contexts}))

def uid_probe(plan,coordinator):
 out=Path(plan['output'])/'uid';out.mkdir();base=plan['base_binding'];tools=Path(plan.get('cohort_root',str(HERE)));cidfile=out/'probe.cid';name='dispatcharr-public-connect-uid-'+os.urandom(6).hex();expected={str(Path(row['path']).relative_to(tools)):row['sha256']for row in base['inputs']if Path(row['path']).is_relative_to(tools)}
 guest="import hashlib,json,os;from pathlib import Path;assert os.geteuid()==65532;names="+repr(sorted(expected))+";print(json.dumps({'UID':os.geteuid(),'files':{n:hashlib.sha256((Path('/tools')/n).read_bytes()).hexdigest()for n in names}}))"
 argv=[base['docker'],'run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges','--user','65532:65532','--pids-limit','16','--memory','64m','--name',name,'--cidfile',str(cidfile),'--mount','type=bind,src='+str(tools)+',dst=/tools,readonly','--entrypoint','/usr/local/bin/dispatcharr-python',base['candidate_image_id'],'-I','-B','-c',guest]
 save(out/'identity.json',{'owned_probe_name':name,'CID_file':str(cidfile),'image_id':base['candidate_image_id'],'app_started':False})
 env=dict(os.environ)
 if plan['Docker_host']:env['DOCKER_HOST']=plan['Docker_host']
 receipt=None;absent=False
 try:
  receipt=coordinator.command(argv,{'cwd':plan['repo'],'environment':env,'output_bytes':262144},out,'execution',30);save(out/'execution.json',receipt)
  assert receipt['actual_exit']==0 and coordinator.settled(receipt)and receipt['failure']is None
  observed=load(receipt['stdout']);assert observed['UID']==65532 and observed['files']==expected
 finally:
  if cidfile.exists():
   cid=cidfile.read_text().strip();assert len(cid)==64 and all(c in'0123456789abcdef'for c in cid)
   state=subprocess.run([base['docker'],'container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'],env=env,capture_output=True,text=True,timeout=15);assert state.returncode==0
   if state.stdout.strip():subprocess.run([base['docker'],'container','rm','--force',cid],env=env,check=True,capture_output=True,timeout=15)
   final=subprocess.run([base['docker'],'container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'],env=env,capture_output=True,text=True,timeout=15);absent=final.returncode==0 and not final.stdout.strip()
  save(out/'cleanup.json',{'exact_owned_CID':cidfile.read_text().strip()if cidfile.exists()else None,'exact_CID_absent':absent,'app_started':False})
  if not absent:
   save(Path(plan['repo'])/'.dispatcharr-recording-cohort-cleanup-required.json',{'owned_run':plan['output'],'stage':'UID probe','exact_owned_CID':cidfile.read_text().strip()if cidfile.exists()else None,'owned_probe_name':name,'exact_CID_absent':False,'missing_CID_is_not_absence':True,'reason':'UID probe cleanup was not independently established; inspect the exact owned CID/name before clearing this marker.'})
 assert absent
 return {'passed':True,'actual_UID':65532,'exact_CID_absent':True,'files_hashed':len(expected),'image_id':base['candidate_image_id']}

def run(args):
 plan=load(args.plan.resolve());out=Path(plan['output']);repo=Path(plan['repo']);assert not(out/'run-started.json').exists(),'OneFreshAttemptPerPlan'
 # This is a real local inter-process lease, not a fabricated global scheduler receipt.
 lock_path=repo/'scripts/rust-milestone-batches/.dispatcher.lock'
 with lock_path.open('a+')as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
  assert not(repo/'.dispatcharr-recording-cohort-cleanup-required.json').exists(),'PreviousOwnedCleanupMustBeVerifiedFirst'
  save(out/'run-started.json',{'pid':os.getpid(),'actual_exclusive_flock':str(lock_path),'claim_persisted':True,'heavy_exclusive':True,'scope':'One local functional cohort. Other external CPU/media workloads must not be used for a performance claim.'})
  for row in plan['base_binding']['inputs']:
   path=Path(row['path']);assert path.is_file()and sha(path)==row['sha256'],('PreparedInputChanged',str(path))
  bound=plan['base_binding'];build=load(bound['candidate_build'])
  assert build['passed']and build['exit_code']==0 and build['image_id']==bound['candidate_image_id']and build['git_revision']==bound['candidate_git_revision']and build['source_tree_sha256']==bound['candidate_source_tree_sha256'],'PreparedCandidateIdentityChanged'
  coordinator=module(repo/'scripts/rust-milestone-batches/coordinator.py','portable_actual_coordinator');cancelled=[]
  def stop(signum,frame):cancelled.append(signum)
  previous={sig:signal.signal(sig,stop)for sig in(signal.SIGINT,signal.SIGTERM)}
  combined=[];safe_cleanup=False;backend_started=None;launcher_error=None;uid_attempted=False;uid_safe=False
  expected_backends=[backend for backend,key in [('Original','source_contexts'),('Native','native_contexts')]if plan[key]];assert expected_backends
  try:
   uid_attempted=True
   uid=uid_probe(plan,coordinator);save(out/'uid-proof.json',uid);uid_safe=uid['passed']and uid['exact_CID_absent']
   for backend,key in [('Original','source_contexts'),('Native','native_contexts')]:
    if cancelled:break
    if not plan[key]:continue
    directory=out/backend;directory.mkdir();binding=dict(plan['base_binding']);contexts=plan[key];seconds=plan['source_seconds']if backend=='Original'else plan['native_seconds']
    binding.update(backend_order=[backend],execution_contexts={'Original':contexts if backend=='Original'else[],'Native':contexts if backend=='Native'else[]},declared_contexts={'Original':contexts if backend=='Original'else[],'Native':contexts if backend=='Native'else[]},whole_cohort_seconds=seconds,execution_scope='Fresh public-export '+backend+' selected functional cohort; all broader parity/performance gates remain unqualified.')
    if backend=='Native'and plan.get('workflow','recording-connect')=='recording-connect':
     sources=list((out/'fixtures').glob('Original-*/recording-connect-capture/report.json'));assert len(sources)==1 and load(sources[0])['status']=='passed'
     binding['local_source_contract']=str(sources[0].parent/'private/locally-collected-contract.json');assert Path(binding['local_source_contract']).is_file()
     authority=[Path(binding['local_source_contract']),sources[0],sources[0].parent/'private/source-payloads.json',sources[0].parents[1]/'independent-cleanup.json',out/'Original/execution.json',out/'Original/results/report.json']
     binding['inputs']=[*binding['inputs'],*[{'path':str(path),'sha256':sha(path)}for path in authority]]
    context={'heavy_exclusive':True,'active_functional_fixtures':1,'attempt':1,'claim_persisted':True,'deadline_monotonic':time.monotonic()+seconds,'cleanup_seconds':90,'actual_local_lease':str(lock_path),'whole_migration_or_performance_authority':False}
    save(directory/'binding.json',binding);save(directory/'context.json',context)
    argv=[sys.executable,'-B',str(Path(plan.get('cohort_root',str(HERE)))/'cohort/run.py'),'--adapter',str(Path(plan.get('cohort_root',str(HERE)))/'fixture_adapter.py'),'--binding',str(directory/'binding.json'),'--global-context',str(directory/'context.json'),'--output',str(directory/'results')]
    env=dict(os.environ)
    if plan['Docker_host']:env['DOCKER_HOST']=plan['Docker_host']
    safe_cleanup=False;backend_started=backend
    receipt=coordinator.command(argv,{'cwd':str(repo),'environment':env,'output_bytes':8388608},directory,'execution',seconds+90);save(directory/'execution.json',receipt)
    report_path=directory/'results/report.json';report=load(report_path)if report_path.is_file()else{}
    safe_cleanup=coordinator.settled(receipt)and report.get('exact_cleanup_passed')is True
    passed=receipt['actual_exit']==0 and receipt['failure']is None and safe_cleanup and report.get('combined_selected_workflow_passed')is True
    combined.append({'backend':backend,'selected_contexts':contexts,'actual_exit':receipt['actual_exit'],'selected_passed':passed,'actual_cleanup_passed':safe_cleanup})
    if not safe_cleanup:
     save(repo/'.dispatcharr-recording-cohort-cleanup-required.json',{'owned_run':str(out),'backend':backend,'fixture_output':plan['base_binding']['fixture_output'],'reason':'Actual cleanup or process-group/EOF proof incomplete. Inspect exact owned fixtures before clearing this marker.'})
    if not passed:break
  except BaseException as error:
   launcher_error={'type':type(error).__name__,'message':str(error)}
   raise
  finally:
   if uid_attempted and not uid_safe:
    uid_cleanup=load(out/'uid/cleanup.json')if(out/'uid/cleanup.json').is_file()else{}
    if uid_cleanup.get('exact_CID_absent')is not True:
     uid_identity=load(out/'uid/identity.json')if(out/'uid/identity.json').is_file()else{}
     save(repo/'.dispatcharr-recording-cohort-cleanup-required.json',{'owned_run':str(out),'stage':'UID probe','identity':uid_identity,'cleanup':uid_cleanup,'reason':'UID cleanup is unknown or failed; missing CID/receipt is not absence.'})
   if backend_started is not None and not safe_cleanup:
    save(repo/'.dispatcharr-recording-cohort-cleanup-required.json',{'owned_run':str(out),'backend':backend_started,'fixture_output':plan['base_binding']['fixture_output'],'reason':'Actual backend execution did not establish complete owned cleanup/EOF/group settlement.'})
   for sig,handler in previous.items():signal.signal(sig,handler)
   save(out/'report.json',{'state':'ACTUAL_PUBLIC_EXPORT_ATTEMPT','backends':combined,'launcher_error':launcher_error,'interruption_requested':cancelled,'interrupt_policy':'Finish current bounded owned cohort/finally; do not start another backend. Forced process termination cannot claim cleanup.','selected_pair_passed':len(combined)==2 and all(x['selected_passed']for x in combined),'selected_workflow_passed':len(combined)==len(expected_backends)and all(x['selected_passed']for x in combined),'full_milestone_passed':False,'performance_verified':False,'export_qualification_requires_review':True})
  return 0 if len(combined)==len(expected_backends)and all(x['selected_passed']for x in combined)and not cancelled else 1

def main():
 parser=argparse.ArgumentParser(description=__doc__);sub=parser.add_subparsers(dest='command',required=True)
 p=sub.add_parser('prepare');p.add_argument('--repo',type=Path,default=HERE.parents[2]);p.add_argument('--output',type=Path,required=True);p.add_argument('--build',type=Path,required=True);p.add_argument('--source-archive',type=Path);p.add_argument('--docker',default='docker');p.add_argument('--docker-host');p.add_argument('--all-native',action='store_true',help='Run all seven affected Native contexts; strict restart remains a known open failure.');p.add_argument('--workflow',choices=['recording-connect','catalogue-series-comskip-vlc','catalogue-six','catalogue-seven','catalogue-eight','catalogue-nine','catalogue-twelve','catalogue-thirteen','catalogue-fourteen'],default='recording-connect',help='Select the existing public Connect pair or retained-Source Native catalogue/comskip/VLC cohort.')
 p=sub.add_parser('run');p.add_argument('--plan',type=Path,required=True)
 args=parser.parse_args()
 if args.command=='prepare':prepare(args);return 0
 return run(args)
if __name__=='__main__':raise SystemExit(main())
