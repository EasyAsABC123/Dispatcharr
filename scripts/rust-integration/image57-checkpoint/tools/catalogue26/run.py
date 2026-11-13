"""BLOCKED real release Main26 catalogue fixture. No app work on import."""
import argparse,datetime,hashlib,io,json,math,os,re,shutil,subprocess,sys,tarfile,threading,time,uuid
from pathlib import Path
import bounded_process as B
import host_mechanics as M
from bounded_stop import collect as bounded_stop
from candidate_inputs import bound_candidate
import main_http as H
EXECUTION_BLOCKED=True
HERE=Path(__file__).resolve().parent
ACTION='image57-production-Main-XC-catalogue26-once'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_bytes())
def save(p,v):Path(p).write_text(json.dumps(v,indent=2,allow_nan=False,default=str)+'\n')
def need(ok,label):
 if not ok:raise ValueError(label)
def command_budget(count,used,cleanup):
 need(type(count)is int and 0<=count<(256 if cleanup else 224),'BoundedCommandReserve')
 available=(64*1048576-65536 if cleanup else 56*1048576)-used;need(available>0,'BoundedOutputReserve');return min(8388608,available)
def shared_loopback(actual,baseline):
 need(type(baseline)is str and re.fullmatch('[0-9a-f]{64}',baseline)is not None and actual=='container:'+baseline,'ActualSharedLoopbackPeerContext')
def redis_settlement(identity,receipt):
 need(identity.get('actual_identity_observed')is True and receipt.get('actual_exit')==0 and type(receipt.get('actual_exit'))is int and receipt.get('collection_invalid')is None and receipt.get('owned_stop_requested')is False and all(receipt.get(k)is True for k in ['stdout_pipe_closed','stderr_pipe_closed','pipe_reader_threads_settled','stdin_writer_settled','pipes_physically_settled']),'ActualOwnedRedisSuccessfulSettlement')
def wait_allowed(job,timeout,allowed):
 try:job.wait(timeout)
 except B.CollectionFailure:
  if job.invalid is not None or job.stopped or not job.physically_settled or job.process.poll()not in allowed:raise
def extract(raw,dst):
 dst.mkdir();dst.chmod(0o755)
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  need(all(not Path(m.name).is_absolute()and'..'not in Path(m.name).parts and(m.isfile()or m.isdir())for m in tar),'SafePinnedGitArchive');tar.extractall(dst,filter='data')
def preflight(repo):
 from tooling_guard import assert_inputs
 assert_inputs(os.environ,read(HERE/'tooling-inputs.json'))
 binding=read(HERE/'candidate-binding.json');proof=bound_candidate(repo/binding['manifest'],binding['image_id'],HERE/'expected-runtime.json',repo/binding['archive']);pins=read(HERE/'pinned-source.json');reference=read(HERE/'reference-inputs.json')
 for name,h in reference['private_inputs'].items():need(sha(HERE/name)==h,'ExactSourceSeedReference:'+name)
 H.reference(read(HERE/'retained-source26.json'))
 selection=read(HERE/'selection.json')
 actual={str(p.relative_to(HERE))for p in HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name not in ('selection.json','seal.json','actual-preexecution-review.json')}
 need(actual=={r['path']for r in selection['files']},'CompleteSealedInputInventory')
 for row in selection['files']:
  rel=Path(row['path']);need(not rel.is_absolute()and'..'not in rel.parts,'SafeInputPath');p=HERE/rel;need(p.is_file()and not p.is_symlink(),'RegularInput');raw=p.read_bytes()
  if str(rel)=='run.py':raw=raw.replace(b'\nEXECUTION_BLOCKED=False\n',b'\nEXECUTION_BLOCKED=True\n')
  need(hashlib.sha256(raw).hexdigest()==row['sha256'],'ExactSealedInput:'+str(rel))
 return binding,proof,pins
def validate_cue(repo,cue,now):
 b,proof,pins=preflight(repo);policy=read(HERE/'policy.json');need(cue.get('authorized')is True and cue.get('action')==ACTION,'ExplicitRootCue');need(cue.get('Main_XC_requests_authorized')is True and all(cue.get(k)is False for k in('Source_business_requests','cfgtest_peer','info_actions','fixed_clock_hook','resource_sampling','performance_acceptance')),'ExactScope')
 need(cue.get('boot_id')==Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'SameBoot')
 need(type(cue.get('nonce'))is str and re.fullmatch('[a-f0-9]{32}',cue['nonce'])is not None,'FreshNonce')
 need(all(type(cue.get(k))in(int,float)and math.isfinite(cue[k])for k in('issued_epoch','expires_epoch'))and cue['issued_epoch']<=now<cue['expires_epoch']and 0<cue['expires_epoch']-cue['issued_epoch']<=1800,'FreshFiniteCue')
 need(cue['selection_sha256']==sha(HERE/'selection.json')and cue['policy_sha256']==sha(HERE/'policy.json')and cue['binding_sha256']==sha(HERE/'candidate-binding.json'),'ExactCueInputBindings');need(cue['new_output']==policy['output'],'FixedFreshNamespace')
 out=repo/policy['output'];need(not out.exists()and not out.is_symlink(),'FreshOutputOnly');pre=repo/policy['host_preflight_path'];need(cue['host_preflight_sha256']==sha(pre),'ExactIdleReceipt');q=read(pre);need(q['passed']is True and q['boot_id']==cue['boot_id']and cue['issued_epoch']-300<=q['checked_epoch']<=cue['issued_epoch'],'FreshIdle')
 return b,proof,pins,policy,out

def qualify_http(d,database):
 reference=H.reference(read(HERE/'retained-source26.json'));schema=read(HERE/'schema.json');need(d['actual_production_Main']is True and d['info_actions_or_fixed_clock_hook']is False and type(d['declared_configured_XC_max_rows'])is int and d['declared_configured_XC_max_rows']==50000,'ActualReleaseScope');need(d['capture_complete']is True and len(d['cases'])==26 and d['fixed_names']==H.NAMES,'ExactMain26')
 need(type(d['phase_started'])in(int,float)and type(d['phase_completed'])in(int,float)and math.isfinite(d['phase_started'])and math.isfinite(d['phase_completed'])and 0<=d['phase_completed']-d['phase_started']<=90,'FixedHTTP90')
 same=[];previous=None;seed=read(HERE/'retained-seed-private.json');declared=[c for c in H.cases()if c['action']in H.ACTIONS]
 for index,(row,golden)in enumerate(zip(d['cases'],reference['cases'],strict=True)):
  need(H.observation_valid(row)and d['phase_started']<=row['began_monotonic']<=row['completed_monotonic']<=d['phase_completed'],'ActualFDInodeTimerClocks')
  query=H.query(declared[index],seed);need(row['method']=='GET'and row['request_query_private']==query,'ExactSQLBackedRoleXCPasswordRequest');from urllib.parse import urlencode
  need(row['request_target_sha256']==hashlib.sha256(('/player_api.php?'+urlencode(query)).encode()).hexdigest(),'ExactRequestTargetHash')
  need(row['actual_Host']=='127.0.0.1:5657'and row['Main_destination']=='127.0.0.1:9192 shared baseline network namespace','ExactSourceHostAndPeerContext');need((row['name'],row['role'],row['action'])==(golden['name'],golden['role'],golden['action']),'ExactCaseRoleAction')
  for side in('before','after'):
   snap=row[side];need(snap['database']==database and sorted(snap['tables'])==schema['tables']and sorted(snap['sequences'])==schema['sequences'],'ActualFull6259OwnAudit');need(row[side+'_named11']==H.named(snap),'ExactNamed11Binding');need(type(row[side+'_raw_Redis'])is list and [r['database']for r in row[side+'_raw_Redis']]==[0,1,2,3],'AllRawRedisRetained')
   for value in snap['tables'].values():need(set(value)=={'rows','sha256'}and type(value['rows'])is int and 0<=value['rows']<=10000 and type(value['sha256'])is str and re.fullmatch('[a-f0-9]{64}',value['sha256'])is not None,'TypedFullTableFingerprints')
   for value in snap['sequences'].values():need(set(value)=={'last_value','is_called'}and type(value['last_value'])is int and type(value['is_called'])is bool,'TypedFullSequenceValues')
  if previous is not None:need(row['before']==previous,'CrossRequestOwnSQLContinuity')
  previous=row['after']
  need(row['before']==row['after'],'NoMainSQLOrSequenceMutation');same.append(row['status']==golden['status']and row['body_hex']==golden['body_hex']and[v for k,v in row['headers']if k.casefold()=='content-type']==[v for k,v in golden['headers']if k.casefold()=='content-type'])
 return {'capture_valid':True,'Main26_Source_body_status_CT_parity':all(same),'cases':26,'Source_XC_business_requests_executed':0,'Source_readiness_control_only':True,'cfgtest_component_transfer':False,'full_M7_or_performance':False}

def execute(repo,cue,binding,proof,pins,policy,out):
 out.mkdir(mode=0o700);shutil.copytree(HERE,out/'controller',ignore=shutil.ignore_patterns('__pycache__'));project='dispatcharr-xc-main57-'+uuid.uuid4().hex[:12];database='rust_capture_'+uuid.uuid4().hex[:16];env={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','POSTGRES_','REDIS_','DATABASE_','DJANGO_','CELERY_'))};jobs=[];redis_job=None;compose=None;claimed=False;before=None;count=0;used=0;cleanup_start=None;cleanup_deadline=None;stop_used=False;started=time.monotonic();status=1;cids={};stop_lock=threading.Lock();stop_done=threading.Event();report={'passed':False,'scope':'Real releaseMain XC catalogue26 exactSourcecontext, noinfo','project':project,'database':database,'candidate':proof,'Source_business_requests':0,'cfgtest_peer':False,'info_actions_or_clock_hook':False,'processes':[],'default_worker_activation_claimed':False}
 def stop_owned():
  nonlocal stop_used
  if not claimed:return
  with stop_lock:
   owner=not stop_used
   if owner:stop_used=True
  if not owner:
   need(stop_done.wait(35),'ExistingStopSettlement35');need(report.get('owned_stop',{}).get('acknowledged')is True,'ExistingStopAcknowledged');return
  receipt={};report['owned_stop']=receipt
  try:bounded_stop(compose+['stop','--timeout','10'],env,out,receipt,limit=65536,timeout=15,save=save)
  finally:stop_done.set()
 def begin_cleanup():
  nonlocal cleanup_start,cleanup_deadline
  if cleanup_start is None:cleanup_start=time.monotonic();cleanup_deadline=cleanup_start+90
 def command(argv,timeout=180,limit=None,allowed=(0,)):
  nonlocal count,used
  cap=command_budget(count,used+(redis_job.used if redis_job is not None else 0),cleanup_deadline is not None);archive=argv[0]=='git';cap=536870912 if archive else cap;cap=min(cap,limit)if limit is not None else cap;count+=1;tag='command-'+str(count);remaining=(cleanup_deadline-time.monotonic())if cleanup_deadline is not None else 1200-(time.monotonic()-started);p=B.Collected(argv,env,cap,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(p);identity=M.identity(p.process);began=time.monotonic()
  try:wait_allowed(p,min(timeout,max(.01,remaining)),allowed)
  finally:
   if not archive:used+=p.used
   row={**identity,**M.settled(p),'argv':argv,'actual_exit':p.process.poll(),'collection_invalid':p.invalid,'owned_stop_requested':p.stopped,'began_monotonic':began,'completed_monotonic':time.monotonic()};report['processes'].append(row);save(out/(tag+'.json'),row)
  need(identity['actual_identity_observed']and p.process.returncode in allowed and p.physically_settled and p.invalid is None,'ActualSettledAllowedCommand');return(out/(tag+'.stdout')).read_bytes()
 def run(*args,**kw):return command(compose+list(args),**kw)
 def helper(action,db=database):return json.loads(run('exec','-T','-e','POSTGRES_DB='+db,'-e','REDIS_PORT='+('6379'if db=='dispatcharr'else'6382'),'baseline','/dispatcharrpy/bin/python','/capture/fixture_db.py',action,timeout=180).splitlines()[-1])
 def ready(argv,seconds):
  deadline=time.monotonic()+seconds
  while True:
   try:return command(argv,timeout=min(5,max(.01,deadline-time.monotonic())))
   except B.CollectionFailure:
    row=report['processes'][-1];need(row['actual_exit']is not None and row['actual_exit']!=0 and row['collection_invalid']is None and row['owned_stop_requested']is False and row['actual_identity_observed']and row.get('pipes_physically_settled')is True,'SettledReadinessOnlyRefusal');need(time.monotonic()<deadline,'BoundedReadiness');time.sleep(.1)
 save(out/'execution-progress.json',report)
 try:
  raw=command(['git','-C',str(repo),'archive',pins['Original_revision']]);need(hashlib.sha256(raw).hexdigest()==pins['Original_full_archive_sha256'],'PinnedOriginalArchive');original=out/'original-source';extract(raw,original);(original/'media').mkdir(exist_ok=True)
  raw=command(['git','-C',str(repo),'archive',pins['support_revision'],'docker','scripts']);support=out/'support';extract(raw,support);save(out/'support-binding.json',{'Git':pins['support_revision'],'archive_sha256':hashlib.sha256(raw).hexdigest()})
  image=pins['Original_image'];base_env={'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'localhost','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PYTHONDONTWRITEBYTECODE':'1','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8','SOURCE_CAPTURE_FIXTURE':'playback-fixture-only','CAPTURE_NONCE':cue['nonce']}
  baseline={'image':image,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/verification/start.sh'],'environment':base_env,'volumes':[str(original)+':/app:ro',str(support/'scripts/rust-playback-baseline')+':/verification:ro',str(support/'scripts/rust-baseline')+':/baseline-tools:ro',str(out/'controller')+':/capture:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=2147483648'],'depends_on':['origin']}
  origin={'image':image,'pull_policy':'never','entrypoint':['/dispatcharrpy/bin/python','/capture/origin.py'],'volumes':[str(out/'controller')+':/capture:ro'],'environment':{'CAPTURE_NONCE':cue['nonce']}}
  data={'image':'debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc','pull_policy':'never','user':'0:0','entrypoint':['sh','-c','mkdir -p /data/logos && chown 65532:65532 /data /data/logos'],'read_only':True,'cap_drop':['ALL'],'cap_add':['CHOWN'],'volumes':['rust-data:/data']}
  candidate={'image':binding['image_id'],'pull_policy':'never','platform':'linux/amd64','network_mode':'service:baseline','depends_on':{'baseline':{'condition':'service_started'},'data-init':{'condition':'service_completed_successfully'}},'read_only':True,'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],'volumes':['rust-data:/data'],'environment':{'DATABASE_URL':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'DJANGO_SECRET_KEY':'playback-fixture-only','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6382/0','RUST_BIND':'0.0.0.0:9192','TMPDIR':'/data','RUST_DATABASE_POOL_SIZE':'8','RUST_MAX_XC_CHANNELS':'50000','TOKIO_WORKER_THREADS':'1','DISPATCHARR_TRUSTED_PROXIES':'none'},'stop_grace_period':'15s'}
  cfg=out/'compose.json';save(cfg,{'services':{'baseline':baseline,'origin':origin,'data-init':data,'candidate':candidate},'networks':{'default':{'internal':True}},'volumes':{'rust-data':{}}});compose=['docker','compose','-p',project,'-f',str(cfg)];claimed=True;run('up','-d','--no-build','--pull','never','baseline','origin')
  ready(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],180)
  run('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop');report['Source_web_and_workers_stopped_before_owned_seed']=True
  before=helper('default-audit','dispatcharr');save(out/'default-before.json',before)
  run('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database+' TEMPLATE template0');helper('migrate')
  argv=compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/capture/owned_process.py','start','source-redis','redis-server','--port','6382','--bind','127.0.0.1','--requirepass','playback-fixture-only','--daemonize','no','--dir','/tmp','--save','','--appendonly','no'];redis_job=B.Collected(argv,env,1048576,stop_owned,stdout_path=out/'owned-redis.stdout',stderr_path=out/'owned-redis.stderr');jobs.append(redis_job);report['owned_redis_identity']=M.identity(redis_job.process);need(report['owned_redis_identity']['actual_identity_observed'],'ActualOwnedRedisHostIdentity')
  ready(compose+['exec','-T','baseline','redis-cli','-p','6382','PING'],15);run('exec','-T','-e','POSTGRES_DB='+database,'-e','REDIS_PORT=6382','baseline','/dispatcharrpy/bin/python','/capture/retained_seed.py');report['owned_before_Main']=helper('audit')
  probe="import os,json,urllib.request;r=urllib.request.Request('http://origin:8080/__capture_state__',headers={'X-Fixture-Nonce':os.environ['CAPTURE_NONCE']});print(urllib.request.urlopen(r,timeout=2).read(65537).decode())"
  origin_before=json.loads(run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',probe));need(origin_before['rows']==origin_before['invalid']==[],'NoInitialOriginContact');report['origin_before']=origin_before
  run('up','-d','--no-build','--pull','never','candidate');
  for role in('baseline','origin','candidate','data-init'):
   ids=run('ps','--all','-q',role).decode().splitlines();need(len(ids)==1,'OneExactServiceCID');cids[role]=ids[0]
  inspected=json.loads(command(['docker','inspect',cids['candidate']]))[0];need(inspected['Image']==binding['image_id']and inspected['Config']['User']=='65532:65532'and inspected['Config']['Entrypoint']==['/usr/local/bin/dispatcharr-rs']and inspected['Path']=='/usr/local/bin/dispatcharr-rs','ActualNormalMainImageUIDEntrypoint');actual_environment=dict(v.split('=',1)for v in inspected['Config']['Env']);need(actual_environment.get('RUST_MAX_XC_CHANNELS')=='50000'and not any('FIXTURE'in k or 'CLOCK'in k or 'VOD_INFO'in k for k in actual_environment),'ActualProductionCapNoInfoClockHook');shared_loopback(inspected['HostConfig']['NetworkMode'],cids['baseline']);report['Main_inspection']=inspected
  ready(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:9192/health/ready',timeout=2).status==200"],30)
  run('exec','-T','-e','POSTGRES_DB='+database,'-e','REDIS_PORT=6382','baseline','/dispatcharrpy/bin/python','/capture/main_http.py',timeout=100,allowed=(0,1));begin_cleanup();http=read(out/'Main26-private.json');need(http.get('_synthetic_only')is not True,'ActualMainCaptureCannotBeSynthetic');report['Main26']=qualify_http(http,database);report['owned_after_Main']=helper('audit');need(report['owned_before_Main']==report['owned_after_Main'],'CompleteOwn6259NoMutation');report['origin_terminal']=json.loads(run('exec','-T','baseline','/dispatcharrpy/bin/python','-c',probe));need(report['origin_terminal']['rows']==report['origin_terminal']['invalid']==[],'ZeroProviderContact');report['passed']=report['Main26']['Main26_Source_body_status_CT_parity'];status=0 if report['passed']else 1
 except BaseException as error:report['error_type']=type(error).__name__;report['error_message']=str(error)[:4096];status=1;report['passed']=False
 finally:
  begin_cleanup()
  if compose is not None:
   for role in('baseline','origin','candidate','data-init'):
    if role not in cids:
     try:
      found=run('ps','--all','-q',role,timeout=10).decode().splitlines();need(len(found)<=1,'OwnedPartialCIDRecovery');report.setdefault('partial_CID_recovery',{})[role]=found
      if found:need(re.fullmatch('[a-f0-9]{64}',found[0])is not None,'ExactCIDFormat');cids[role]=found[0]
     except BaseException as error:report.setdefault('cleanup_errors',[]).append('CIDRecovery:'+role+':'+type(error).__name__);report['passed']=False;status=1
   def stop_Main():
    run('stop','--timeout','15','candidate',timeout=30)
    if 'candidate'in cids:
     state=json.loads(command(['docker','inspect',cids['candidate']],timeout=10))[0]['State'];report['actual_Main_shutdown_state']=state;need(state['Running']is False and state['ExitCode']==0 and state['OOMKilled']is False,'ActualNormalMainShutdown0')
     until=time.monotonic()+10;observations=[]
     while True:
      raw=run('exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,state,backend_start FROM pg_stat_activity WHERE datname='"+database+"') t",timeout=10);rows=json.loads(raw);observations.append({'monotonic':time.monotonic(),'actual_backends':rows});save(out/'backends-after-Main-exit.json',observations)
      if rows==[]:report['owned_SQL_backends_absent_after_Main_exit']=True;break
      need(time.monotonic()<until,'MainExitedBackendAbsence10');time.sleep(.1)
   for action,label in[(stop_Main,'MainStop'),(lambda:run('exec','-T','baseline','/dispatcharrpy/bin/python','/capture/owned_process.py','stop','source-redis',timeout=20),'RedisStop')]:
    try:action()
    except BaseException as error:report.setdefault('cleanup_errors',[]).append(label+':'+type(error).__name__);report['passed']=False;status=1
   if before is not None:
    try:after=helper('default-audit','dispatcharr');save(out/'default-after.json',after);need(before==after and sha(out/'default-before.json')==sha(out/'default-after.json'),'FullDefault6259UnchangedLiteralBytes');report['default_full6259_unchanged']=True
    except BaseException as error:report.setdefault('cleanup_errors',[]).append('DefaultAudit:'+type(error).__name__);report['passed']=False;status=1
   try:run('down','--volumes','--remove-orphans','--timeout','10',timeout=45)
   except BaseException as error:report.setdefault('cleanup_errors',[]).append('Down:'+type(error).__name__);report['passed']=False;status=1
   for kind,argv in{'containers':['container','ls','--all','--format','{{.ID}}'],'networks':['network','ls','--format','{{.ID}}'],'volumes':['volume','ls','--format','{{.Name}}']}.items():
    try:raw=command(['docker']+argv+['--filter','label=com.docker.compose.project='+project],timeout=10);need(not raw.strip(),'OwnedResourcesAbsent');report.setdefault('cleanup_inventories',{})[kind]=raw.decode()
    except BaseException as error:report.setdefault('cleanup_errors',[]).append(kind+':'+type(error).__name__);report['passed']=False;status=1
   for role,cid in cids.items():
    try:raw=command(['docker','container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'],timeout=10);need(not raw.strip(),'ExactCIDAbsent');report.setdefault('exact_CID_absence',{})[role]=True
    except BaseException as error:report.setdefault('cleanup_errors',[]).append(role+'CID:'+type(error).__name__);report['passed']=False;status=1
  for job in jobs:
   if job is redis_job or job.process.poll()is None:
    try:job.wait(5)
    except BaseException as error:report.setdefault('cleanup_errors',[]).append('PipeWait:'+type(error).__name__);report['passed']=False;status=1
  report['all_host_jobs_settled']=all(p.physically_settled and p.process.poll()is not None for p in jobs);
  if redis_job is not None:
   report['owned_redis_settlement']={**M.settled(redis_job),'actual_exit':redis_job.process.poll(),'collection_invalid':redis_job.invalid,'owned_stop_requested':redis_job.stopped}
   try:redis_settlement(report['owned_redis_identity'],report['owned_redis_settlement'])
   except BaseException as error:report.setdefault('cleanup_errors',[]).append('RedisSettlement:'+type(error).__name__);report['passed']=False;status=1
  report['aggregate_command_and_Redis_output_bytes']=used+(redis_job.used if redis_job is not None else 0)
  if report['aggregate_command_and_Redis_output_bytes']>64*1048576-65536:report['passed']=False;status=1
  absence=[]
  for identity in report['processes']+([report['owned_redis_identity']]if 'owned_redis_identity'in report else[]):
   pid=identity['host_PID'];before_start=identity['host_PID_starttime']
   if not identity['actual_identity_observed']:
    absence.append({'PID':pid,'observed_starttime':before_start,'identity_unknown':True,'original_PID_absent':False});report['passed']=False;status=1;continue
   try:
    path=Path('/proc')/str(pid)/'stat';after_start=path.read_text().rsplit(')',1)[1].split()[19]if path.exists()else None;absence.append({'PID':pid,'observed_starttime':before_start,'after_starttime':after_start,'original_PID_absent':after_start is None or after_start!=before_start})
   except BaseException as error:absence.append({'PID':pid,'identity_observation_error':type(error).__name__,'original_PID_absent':False});report['passed']=False;status=1
  report['actual_host_PID_absences']=absence
  if not all(v['original_PID_absent']for v in absence):report['passed']=False;status=1
  report['cleanup_elapsed_seconds']=time.monotonic()-cleanup_start;report['cleanup_within90']=0<=report['cleanup_elapsed_seconds']<=90
  if not report['all_host_jobs_settled']or not report['cleanup_within90']:report['passed']=False;status=1
  report['actual_controller_exit']=status;save(out/'summary-private.json',report)
 return status

def main():
 if EXECUTION_BLOCKED:raise RuntimeError('ROOT_REVIEW_MAIN57_CUE_AND_SLOT_REQUIRED')
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--cue',type=Path,required=True);a=p.parse_args();repo=a.repo.resolve();need(repo==Path('/home/justin/github/Dispatcharr'),'ExactRepository');cue=read(a.cue);binding,proof,pins,policy,out=validate_cue(repo,cue,time.time());need(a.cue.resolve()==repo/policy['cue_path'],'ExactCuePath');claim=a.cue.with_name(a.cue.stem+'.claimed.json');fd=os.open(claim,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
 with os.fdopen(fd,'w')as f:json.dump({'cue_sha256':sha(a.cue),'selection_sha256':sha(HERE/'selection.json'),'pid':os.getpid(),'nonce':cue['nonce'],'no_retry':True},f)
 return execute(repo,cue,binding,proof,pins,policy,out)
if __name__=='__main__':raise SystemExit(main())
