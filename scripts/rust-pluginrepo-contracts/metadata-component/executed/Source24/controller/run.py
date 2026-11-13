"""UNEXECUTED Source24 plugin repository controller. Default is read-only seal check."""
import argparse,datetime,hashlib,json,os,re,subprocess,sys,time,uuid,threading
from pathlib import Path
from bounded_process import Collected
from bounded_stop import collect as bounded_stop
from fixture_guards import database,require
from effects import validate
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f';REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):return json.loads(Path(p).read_bytes())
def save(p,v):Path(p).write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def identity(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def inventory_paths():
 return sorted([p for folder in ['controller','inputs','history']for p in(ROOT/folder).rglob('*')if p.is_file()and'__pycache__'not in p.parts]+[ROOT/name for name in ['execution-plan.json','README.md','Original-LICENSE','local-controls.log']])
def check_seal(path):
 seal=read(path);require(seal['state']=='SEALED_UNEXECUTED_SOURCE24_PLUGIN_REPOSITORIES'and seal['Original_revision']==REV and seal['image']==IMAGE and seal['requests']==24,'ExactSource24SealIdentity');require({r['path']for r in seal['files']}=={str(p.relative_to(ROOT))for p in inventory_paths()},'CompleteSealedInputInventory')
 for row in seal['files']:
  rel=Path(row['path']);require(not rel.is_absolute()and'..'not in rel.parts,'SafeSealedPath');p=ROOT/rel;require(p.is_file()and not p.is_symlink()and p.stat().st_size==row['bytes']and sha(p)==row['sha256'],'ExactSealedInput:'+str(rel))
 require(sha(ROOT/'inputs/original-source.tar')=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive');require(sha(ROOT/'inputs/definition-seal.json')=='c5f0283eec0ae5fa05450f2e84424dc78473451efb0bb1c169c9d8cb1aff43b2','ExactDefinition2')
 definition=read(ROOT/'inputs/definition-seal.json')
 for row in definition['files']:
  target=ROOT/'inputs/definition2'/row['path'];require(target.is_file()and target.stat().st_size==row['bytes']and sha(target)==row['sha256'],'ExactDefinition2Reconstruction:'+row['path'])
 require(sha(ROOT/'inputs/plan.json')==sha(ROOT/'inputs/definition2/plan.json'),'OneExactPlanBinding')
 deps=read(ROOT/'inputs/actual-dependencies.json');require(len(deps)==55,'Exact55ActualDependencyClosure')
 for row in deps:require(sha(ROOT/row['path'])==row['sha256'],'ExactCapturedActualImageDependency')
 for group in ['auth23','parser23']:
  proof=read(ROOT/'inputs'/(group+'-proof.json'));require(proof['passed']is True and proof['state']=='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'and proof['container_started']is False and proof['container_program_executions']==0 and proof['exact_CID_absent']is True and proof['all_project_containers_absent']is True,'PriorNeverStartedDependencyProof:'+group);require(proof['created_inspect']['State']['Pid']==proof['final_before_remove']['State']['Pid']==0,'DependencyCapturePID0')
 proof=read(ROOT/'inputs/serializer9-proof.json');require(proof['passed']is True and proof['Source_started']is False and proof['app_imports']is False and proof['all_nine_equal']is True and proof['exact_container_absent']is True and proof['created_inspect']['State']['Pid']==proof['final_before_remove']['Pid']==0,'PriorNeverStartedSerializerProof')
 from tooling_guard import assert_inputs
 assert_inputs(os.environ,read(ROOT/'inputs/tooling-inputs.json'));return seal

def command_budget(count,used,cleanup):
 require(type(count)is int and 0<=count<(255 if cleanup else 223),'FiniteCommandReserve');cap=(64*1048576-65536 if cleanup else 56*1048576)-used;require(cap>0,'TotalOutputAndStopReserve');return min(8388608,cap)
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--seal',type=Path,default=ROOT/'seal.json');p.add_argument('--execute-reviewed-seal');p.add_argument('--output',type=Path);a=p.parse_args();check_seal(a.seal)
 if a.execute_reviewed_seal is None:print(json.dumps({'passed':True,'readonly':True,'seal_sha256':sha(a.seal),'Source_requests':0}));return 0
 require(a.execute_reviewed_seal==sha(a.seal),'ExactRootReviewedSealArgument');repo=Path('/home/justin/github/Dispatcharr');out=repo/'artifacts/rust/m8-plugin-repositories-source-1';require(a.output is not None and a.output.resolve()==out and not out.exists()and not out.is_symlink(),'FixedFreshOutputOnly');out.mkdir(mode=0o700);nonce=uuid.uuid4().hex[:10];db=database('rust_probe_pluginrepo_'+nonce);project='dispatcharr-pluginrepo-source-'+nonce;source=out/'source';source.mkdir();env={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','POSTGRES_','REDIS_','DATABASE_','DJANGO_','CELERY_'))};base=['docker','compose','-p',project,'-f',str(out/'compose.json')];start=time.monotonic();cleanup_start=None;deadline=None;claimed=False;container=None;before=None;count=used=0;jobs=[];stop_lock=threading.Lock();stop_done=threading.Event();stopping=False;status=1;report={'passed':False,'project':project,'database':db,'Source_only':True,'Source_framework_not_TCP':True,'Native_or_Main_runs':0,'reviewed_seal':sha(a.seal),'processes':[],'Source_requests':0,'web_workers_media_plugin_discovery_started':False,'resource_sampler':False}
 def begin_cleanup():
  nonlocal cleanup_start,deadline
  if cleanup_start is None:cleanup_start=time.monotonic();deadline=cleanup_start+90;report['cleanup_started_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
 def stop():
  nonlocal stopping
  if not claimed:return
  with stop_lock:owner=not stopping;stopping=True
  if not owner:require(stop_done.wait(35)and report.get('stop_receipt',{}).get('acknowledged')is True,'ActualExistingStopSettlement');return
  receipt={};report['stop_receipt']=receipt
  try:bounded_stop(base+['stop','--timeout','10'],env,out,receipt,timeout=15,limit=65536,save=save)
  finally:stop_done.set()
 def command(argv,timeout=180):
  nonlocal count,used
  cap=command_budget(count,used,deadline is not None);count+=1;tag='command-'+str(count);remaining=deadline-time.monotonic()if deadline is not None else 1200-(time.monotonic()-start);job=Collected(argv,env,cap,stop,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);observed=identity(job.process.pid);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
  try:require(observed is not None and observed.isdigit(),'ActualHostStartIdentityBeforeAcceptance');job.wait(min(timeout,max(.01,remaining)))
  finally:
   used+=job.used;row={'argv':argv,'pid':job.process.pid,'observed_starttime':observed,'actual_identity_observed':observed is not None,'began_utc':began,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'actual_exit':job.process.poll(),'collection_invalid':job.invalid,'physically_settled':job.physically_settled,'readers_settled':all(not t.is_alive()for t in job.threads),'writer_settled':job.writer is None or not job.writer.is_alive()};report['processes'].append(row);save(out/(tag+'.json'),row)
  require(job.process.returncode==0 and job.invalid is None and job.physically_settled,'ActualWait0Pipes');return(out/(tag+'.stdout')).read_text()
 def run(*args,**kw):return command(base+list(args),**kw)
 def shell(dbname,script,label=None,timeout=150):
  entries=['POSTGRES_DB='+dbname,'PLUGIN_REPO_DATABASE='+dbname,'PLUGIN_REPO_AUTHORITY=owned-source-repository-only','REDIS_DB='+('0'if label else'1'),'PGOPTIONS=-c statement_timeout=7000 -c lock_timeout=7000']
  if label:entries.append('PLUGIN_REPO_AUDIT_LABEL='+label)
  return run('exec','-T',*[x for e in entries for x in ['-e',e]],'baseline','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy;runpy.run_path('/plugin-tools/"+script+"',run_name='__main__')",timeout=timeout)
 def audit(label):shell('dispatcharr','default_audit.py',label,120);return read(out/('default-'+label+'-private.json'))
 def inventories():
  result={}
  for kind,args in {'containers':['container','ls','--all','--no-trunc','--format','{{.ID}}'],'networks':['network','ls','--format','{{.ID}}'],'volumes':['volume','ls','--format','{{.Name}}']}.items():result[kind]=command(['docker']+args+['--filter','label=com.docker.compose.project='+project],timeout=15).splitlines()
  return result
 try:
  command(['tar','-xf',str(ROOT/'inputs/original-source.tar'),'--no-same-owner','-C',str(source)]);(source/'media').mkdir(exist_ok=True);require(not any(inventories().values()),'FreshOwnedProjectInitiallyEmpty')
  settings={'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'owned-pluginrepo-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','CELERY_BROKER_URL':'redis://:playback-fixture-only@127.0.0.1:6379/2','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@127.0.0.1:6379/3','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/plugin-tools:/app','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'}
  config={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/plugin-tools/start.sh'],'environment':settings,'volumes':[str(source)+':/app:ro',str(HERE)+':/plugin-tools:ro',str(ROOT/'inputs')+':/plugin-inputs:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1g','pids_limit':256,'network_mode':'none'}}};save(out/'compose.json',config);claimed=True;run('up','-d','--no-build','--pull','never','baseline');ids=run('ps','--all','-q','baseline').splitlines();require(len(ids)==1,'OneExactOwnedSourceContainer');container=ids[0];inspection=json.loads(command(['docker','inspect',container]));save(out/'container-inspect-private.json',inspection);require(inspection[0]['Config']['Image']==IMAGE and inspection[0]['Config']['Labels']['com.docker.compose.project']==project and inspection[0]['HostConfig']['NetworkMode']=='none','ExactPinnedImageAndProjectLabel')
  ready=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():require(time.monotonic()<ready,'Bootstrap180');time.sleep(.2)
  before=audit('before');run('exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr');report['Source_command_next']=count+1;shell(db,'source_fixture.py',timeout=150);begin_cleanup();actual=read(out/'plugin-repositories-private.json');require(actual.get('_synthetic_only')is not True,'ActualSourceCannotBeSyntheticControl');qualified=validate(actual,read(ROOT/'inputs/plan.json'),read(ROOT/'inputs/schema62-authority.json'));save(out/'effect-validation.json',qualified);report['Source_requests']=actual['actual_request_count'];report['passed']=qualified['passed'];status=0 if qualified['passed']else 1
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error),'bytes':len(str(error).encode()),'sha256':hashlib.sha256(str(error).encode()).hexdigest()};status=1;report['passed']=False
 finally:
  begin_cleanup()
  def cleanup(label,action):
   nonlocal status
   try:return action()
   except BaseException as error:report.setdefault('cleanup_errors',[]).append({'label':label,'type':type(error).__name__,'message':str(error)});report['passed']=False;status=1
  capture=out/'plugin-repositories-private.json'
  if capture.exists():
   actual=cleanup('ReadPartialCapture',lambda:read(capture))
   if actual is not None:
    if actual.get('_synthetic_only')is True:report['passed']=False;status=1
    report['Source_requests']=actual.get('actual_request_count',0);qualified=cleanup('RetainedEffects',lambda:validate(actual,read(ROOT/'inputs/plan.json'),read(ROOT/'inputs/schema62-authority.json')))
    if qualified is not None:save(out/'effect-validation.json',qualified)
  if container is not None:
   top=cleanup('OwnedServiceHostPIDCapture',lambda:command(['docker','top',container,'-eo','pid'],timeout=10));pids=[]
   if top is not None:
    for line in top.splitlines()[1:]:
     pid=int(line.strip());starttime=identity(pid);pids.append({'pid':pid,'observed_starttime':starttime,'actual_identity_observed':starttime is not None})
    save(out/'owned-service-host-PIDs.json',pids)
   else:pids=[]
   def backend():
    raw=run('exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,state,backend_start FROM pg_stat_activity WHERE datname='"+db+"') t",timeout=10);rows=json.loads(raw);save(out/'owned-backends-after-Source.json',rows);require(rows==[],'OwnedBackendsAbsentAfterSourceProcessExit');return True
   report['owned_backends_absent_after_Source_exit']=cleanup('OwnedBackendObserver',backend)
   if (out/'bootstrap-ready').exists():
    after=cleanup('DefaultAfter',lambda:audit('after'));report['default_full6259_and_named11_literal_bytes_equal']=before is not None and after==before and sha(out/'default-before-private.json')==sha(out/'default-after-private.json')if after is not None else False
    if not report['default_full6259_and_named11_literal_bytes_equal']:report['passed']=False;status=1
  else:pids=[]
  if claimed:
   cleanup('RetainOwnedServiceLogs',lambda:run('logs','--no-color','baseline',timeout=15))
   cleanup('ComposeDown',lambda:run('down','--volumes','--remove-orphans','--timeout','10',timeout=45));remaining=cleanup('FreshOwnedInventories',inventories);report['remaining']=remaining
   if remaining is None or any(remaining.values()):report['passed']=False;status=1
  if container:
   raw=cleanup('ExactCIDAbsent',lambda:command(['docker','container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],timeout=10));report['exact_CID_absent']=raw is not None and not raw.strip()
   if not report['exact_CID_absent']:report['passed']=False;status=1
  for job in jobs:
   if not job.physically_settled:cleanup('WaitPipes',lambda job=job:job.wait(5))
  gone=[]
  stop_pids=[]
  if 'stop_receipt'in report:stop_pids=[{'pid':report['stop_receipt']['pid'],'observed_starttime':report['stop_receipt'].get('starttime')}]
  for row in report['processes']+pids+stop_pids:
   pid=row['pid'];observed=row['observed_starttime'];gone.append({'pid':pid,'observed_starttime':observed,'actual_identity_observed':observed is not None,'original_absent':observed is not None and identity(pid)!=observed})
  save(out/'all-original-host-PID-absence.json',gone);report['all_original_host_PIDs_absent']=all(r['original_absent']for r in gone);report['all_children_waited_and_pipes_settled']=all(j.physically_settled and j.process.poll()is not None for j in jobs);report['cleanup_elapsed_seconds']=time.monotonic()-cleanup_start;report['cleanup_within90']=report['cleanup_elapsed_seconds']<=90;report['actual_command_count']=count+int(stopping);report['aggregate_command_output_bytes']=used;report['aggregate_command_and_stop_output_bytes']=used+sum((out/name).stat().st_size for name in ['owned-stop.stdout','owned-stop.stderr']if(out/name).exists());report['total_output_within64MiB']=report['aggregate_command_and_stop_output_bytes']<=67108864
  if not report['all_original_host_PIDs_absent']or not report['all_children_waited_and_pipes_settled']or not report['cleanup_within90']or not report['total_output_within64MiB']:report['passed']=False;status=1
  report['actual_controller_exit']=status;report['settled_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();save(out/'execution-report.json',report)
 return status
if __name__=='__main__':raise SystemExit(main())
