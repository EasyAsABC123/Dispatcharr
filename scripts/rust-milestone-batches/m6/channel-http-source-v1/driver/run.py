"""One bounded ordinary Source HTTP producer authority node using the existing global dispatcher."""
import argparse,fcntl,hashlib,importlib.util,json,os,re,shutil,sys,tarfile,time,uuid
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
DOCKER=str(ROOT/'artifacts/rust/tooling/docker/docker');COORD=ROOT/'scripts/rust-milestone-batches/coordinator.py'
PRIOR=ROOT/'artifacts/rust/m478-source-normal-wsgi-cache-1'
def load(p,n):s=importlib.util.spec_from_file_location(n,p);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
C=load(COORD,'existing_coordinator')
def save(p,v):p.write_text(json.dumps(v,indent=2,default=str)+'\n')
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def need(v,label):
 if not v:raise RuntimeError(label)
def controller(out):
 lease=json.loads((HERE/('lease-'+out.name+'.json')).read_text())
 need(lease.get('state')=='acquired'and lease.get('dispatcher_pid')==os.getppid(),'ActualParentDispatcherLease')
 need(Path('/proc',str(os.getppid()),'stat').read_text().rsplit(')',1)[1].split()[19]==lease['dispatcher_start_generation'],'ExactLiveDispatcherGeneration')
 with (COORD.parent/'.dispatcher.lock').open('a')as probe:
  try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB)
  except BlockingIOError:pass
  else:fcntl.flock(probe,fcntl.LOCK_UN);raise RuntimeError('GlobalDispatcherLockNotHeld')
 began=time.monotonic();deadline=began+600;cleanup_deadline=None;serial=0;out.mkdir();report={'passed':False,'Source_API_calls':0,'Source_case_replay':False,'commands':[],'lease_started_monotonic':began,'functional_deadline_monotonic':deadline,'container_ids':[]};save(out/'report.json',report)
 tools=out/'tools';shutil.copytree(HERE/'tools',tools,ignore=shutil.ignore_patterns('__pycache__','*.pyc'));
 shutil.copytree(HERE/'inputs/pg-init',out/'pg-init');shutil.copytree(HERE/'inputs/schema',out/'schema');source=out/'Original';source.mkdir()
 archive=ROOT/'artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar';need(sha(archive)=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','PinnedOriginalArchive')
 with tarfile.open(archive)as t:
  for member in t:
   target=source/member.name;need(not Path(member.name).is_absolute() and '..'not in Path(member.name).parts and (member.isfile()or member.isdir()),'SafeOriginalRegularClosure')
   if member.isdir():target.mkdir(parents=True,exist_ok=True)
   else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(t.extractfile(member).read());target.chmod(0o644)
 for path in list(source.rglob('*'))+[source,tools,out]:
  if path.is_dir():path.chmod(0o755)
 for path in tools.iterdir():path.chmod(0o644)
 project='dispatcharr-channel-http-source-'+uuid.uuid4().hex[:10];database='rust_probe_channel_http_'+uuid.uuid4().hex[:10];report.update(project=project,database=database,source_archive_sha256=sha(archive));save(out/'report.json',report)
 environment=load(tools/'bootstrap_env.py','storage_env').storage_environment()
 service={'image':IMAGE,'pull_policy':'never','network_mode':'none','entrypoint':['bash','/tools/start.sh'],'environment':environment,'volumes':[str(tools)+':/tools:ro',str(tools)+':/native-tools:ro',str(source)+':/app:ro',str(out/'schema')+':/native-source:ro',str(out/'pg-init')+':/pg-bootstrap:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'cpus':1,'mem_limit':'1536m','pids_limit':128,'stop_grace_period':'10s'}
 save(out/'compose.json',{'services':{'source':service}});base=[DOCKER,'compose','-p',project,'-f',str(out/'compose.json')];env={'DOCKER_HOST':'unix:///tmp/dispatcharr-docker.sock','DOCKER_CONFIG':str(ROOT/'artifacts/rust/tooling/docker-config')}
 def command(argv,seconds=30,check=True):
  nonlocal serial
  limit=cleanup_deadline if cleanup_deadline else deadline;left=limit-time.monotonic();need(left>0,'OneAbsoluteBudget');serial+=1;receipt=C.command(argv,{'output_bytes':4*1024*1024,'environment':env},out,'command-'+str(serial).zfill(3),min(seconds,left));report['commands'].append(receipt);save(out/'report.json',report)
  if check:need(receipt['actual_exit']==0 and not receipt['failure']and C.settled(receipt),'ActualWaitPipesExit0')
  return Path(receipt['stdout']).read_bytes(),receipt
 def dc(*args,**kwargs):return command(base+list(args),**kwargs)
 try:
  before=command([DOCKER,'container','ls','--all','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'])[0];need(not before.strip(),'FreshProject')
  dc('up','--detach','--pull','never',seconds=120);cid=dc('ps','--all','--quiet','source')[0].decode().strip();need(re.fullmatch('[a-f0-9]{64}',cid),'ExactCID');report['container_ids']=[cid]
  while time.monotonic()<deadline:
   _,r=dc('exec','-T','source','test','-f','/evidence/bootstrap-ready',seconds=10,check=False)
   if r['actual_exit']==0:break
   time.sleep(.1)
  else:raise TimeoutError('Storage bootstrap deadline')
  original_default=dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/tools/state.py')[0];(out/'default-before.json').write_bytes(original_default)
  dc('exec','-T','source','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database+' TEMPLATE dispatcharr')
  guest_env={'POSTGRES_DB':database,'JOB_DISPOSABLE_FIXTURE':'playback-fixture-only','REDIS_DB':'10','CELERY_BROKER_URL':'redis://:playback-fixture-only@127.0.0.1:6379/8','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@127.0.0.1:6379/9','PYTHONPATH':'/app:/tools'}
  argv=['exec','-T']
  for k,v in guest_env.items():argv+=['-e',k+'='+v]
  argv+=['source','/dispatcharrpy/bin/python','-B','/tools/guest.py'];_,guest=dc(*argv,seconds=480,check=False);report['actual_guest_exit']=guest['actual_exit'];report['guest_settled']=C.settled(guest)and not guest['failure']
  after=dc('exec','-T','source','/dispatcharrpy/bin/python','-B','/tools/state.py')[0];(out/'default-after.json').write_bytes(after);report['default_unchanged']=json.loads(original_default)==json.loads(after)
  source_report=json.loads((out/'source-report.json').read_text());report['cases']=len(source_report['cases']);report['source_passed']=source_report['passed'];report['Source_API_calls']=source_report['Source_API_calls'];report['passed']=guest['actual_exit']==0 and report['guest_settled']and report['default_unchanged']and source_report['passed']
 except BaseException as error:report['primary_error']={'type':type(error).__name__,'message':str(error)}
 finally:
  cleanup_deadline=time.monotonic()+90;report['cleanup_deadline_monotonic']=cleanup_deadline;save(out/'report.json',report)
  try:
   dc('logs','--no-color',seconds=10,check=False);dc('down','--timeout','10','--volumes','--remove-orphans',seconds=35)
   for kind in ['container','network','volume']:
    raw,_=command([DOCKER,kind,'ls','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}']);need(not raw.strip(),'ExactProject'+kind+'Absent')
   for cid in report['container_ids']:
    raw,_=command([DOCKER,'container','ls','--all','--no-trunc','--filter','id='+cid,'--format','{{.ID}}']);need(not raw.strip(),'ExactOriginalCIDAbsent')
   report['exact_owned_absence']=True
  except BaseException as error:report['cleanup_error']={'type':type(error).__name__,'message':str(error)};report['passed']=False
  report['all_CLI_groups_settled']=all(C.settled(r) and not r['failure']for r in report['commands']);report['passed']=report['passed']and report.get('exact_owned_absence')is True and report['all_CLI_groups_settled'];report['lease_ended_monotonic']=time.monotonic();report['actual_controller_exit']=0 if report['passed']else 1;save(out/'report.json',report)
 return report['actual_controller_exit']
def independent(out):
 r=json.loads((out/'report.json').read_text());checks=[]
 for kind in ['container','network','volume']:
  directory=out/'independent';directory.mkdir(exist_ok=True);receipt=C.command([DOCKER,kind,'ls','--filter','label=com.docker.compose.project='+r['project'],'--format','{{.ID}}'],{'output_bytes':65536,'environment':{'DOCKER_HOST':'unix:///tmp/dispatcharr-docker.sock','DOCKER_CONFIG':str(ROOT/'artifacts/rust/tooling/docker-config')}},directory,kind,min(15,max(.01,r['cleanup_deadline_monotonic']-time.monotonic())));checks.append(receipt['actual_exit']==0 and C.settled(receipt)and not receipt['failure']and not Path(receipt['stdout']).read_bytes().strip())
 proof={'exact_owned_absence':all(checks)and r.get('exact_owned_absence')is True,'checks':checks};save(out/'independent-cleanup.json',proof);print(json.dumps(proof));return 0 if proof['exact_owned_absence']else 1
def dispatch(out):
 inputs={str(p):sha(p)for p in [HERE/'run.py',COORD,ROOT/'artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar']+[p for p in HERE.rglob('*')if p.is_file()and '__pycache__'not in p.parts and p.name!='input-sha256.json']};candidate={'kind':'newSource4channel-HTTP-producer-authority','pinned_Original':IMAGE,'Source_API_replay':False}
 node={'id':'source/channel-maintenance-HTTP-producer','depends_on':[],'heavy':False,'resources':['owned-channel-source-fixture'],'entrypoint':'new actual Original TCP four-channel producer cohort'}
 bound={'argv':[sys.executable,str(HERE/'run.py'),'--controller',str(out)],'cleanup_argv':[sys.executable,str(HERE/'run.py'),'--cleanup',str(out)],'evaluator_argv':[sys.executable,str(HERE/'run.py'),'--evaluate',str(out)],'seconds':690,'cleanup_seconds':90,'output_bytes':4*1024*1024,'input_sha256':inputs}
 with (COORD.parent/'.dispatcher.lock').open('a')as lock:
  fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);save(HERE/('lease-'+out.name+'.json'),{'state':'acquired','dispatcher_pid':os.getpid(),'dispatcher_start_generation':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'monotonic':time.monotonic(),'node':node,'binding':bound,'candidate':candidate});report=C.schedule([node],{node['id']:bound},out.with_name(out.name+'-dispatcher'),candidate);save(HERE/('lease-'+out.name+'.json'),{'state':'released'if not report['cleanup_uncertain']else'cleanup_uncertain','monotonic':time.monotonic(),'report':str(out.with_name(out.name+'-dispatcher')/'report.json')})
 return 0 if report['full_batch_passed']else 1
p=argparse.ArgumentParser();g=p.add_mutually_exclusive_group(required=True)
for key in ['dispatch','controller','cleanup','evaluate']:g.add_argument('--'+key,type=Path)
a=p.parse_args()
if a.dispatch:raise SystemExit(dispatch(a.dispatch.absolute()))
if a.controller:raise SystemExit(controller(a.controller))
if a.cleanup:raise SystemExit(independent(a.cleanup))
if a.evaluate:
 r=json.loads((a.evaluate/'report.json').read_text());proof={'semantic_assertions_passed':r['passed'],'candidate':{'kind':'newSource4channel-HTTP-producer-authority','pinned_Original':IMAGE,'Source_API_replay':False},'entrypoint':'new actual Original TCP four-channel producer cohort','full_required_workflow':r['passed']};print(json.dumps(proof));raise SystemExit(0 if r['passed']else 1)
