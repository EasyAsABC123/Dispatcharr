#!/usr/bin/env python3
"""Complete blocked Source-only special-group executor proposal; Root cue required."""
import argparse,base64,datetime,hashlib,json,os,re,shutil,tarfile,threading,time,sys
from pathlib import Path
M6_ROOT=Path(__file__).resolve().parent
SHARED=M6_ROOT.parent/"m235/source_workflows"
sys.path.insert(0,str(SHARED))
from bounded_process import Collected
from bounded_stop import collect as stop_collect
from host_helpers import require,start,environment,collected
from default_isolation import compare as compare_default
import tooling_guard as T
REPO=Path('/home/justin/github/Dispatcharr');ROOT=SHARED
ARCHIVE=REPO/'artifacts/rust/m5-core-settings-source-executor-2/inputs/original-source.tar'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
OUTPUT=None  # assigned from the one global batch context, never an old output/cue
SOURCE_CASES=None  # explicit missing-domain selection, never replay a qualified set
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def save(p,v):Path(p).write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def selfcheck():
 # Global registry input closure and Original data are validated by batch adapter.
 # This is called again during final cleanup, never an older per-helper seal.
 for path,digest in json.loads((ROOT/'inputs/Original-pins.json').read_text()).items():
  require(sha(ROOT/'inputs/Original'/path)==digest,'ExactPinnedOriginal:'+path)
 require(sha(ARCHIVE)=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive')
 return json.loads((ROOT/'definition/plan.json').read_text())

def execute(plan,cue):
 global ROOT
 out=OUTPUT
 # Freeze reusable tooling and the guest before the first application command.
 # Earlier retained epochs lacked this binding; their manifests keep that limit.
 shared=out/'frozen-shared-tools';guest=out/'frozen-m6-tools'
 shutil.copytree(ROOT,shared,ignore=shutil.ignore_patterns('__pycache__','*.pyc'))
 guest.mkdir()
 for path in M6_ROOT.iterdir():
  if path.is_file()and path.suffix in('.py','.json','.md'):shutil.copyfile(path,guest/path.name)
 for root in(shared,guest):
  for path in root.rglob('*'):
   if path.is_file():path.chmod(0o444)
   elif path.is_dir():path.chmod(0o755)
  root.chmod(0o755)
 save(out/'tool-source-sha256.json',{str(path.relative_to(out)):sha(path)for root in(shared,guest)for path in root.rglob('*')if path.is_file()})
 ROOT=shared
 began=time.monotonic();work_deadline=began+450;cleanup_deadline=None;meta=json.loads((ROOT/'tooling-inputs.json').read_text());env=environment(meta);docker=meta['docker']['path'];project='dispatcharr-m6-source-'+cue['nonce'];report={'passed':False,'capture_only':True,'actual_Source_requests':0,'actual_Native_requests':0,'Source32_replay':False,'Source51_replay':False,'workers_or_beat_started':False,'project':project,'commands':[],'cohorts':[]};roles={};jobs=[];serial=0;owned=False;first=None;stopping=False;stop_event=threading.Event();compose=out/'compose.json';base=[docker,'compose','-p',project,'-f',str(compose)]
 def left():return max(.01,(cleanup_deadline if cleanup_deadline else work_deadline)-time.monotonic())
 def stop_owned():
  nonlocal stopping
  if not owned:return
  if stopping:require(stop_event.wait(20),'OwnedStopWait');return
  stopping=True
  try:stop_collect(base+['stop','--timeout','10'],env,out,{},limit=1048576,timeout=min(15,left()))
  finally:stop_event.set()
 def command(argv,seconds=30):
  nonlocal serial
  require(left()>.01,'DeclaredHostBudget');serial+=1;tag='command-'+str(serial).zfill(3);p=out/(tag+'.stdout');e=out/(tag+'.stderr');t=time.time();job=Collected(argv,env,1048576,stop_owned,stdout_path=p,stderr_path=e);identity=start(job.process.pid);require(identity is not None,'ActualChildStarttime');jobs.append(job)
  try:return collected(job,min(seconds,left()),p)
  finally:
   row={'argv':argv,'cwd':str(REPO),'observer_PID_namespace':os.readlink('/proc/self/ns/pid'),'PID':job.process.pid,'starttime':identity,'began_epoch':t,'completed_epoch':time.time(),'actual_exit':job.process.poll(),'invalid':job.invalid,'physically_settled':job.physically_settled,'pipe_threads_settled':not any(t.is_alive()for t in job.threads),'stdout_sha256':sha(p),'stderr_sha256':sha(e)};report['commands'].append(row);save(out/(tag+'.json'),row);save(out/'execution-private.json',report)
 def run(*args,seconds=30):return command(base+list(args),seconds)
 def shell(stage,group=None,label=None):
  db='dispatcharr'if stage=='audit'else'rust_probe_backup_jobs_'+cue['nonce'];redisdb=7 if stage=='audit'else next(x['Redis_database']for x in plan['cohorts']if x['group']==group);log='default'if stage=='audit'else group
  return run('exec','-T','-e','SPECIAL_SOURCE_AUTHORITY=owned-new-special-only','-e','SPECIAL_STAGE='+stage,'-e','SPECIAL_NONCE='+cue['nonce'],'-e','PROFILE_SOURCE_AUTHORITY=StreamProfile18-owner6-only','-e','PROFILE_SOURCE_NONCE='+cue['nonce'],'-e','HEALTH_SOURCE_AUTHORITY=one-new-TS-cache-only','-e','HEALTH_SOURCE_NONCE='+cue['nonce'],'-e','SPECIAL_GROUP='+(group or''),'-e','SPECIAL_AUDIT_LABEL='+(label or''),'-e','POSTGRES_DB='+db,'-e','REDIS_DB='+str(redisdb),'-e','REDIS_PORT='+('6380'if stage=='exercise'else'6379'),'-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:'+('6380'if stage=='exercise'else'6379')+'/'+str(redisdb),'-e','DISPATCHARR_LOG_DIR=/evidence/special-logs/'+log,'baseline','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy;runpy.run_path('/special-root/source_fixture.py',run_name='__main__')",seconds=180 if stage=='audit'else 60 if stage=='profile'else 40)
 def inventory():
  return {kind:command([docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format',fmt]).decode().splitlines()for kind,fmt in [('container','{{.ID}}'),('network','{{.ID}}'),('volume','{{.Name}}')]}
 try:
  def metadata_runner(argv,**kwargs):
   from types import SimpleNamespace
   raw=command(argv,seconds=5);return SimpleNamespace(returncode=0,stdout=raw.decode(),stderr=(out/('command-'+str(serial).zfill(3)+'.stderr')).read_text())
  report['tooling_preflight']=T.preflight(env,meta,metadata_runner)
  require(not any(inventory().values()),'FreshOwnedProjectAbsent');info=json.loads(command([docker,'image','inspect',IMAGE]))[0];require(IMAGE in info['RepoDigests'],'PinnedOriginalLocalImage')
  source=out/'Original';source.mkdir();command(['tar','-xf',str(ARCHIVE),'--no-same-owner','-C',str(source)],seconds=30);(source/'media').mkdir(exist_ok=True)
  common={'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_DB':'7','REDIS_URL':'redis://:playback-fixture-only@127.0.0.1:6379/7','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','DISPATCHARR_LOG_DIR':'/evidence/special-logs/default','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/special-root:/app','PUID':'1000','PGID':'1000','TZ':'UTC','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'}
  save(compose,{'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','network_mode':'none','working_dir':'/app','entrypoint':['bash','/special-root/start.sh'],'environment':common,'volumes':[str(source)+':/app:ro',str(ROOT)+':/special-root:ro',str(ROOT/'inputs')+':/health-root:ro',str(guest)+':/m6-root:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=268435456'],'mem_limit':'1g','pids_limit':256}}});owned=True;run('up','-d','--no-build','--pull','never','baseline');cid=run('ps','--all','-q','baseline').decode().strip();require(re.fullmatch('[a-f0-9]{64}',cid),'ActualOwnedFullCID');v=json.loads(command([docker,'inspect',cid]))[0];require(v['Config']['Labels']['com.docker.compose.project']==project and v['HostConfig']['NetworkMode']=='none','ExactOwnedNetworkNone');require(v['State']['Running']is True and start(v['State']['Pid'])is not None,'ActualRunningBaselineIdentity');roles['baseline']={'CID':cid,'observer_PID_namespace':os.readlink('/proc/self/ns/pid'),'host_PID':v['State']['Pid'],'host_starttime':start(v['State']['Pid'])};save(out/'owned-role.json',roles)
  end=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():require(time.monotonic()<end and left()>.01,'Bootstrap180');time.sleep(.1)
  shell('audit',label='before');before=json.loads((out/'default-before.json').read_text());require(before['capture_valid']is True,'DefaultBeforeComplete')
  database='rust_probe_backup_jobs_'+cue['nonce'];require(re.fullmatch('rust_probe_backup_jobs_[a-f0-9]{10}',database),'ExactOwnedCloneName')
  run('exec','-T','baseline','psql','-X','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database+' TEMPLATE dispatcharr')
  # Only the owned clone is reset. The default template is audited byte-for-byte.
  reset="DELETE FROM plugins_pluginconfig; DELETE FROM plugins_pluginrepo; DELETE FROM dispatcharr_connect_deliverylog; DELETE FROM dispatcharr_connect_eventsubscription; DELETE FROM dispatcharr_connect_integration;"
  run('exec','-T','baseline','psql','-X','-h','/var/run/postgresql','-U','dispatch','-d',database,'-v','ON_ERROR_STOP=1','-c',reset)
  run('exec','-T','baseline','mkdir','-p','/evidence/m6-empty-plugins')
  config=json.loads((M6_ROOT/'Source-input-template.json').read_text());now=datetime.datetime.now(datetime.timezone.utc)
  xml=('<?xml version="1.0"?><tv><channel id="m6.batch.owned"><display-name>Owned</display-name></channel><programme channel="m6.batch.owned" start="'+(now+datetime.timedelta(minutes=5)).strftime('%Y%m%d%H%M%S +0000')+'" stop="'+(now+datetime.timedelta(minutes=35)).strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned M6 Programme</title></programme></tv>').encode()
  config.update(Source_cases=SOURCE_CASES,root_authorized=True,database=database,output='/evidence/m6-missing-domains',prefix='m6_'+cue['nonce'],XMLTV_base64=base64.b64encode(xml).decode());save(out/'m6-inputs.json',config)
  args=['exec','-T','-e','POSTGRES_DB='+database,'-e','POSTGRES_HOST=127.0.0.1','-e','DISPATCHARR_PLUGINS_DIR=/evidence/m6-empty-plugins','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','-e','REDIS_DB=0','-e','REDIS_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','-e','PYTHONPATH=/m6-root:/app','baseline','/dispatcharrpy/bin/python','/m6-root/run_source_extension.py','--inputs','/evidence/m6-inputs.json']
  run(*args,seconds=180)
  raw=json.loads((out/'m6-missing-domains/Source-extension.json').read_text());report['Source_extension']={'raw_sha256':sha(out/'m6-missing-domains/Source-extension.json'),'passed':raw['passed'],'actual_cases':len(raw.get('report',{}).get('cases',[]))};report['workers_or_beat_started']=True
  require(raw['passed']is True,'ActualWholeMissingSourceCohort');require(report['Source_extension']['actual_cases']==(4 if SOURCE_CASES=='versions-only'else 10),'ActualSelectedSourceCases');report['capture_complete']=True
 except BaseException as e:first=e;report['first_failure']={'type':type(e).__name__,'message':str(e)[:4096]}
 finally:
  cleanup_deadline=min(began+540,time.monotonic()+90);steps=[];report['cleanup_steps']=steps
  def cleanup(name,fn):
   try:value=fn();steps.append({'name':name,'passed':True,'value':value})
   except BaseException as e:steps.append({'name':name,'passed':False,'type':type(e).__name__,'message':str(e)[:2048]})
   save(out/'execution-private.json',report)
  def after_audit():
   shell('audit',label='after');a=json.loads((out/'default-after.json').read_text());b=json.loads((out/'default-before.json').read_text());report['default_SQL_and_cache_isolation']=compare_default(b,a);return report['default_SQL_and_cache_isolation']
  def discover_partial():
   if not owned:return {'performed':False}
   ids=run('ps','--all','-q','baseline').decode().splitlines()
   if not ids:return {'role_observed_absent':True}
   require(len(ids)==1 and re.fullmatch('[a-f0-9]{64}',ids[0]),'OneExactPartialBaselineCID');cid=ids[0];v=json.loads(command([docker,'inspect',cid]))[0];require(v['Config']['Labels']['com.docker.compose.project']==project and v['Config']['Labels']['com.docker.compose.service']=='baseline'and v['Config']['Image']==IMAGE,'PartialOwnedLabelAndImage')
   if 'baseline'not in roles:
    row={'CID':cid,'partial_setup_discovered':True,'observed_state':v['State']}
    if v['State']['Running']:
     identity=start(v['State']['Pid']);require(identity is not None,'ActualPartialRunningIdentity');row.update(host_PID=v['State']['Pid'],host_starttime=identity,observer_PID_namespace=os.readlink('/proc/self/ns/pid'))
    roles['baseline']=row;save(out/'owned-role.json',roles)
   else:require(roles['baseline']['CID']==cid,'SameOriginalBaselineCID')
   return {'exact_CID_observed':cid}
  cleanup('discover_partial_owned_baseline',discover_partial)
  if owned:cleanup('default_after_full_audit',after_audit)
  else:steps.append({'name':'default_after_full_audit','passed':False,'status':'not_run','reason':'No fixture was created'})
  def backends():
   x=run('exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_stat_activity WHERE datname='rust_probe_backup_jobs_"+cue['nonce']+"'");require(x.strip()==b'0','OwnedBackendAbsenceUnixSocket');return True
  if owned:cleanup('owned_backend_absence_Unix',backends)
  else:steps.append({'name':'owned_backend_absence_Unix','passed':False,'status':'not_run','reason':'No fixture was created'})
  def stopped_source():
   run('stop','--timeout','25','baseline',seconds=30)
   if'baseline'in roles:
    v=json.loads(command([docker,'inspect',roles['baseline']['CID']]))[0];require(v['State']['Running']is False and v['State']['ExitCode']==0,'NormalPGRedisStop0');return v['State']
  if owned:cleanup('normal_management_PGRedis_stop',stopped_source);cleanup('owned_compose_down',lambda:run('down','--volumes','--remove-orphans','--timeout','10').decode())
  def fresh():
   data=inventory();require(not any(data.values()),'FreshExactProjectEmpty')
   for role,row in roles.items():require(not command([docker,'container','ls','--all','--no-trunc','--filter','id='+row['CID'],'--format','{{.ID}}']).strip(),'ExactOriginalCIDAbsent')
   return data
  if owned:cleanup('fresh_exact_project_and_CID_absence',fresh)
  else:steps.append({'name':'fresh_exact_project_and_CID_absence','passed':False,'status':'not_run','reason':'Docker preflight failed before resource creation'})
  def processes():
   require(all(j.process.poll()is not None and all(not t.is_alive()for t in j.threads)for j in jobs),'AllOwnedCLIsWaitedPipesJoined')
   for row in report['commands']:
    require(row['observer_PID_namespace']==os.readlink('/proc/self/ns/pid'),'SameObserverPIDNamespaceForCLIClaim');require(start(row['PID'])!=row['starttime'],'EveryOriginalCLIPIDStartGone')
   for row in roles.values():
    if 'host_PID'in row:
     require(row.get('observer_PID_namespace')==os.readlink('/proc/self/ns/pid'),'SameObserverPIDNamespaceForContainerClaim');require(start(row['host_PID'])!=row['host_starttime'],'OriginalContainerHostPIDGone')
   return {'CLI_identities':len(report['commands']),'container_identities':len(roles)}
  cleanup('physical_CLI_container_settlement',processes);cleanup('final_frozen_input_recheck',lambda:bool(selfcheck()));report['cleanup_steps']=steps;report['elapsed_seconds']=time.monotonic()-began;report['passed']=first is None and report.get('capture_complete')is True and all(s['passed']for s in steps)and report['elapsed_seconds']<=540;report['controller_exit']=0 if report['passed']else 1;save(out/'execution-private.json',report)
 return report['controller_exit']
# Executed only through the single milestone batch adapter/global dispatcher.

if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--output',required=True,type=Path);p.add_argument('--nonce',required=True);p.add_argument('--cases',required=True,choices=['all','versions-only']);a=p.parse_args();SOURCE_CASES=a.cases;require(re.fullmatch('[a-f0-9]{10}',a.nonce),'OwnedNonce');OUTPUT=a.output.resolve();require(not OUTPUT.exists(),'FreshOutput');OUTPUT.mkdir(mode=0o777);OUTPUT.chmod(0o777);raise SystemExit(execute(selfcheck(),{'nonce':a.nonce}))
