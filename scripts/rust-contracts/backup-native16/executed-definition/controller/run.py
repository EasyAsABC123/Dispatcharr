#!/usr/bin/env python3
"""Default read-only exact preflight; reviewed once-only Native16 owned capture."""
import argparse,datetime,hashlib,json,os,re,subprocess,time,uuid
from pathlib import Path
from bounded_process import Collected
from bounded_stop import collect as bounded_stop
import threading
from validate_native import validate
ROOT=Path(__file__).resolve().parents[1]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
TEST='backup_application::source16_peer::source16_real_credentials_router_and_worker_capture'
def require(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def check():
 seal=json.loads((ROOT/'seal.json').read_text());require(seal['state']=='COMPILED_NATIVE16_EXECUTION_REVIEW','CompiledExecutionOnly')
 paths=[p for d in ('controller','inputs','build')for p in(ROOT/d).rglob('*')if p.is_file()and'__pycache__'not in p.parts]+[ROOT/'outside-peer.json',ROOT/'README.md',ROOT/'execution-plan.json',ROOT/'local-controls.log',ROOT/'ELF-input-copy.json',ROOT/'prior-unexecuted-seal.json',ROOT/'ELF-revision2-input-copy.json',ROOT/'revision1-to2.diff']
 require(set(seal['local_inputs'])=={str(p.relative_to(ROOT))for p in paths},'CompleteLocalSealedInventory')
 for relative,digest in seal['local_inputs'].items():
  p=ROOT/relative;require(not p.is_symlink()and p.is_file()and sha(p)==digest,'ExactInput:'+relative)
 source=Path(seal['native_source_root']).resolve();expected=json.loads((ROOT/'inputs/full802-map.json').read_text());actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()};require(actual==expected and len(actual)==802,'Exact802Source');require(all(p.stat().st_mode&0o222==0 for p in source.rglob('*')if p.is_file()),'Readonly802')
 build=json.loads((ROOT/'build/build.json').read_text());peer=json.loads((ROOT/'outside-peer.json').read_text());require(build['passed']is True and build['state']=='SETTLED'and len(build['commands'])==5 and all(r['actual_wait']==r['actual_poll']==0 for r in build['commands']),'FiveActualCompileWaitPoll0');require(json.loads((ROOT/'build/source-before.json').read_text())==expected==json.loads((ROOT/'build/source-after.json').read_text()),'Compiled802PrePostExact');require(peer['compiled_source_root']==str(source)and peer['full_source_map_sha256']==sha(ROOT/'inputs/full802-map.json'),'ELFExactSourceBinding');require(sha(ROOT/'inputs/backup-source16-job-tests')==seal['ELF_sha256']==peer['ELF_sha256']and(ROOT/'inputs/backup-source16-job-tests').stat().st_size==peer['ELF_bytes'],'CompletedELFHashSize');require(sha(ROOT/'inputs/Source16-projection.json')=='5f0ce76f0d308da14cce1c0f57899577dd14732e1bc818ee916864d2d1902760','StrictProjectedSource16');require(datetime.datetime.fromisoformat(peer['copied_utc'])>=datetime.datetime.fromisoformat(build['completed_utc']),'ELFCopiedAfterAllCompleted')
 return seal

def cleanup_window(started,observed):
 import math
 elapsed=observed-started
 return {'elapsed_seconds':elapsed,'within_declared_90_seconds':math.isfinite(elapsed)and 0<=elapsed<=90}

def execute(args,seal):
 require(args.execute_reviewed_seal==sha(ROOT/'seal.json'),'ExactReviewedSeal');require(args.output is not None,'FreshOutputRequired');out=args.output.resolve();out.mkdir(parents=True,exist_ok=False,mode=0o700);project='dispatcharr-backupjob-native-'+uuid.uuid4().hex[:10];database='rust_probe_backupjob_native_'+uuid.uuid4().hex[:10];require(re.fullmatch('rust_probe_backupjob_native_[a-f0-9]{10}',database)is not None,'OwnedDatabase')
 env={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','CELERY_','DJANGO_'))};source=Path(seal['native_source_root']).resolve();service={'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/tmp','entrypoint':['bash','/native-tools/start.sh'],'environment':{'DISPATCHARR_ENV':'aio','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PUID':'1000','PGID':'1000','PYTHONDONTWRITEBYTECODE':'1','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(ROOT/'controller')+':/native-tools:ro',str(ROOT/'inputs/pg-init')+':/pg-bootstrap:ro',str(ROOT/'inputs')+':/native-inputs:ro',str(ROOT)+':/native-root:ro',str(source)+':/native-source:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1536m','pids_limit':256,'networks':['default']};save(out/'compose.json',{'services':{'baseline':service},'networks':{'default':{'internal':True}}});compose=[args.docker,'compose','-p',project,'-f',str(out/'compose.json')];jobs=[];serial=0;claimed=False;container=None;before=None;started=False;cleanup_deadline=None;cleanup_started=None;stop_started=False;stop_done=threading.Event();status=1;pid_records=[];began=time.monotonic();report={'passed':False,'project':project,'database':database,'Native_only':True,'Source_application_runs':0,'Main_runs':0,'resource_sampler':False,'Source16_framework_scope_preserved':True,'seal_sha256':sha(ROOT/'seal.json'),'processes':[]}
 def stop():
  nonlocal stop_started
  if not claimed:return
  if stop_started:
   require(stop_done.wait(35),'ExistingOwnedStopSettlementDeadline');require(report.get('owned_stop_receipt',{}).get('acknowledged')is True,'PriorOwnedStopWasNotAcknowledged');return
  stop_started=True;receipt={};report['owned_stop_receipt']=receipt
  try:
   remaining=15 if cleanup_deadline is None else min(15,max(.01,cleanup_deadline-time.monotonic()))
   bounded_stop(compose+['stop','--timeout','10'],env,out,receipt,timeout=remaining,save=save)
  finally:stop_done.set()
 def begin_cleanup():
  nonlocal cleanup_started,cleanup_deadline
  if cleanup_started is None:
   cleanup_started=time.monotonic();cleanup_deadline=cleanup_started+90;report['cleanup_started_utc']=now();report['cleanup_budget_seconds']=90
 def command(argv,timeout=120,kind=None):
  nonlocal serial
  timeout=min(timeout,max(.01,(cleanup_deadline-time.monotonic())if cleanup_deadline is not None else 1200-(time.monotonic()-began)));serial+=1;tag=f'command-{serial:03d}';job=Collected(argv,env,8388608,stop,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);identity=start(job.process.pid);when=now()
  try:job.wait(timeout)
  finally:
   row={'argv':argv,'kind':kind,'pid':job.process.pid,'starttime':identity,'actual_exit':job.process.poll(),'collection_invalid':job.invalid,'owned_stop_requested':job.stopped,'physically_settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'began_utc':when,'completed_utc':now()};save(out/(tag+'.json'),row);report['processes'].append(row)
  return(out/(tag+'.stdout')).read_text()
 def inventories():return {kind:command([args.docker]+argv+['--filter','label=com.docker.compose.project='+project],15)for kind,argv in{'containers':['container','ls','--all','--no-trunc','--format','{{.ID}}'],'networks':['network','ls','--format','{{.ID}}'],'volumes':['volume','ls','--format','{{.Name}}']}.items()}
 def audit(stage,label,db):
  command(compose+['exec','-T','-e','NOTIFICATION_NATIVE_AUDIT=owned-native-only','-e','NOTIFICATION_AUDIT_STAGE='+stage,'-e','NOTIFICATION_AUDIT_LABEL='+label,'-e','NOTIFICATION_AUDIT_DATABASE='+db,'baseline','/dispatcharrpy/bin/python','/native-tools/audit.py'],45);value=json.loads((out/(label+'.json')).read_text());require(value['passed']is True and value['Django_or_Source_application_loaded']is False,'NoSourceReadOnlyAudit');return value['value']
 try:
  initial=inventories();save(out/'initial-project-inventories.json',initial);require(not any(v.strip()for v in initial.values()),'InitiallyAbsentProject');claimed=True;command(compose+['up','-d','--no-build','--pull','never','baseline']);deadline=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'StorageBootstrap180');time.sleep(.1)
  ids=command(compose+['ps','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedContainer');container=ids[0];inspection=json.loads(command([args.docker,'inspect',container]));save(out/'container-inspect.json',inspection);require(inspection[0]['Config']['Image']==IMAGE and inspection[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedStorageImage');before=audit('default','default-before','dispatcharr')
  absence=command(compose+['exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+database+"'"]);require(absence.strip()=='0','FreshTargetDB');command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database]);command(compose+['exec','-T','baseline','redis-cli','--raw','SET','rust-probe-backup-native-owner',database]);audit('cache','cache-before',database);loader=command(compose+['exec','-T','baseline','ldd','/native-inputs/backup-source16-job-tests'],30);require('not found'not in loader,'ActualELFLoaderClosure');started=True
  report['Native_process_command_next']=serial+1;save(out/'execution-progress.json',report)
  command(compose+['exec','-T','-e','RUST_BACKTRACE=1','-e','BACKUP_SOURCE16_NATIVE=owned-native-only','-e','BACKUP_SOURCE16_INPUTS=/native-inputs','-e','BACKUP_SOURCE16_OUTPUT=/evidence','-e','POSTGRES_DB='+database,'-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'-e','BACKUP_CACHE_URL=redis://:playback-fixture-only@127.0.0.1:6379/1','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@127.0.0.1:6379/2','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@127.0.0.1:6379/3','baseline','/native-inputs/backup-source16-job-tests',TEST,'--exact','--ignored','--nocapture','--test-threads=1'],360,'actual-Native16-capture-once')
  begin_cleanup()
  actual=json.loads((out/'native-source16.json').read_text());qualified=validate(actual);save(out/'effect-validation.json',qualified);report['passed']=qualified['passed'];report['actual_request_count']=actual['actual_request_count'];status=0 if qualified['passed']else 1
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)[:4096],'message_full_bytes':len(str(error).encode()),'message_full_sha256':hashlib.sha256(str(error).encode()).hexdigest()};status=1;report['passed']=False
 finally:
  begin_cleanup()
  capture=out/'native-source16.json'
  if capture.exists():
   try:
    actual=json.loads(capture.read_text());report['actual_request_count']=actual.get('actual_request_count',len(actual.get('requests',[])));qualified=validate(actual);save(out/'effect-validation.json',qualified)
   except BaseException as error:report['effect_validation_error']=type(error).__name__+':'+str(error)[:4096];report['passed']=False;status=1
  if started:
   try:
    audit('cache','cache-after',database);deadline=time.monotonic()+10;observations=[]
    while True:
     raw=command(compose+['exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,backend_start,state,wait_event_type,wait_event FROM pg_stat_activity WHERE datname='"+database+"') t"],10,'exact-owned-backend-after-ELF-exit');values=json.loads(raw);observations.append({'utc':now(),'actual_backends':values});save(out/'backend-after-ELF-exit.json',observations)
     if values==[]:report['owned_backends_absent_after_process_exit']=True;break
     require(time.monotonic()<deadline,'OwnedBackendPhysicalAbsence10');time.sleep(.1)
   except BaseException as error:report['backend_or_cache_cleanup_error']=str(error)[:4096];report['passed']=False;status=1
  if before is not None:
   try:after=audit('default','default-after','dispatcharr');report['default11_exact']=before==after;require(before==after,'Default11Exact')
   except BaseException as error:report['default_after_error']=str(error)[:4096];report['passed']=False;status=1
  if claimed:
   try:
    if container:
     listing=command([args.docker,'top',container,'-eo','pid,comm,args'],15);save(out/'precleanup-processes.json',{'raw':listing})
     for line in listing.splitlines()[1:]:
      if line.strip():pid=int(line.split()[0]);pid_records.append({'pid':pid,'starttime':start(pid)})
     save(out/'precleanup-PIDs.json',pid_records);require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|backup-source16-job-tests)\b',listing),'NoAppOrNativeProcessLeft')
   except BaseException as error:report['precleanup_error']=str(error)[:4096];report['passed']=False;status=1
   try:command(compose+['down','--volumes','--remove-orphans','--timeout','10'],60)
   except BaseException as error:report['down_error']=str(error)[:4096];report['passed']=False;status=1
   try:
    remaining=inventories();save(out/'cleanup-project-inventories.json',remaining);require(not any(v.strip()for v in remaining.values()),'AllOwnedProjectResourcesAbsent')
    if container:raw=command([args.docker,'container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],15);save(out/'exact-CID-absence.json',{'CID':container,'raw':raw});require(not raw.strip(),'ExactCIDAbsent')
    absence=[dict(r,after_starttime=start(r['pid']),original_process_absent=start(r['pid'])is None or start(r['pid'])!=r['starttime'])for r in pid_records];save(out/'cleanup-PID-absence.json',absence);require(all(r['original_process_absent']for r in absence),'OwnedPIDsAbsent')
   except BaseException as error:report['cleanup_error']=str(error)[:4096];report['passed']=False;status=1
  report['all_command_processes_settled']=all(j.physically_settled and j.process.poll()is not None and not any(t.is_alive()for t in j.threads)for j in jobs)
  if not report['all_command_processes_settled']:report['passed']=False;status=1
  window=cleanup_window(cleanup_started,time.monotonic());report['cleanup_elapsed_seconds']=window['elapsed_seconds'];report['cleanup_within_declared_90_seconds']=window['within_declared_90_seconds']
  if not report['cleanup_within_declared_90_seconds']:report['passed']=False;status=1
  report['actual_controller_exit']=status;report['settled_utc']=now();save(out/'execution-report.json',report)
 return status

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute-reviewed-seal');p.add_argument('--output',type=Path);p.add_argument('--docker',default='docker');args=p.parse_args();seal=check()
 if args.execute_reviewed_seal is None:print(json.dumps({'passed':True,'seal_sha256':sha(ROOT/'seal.json'),'Native_Source_Docker_runs':0}));return 0
 return execute(args,seal)
if __name__=='__main__':raise SystemExit(main())
