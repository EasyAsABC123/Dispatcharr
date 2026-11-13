#!/usr/bin/env python3
"""Default read-only exact preflight; reviewed once-only backup physical case5 or6 owned capture."""
import argparse,datetime,hashlib,json,os,re,subprocess,time,uuid
from pathlib import Path
from bounded_process import Collected
from bounded_stop import collect as bounded_stop
import threading
from validate_physical import validate
from bootstrap_env import storage_environment,audit_helpers
ROOT=Path(__file__).resolve().parents[1]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
TESTS={i:'rest_reads::physical_tests::real_retained_rest_read_error_ready_case'for i in(1,2)}
def require(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def mounted_closure():
 source=Path(json.loads((ROOT/'seal.json').read_text())['native_source_root'])
 expected=[ROOT/'inputs/schema62-authority.json',ROOT/'inputs/imported-existing-version.json',ROOT/'inputs/pg-contracts.json',ROOT/'inputs/pg-init/01-user-setup.sh',ROOT/'inputs/pg-init/02-postgres.sh']+[source/'rust/crates/store/schema'/name for name in('original-1241b3-schema.sql','original-1241b3-defaults.sql','original-1241b3-fresh-timestamps.sql')]
 require(all(p.is_file()and p.stat().st_size>0 for p in expected),'AllLiteralMountedRuntimeFilesPresent')
 return {'passed':True,'paths':[str(p)for p in expected]}
def check():
 seal=json.loads((ROOT/'seal.json').read_text());require(seal['state']=='COMPILED_INERT_REST_READ_PHYSICAL_REVIEW','CompiledInertOnly')
 actual={p.relative_to(ROOT).as_posix():sha(p)for d in('controller','inputs','build')for p in(ROOT/d).rglob('*')if p.is_file()and '__pycache__'not in p.parts and p.name!='native-store-tests'}
 for name in('README.md','execution-plan.json','compile-seal.json'):actual[name]=sha(ROOT/name)
 require(actual==seal['local_inputs'],'CompleteExactLocalInputClosure')
 source=Path(seal['native_source_root']);expected=json.loads((ROOT/'inputs/full-source-map.json').read_text());observed={p.relative_to(source).as_posix():sha(p)for p in source.rglob('*')if p.is_file()};require(observed==expected and len(expected)==852,'Exact852Source');require(all(p.stat().st_mode&0o222==0 for p in source.rglob('*')if p.is_file()),'Readonly852')
 build=json.loads((ROOT/'build/build.json').read_text());require(build['passed']is True and len(build['commands'])==5 and all(r['actual_wait_exit']==r['actual_poll_exit']==0 for r in build['commands']),'FiveActualWaitPoll0')
 peer=json.loads((ROOT/'build/outside-peer.json').read_text());require(sha(ROOT/'inputs/native-store-tests')==peer['sha256']and(ROOT/'inputs/native-store-tests').stat().st_size==peer['bytes'],'CompletedELFExact');require(peer['copied_epoch']>=max(r['completed_epoch']for r in build['commands']),'CopyAfterAllCompleted');require(peer['source_map_sha256']==sha(ROOT/'inputs/full-source-map.json'),'ActualCompiledSourceBinding')
 require(mounted_closure()['passed']is True,'RecursiveMountedClosure');require(audit_helpers(ROOT,storage_environment())['passed']is True,'AllPinnedHelperEnvironmentVariablesProvided');return seal

def command_budget(serial,used,cleaning):
 require(type(serial)is int and serial>=0 and type(used)is int and used>=0 and type(cleaning)is bool,'TypedCommandBudget')
 require(serial<(256 if cleaning else 224),'FixedCommandCountWithCleanupReserve')
 # Root1–4 total64MiB/normal56MiB; reserve64KiB inside total for the sole neutral stop CLI.
 available=((64*1048576-65536)if cleaning else 56*1048576)-used
 require(available>0,'FixedAggregateOutputWithCleanupReserve')
 return min(1048576,available)

def cleanup_window(started,observed):
 import math
 elapsed=observed-started
 return {'elapsed_seconds':elapsed,'within_declared_90_seconds':math.isfinite(elapsed)and 0<=elapsed<=90}

def execute(args,seal):
 require(args.execute_reviewed_seal==sha(ROOT/'seal.json'),'ExactReviewedSeal');require(args.output is not None,'FreshOutputRequired');out=args.output.resolve();require(out.parent==ROOT.parent and re.fullmatch('m5-rest-read-physical-sql-'+str(args.case)+'-[1-9][0-9]*',out.name)is not None,'FixedFreshCaseOutputNamespace');out.mkdir(parents=True,exist_ok=False,mode=0o700);project='dispatcharr-rest-read-'+uuid.uuid4().hex[:10];database='rust_probe_rest_reads_'+uuid.uuid4().hex[:10];require(re.fullmatch('rust_probe_rest_reads_[a-f0-9]{10}',database)is not None,'OwnedDatabase')
 env={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','CELERY_','DJANGO_'))};source=Path(seal['native_source_root']).resolve();service={'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/tmp','entrypoint':['bash','/native-tools/start.sh'],'environment':storage_environment(),'volumes':[str(ROOT/'controller')+':/native-tools:ro',str(ROOT/'inputs/pg-init')+':/pg-bootstrap:ro',str(ROOT/'inputs')+':/native-inputs:ro',str(ROOT)+':/native-root:ro',str(source)+':/native-source:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1536m','pids_limit':256,'networks':['default']};save(out/'compose.json',{'services':{'baseline':service},'networks':{'default':{'internal':True}}});compose=[args.docker,'compose','-p',project,'-f',str(out/'compose.json')];jobs=[];serial=0;collected_output=0;claimed=False;container=None;before=None;started=False;cleanup_deadline=None;cleanup_started=None;stop_started=False;stop_done=threading.Event();status=1;pid_records=[];began=time.monotonic();report={'passed':False,'project':project,'database':database,'Native_only':True,'Source_application_runs':0,'Main_runs':0,'resource_sampler':False,'case':args.case,'actual_existing_four_writer_permits':True,'physical_outcomes_unmeasured_before_run':True,'seal_sha256':sha(ROOT/'seal.json'),'processes':[]}
 def stop():
  nonlocal stop_started
  if not claimed:return
  if stop_started:
   require(stop_done.wait(35),'ExistingOwnedStopSettlementDeadline');require(report.get('owned_stop_receipt',{}).get('acknowledged')is True,'PriorOwnedStopWasNotAcknowledged');return
  stop_started=True;receipt={};report['owned_stop_receipt']=receipt
  try:
   remaining=15 if cleanup_deadline is None else min(15,max(.01,cleanup_deadline-time.monotonic()))
   bounded_stop(compose+['stop','--timeout','10'],env,out,receipt,limit=65536,timeout=remaining,save=save)
  finally:stop_done.set()
 def begin_cleanup():
  nonlocal cleanup_started,cleanup_deadline
  if cleanup_started is None:
   cleanup_started=time.monotonic();cleanup_deadline=cleanup_started+90;report['cleanup_started_utc']=now();report['cleanup_budget_seconds']=90
 def command(argv,timeout=120,kind=None):
  nonlocal serial,collected_output
  output_limit=command_budget(serial,collected_output,cleanup_deadline is not None)
  timeout=min(timeout,max(.01,(cleanup_deadline-time.monotonic())if cleanup_deadline is not None else 1200-(time.monotonic()-began)));serial+=1;tag=f'command-{serial:03d}';job=Collected(argv,env,output_limit,stop,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);identity=start(job.process.pid);when=now()
  try:job.wait(timeout)
  finally:
   collected_output+=job.used;report['aggregate_command_collected_output_bytes']=collected_output
   row={'argv':argv,'kind':kind,'pid':job.process.pid,'starttime':identity,'actual_exit':job.process.poll(),'collection_invalid':job.invalid,'owned_stop_requested':job.stopped,'physically_settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'began_utc':when,'completed_utc':now()};save(out/(tag+'.json'),row);report['processes'].append(row)
  require(type(identity)is str and identity.isdigit()and int(identity)>0,'ActualCommandPIDStartObservedBeforeAcceptance')
  return(out/(tag+'.stdout')).read_text()
 def inventories():return {kind:command([args.docker]+argv+['--filter','label=com.docker.compose.project='+project],15)for kind,argv in{'containers':['container','ls','--all','--no-trunc','--format','{{.ID}}'],'networks':['network','ls','--format','{{.ID}}'],'volumes':['volume','ls','--format','{{.Name}}']}.items()}
 def audit(stage,label,db):
  command(compose+['exec','-T','-e','BACKUP_PHYSICAL_AUDIT=owned-native-only','-e','BACKUP_PHYSICAL_AUDIT_STAGE='+stage,'-e','BACKUP_PHYSICAL_AUDIT_LABEL='+label,'-e','BACKUP_PHYSICAL_AUDIT_DATABASE='+db,'baseline','/dispatcharrpy/bin/python','/native-tools/audit.py'],45);value=json.loads((out/(label+'.json')).read_text());require(value['passed']is True and value['Django_or_Source_application_loaded']is False,'NoSourceReadOnlyAudit');return value['value']
 try:
  initial=inventories();save(out/'initial-project-inventories.json',initial);require(not any(v.strip()for v in initial.values()),'InitiallyAbsentProject');claimed=True;command(compose+['up','-d','--no-build','--pull','never','baseline'])
  ids=command(compose+['ps','--all','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedContainer');container=ids[0];inspection=json.loads(command([args.docker,'inspect',container]));save(out/'container-inspect.json',inspection);require(inspection[0]['Config']['Image']==IMAGE and inspection[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedStorageImage')
  deadline=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'StorageBootstrap180');time.sleep(.1)
  before=audit('default','default-before','dispatcharr')
  absence=command(compose+['exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+database+"'"]);require(absence.strip()=='0','FreshTargetDB');command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database]);command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d',database,'-v','ON_ERROR_STOP=1','-1','-f','/native-source/rust/crates/store/schema/original-1241b3-schema.sql','-f','/native-source/rust/crates/store/schema/original-1241b3-defaults.sql','-f','/native-source/rust/crates/store/schema/original-1241b3-fresh-timestamps.sql']);command(compose+['exec','-T','baseline','redis-cli','--raw','SET','rust-probe-rest-reads-owner',database]);command(compose+['exec','-T','-e','POSTGRES_DB='+database,'baseline','/dispatcharrpy/bin/python','/native-tools/seed.py']);owned_before=audit('owned','owned-before',database);audit('cache','cache-before',database);loader=command(compose+['exec','-T','baseline','ldd','/native-inputs/native-store-tests'],30);require('not found'not in loader,'ActualELFLoaderClosure');started=True
  report['Native_process_command_next']=serial+1;save(out/'execution-progress.json',report)
  extra=['-e','RUST_BACKTRACE=1','-e','POSTGRES_DB='+database,'-e','DATABASE_TEST_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'-e','DISPATCHARR_TIME_ZONE=UTC','-e','TZ=UTC']
  extra+=['-e','REST_READS_AUTHORITY=owned-native-rest-read-only','-e','REST_READS_CASE='+str(args.case),'-e','REST_READS_ROOT=/native-root','-e','REST_READS_OUTPUT=/evidence','-e','REST_READS_CACHE_URL=redis://:playback-fixture-only@127.0.0.1:6379/1']
  command(compose+['exec','-T']+extra+['baseline','/native-inputs/native-store-tests',TESTS[args.case],'--exact','--ignored','--nocapture','--test-threads=1'],100,'actual-rest-read-case-'+str(args.case)+'-once')
  begin_cleanup()
  actual=json.loads((out/'rest-reads-private.json').read_text());require(actual.get('_synthetic_only')is not True,'ActualCaptureCannotBeSynthetic');require(actual['database']==database and actual['case']==args.case,'ExactActualCaseAndDatabase');qualified=validate(actual,args.case,out);save(out/'effect-validation.json',qualified);report['passed']=qualified['passed'];report['actual_request_count']=actual.get('actual_request_count',0);status=0 if qualified['passed']else 1
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)[:4096],'message_full_bytes':len(str(error).encode()),'message_full_sha256':hashlib.sha256(str(error).encode()).hexdigest()};status=1;report['passed']=False
 finally:
  begin_cleanup()
  capture=out/'rest-reads-private.json'
  if capture.exists():
   try:
    actual=json.loads(capture.read_text());require(actual.get('_synthetic_only')is not True,'ActualCaptureCannotBeSynthetic');report['actual_request_count']=actual.get('actual_request_count',0);require(actual['database']==database and actual['case']==args.case,'ExactActualCaseAndDatabase');qualified=validate(actual,args.case,out);save(out/'effect-validation.json',qualified)
   except BaseException as error:report['effect_validation_error']=type(error).__name__+':'+str(error)[:4096];report['passed']=False;status=1
  if started:
   try:
    owned_after=audit('owned','owned-after',database);report['owned_full62tables59seq_exact']=owned_before==owned_after;require(owned_before==owned_after and(out/'owned-before.json').read_bytes()==(out/'owned-after.json').read_bytes(),'OwnedFull62_59Unchanged');audit('cache','cache-after',database);require(json.loads((out/'cache-before.json').read_text())['value']==json.loads((out/'cache-after.json').read_text())['value'],'AllFourRedisRawUnchanged');deadline=time.monotonic()+10;observations=[]
    while True:
     raw=command(compose+['exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,backend_start,state,wait_event_type,wait_event FROM pg_stat_activity WHERE datname='"+database+"') t"],10,'exact-owned-backend-after-ELF-exit');values=json.loads(raw);observations.append({'utc':now(),'actual_backends':values});save(out/'backend-after-ELF-exit.json',observations)
     if values==[]:report['owned_backends_absent_after_process_exit']=True;break
     require(time.monotonic()<deadline,'OwnedBackendPhysicalAbsence10');time.sleep(.1)
   except BaseException as error:report['backend_or_cache_cleanup_error']=str(error)[:4096];report['passed']=False;status=1
  if before is not None:
   try:after=audit('default','default-after','dispatcharr');report['default11_full62tables59seq_exact']=before==after;require(before==after and(out/'default-before.json').read_bytes()==(out/'default-after.json').read_bytes(),'Default11AndFull62_59Exact')
   except BaseException as error:report['default_after_error']=str(error)[:4096];report['passed']=False;status=1
  if claimed:
   if container is None:
    try:
     recovery=command(compose+['ps','--all','-q','baseline'],15).splitlines();save(out/'partial-up-exact-CID-recovery.json',{'ids':recovery});require(len(recovery)<=1,'OneOwnedPartialService')
     if recovery:container=recovery[0]
    except BaseException as error:report['CID_recovery_error']=str(error)[:4096];report['passed']=False;status=1
   try:
    if container:
     listing=command([args.docker,'top',container,'-eo','pid,comm,args'],15);save(out/'precleanup-processes.json',{'raw':listing})
     for line in listing.splitlines()[1:]:
      if line.strip():
       pid=int(line.split()[0]);identity=start(pid);require(identity is not None,'ActualDockerTopHostPIDStartRequired');pid_records.append({'pid':pid,'starttime':identity,'PID_namespace':os.readlink('/proc/self/ns/pid'),'scope':'Docker top host PID with actual current /proc start observed'})
     save(out/'precleanup-PIDs.json',pid_records);require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|native-store-tests)\b',listing),'NoAppOrNativeProcessLeft')
   except BaseException as error:report['precleanup_error']=str(error)[:4096];report['passed']=False;status=1
   try:command(compose+['down','--volumes','--remove-orphans','--timeout','10'],60)
   except BaseException as error:report['down_error']=str(error)[:4096];report['passed']=False;status=1
   try:
    remaining=inventories();save(out/'cleanup-project-inventories.json',remaining);require(not any(v.strip()for v in remaining.values()),'AllOwnedProjectResourcesAbsent')
    if container:raw=command([args.docker,'container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],15);save(out/'exact-CID-absence.json',{'CID':container,'raw':raw});require(not raw.strip(),'ExactCIDAbsent')
    absence=[dict(r,after_starttime=start(r['pid']),original_process_absent=start(r['pid'])is None or start(r['pid'])!=r['starttime'])for r in pid_records];save(out/'cleanup-PID-absence.json',absence);require(all(r['original_process_absent']for r in absence),'OwnedPIDsAbsent')
   except BaseException as error:report['cleanup_error']=str(error)[:4096];report['passed']=False;status=1
  report['all_command_processes_settled']=len(report['processes'])==serial and all(type(row['starttime'])is str and row['starttime'].isdigit()and int(row['starttime'])>0 for row in report['processes'])and all(j.physically_settled and j.process.poll()is not None and not any(t.is_alive()for t in j.threads)for j in jobs)
  if not report['all_command_processes_settled']:report['passed']=False;status=1
  window=cleanup_window(cleanup_started,time.monotonic());report['cleanup_elapsed_seconds']=window['elapsed_seconds'];report['cleanup_within_declared_90_seconds']=window['within_declared_90_seconds']
  if not report['cleanup_within_declared_90_seconds']:report['passed']=False;status=1
  stop_bytes=sum((out/name).stat().st_size for name in('owned-stop.stdout','owned-stop.stderr')if(out/name).exists());report['aggregate_command_and_stop_output_bytes']=collected_output+stop_bytes;report['fixed_total_output64MiB']=collected_output+stop_bytes<=64*1048576;report['actual_command_count']=serial;report['normal_command_limit']=224;report['total_command_limit']=256;report['sole_neutral_stop_limit_bytes']=65536
  if not report['fixed_total_output64MiB']:report['passed']=False;status=1
  report['actual_controller_exit']=status;report['settled_utc']=now();save(out/'execution-report.json',report)
 return status

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute-reviewed-seal');p.add_argument('--case',type=int,choices=(1,2));p.add_argument('--output',type=Path);p.add_argument('--docker',default='docker');args=p.parse_args();seal=check()
 if args.execute_reviewed_seal is None:print(json.dumps({'passed':True,'seal_sha256':sha(ROOT/'seal.json'),'Native_Source_Docker_runs':0}));return 0
 require(args.case in TESTS,'ExactlyDeclaredRestReadCase1Or2');return execute(args,seal)
if __name__=='__main__':raise SystemExit(main())
