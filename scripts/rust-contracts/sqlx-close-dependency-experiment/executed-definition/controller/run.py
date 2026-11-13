#!/usr/bin/env python3
"""Read-only default; one explicitly selected reviewed dependency3 variant."""
import argparse,datetime,hashlib,json,os,re,subprocess,time,uuid
from pathlib import Path
from bounded_process import Collected
from validate_dependency import validate_directory
ROOT=Path(__file__).resolve().parents[1]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
TEST='first_close_three_cases'
def require(ok,label):
 if not ok:raise ValueError(label)
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def start(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def stages0(rows):return all(r['state']=='SETTLED'and r['actual_wait']==r['actual_poll']==0 for r in rows)
def check():
 seal=json.loads((ROOT/'seal.json').read_text());require(seal['state']=='COMPILED_FIXED_DEPENDENCY3_REQUIRES_REVIEW','ReviewedCompiledState')
 for relative,digest in seal['local_inputs'].items():
  p=ROOT/relative;require(not p.is_symlink()and p.is_file()and sha(p)==digest,'ExactInput:'+relative)
 require(set(seal['variants'])=={'baseline','candidate'},'FixedTwoVariants')
 for variant,binding in seal['variants'].items():
  source=Path(binding['source_root']).resolve();expected=json.loads((ROOT/('inputs/'+variant+'-full898-map.json')).read_text());actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()};require(actual==expected and len(actual)==898,'Exact898Source:'+variant);require(all(p.stat().st_mode&0o222==0 for p in source.rglob('*')if p.is_file()),'ReadonlySource:'+variant)
  peer=json.loads((ROOT/('build/'+variant+'/outside-peer.json')).read_text());elf=ROOT/('inputs/'+variant+'-sqlx-close-tests');require(sha(elf)==binding['ELF_sha256']==peer['ELF_sha256']and elf.stat().st_size==peer['ELF_bytes']and peer['compiled_source_root']==str(source)and peer['full898_map_sha256']==sha(ROOT/('inputs/'+variant+'-full898-map.json')),'ExactCompletedELFBinding:'+variant)
  if variant=='baseline':
   build=json.loads((ROOT/'build/baseline/build.json').read_text());require(build['passed']is True and build['state']=='SETTLED'and len(build['commands'])==5 and stages0(build['commands']),'BaselineFiveActualStages0');require(json.loads((ROOT/'build/baseline/source-before.json').read_text())==expected==json.loads((ROOT/'build/baseline/source-after.json').read_text()),'BaselinePrePost898');completed=build['completed_utc']
  else:
   old=json.loads((ROOT/'build/candidate-original/build.json').read_text());recovery=json.loads((ROOT/'build/candidate/recovery.json').read_text());qualification=json.loads((ROOT/'build/candidate/qualification.json').read_text());require(old['passed']is False and old['actual_wrapper_exit']==1 and len(old['commands'])==4 and stages0(old['commands'][:3])and old['commands'][3]['actual_wait']==old['commands'][3]['actual_poll']==101,'OriginalENOSPCNegativePreserved');require('No space left on device'in(ROOT/'build/candidate-original/03-existing-pure-notifications.stderr').read_text(),'LiteralENOSPCDiagnostic');require(recovery['passed']is True and recovery['state']=='SETTLED'and len(recovery['commands'])==2 and stages0(recovery['commands'])and recovery['target_reused_or_transplanted']is False and recovery['repeated_successful_fmt_non_test_strict_commands']==0,'OnlyTwoActualRecoveryStages0FreshHomeTarget');require([r['name']for r in recovery['commands']]==['existing-pure-notifications','dependency-test-ELF-no-run'],'ExactlyFailedAndUnstartedStages');require(qualification['original_attempt_passed']is False and qualification['original_ENOSPC_negative_retained']is True and [r['command']for r in qualification['required_successful_stages']]==old['commands'][:3]+recovery['commands'],'NoFakeFiveCommandOriginalPass');require(old['source_root']==recovery['source_root']==str(source),'SameActualSourceBothAttempts')
   for directory in('candidate-original','candidate'):
    require(json.loads((ROOT/('build/'+directory+'/source-before.json')).read_text())==expected==json.loads((ROOT/('build/'+directory+'/source-after.json')).read_text()),'CandidateBothAttemptsPrePost898')
   require(datetime.datetime.fromisoformat(old['completed_utc'])<=datetime.datetime.fromisoformat(recovery['started_utc']),'OriginalFailedAttemptSettledBeforeFreshRecovery');completed=recovery['completed_utc']
  require(datetime.datetime.fromisoformat(peer['copied_utc'])>=datetime.datetime.fromisoformat(completed),'ELFCopiedAfterAllRequiredStagesCompleted:'+variant)
 return seal

def execute(args,seal):
 require(args.execute_reviewed_seal==sha(ROOT/'seal.json'),'ExactReviewedSeal');require(args.variant in('baseline','candidate')and args.output is not None,'ExplicitVariantFreshOutput');out=args.output.resolve();out.mkdir(parents=True,exist_ok=False,mode=0o700);(out/'rust').mkdir();project='dispatcharr-sqlxclose-'+args.variant+'-'+uuid.uuid4().hex[:10];databases=['rust_probe_sqlx_close_'+uuid.uuid4().hex[:10]for _ in range(3)];require(len(set(databases))==3 and all(re.fullmatch('rust_probe_sqlx_close_[a-f0-9]{10}',n)for n in databases),'ThreeOwnedDatabases');database=databases[0];source=Path(seal['variants'][args.variant]['source_root']).resolve();env={k:v for k,v in os.environ.items()if not k.startswith(('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_'))}
 service={'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/tmp','entrypoint':['bash','/native-tools/start.sh'],'environment':{'DISPATCHARR_ENV':'aio','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PUID':'1000','PGID':'1000','PYTHONDONTWRITEBYTECODE':'1','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(ROOT/'controller')+':/native-tools:ro',str(ROOT/'inputs/pg-init')+':/pg-bootstrap:ro',str(ROOT/'inputs')+':/native-inputs:ro',str(ROOT)+':/native-root:ro',str(source)+':/native-source:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1g','pids_limit':256,'networks':['default']};save(out/'compose.json',{'services':{'baseline':service},'networks':{'default':{'internal':True}}});compose=[args.docker,'compose','-p',project,'-f',str(out/'compose.json')];serial=0;claimed=False;container=None;before=None;started=False;jobs=[];pid_records=[];cleanup_deadline=None;status=1;begin=time.monotonic();report={'passed':False,'variant':args.variant,'project':project,'databases':databases,'Source_Main_API_runs':0,'production_Main_shutdown_claim':False,'known_notification2_4_6_negatives_waived':False,'processes':[],'seal_sha256':sha(ROOT/'seal.json')}
 def stop():
  if claimed:
   argv=compose+['stop','--timeout','10'];p=subprocess.run(argv,env=env,stdin=subprocess.DEVNULL,capture_output=True,timeout=30);save(out/('owned-stop-'+uuid.uuid4().hex+'.json'),{'argv':argv,'exit':p.returncode,'stdout_hex':p.stdout.hex(),'stderr_hex':p.stderr.hex(),'utc':now()});require(p.returncode==0,'OwnedStopAcknowledged')
 def command(argv,timeout=120,kind=None):
  nonlocal serial
  timeout=min(timeout,max(.01,(cleanup_deadline-time.monotonic())if cleanup_deadline is not None else 1200-(time.monotonic()-begin)));serial+=1;tag=f'command-{serial:03d}';job=Collected(argv,env,8388608,stop,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);identity=start(job.process.pid);when=now()
  try:job.wait(timeout)
  finally:
   row={'argv':argv,'kind':kind,'pid':job.process.pid,'starttime':identity,'actual_exit':job.process.poll(),'collection_invalid':job.invalid,'owned_stop_requested':job.stopped,'physically_settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'began_utc':when,'completed_utc':now()};save(out/(tag+'.json'),row);report['processes'].append(row)
  return(out/(tag+'.stdout')).read_text()
 def inventories():return {kind:command([args.docker]+argv+['--filter','label=com.docker.compose.project='+project],15)for kind,argv in{'containers':['container','ls','--all','--no-trunc','--format','{{.ID}}'],'networks':['network','ls','--format','{{.ID}}'],'volumes':['volume','ls','--format','{{.Name}}']}.items()}
 def audit(stage,label,db):
  command(compose+['exec','-T','-e','NOTIFICATION_NATIVE_AUDIT=owned-native-only','-e','NOTIFICATION_AUDIT_STAGE='+stage,'-e','NOTIFICATION_AUDIT_LABEL='+label,'-e','NOTIFICATION_AUDIT_DATABASE='+db,'baseline','/dispatcharrpy/bin/python','/native-tools/audit.py'],45);value=json.loads((out/(label+'.json')).read_text());require(value['passed']is True and value['Django_or_Source_application_loaded']is False,'ReadOnlyNoSourceAudit');return value['value']
 try:
  initial=inventories();save(out/'initial-project-inventories.json',initial);require(not any(v.strip()for v in initial.values()),'InitiallyAbsentProject');claimed=True;command(compose+['up','-d','--no-build','--pull','never','baseline']);deadline=time.monotonic()+180
  while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'StorageBootstrap180');time.sleep(.1)
  ids=command(compose+['ps','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedStorageContainer');container=ids[0];inspection=json.loads(command([args.docker,'inspect',container]));save(out/'container-inspect.json',inspection);require(inspection[0]['Config']['Image']==IMAGE and inspection[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedStorageImage');before=audit('default','default-before','dispatcharr')
  for db in databases:
   absence=command(compose+['exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+db+"'"]);require(absence.strip()=='0','FreshTargetDB');command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db])
  command(compose+['exec','-T','baseline','redis-cli','--raw','SET','rust-probe-notification-native-owner',database]);audit('cache','cache-before',database);elf='/native-inputs/'+args.variant+'-sqlx-close-tests';loader=command(compose+['exec','-T','baseline','ldd',elf],30);require('not found'not in loader,'ActualELFLoaderClosure');started=True;report['ELF_command_next']=serial+1
  try:command(compose+['exec','-T','-e','RUST_BACKTRACE=1','-e','SQLX_CLOSE_DEPENDENCY_FIXTURE=owned-dependency-only','-e','SQLX_CLOSE_ROOT=/native-root','-e','SQLX_CLOSE_OUTPUT=/evidence/rust','-e','SQLX_CLOSE_VARIANT='+args.variant,'-e','SQLX_CLOSE_DATABASES='+json.dumps(databases,separators=(',',':')),'baseline',elf,TEST,'--exact','--ignored','--nocapture','--test-threads=1'],330,'actual-dependency3-'+args.variant)
  except BaseException:
   last=report['processes'][-1];require(last['actual_exit']==101 and last['collection_invalid']is None and last['owned_stop_requested']is False and last['physically_settled']is True and last['pipes_settled']is True,'OnlySettledContractNegativeMayBeObserved')
  qualified=validate_directory(out/'rust',args.variant);save(out/'effect-validation.json',qualified);report['passed']=qualified['passed'];report['actual_cases']=qualified['actual_cases'];status=0 if qualified['passed']else 1
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)[:4096],'message_full_bytes':len(str(error).encode()),'message_full_sha256':hashlib.sha256(str(error).encode()).hexdigest()};report['passed']=False;status=1
 finally:
  if(out/'rust/dependency-report.json').exists():
   try:actual=json.loads((out/'rust/dependency-report.json').read_text());report['captured_case_count']=len(actual.get('cases',[]));qualified=validate_directory(out/'rust',args.variant);save(out/'effect-validation.json',qualified)
   except BaseException as error:report['observation_validation_error']=str(error)[:4096];report['passed']=False;status=1
  if started:
   try:
    cache_after=audit('cache','cache-after',database);deadline=time.monotonic()+10;observations=[];quoted=','.join("'"+db+"'"for db in databases)
    while True:
     raw=command(compose+['exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,datname,backend_start,state,wait_event_type,wait_event FROM pg_stat_activity WHERE datname IN ("+quoted+")) t"],10,'actual-all-owned-target-backends-after-ELF-exit');values=json.loads(raw);observations.append({'utc':now(),'actual_backends':values});save(out/'backend-after-ELF-exit.json',observations)
     if values==[]:report['owned_backends_absent_after_process_exit']=True;break
     require(time.monotonic()<deadline,'AllTargetBackendAbsence10');time.sleep(.1)
   except BaseException as error:report['backend_or_cache_cleanup_error']=str(error)[:4096];report['passed']=False;status=1
  if before is not None:
   try:after=audit('default','default-after','dispatcharr');report['default11_exact']=before==after;require(before==after,'Default11Exact')
   except BaseException as error:report['default_after_error']=str(error)[:4096];report['passed']=False;status=1
  if claimed:
   cleanup_deadline=time.monotonic()+90
   try:
    if container:
     listing=command([args.docker,'top',container,'-eo','pid,comm,args'],15);save(out/'precleanup-processes.json',{'raw':listing})
     for line in listing.splitlines()[1:]:
      if line.strip():pid=int(line.split()[0]);pid_records.append({'pid':pid,'starttime':start(pid)})
     save(out/'precleanup-PIDs.json',pid_records);require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|sqlx-close-tests|owned_pg_proxy.py)\b',listing),'NoAppELFOrPeerProcessLeft')
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
  report['actual_controller_exit']=status;report['settled_utc']=now();save(out/'execution-report.json',report)
 return status

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--execute-reviewed-seal');p.add_argument('--variant',choices=['baseline','candidate']);p.add_argument('--output',type=Path);p.add_argument('--docker',default='docker');args=p.parse_args();seal=check()
 if args.execute_reviewed_seal is None:print(json.dumps({'passed':True,'seal_sha256':sha(ROOT/'seal.json'),'PG_Source_Main_API_runs':0}));return 0
 return execute(args,seal)
if __name__=='__main__':raise SystemExit(main())
