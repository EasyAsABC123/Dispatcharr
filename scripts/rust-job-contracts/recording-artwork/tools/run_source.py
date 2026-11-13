#!/usr/bin/env python3
"""Bounded Original Refresh/native parented Series/Original artwork interoperability host executor; explicit reviewed inputs and fresh output only.
Original Dispatcharr/dependency authors retain application/algorithm/license credit.
"""
import argparse,datetime,hashlib,importlib.util,json,os,re,shutil,subprocess,sys,time,uuid,signal
from pathlib import Path
from bounded_process import Collected
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REQUIRED={'run_source.py','bounded_process.py','source_configured.py','observe.py','verify_roles.py','ledger_io.py','support-inputs.json','contract.py','roles.py','owned_worker.py','seed_spec.py','pair_contract.py','row_compare.py','reference.json','primitive_cache.py'}
SAFE=('PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def proc(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
def observed_pid_record(cid,pid,starttime):
 require(type(cid)is str and re.fullmatch('[a-f0-9]{64}',cid)is not None,'ObservedExactCID')
 require(type(pid)is int and pid>0 and type(starttime)is str and re.fullmatch('[0-9]+',starttime)is not None and int(starttime)>0,'ObservedOriginalHostPIDStartRequired')
 return {'CID':cid,'pid':pid,'starttime':starttime}
def exact_absent_cid(code,stdout,stderr,cid):
 if type(code)is not int or code!=1 or not re.fullmatch('[a-f0-9]{64}',cid):return False
 try:text=stderr.decode('utf-8',errors='strict').strip()
 except UnicodeDecodeError:return False
 return stdout.strip()in(b'',b'[]')and text.casefold()==('error: no such object: '+cid).casefold()
def cleanup_budget_status(started,finished,seconds=90):
 elapsed=finished-started
 return {'budget_seconds':seconds,'elapsed_seconds':elapsed,'within_budget':type(seconds)is int and seconds==90 and 0<=elapsed<=seconds,'scope':'Acceptance deadline only; physical stop/join may finish later but can never qualify then'}
def settle_interrupted_collection(job,reason):
 """Attempt actual host CLI wait/pipes even if exact service stop fails.
 Original invalid/settlement flags stay unchanged. Host CLI exit never proves
 the container child stopped; outer finally must still down/audit the project.
 """
 receipt={'reason':reason,'abort_error':None,'actual_cli_wait_completed':False,'actual_cli_exit':None,'host_cli_forced_kill':False}
 try:job.abort(reason)
 except BaseException as error:receipt['abort_error']=type(error).__name__+':'+str(error)[:512]
 try:
  if job.process.poll()is None:job.process.terminate()
  try:code=job.process.wait(timeout=10)
  except subprocess.TimeoutExpired:
   receipt['host_cli_forced_kill']=True;job.process.kill();code=job.process.wait(timeout=5)
  receipt.update(actual_cli_wait_completed=True,actual_cli_exit=code)
 except BaseException as error:receipt['actual_cli_wait_error']=type(error).__name__
 deadline=time.monotonic()+10
 for thread in job.threads+([job.writer]if job.writer else[]):
  try:thread.join(max(0,deadline-time.monotonic()))
  except BaseException as error:receipt.setdefault('pipe_join_errors',[]).append(type(error).__name__)
 pipes_settled=not any(t.is_alive()for t in job.threads)and not(job.writer and job.writer.is_alive());receipt['actual_cli_capture_files_closed']=False
 if pipes_settled:
  try:job._close_files();receipt['actual_cli_capture_files_closed']=True
  except BaseException as error:receipt['actual_cli_capture_close_error']=type(error).__name__
 receipt.update(actual_cli_pipes_settled=pipes_settled,original_collector_invalid=job.invalid,original_collector_physically_settled=job.physically_settled,exact_service_stop_acknowledged=job.stopped and receipt['abort_error']is None,container_child_absence_proven_by_cli_collection=False)
 return receipt
def environment():
 denied=('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','BASELINE_','BENCHMARK_','FIXTURE_')
 return{k:v for k,v in os.environ.items()if not k.startswith(denied)and k!='ENABLE_VOD_FIXTURE'}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--execution-inputs',type=Path,required=True);a=p.parse_args();tools=Path(__file__).resolve().parent;inputs=json.loads(a.execution_inputs.read_text());out=a.output.resolve();support=a.support_root.resolve();env=environment()
 require(inputs.get('authorized')is True,'ExplicitRootParentedPairAuthority');require(inputs.get('pinned_source_proof_sha256')==sha(tools.parent/'pinned-source-proof.json'),'ExactPinnedSourceProof');require(inputs.get('case_plan_sha256')==sha(tools.parent/'case-plan.json'),'ExactCasePlan');require(set(inputs.get('tools_sha256',{}))==REQUIRED,'ExactFifteenTools')
 for name,h in inputs['tools_sha256'].items():require(sha(tools/name)==h,'ExactTool:'+name)
 peer=inputs['Native_elf'];require(Path(peer['path']).is_file()and sha(peer['path'])==peer['sha256']and Path(peer['path']).open('rb').read(4)==b'\x7fELF','ExactOutsideNativeELF')
 build=inputs['Native_build_record'];require(sha(build['path'])==build['sha256'],'ExactBuildRecord');record=json.loads(Path(build['path']).read_text());require(record['all_waits_zero']is True and all(c['settled']is True and c['exit']==0 for c in record['commands'])and record['source_map_sha256']==inputs['Native_source_map']['sha256']and record['outside_ELFs'][peer['path']]['sha256']==peer['sha256'],'ActualSuccessfulSameSourceELFBuild')
 mapping=inputs['Native_source_map'];require(sha(mapping['path'])==mapping['sha256'],'FullNativeSourceMap');require(all(sha(Path(inputs['Native_source_root'])/name)==value for name,value in json.loads(Path(mapping['path']).read_text()).items()),'EveryNativeSourceAndCoupledByte')
 require(inputs['Source_reference_sha256']==json.loads((tools/'reference.json').read_text())['raw_Source_sha256'],'ActualSourceReference')
 require(inputs.get('Original_archive_sha256')==ARCHIVE and re.fullmatch('sha256:[a-f0-9]{64}',inputs.get('Original_image_id','')),'PinnedOriginalIdentity');require(inputs.get('output')==str(out)and not out.exists(),'FixedFreshOutput')
 docker=shutil.which('docker');require(docker and sha(docker)==inputs.get('docker_executable_sha256')and env.get('DOCKER_HOST')==inputs.get('Docker_host'),'BoundDocker')
 require(inputs.get('tooling_environment')=={k:env.get(k)for k in SAFE},'ExactSixSafeExports')
 for key in ['tooling_environment_script','compose_executable']:
  row=inputs[key];require(Path(row['path']).is_file()and sha(row['path'])==row['sha256'],'BoundTool:'+key)
 for name,h in json.loads((tools/'support-inputs.json').read_text()).items():require(sha(support/name)==h,'ExactSupport:'+name)
 # Read-only preflight precedes fixture output/claim. Its separate receipt is
 # retained even if the daemon is unavailable; it cannot authorize a workload.
 signal_path=out.parent/(out.name+'-host-signal.json');require(not signal_path.exists(),'FreshHostSignalReceipt')
 signal_records=[];handling_signal=False
 def host_signal(signum,frame):
  nonlocal handling_signal
  signal_records.append({'signal':signum,'pid':os.getpid(),'starttime':proc(os.getpid()),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
  save(signal_path,{'actual_signals':signal_records,'scope':'Observed host signal only, not process settlement'})
  if handling_signal:return
  handling_signal=True
  raise KeyboardInterrupt('OwnedHostSignal'+str(signum))
 for signum in(signal.SIGINT,signal.SIGTERM):signal.signal(signum,host_signal)
 preflight=[];preflight_path=out.parent/(out.name+'-host-preflight.json');require(not preflight_path.exists(),'FreshPreflightReceipt')
 for argv in [[docker,'version','--format','{{json .Server.Version}}'],[docker,'compose','version','--short'],[docker,'info','--format','{{json .ServerVersion}}']]:
  job=Collected(argv,env,65536,lambda:None);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
  try:job.wait(15)
  except BaseException:
   if job.physically_settled and job.invalid is None:raise
   interrupted=settle_interrupted_collection(job,'PreflightInterrupted');save(out.parent/(out.name+'-preflight-interrupted.json'),interrupted)
   raise
  finally:
   preflight.append({'argv':argv,'began_utc':began,'exit':job.poll(),'stdout_hex':bytes(job.output['stdout']).hex(),'stderr_hex':bytes(job.output['stderr']).hex(),'settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads)});save(preflight_path,preflight)
 out.mkdir(parents=True,exist_ok=False,mode=0o700);frozen=out/'support';shutil.copytree(support,frozen,ignore=shutil.ignore_patterns('__pycache__'));mounted=out/'job-tools';mounted.mkdir()
 for name in REQUIRED:shutil.copy2(tools/name,mounted/name)
 for src,dst in [(a.execution_inputs,'execution-inputs.json'),(tools.parent/'case-plan.json','case-plan.json'),(tools.parent/'pinned-source-proof.json','pinned-source-proof.json')]:shutil.copy2(src,out/dst)
 shutil.copy2(peer['path'],out/'native-parented-peer');(out/'native-parented-peer').chmod(0o755);require(sha(out/'native-parented-peer')==peer['sha256'],'MountedExactNativeELF')
 save(out/'controller-context.json',{'lifecycle_began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'scope':'Actual host UTC before fixture/bootstrap; not a SQL clock injection'})
 sys.path.insert(0,str(frozen/'scripts/rust-integration'));from fixture_ports import write_port_overlay
 project='dispatcharr-configured-source-'+uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+uuid.uuid4().hex[:10];source=out/'source';source.mkdir();env.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
 overlay=out/'source.yml';save(overlay,{'services':{'baseline':{'volumes':[str(mounted)+':/job-tools:ro',str(out/'native-parented-peer')+':/native-parented-peer:ro',str(frozen/'scripts/rust-integration')+':/backup-tools:ro'],'cpus':1,'mem_limit':'1g','pids_limit':256},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time;time.sleep(3600)'],'cpus':.1,'mem_limit':'128m','pids_limit':32}},'networks':{'default':{'internal':True}}});ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=[docker,'compose','-p',project,'-f',str(frozen/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
 report={'passed':False,'Source_verified':False,'Native_verified':False,'performance_verified':False,'project':project,'database':database,'scope':'Actual Original Refresh/Series producer -> Native local-VODLogo artwork and Original AsyncResult; owned routes only, default descendants remain intact; no provider/Main/fullgraph/performance claim','execution_inputs_sha256':sha(a.execution_inputs),'Original_revision':BASE,'Original_archive_sha256':ARCHIVE,'processes':[]};serial=0;before=None;cid=None;claimed=False;status=1;cleanup_deadline=None;cleanup_started=None;owned_pids=[];owned_containers=[]
 def flush():save(out/'integration.json',report)
 def begin_cleanup():
  nonlocal cleanup_started,cleanup_deadline
  if cleanup_started is None:cleanup_started=time.monotonic();cleanup_deadline=cleanup_started+90
 def stop_owned():
  if not claimed:return
  begin_cleanup()
  # Independent bounded collector avoids recursively invoking this callback.
  stem=out/('owned-stop-'+uuid.uuid4().hex);job=Collected(compose+['stop','--timeout','10'],env,65536,lambda:None,stdout_path=str(stem)+'.stdout',stderr_path=str(stem)+'.stderr')
  try:job.wait(30)
  finally:save(str(stem)+'.json',{'exit':job.poll(),'settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'invalid':job.invalid})
 def command(argv,body=None,timeout=180,allow_nonzero=False):
  nonlocal serial
  if cleanup_deadline is not None:timeout=min(timeout,max(.01,cleanup_deadline-time.monotonic()))
  serial+=1;tag='command-'+str(serial).zfill(3);record={'argv':argv,'cwd':os.getcwd(),'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':None,'starttime':None,'spawn_state':'constructor_not_called'};save(out/(tag+'.started.json'),record)
  try:job=Collected(argv,env,1048576,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'),body=body.encode()if isinstance(body,str)else body)
  except BaseException as error:
   record.update(spawn_state='constructor_did_not_return',constructor_error=type(error).__name__,exit=None,physically_settled=False,pipes_settled=False,collection_invalid='UnknownConstructorSettlement');save(out/(tag+'.json'),record);report['processes'].append(record);flush();raise
  record.update(pid=job.process.pid,starttime=proc(job.process.pid),spawn_state='actual_child_started');save(out/(tag+'.started.json'),record)
  try:job.wait(timeout)
  except RuntimeError:
   if not job.physically_settled:
    interrupted=settle_interrupted_collection(job,'CollectionFailureTail');save(out/(tag+'.failed-collection-tail.json'),interrupted)
   if not(allow_nonzero and job.physically_settled and job.invalid is None):raise
  except BaseException:
   # A host signal cannot orphan docker-exec's actual container child.
   # Stop the exact service first, then retain host wait/pipes even on failure.
   interrupted=settle_interrupted_collection(job,'HostInterrupted');save(out/(tag+'.interrupted-collection.json'),interrupted)
   raise
  finally:
   record.update(exit=job.poll(),completed_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),collection_invalid=job.invalid,physically_settled=job.physically_settled,pipes_settled=not any(t.is_alive()for t in job.threads),owned_stop_requested=job.stopped);save(out/(tag+'.json'),record);report['processes'].append(record);flush()
  return(out/(tag+'.stdout')).read_bytes(),(out/(tag+'.stderr')).read_bytes(),job.poll()
 def text(argv,body=None,timeout=180):return command(argv,body,timeout)[0].decode()
 def manifest(db,action):
  raw=text(compose+['exec','-T','-e','POSTGRES_DB='+db,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':action}));return json.loads(raw.splitlines()[-1])
 def inventory():
  data={}
  for kind in ['container','network','volume']:
   argv=[docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'if kind!='volume'else'{{.Name}}'];raw,err,code=command(argv,timeout=15);data[kind]={'argv':argv,'exit':code,'stdout_hex':raw.hex(),'stderr_hex':err.hex(),'empty':raw.strip()==b''}
  return data
 try:
  archive=out/'original-source.tar';text(['git','-C',str(a.repo.resolve()),'archive','--format=tar','--output',str(archive),BASE]);require(sha(archive)==ARCHIVE,'ExactOriginalArchive');text(['tar','-xf',str(archive),'--no-same-owner','-C',str(source)]);(source/'media').mkdir(exist_ok=True);pinned=json.loads((tools.parent/'pinned-source-proof.json').read_text());require(all(sha(source/name)==row['sha256']for name,row in pinned['modules'].items()),'PinnedSourceModulesBeforeApp')
  require(all(x['empty']for x in inventory().values()),'OwnedProjectInitiallyAbsent');claimed=True;text(compose+['up','-d','--no-build','--pull','never','baseline']);cid=text(compose+['ps','-q','baseline']).strip();require(re.fullmatch('[a-f0-9]{64}',cid),'ExactCID');identity=json.loads(text([docker,'inspect',cid]))[0];save(out/'actual-container-inspect.json',identity);require(identity['Image']==inputs['Original_image_id']and identity['Config']['Image']==IMAGE,'ActualPinnedOriginalImage');report['Original_container']={'id':cid,'image_id':identity['Image'],'image_reference':IMAGE};owned_containers=text(compose+['ps','-q']).splitlines();require(cid in owned_containers and all(re.fullmatch('[a-f0-9]{64}',c)for c in owned_containers),'ActualOwnedContainerIDs');report['owned_containers']=owned_containers
  end=time.monotonic()+180
  while True:
   raw,err,code=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=min(5,max(.01,end-time.monotonic())),allow_nonzero=True)
   if code==0:
    require(time.monotonic()<=end,'FixedReadiness180s');break
   require(time.monotonic()<end,'FixedReadiness180s');time.sleep(.2)
  before=manifest('dispatcharr','baseline_manifest');save(out/'default-before-private.json',before);text(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database]);report['seed']=manifest(database,'seed')
  stopped=json.loads(text(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']));save(out/'web-stop.json',stopped);require(stopped.get('ok')is True and stopped.get('running')is False and type(stopped.get('pid'))is int and type(stopped.get('exit_code'))is int,'ActualOwnedWebStopped');report['web_stop']=stopped
  text(compose+['exec','-T','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/job-tools/verify_roles.py'],timeout=30)
  argv=compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@localhost:5432/'+database,'-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/job-tools/source_configured.py'];report['controller_argv']=argv;flush()
  try:text(argv,timeout=600)
  finally:
   begin_cleanup();last=report['processes'][-1]if report['processes']else{};matched=last.get('argv')==argv;report['actual_controller_exit']=last.get('exit')if matched else None;report['actual_controller_settled']=last.get('physically_settled')if matched else False;flush()
  require(all(sha(source/name)==row['sha256']for name,row in pinned['modules'].items()),'PinnedSourceModulesAfterApp');actual=json.loads((out/'source-parented-recordings-private.json').read_text());report['Native_raw_report_sha256']=sha(out/'source-parented-recordings-private.json');sys.path.insert(0,str(tools));from pair_contract import validate;report['native_acceptance']=validate(0,actual,json.loads((tools/'reference.json').read_text()));report.update(passed=True,Source_verified=False,Source_reference_verified=True,Native_verified=True,cases=5,checks=len(actual['checks']));status=0
 except BaseException as error:report.update(passed=False,error_type=type(error).__name__,error=str(error)[:4096]);status=1
 finally:
  begin_cleanup()
  if before is not None:
   try:after=manifest('dispatcharr','baseline_manifest');save(out/'default-after-private.json',after);require(before==after and (out/'default-before-private.json').read_bytes()==(out/'default-after-private.json').read_bytes(),'ActualDefault62Tables59SequencesExact');report['default_database_unchanged']=True;report['default_audit_scope']={key:len(value)for key,value in before['manifest'].items()};require(report['default_audit_scope']=={'columns':501,'constraints':198,'indexes':255,'sequences':59,'tables':62},'PinnedActualDefaultSchemaScope')
   except BaseException as error:report.update(passed=False,default_audit_unknown=type(error).__name__);status=1
  try:
   for owned_cid in owned_containers or ([cid]if cid else[]):
    rows=text([docker,'top',owned_cid,'-eo','pid,comm,args'],timeout=15);save(out/('precleanup-processes-'+owned_cid+'.json'),{'CID':owned_cid,'raw':rows})
    for line in rows.splitlines()[1:]:
     fields=line.split(None,2)
     if fields and fields[0].isdigit():pid=int(fields[0]);owned_pids.append(observed_pid_record(owned_cid,pid,proc(pid)))
   save(out/'precleanup-PIDs.json',owned_pids)
  except BaseException as error:report.update(passed=False,PID_capture_unknown=type(error).__name__);status=1
  try:
   if claimed:text(compose+['down','--volumes','--remove-orphans'],timeout=45)
  except BaseException as error:report.update(passed=False,cleanup_error=type(error).__name__);status=1
  try:
   inv=inventory();save(out/'independent-cleanup.json',inv);require(all(v['empty']for v in inv.values()),'AllProjectResourcesAbsent')
   for owned_cid in owned_containers or ([cid]if cid else[]):
    raw,err,code=command([docker,'inspect',owned_cid],timeout=15,allow_nonzero=True);absent=exact_absent_cid(code,raw,err,owned_cid);save(out/('exact-CID-absence-'+owned_cid+'.json'),{'id':owned_cid,'exit':code,'stdout_hex':raw.hex(),'stderr_hex':err.hex(),'absent':absent});require(absent,'ExactFormerCIDAbsent')
   observations=[{**r,'after_starttime':proc(r['pid']),'original_process_absent':proc(r['pid'])is None or proc(r['pid'])!=r['starttime']}for r in owned_pids];save(out/'cleanup-PID-absence.json',observations);require(all(r['original_process_absent']for r in observations),'ExactOwnedPIDsAbsent');require(all(r['physically_settled']and r['pipes_settled']and r['collection_invalid']is None for r in report['processes']),'AllActualCommandsSettled')
  except BaseException as error:report.update(passed=False,cleanup_unknown=type(error).__name__);status=1
  report['cleanup_budget']=cleanup_budget_status(cleanup_started,time.monotonic())
  if not report['cleanup_budget']['within_budget']:report.update(passed=False,cleanup_budget_exceeded=True);status=1
  report['actual_host_exit']=status;flush()
 return status
if __name__=='__main__':raise SystemExit(main())
