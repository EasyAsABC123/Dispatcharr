#!/usr/bin/env python3
"""Bounded Native-only backup callback physical host executor; explicit reviewed inputs and fresh output only.
Original Dispatcharr/dependency authors retain application/algorithm/license credit.
"""
import argparse,datetime,hashlib,importlib.util,json,os,re,shutil,subprocess,sys,time,uuid,signal
from pathlib import Path
from bounded_process import Collected
EXECUTION_BLOCKED=False
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REQUIRED={'run.py','bounded_process.py','owned_pg_proxy.py','owned_resp_proxy.py','support-inputs.json','test_pg_contract.py','test_resp_contract.py','test_controller.py'}
SAFE=('PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def proc(pid):
 try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
 except FileNotFoundError:return None
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
def command_budget(serial, used, cleaning):
 require(type(serial)is int and serial>=0 and type(used)is int and used>=0 and type(cleaning)is bool,'TypedCommandBudget')
 require(serial<(256 if cleaning else 224),'FixedCommandCountWithCleanupReserve')
 available=(64 if cleaning else 56)*1048576-used
 require(available>0,'FixedAggregateOutputWithCleanupReserve')
 return min(1048576,available)
def same(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return a.keys()==b.keys()and all(same(v,b[k])for k,v in a.items())
 if isinstance(a,list):return len(a)==len(b)and all(same(x,y)for x,y in zip(a,b))
 return a==b
def qualify_rust_result(raw):
 text=raw.decode('utf-8');name='backup_files::callback_physical_tests::backup_callbacks_actual_sql_and_cache_ownership';lines=text.splitlines();matched=[re.fullmatch(r'test result: ok\. 1 passed; 0 failed; 0 ignored; 0 measured; 250 filtered out; finished in ([0-9]+(?:\.[0-9]+)?)s',line)for line in lines if line.startswith('test result:')]
 require(len(matched)==1 and matched[0]is not None and lines.count('test '+name+' ... ok')==1,'ExactSingleActualRustTestAnd251Inventory')
 require(0<=float(matched[0].group(1))<=90,'RustReportedCase90')
 return {'actual_passed':1,'actual_filtered':250,'reported_seconds':float(matched[0].group(1))}
def validate_native_capture(value,case,database):
 import math
 require(type(case)is int and case in(1,2,3,4)and value['case']==case and type(value['case'])is int and value['database']==database,'ExactActualCaseDatabase')
 require(value['capture_complete']is True and value['capture_only']is True and value['passed']is False,'ActualCompleteCaptureNotFabricatedPassingReport')
 require(type(value['Source_runs'])is int and type(value['Main_runs'])is int and value['Source_runs']==value['Main_runs']==0,'NativeOnly')
 require(same(value['before'],value['after']) and same(value['before_credentials'],value['after_credentials']),'CapturedSQLAndCredentialEffectsEqual')
 require(value['request_slots_while_held']==3 and value['early_shutdown_returned']is False and value['final_shutdown_returned']is True,'ExistingFourSlotPhysicalDrain')
 duration=value['actual_physical_hold_seconds'];require(type(duration)in(int,float)and math.isfinite(duration)and 0<=duration<=25,'ActualPhysicalHold25')
 for name in ['cache_before_request','cache_after_request']:
  require(type(value[name]['raw_DUMP_PTTL'])is dict,'ActualCacheTypedDumpSnapshot')
 before={k:v['dump_hex']for k,v in value['cache_before_request']['raw_DUMP_PTTL'].items()};after={k:v['dump_hex']for k,v in value['cache_after_request']['raw_DUMP_PTTL'].items()};require(before==after,'ActualCacheValuesUnchangedNotTTLFreeze')
 if case in(1,2):require(value['caller_JoinHandle_abort_consumed']is True and value['HTTP_FD_loss_claimed']is False,'ActualCallerAbortNotInferredFDLoss')
 if case==3:
  require(value['actual_HTTP_FD_shutdown_and_drop']is True and value['handler_abort_claimed']is False and value['HTTP_server_graceful_wait0']is True,'ActualClientFDLossAndServerWait')
  fate=value['actual_HTTP_response_write_fate'];require(all(type(fate[k])is int for k in ['accepted','handler_started','handler_cancelled','bytes_read','bytes_written'])and 0<fate['bytes_read']<=65536 and 0<=fate['bytes_written']<=65536 and type(fate['read_eof'])is bool and type(fate['write_errors'])is list and len(fate['write_errors'])<=8 and all(type(x)is str and len(x)<=64 for x in fate['write_errors']),'TypedActualHTTPFate');require(fate['accepted']==1 and fate['handler_started']==1 and fate['socket_dropped']is True and fate['capacity_exceeded']is False,'ActualBoundedTransportFate')
  require(all(type(n)is int for n in fate['response_statuses'])and fate['response_statuses']in([],[401])and fate['handler_cancelled']+len(fate['response_statuses'])==1 and value['HTTP_write_failure_or_response_status_inferred_from_client_loss']is False,'ObservedHandlerAndWriteFateNoInference')
 if case==4:require(same(value['logical_response_before_physical_reply'],{'status':401})and value['later_pipeline_write_arrival_claimed']is False,'ActualLogical401AndDuplexReceipts')
 else:
  terminal=value['actual_target_endpoint_settlement'];require(terminal['original_socket_inodes_absent']is True and terminal['actual_backend_absent']==[] and len(terminal['actual_prior_client_FDs'])==1,'ActualTargetSocketAndPGBackendGone')
  require(value['same_SQL_pool_reuse_after_target_physical_close']is True,'ActualPoolReuseAfterClose')
 return {'capture_valid':True,'case':case,'no_Source_Main_or_fullM8_acceptance':True}

def environment():
 denied=('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','BASELINE_','BENCHMARK_','FIXTURE_')
 return{k:v for k,v in os.environ.items()if not k.startswith(denied)and k!='ENABLE_VOD_FIXTURE'}
def main():
 if EXECUTION_BLOCKED:raise RuntimeError('ROOT_SEALED_NATIVE_PHYSICAL_ONCE_REVIEW_REQUIRED')
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--execution-inputs',type=Path,required=True);a=p.parse_args();tools=Path(__file__).resolve().parent;inputs=json.loads(a.execution_inputs.read_text());out=a.output.resolve();support=a.support_root.resolve();env=environment()
 require(inputs.get('allow_native_physical_capture')is True,'ExplicitRootNativePhysicalAuthority');require(inputs.get('pinned_source_proof_sha256')==sha(tools.parent/'pinned-source-proof.json'),'ExactPinnedSourceProof');require(inputs.get('case_plan_sha256')==sha(tools.parent/'case-plan.json'),'ExactCasePlan');require(set(inputs.get('tools_sha256',{}))==REQUIRED,'ExactElevenTools')
 for name,h in inputs['tools_sha256'].items():require(sha(tools/name)==h,'ExactTool:'+name)
 for name,h in inputs['fixture_sha256'].items():require(sha(tools.parent/name)==h,'ExactFixture:'+name)
 require(sha(Path(inputs['native_ELF']))==inputs['native_ELF_sha256'],'ExactQualifiedNativeELF')
 native_map=json.loads(Path(inputs['native_source_map']).read_text());require(sha(Path(inputs['native_source_map']))==inputs['native_source_map_sha256'],'ExactNativeSourceMap')
 for name,h in native_map.items():require(sha(Path(inputs['native_source_root'])/name)==h,'ExactNativeSource:'+name)
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
 out.mkdir(parents=True,exist_ok=False,mode=0o700);frozen=out/'support';shutil.copytree(support,frozen,ignore=shutil.ignore_patterns('__pycache__'));mounted=out/'controller';mounted.mkdir()
 for name in REQUIRED:shutil.copy2(tools/name,mounted/name)
 shutil.copytree(tools.parent/'inputs',out/'inputs');shutil.copyfile(inputs['native_ELF'],out/'native-store-tests');(out/'native-store-tests').chmod(0o755);require(sha(out/'native-store-tests')==inputs['native_ELF_sha256'],'ExactMountedNativeELF')
 for src,dst in [(a.execution_inputs,'execution-inputs.json'),(tools.parent/'case-plan.json','case-plan.json'),(tools.parent/'pinned-source-proof.json','pinned-source-proof.json')]:shutil.copy2(src,out/dst)
 sys.path.insert(0,str(frozen/'scripts/rust-integration'));from fixture_ports import write_port_overlay
 project='dispatcharr-backup-physical-'+uuid.uuid4().hex[:10];database='rust_probe_backup_physical_'+uuid.uuid4().hex[:10];source=out/'source';source.mkdir();env.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
 overlay=out/'source.yml';save(overlay,{'services':{'baseline':{'volumes':[str(out)+':/backup-physical:ro',str(frozen/'scripts/rust-integration')+':/backup-tools:ro'],'cpus':1,'mem_limit':'1g','pids_limit':256},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time;time.sleep(3600)'],'cpus':.1,'mem_limit':'128m','pids_limit':32}},'networks':{'default':{'internal':True}}});ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=[docker,'compose','-p',project,'-f',str(frozen/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
 report={'passed':False,'Source_verified':False,'Native_verified':False,'performance_verified':False,'project':project,'database':database,'scope':'Four explicit Native-only backup callback physical controls; no Source business requests/Main/physical5and6/performance claim','execution_inputs_sha256':sha(a.execution_inputs),'Original_revision':BASE,'Original_archive_sha256':ARCHIVE,'processes':[],'cases':[]};serial=0;collected_output=0;before=None;cid=None;claimed=False;status=1;cleanup_deadline=None;cleanup_started=None;owned_pids=[];owned_containers=[]
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
  nonlocal serial,collected_output
  output_limit=command_budget(serial,collected_output,cleanup_deadline is not None)
  if cleanup_deadline is not None:timeout=min(timeout,max(.01,cleanup_deadline-time.monotonic()))
  serial+=1;tag='command-'+str(serial).zfill(3);record={'argv':argv,'cwd':os.getcwd(),'began_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'pid':None,'starttime':None,'spawn_state':'constructor_not_called'};save(out/(tag+'.started.json'),record)
  try:job=Collected(argv,env,output_limit,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'),body=body.encode()if isinstance(body,str)else body)
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
   collected_output+=job.used;report['aggregate_collected_output_bytes']=collected_output
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
  before=manifest('dispatcharr','baseline_manifest');save(out/'default-before-private.json',before);text(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database]);report['empty_native_database_created']=True
  stopped=json.loads(text(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']));save(out/'web-stop.json',stopped);require(stopped.get('ok')is True and stopped.get('running')is False and type(stopped.get('pid'))is int and type(stopped.get('exit_code'))is int,'ActualOwnedWebStopped');report['web_stop']=stopped
  phase_deadline=time.monotonic()+1200
  for case in range(1,5):
   require(time.monotonic()<phase_deadline,'WholeNativePhase1200')
   db=database if case==1 else 'rust_probe_backup_physical_'+uuid.uuid4().hex[:10]
   if case!=1:text(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db])
   case_dir=out/('case-'+str(case));case_dir.mkdir();cache_url='redis://:playback-fixture-only@localhost:6379/'+str(9+case)
   require(sha(out/'native-store-tests')==inputs['native_ELF_sha256'],'UnchangedNativeELFBeforeEachCase')
   argv=compose+['exec','-T','-e','POSTGRES_DB='+db,'-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@localhost:5432/'+db,'-e','BACKUP_CACHE_URL='+cache_url,'-e','BACKUP_PHYSICAL_AUTHORITY=owned-native-only','-e','BACKUP_PHYSICAL_CASE='+str(case),'-e','BACKUP_PHYSICAL_INPUTS=/backup-physical','-e','BACKUP_PHYSICAL_OUTPUT=/evidence/'+case_dir.name,'baseline','/evidence/native-store-tests','backup_files::callback_physical_tests::backup_callbacks_actual_sql_and_cache_ownership','--exact','--ignored','--nocapture','--test-threads=1']
   began=time.monotonic();raw,err,code=command(argv,timeout=min(100,max(.01,phase_deadline-time.monotonic())));capture=json.loads((case_dir/'callback-case.json').read_text())
   require(code==0 and capture['capture_complete']is True and capture['case']==case and capture['database']==db,'ActualCompleteNativePhysicalCase')
   validation=validate_native_capture(capture,case,db);save(case_dir/'Root-capture-validation.json',validation)
   rust_result=qualify_rust_result(raw);save(case_dir/'Root-actual-Rust-result.json',rust_result)
   report['cases'].append({'case':case,'actual_exit':code,'elapsed_seconds':time.monotonic()-began,'capture_sha256':sha(case_dir/'callback-case.json'),'native_ELF_sha256':sha(out/'native-store-tests')});flush()
  require(len(report['cases'])==4,'ExactFixedFourCases')
  require(all(sha(source/name)==row['sha256']for name,row in pinned['modules'].items()),'PinnedSourceModulesAfterCases');report['Original_pinned11modules_after_cases_unchanged']=True
  report.update(passed=True,Native_verified=True,Source_verified=False,physical_cases=4);status=0

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
     if fields and fields[0].isdigit():pid=int(fields[0]);owned_pids.append({'CID':owned_cid,'pid':pid,'starttime':proc(pid)})
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
