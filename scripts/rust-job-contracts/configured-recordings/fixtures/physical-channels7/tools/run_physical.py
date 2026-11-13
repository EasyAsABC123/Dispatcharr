#!/usr/bin/env python3
"""Bounded configured-parent physical/refusal host executor; explicit reviewed inputs and fresh output only.
Original Dispatcharr/dependency authors retain application/algorithm/license credit.
"""
import argparse,datetime,hashlib,importlib.util,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
from bounded_process import Collected
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REQUIRED={'observe.py', 'run_physical.py', 'roles.py', 'reference.json', 'source_physical.py', 'bounded_process.py', 'interactive_peer.py', 'redis_reply_proxy.py', 'verify_roles.py', 'physical-policy.json', 'physical_contract.py', 'fixture_endpoints.py', 'support-inputs.json', 'normal_contract.py', 'source_contract.py', 'ledger_io.py', 'pg_reply_proxy.py', 'acl_probes.py'}
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
def environment():
 denied=('GIT_','COMPOSE_','DATABASE_','POSTGRES_','REDIS_','DJANGO_','CELERY_','BASELINE_','BENCHMARK_','FIXTURE_')
 return{k:v for k,v in os.environ.items()if not k.startswith(denied)and k!='ENABLE_VOD_FIXTURE'}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--execution-inputs',type=Path,required=True);a=p.parse_args();tools=Path(__file__).resolve().parent;inputs=json.loads(a.execution_inputs.read_text());out=a.output.resolve();support=a.support_root.resolve();env=environment()
 require(inputs.get('authorized')is True,'ExplicitRootPhysicalOnceAuthority');require(inputs.get('mode')=='physical-channels-onward','FiniteMode');require(inputs.get('case_plan_sha256')==sha(tools.parent/'case-plan.json'),'ExactCasePlan');require(set(inputs.get('tools_sha256',{}))==REQUIRED,'ExactPhysicalToolClosure')
 for name,h in inputs['tools_sha256'].items():require(sha(tools/name)==h,'ExactTool:'+name)
 peer=inputs['Native_elf'];require(Path(peer['path']).is_file()and sha(peer['path'])==peer['sha256']and Path(peer['path']).open('rb').read(4)==b'\x7fELF','ExactOutsideNativeELF')
 build=inputs['Native_build_record'];require(sha(build['path'])==build['sha256'],'ExactNativeBuildRecord');built=json.loads(Path(build['path']).read_text());require(built['copied_after_all_waits_zero']is True and built['binary']['sha256']==peer['sha256']and built['source_map_sha256']==inputs['Native_source_map_sha256'],'BoundSameSourceNativeBuild')
 require(inputs.get('Original_archive_sha256')==ARCHIVE and re.fullmatch('sha256:[a-f0-9]{64}',inputs.get('Original_image_id','')),'PinnedOriginalIdentity');require(inputs.get('output')==str(out)and not out.exists(),'FixedFreshOutput');plan=json.loads((tools.parent/'case-plan.json').read_text());require(plan['outputs'][inputs['mode']]==str(out),'BoundModeOutput')
 docker=shutil.which('docker');require(docker and sha(docker)==inputs.get('docker_executable_sha256')and env.get('DOCKER_HOST')==inputs.get('Docker_host'),'BoundDocker')
 require(inputs.get('tooling_environment')=={k:env.get(k)for k in SAFE},'ExactSixSafeExports')
 for key in ['tooling_environment_script','compose_executable']:
  row=inputs[key];require(Path(row['path']).is_file()and sha(row['path'])==row['sha256'],'BoundTool:'+key)
 for name,h in json.loads((tools/'support-inputs.json').read_text()).items():require(sha(support/name)==h,'ExactSupport:'+name)
 # Read-only preflight precedes fixture output/claim. Its separate receipt is
 # retained even if the daemon is unavailable; it cannot authorize a workload.
 preflight=[];preflight_path=out.parent/(out.name+'-host-preflight.json');require(not preflight_path.exists(),'FreshPreflightReceipt')
 for argv in [[docker,'version','--format','{{json .Server.Version}}'],[docker,'compose','version','--short'],[docker,'info','--format','{{json .ServerVersion}}']]:
  job=Collected(argv,env,65536,lambda:None);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
  try:job.wait(15)
  finally:
   preflight.append({'argv':argv,'began_utc':began,'exit':job.poll(),'stdout_hex':bytes(job.output['stdout']).hex(),'stderr_hex':bytes(job.output['stderr']).hex(),'settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads)});save(preflight_path,preflight)
 out.mkdir(parents=True,exist_ok=False,mode=0o700);frozen=out/'support';shutil.copytree(support,frozen,ignore=shutil.ignore_patterns('__pycache__'));mounted=out/'job-tools';mounted.mkdir()
 for name in REQUIRED:shutil.copy2(tools/name,mounted/name)
 for src,dst in [(a.execution_inputs,'execution-inputs.json'),(tools.parent/'case-plan.json','case-plan.json'),(tools.parent/'pinned-source-proof.json','pinned-source-proof.json')]:shutil.copy2(src,out/dst)
 sys.path.insert(0,str(frozen/'scripts/rust-integration'));from fixture_ports import write_port_overlay
 project='dispatcharr-configured-physical-'+uuid.uuid4().hex[:10];database='rust_probe_backup_jobs_'+uuid.uuid4().hex[:10];source=out/'source';source.mkdir();env.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),DISPATCHARR_ENV='aio')
 overlay=out/'source.yml';save(overlay,{'services':{'baseline':{'volumes':[str(mounted)+':/job-tools:ro',str(frozen/'scripts/rust-integration')+':/backup-tools:ro',peer['path']+':/native-configured-peer:ro'],'cpus':1,'mem_limit':'1g','pids_limit':256},'origin':{'entrypoint':['/dispatcharrpy/bin/python','-c','import time;time.sleep(3600)'],'cpus':.1,'mem_limit':'128m','pids_limit':32}},'networks':{'default':{'internal':True}}});ports=out/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]});compose=[docker,'compose','-p',project,'-f',str(frozen/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
 report={'passed':False,'Source_verified':False,'Native_verified':False,'performance_verified':False,'project':project,'database':database,'scope':'Native physical/refusal controls on actual configured normal library paths; Original producer/AsyncResult, no Source timing parity/default FIFO/Main/fullworkflow','execution_inputs_sha256':sha(a.execution_inputs),'Original_revision':BASE,'Original_archive_sha256':ARCHIVE,'processes':[]};serial=0;before=None;cid=None;claimed=False;status=1;cleanup_deadline=None;owned_pids=[];owned_containers=[]
 def flush():save(out/'integration.json',report)
 def stop_owned():
  if not claimed:return
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
   if not(allow_nonzero and job.physically_settled and job.invalid is None):raise
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
  archive=out/'original-source.tar';text(['git','-C',str(a.repo.resolve()),'archive','--format=tar','--output',str(archive),BASE]);require(sha(archive)==ARCHIVE,'ExactOriginalArchive');text(['tar','-xf',str(archive),'--no-same-owner','-C',str(source)]);(source/'media').mkdir(exist_ok=True)
  require(all(x['empty']for x in inventory().values()),'OwnedProjectInitiallyAbsent');claimed=True;text(compose+['up','-d','--no-build','--pull','never','baseline']);cid=text(compose+['ps','-q','baseline']).strip();require(re.fullmatch('[a-f0-9]{64}',cid),'ExactCID');identity=json.loads(text([docker,'inspect',cid]))[0];save(out/'actual-container-inspect.json',identity);require(identity['Image']==inputs['Original_image_id']and identity['Config']['Image']==IMAGE,'ActualPinnedOriginalImage');report['Original_container']={'id':cid,'image_id':identity['Image'],'image_reference':IMAGE};owned_containers=text(compose+['ps','-q']).splitlines();require(cid in owned_containers and all(re.fullmatch('[a-f0-9]{64}',c)for c in owned_containers),'ActualOwnedContainerIDs');report['owned_containers']=owned_containers
  end=time.monotonic()+180
  while True:
   raw,err,code=command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=5,allow_nonzero=True)
   if code==0:break
   require(time.monotonic()<end,'FixedReadiness180s');time.sleep(.2)
  before=manifest('dispatcharr','baseline_manifest');save(out/'default-before-private.json',before);text(compose+['exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database]);report['seed']=manifest(database,'seed')
  stopped=json.loads(text(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']));save(out/'web-stop.json',stopped);require(stopped.get('ok')is True and stopped.get('running')is False and type(stopped.get('pid'))is int and type(stopped.get('exit_code'))is int,'ActualOwnedWebStopped');report['web_stop']=stopped
  text(compose+['exec','-T','-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/job-tools/verify_roles.py'],timeout=30)
  save(out/'controller-context.json',{'lifecycle_began_utc':report['processes'][0]['began_utc'],'host_first_command_receipt':'command-001.json','mode':inputs['mode']})
  argv=compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','DATABASE_URL=postgres://dispatch:playback-fixture-only@localhost:5432/'+database,'-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/job-tools/source_physical.py'];report['controller_argv']=argv;flush()
  try:text(argv,timeout=600)
  finally:
   last=report['processes'][-1]if report['processes']else{};matched=last.get('argv')==argv;report['actual_controller_exit']=last.get('exit')if matched else None;report['actual_controller_settled']=last.get('physically_settled')if matched else False;flush()
  actual=json.loads((out/'source-configured-recordings-private.json').read_text());report['Native_raw_report_sha256']=sha(out/'source-configured-recordings-private.json');spec=importlib.util.spec_from_file_location('_configured_native_contract_only',tools/'physical_contract.py');contract=importlib.util.module_from_spec(spec);spec.loader.exec_module(contract);report['native_acceptance']=contract.validate_report(report['actual_controller_exit'],actual,inputs['mode']);report.update(passed=True,Source_verified=False,Source_reference_verified=True,Native_verified=True,cases=len(actual['cases']),checks=len(actual['checks']));status=0
 except BaseException as error:report.update(passed=False,error_type=type(error).__name__,error=str(error)[:4096]);status=1
 finally:
  cleanup_started=time.monotonic();cleanup_deadline=cleanup_started+90
  if before is not None:
   try:after=manifest('dispatcharr','baseline_manifest');save(out/'default-after-private.json',after);require(before==after,'DefaultElevenExact');report['default_database_unchanged']=True
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
