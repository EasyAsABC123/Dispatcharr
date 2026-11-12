#!/usr/bin/env python3
"""Bounded observer INTEGRATION CONTRACT ONLY; no resource-budget acceptance.
Original Dispatcharr/dependency authors retain all credit and licenses.
Resource7's two entry hardblocks are copied unchanged and never called.
"""
import argparse,hashlib,importlib.util,json,os,shutil,subprocess,sys,time,uuid,urllib.request
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def load(path,name):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def validate(plan,approval,boot,now,plan_sha,files):
 if plan.get('scope')!='m0-resource-observer-integration-contract' or plan.get('controller_sha256')!=files:raise ValueError('ContractInputBinding')
 if approval.get('scope')!=plan['scope']or approval.get('approved')is not True or approval.get('plan_sha256')!=plan_sha or approval.get('boot_id')!=boot:raise ValueError('MissingExactRootApproval')
 issued,expires=approval.get('issued_unix'),approval.get('expires_unix')
 import math
 if not all(type(x)in(int,float)and math.isfinite(x)for x in[issued,expires,now])or not issued<=now<expires or now-issued>300 or not 0<expires-issued<=1800:raise ValueError('ContractApprovalTime')
 if plan.get('full_resource_window_enabled')is not False or plan.get('Native_enabled')is not False or plan['contract_policy']['observation_schedule_seconds']!=10:raise ValueError('ContractScope')
def prepare_mount(controller):
 path=controller/'proc_snapshot.py';before=hashlib.sha256(path.read_bytes()).hexdigest();path.chmod(0o444)
 if path.stat().st_mode&0o777!=0o444 or hashlib.sha256(path.read_bytes()).hexdigest()!=before:raise ValueError('CensusMountPreparation')
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--plan',type=Path,required=True);p.add_argument('--approval',type=Path,required=True);a=p.parse_args()
 raw=a.plan.read_bytes();plan=json.loads(raw);approval=json.loads(a.approval.read_text());files={f.name:hashlib.sha256(f.read_bytes()).hexdigest()for f in HERE.iterdir()if f.is_file()}
 validate(plan,approval,Path('/proc/sys/kernel/random/boot_id').read_text().strip(),time.time(),hashlib.sha256(raw).hexdigest(),files)
 out=a.output.resolve()
 if out.exists()or out==HERE or HERE in out.parents:raise ValueError('FreshContractOutputRequired')
 out.mkdir(parents=True);controller=out/'controller';shutil.copytree(HERE,controller,ignore=shutil.ignore_patterns('__pycache__'));prepare_mount(controller)
 save(out/'approval-binding.json',{'plan_sha256':hashlib.sha256(raw).hexdigest(),'approval_sha256':hashlib.sha256(a.approval.read_bytes()).hexdigest(),'scope':plan['scope']});(out/'approved-plan.json').write_bytes(raw)
 sys.path.insert(0,str(controller));from supervision import WorkDeadline,terminal
 report={'passed':False,'Source_budget_acceptance':None,'baseline_accepted':False,'resource_acceptance':False,'Native_enabled':False,'M0_complete':False,'scope':plan['scope']}
 try:return load(controller/'contract.py','frozen_resource_contract').execute_contract(a.repo.resolve(),out,controller,report,WorkDeadline(900),plan['contract_policy'])
 except BaseException as error:
  report.update(passed=False,error_type=type(error).__name__,error_message=str(error)[:1024]);terminal(report,out,[]);return 1
def execute_contract(root,out,controller,report,deadline,contract_policy):
 sys.path.insert(0,str(controller))
 policy=json.loads((controller/'policy.json').read_text())
 report.update({'passed':False,'scope':__doc__,'performance_verified':False,'M0_complete':False,'baseline_accepted':False,'Native_enabled':False,'repetition':None,'project':'dispatcharr-m0-concurrent-'+uuid.uuid4().hex[:10]})
 save(out/'seal.json',{'policy_sha256':hashlib.sha256((controller/'policy.json').read_bytes()).hexdigest(),'controller_sha256':{v.name:hashlib.sha256(v.read_bytes()).hexdigest()for v in controller.iterdir()if v.is_file()}})
 tools=out/'support';tools.mkdir();support_archive=subprocess.check_output(['git','-C',str(root),'archive','21a152a27a5800592c66e412a7fac67509a5d485','scripts','docker'],timeout=deadline.remaining(30));subprocess.run(['tar','-x','--no-same-owner','-C',str(tools)],input=support_archive,check=True,timeout=deadline.remaining(30));save(out/'support-binding.json',{'git_revision':'21a152a27a5800592c66e412a7fac67509a5d485','archive_sha256':hashlib.sha256(support_archive).hexdigest()});source=out/'original-source';source.mkdir(mode=0o755);source.chmod(0o755);raw=subprocess.check_output(['git','-C',str(root),'archive',BASE],timeout=deadline.remaining(30));assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=raw,check=True,timeout=deadline.remaining(30));(source/'media').mkdir(exist_ok=True)
 sys.path[:0]=[str(tools/'scripts/rust-integration'),str(tools/'scripts/rust-guide-perf')];ports=load(tools/'scripts/rust-integration/fixture_ports.py','concurrent_ports');work=load(controller/'workload.py','concurrent_work');observer=load(controller/'observer.py','concurrent_observer');database='rust_probe_backup_jobs_'+uuid.uuid4().hex[:10];resource_module=load(controller/'resource_window.py','concurrent_resource_window');corrected=load(controller/'coverage.py','concurrent_denied_coverage');metadata=load(controller/'metadata.py','concurrent_metadata');bounded=load(controller/'bounded_process.py','concurrent_bounded_process')
 env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='600',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')
 common={'DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':database,'POSTGRES_HOST':'baseline','POSTGRES_PORT':'5432','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','REDIS_HOST':'baseline','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','CELERY_BROKER_URL':'redis://:playback-fixture-only@baseline:6379/8','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@baseline:6379/9','JOB_DISPOSABLE_FIXTURE':'playback-fixture-only','DISPATCHARR_ENABLE_IP_LOOKUP':'false','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_LOG_LEVEL':'WARNING','PYTHONPATH':'/concurrent:/app'}
 services={'baseline':{'volumes':[str(controller)+':/concurrent:ro',str(tools/'scripts/rust-integration')+':/backup-tools:ro']},'origin':{}}
 for name in ['source','worker']:
  services[name]={'image':IMAGE,'working_dir':'/app','environment':common,'volumes':[str(source)+':/app:ro',str(controller)+':/concurrent:ro',str(tools/'scripts/rust-playback-baseline')+':/verification:ro',str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro',str(out)+':/evidence','concurrent-media:/app/media'],'tmpfs':['/tmp:mode=1777,size=134217728','/data:mode=1777,size=67108864'],'stop_grace_period':'15s'}
 services['source']['networks']=['default','ingress'];services['source'].update(entrypoint=['/dispatcharrpy/bin/python','/m4-perf/source_web.py'],ports=['127.0.0.1::5656'])
 services['worker']['entrypoint']=['/dispatcharrpy/bin/celery','-A','concurrent_task_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-m0@fixture'];services['worker']['environment']={**common,'C_FORCE_ROOT':'true'}
 for name,limits in policy['caps'].items():services[name].update(cpus=limits['nano_cpus']/1e9,mem_limit=limits['memory_bytes']);services[name].setdefault('volumes',[]).append(str(controller/'proc_snapshot.py')+':/m0-census.py:ro')
 overlay=out/'fixture.json';save(overlay,{'services':services,'volumes':{'concurrent-media':{}},'networks':{'default':{'internal':True},'ingress':{'internal':False}}});portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'source':[5656]});compose=['docker','compose','-p',report['project'],'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(portfile)]
 def stop_owned():
  from owned_stop import stop_project
  report['owned_stop']=stop_project(compose,env,report['project'],policy['resource_draft']['supervision']['owned_stop_seconds'],run=registry.stop_control)
 from supervision import Registry,PayloadBudget
 limits=policy['resource_draft']['supervision'];budget=PayloadBudget(limits['host_observer_retained_payload_bytes'],limits['cleanup_reserved_payload_bytes']);budget.charge(limits['external_child_payload_reservation_bytes'])
 registry=Registry(bounded.Collected,env,out,stop_owned,limits,budget,deadline)
 def command(argv,body=None,timeout=180):return registry.run(argv,body,timeout)
 def cleanup_command(argv,timeout=None):return registry.run(argv,timeout=limits["cleanup_control_command_seconds"]if timeout is None else timeout,cleanup=True)

 def cleanup_counter_command(argv,timeout=5):
  from final_counter_authority import FinalCounterCommand
  return FinalCounterCommand(registry,selected,observer.CGROUP_SCRIPT)(argv,timeout)

 def execute(*argv,body=None,timeout=180):return command(compose+list(argv),body,timeout)
 def endpoint(name,port):return 'http://'+execute('port',name,str(port)).decode().strip()
 def get(url,token=None):
  with urllib.request.urlopen(urllib.request.Request(url,headers={'Authorization':'Bearer '+token}if token else{}),timeout=3)as response:return json.load(response)
 def ready(url):
  until=time.monotonic()+180
  while True:
   try:return get(url)
   except Exception:
    if time.monotonic()>until:raise TimeoutError('OwnedReadiness')
    time.sleep(.25)
 def helper(db,action):return json.loads(execute('exec','-T','-e','POSTGRES_DB='+db,'-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py',body=json.dumps({'action':action}).encode()).splitlines()[-1])
 before=None;job=None;joblog=None;window=None;window_closed=False
 try:
  execute('up','-d','baseline');baseline_ready_deadline=time.monotonic()+180
  while True:
   try:
    execute('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200",timeout=5);break
   except (subprocess.SubprocessError,bounded.CollectionFailure):
    if time.monotonic()>baseline_ready_deadline:raise TimeoutError('InternalBaselineReadiness')
    time.sleep(.25)
  before=helper('dispatcharr','baseline_manifest');execute('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database);helper(database,'seed')
  execute('exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/concurrent/seed.py');seed=json.loads((out/'concurrent-seed-private.json').read_text());assert seed['database']==database and seed['profile_max_streams']==1
  execute('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop');execute('up','-d','--no-deps','source');base=endpoint('source',5656);ready(base+'/api/core/version/');assert get(base+'/api/accounts/users/me/',seed['token'])['username']==seed['username'];channel=get(base+'/api/channels/channels/'+str(seed['channel_pk'])+'/',seed['token']);assert channel['uuid']==seed['channel_uuid'];report['copied_DB_HTTP_binding']=True
  # Only known startup plugin messages may be removed, before any workload.
  execute('exec','-T','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','baseline','/dispatcharrpy/bin/python','-c',"import redis,json,os;r=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);assert r.llen('celery')<=64;v=r.lrange('celery',0,63);assert all(json.loads(x)['headers']['task']=='apps.plugins.tasks.refresh_plugin_repos'for x in v);r.delete('celery')")
  execute('up','-d','--no-deps','worker')
  until=time.monotonic()+60
  while not(out/'concurrent-worker-ready.json').exists():
   if time.monotonic()>until:raise TimeoutError('WorkerReady')
   time.sleep(.1)
  execute('exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/concurrent/django_overflow.py')
  report['genuine_django_overflow']=json.loads((out/'django-overflow/proof.json').read_text());assert report['genuine_django_overflow']['passed']
  selected={}
  for name,service_limits in policy['caps'].items():
   cid=execute('ps','-q',name).decode().strip();record=json.loads(command(['docker','inspect',cid]))[0];assert record['HostConfig']['Memory']==service_limits['memory_bytes']and record['HostConfig']['NanoCpus']==service_limits['nano_cpus'];selected[name]={'id':cid,'limits':service_limits}
  group_observer=observer.Observer(selected,tools,command=command)
  report['bootstrap_failed']=False
  metadata.capture(command,selected,policy,source,controller,out,budget=budget)
  window=resource_module.Window(group_observer,selected,command,policy,out/'observer-contract',corrected,registry=registry,budget=budget,deadline=deadline,final_counter_command=cleanup_counter_command)
  window.label('observer-contract-only');window.start();scheduled_end=window.started+contract_policy['observation_schedule_seconds']
  while time.monotonic()<scheduled_end:
   window.check();window.stop.wait(min(.1,max(0,scheduled_end-time.monotonic())))
  observed=window.close();report['observer_contract']=observed
  rows=[r for r in window.samples.rows if r['completed_monotonic']<=scheduled_end and r['phase_stable']]
  cpu=[r for r in rows if r['cpu_interval_stable']and set(r.get('cpu_intervals',{}))==set(selected)]
  from coverage import eligible_census_row
  censuses=[r for r in window.censuses.rows if r['completed_monotonic']<=scheduled_end and eligible_census_row(r)]
  checks={'resource_rows':len(rows)>=contract_policy['minimum_four_role_resource_rows'],'CPU_intervals':len(cpu)>=contract_policy['minimum_four_role_CPU_intervals'],'complete_censuses':len(censuses)>=contract_policy['minimum_complete_four_role_censuses'],'no_observer_error':not window.errors,'threads_drained':observed['observer_threads_drained'],'no_OOM_or_counter_reset':'whole_resource_window'in observed}
  report['contract_checks']=checks;report['counts']={'resource_rows':len(rows),'CPU_intervals':len(cpu),'complete_censuses':len(censuses)}
  if not all(checks.values()):raise ValueError('ObserverContractShortageOrError')
  # Timed before deliberate fixture destruction; no post-stop SQL claim.
  report['default_database_audit_before_deliberate_stop']=helper('dispatcharr','baseline_manifest')==before
  if not report['default_database_audit_before_deliberate_stop']:raise ValueError('DefaultDatabaseChanged')
  worker=selected['worker']['id'];fault=registry.launch(['docker','exec',worker,'/dispatcharrpy/bin/python','-c',"import time;print('owned-helper-ready',flush=True);time.sleep(60)"],limit=4096)
  ready_until=time.monotonic()+contract_policy['helper_ready_deadline_seconds']
  while True:
   with fault.lock:ready=b'owned-helper-ready\n'in fault.output['stdout']
   if ready:break
   if time.monotonic()>=ready_until:raise TimeoutError('ActualHelperNotReady')
   if fault.poll()is not None:raise RuntimeError('ActualHelperExitedBeforeFault')
   time.sleep(.01)
  began=time.monotonic();error=None
  try:fault.wait(contract_policy['helper_observer_timeout_seconds'])
  except bounded.CollectionFailure as failure:error=str(failure)
  state=registry.retire(fault)
  report['physical_fault']={'fault':'actual owned-worker helper stdout pipe held; no application SQL/task stub','expected_collection_failure':'CollectionDeadline','actual_collection_failure':error,'host_settlement':state,'elapsed_seconds':time.monotonic()-began,'owned_stop':report.get('owned_stop'),'no_resource_sampling_during_fault':True}
  if error!='CollectionDeadline'or not state['drained']or not state['owned_stop_acknowledged']or state['pipes_alive']!=0 or report.get('owned_stop',{}).get('container_count')!=4:raise ValueError('PhysicalStopDrainContract')
  report['passed']=True
 except BaseException as error:
  report.update(passed=False,error_type=type(error).__name__,error_message=str(error)[:1024])
 finally:
  from finalization import finish as finish_terminal
  def audit_default():
   return report.get('default_database_audit_before_deliberate_stop')
  def logs():
   data=cleanup_command(compose+['logs','--no-color','--tail','200']);(out/'compose-private.log').write_bytes(data);return {'bytes':len(data)}
  def inventory(kind):
   argv={'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}[kind]
   return cleanup_command(['docker',*argv,'--filter','label=com.docker.compose.project='+report['project']],limits['cleanup_inventory_seconds']).decode().strip()
  report['host_budget_before_cleanup']=budget.snapshot()
  finish_terminal(report,out,window,registry,audit_default,logs,lambda:cleanup_command(compose+['down','--volumes','--remove-orphans','--timeout','10'],limits['cleanup_compose_down_seconds']),inventory,limits)
  report['host_budget_after_cleanup']=budget.snapshot();from supervision import terminal;terminal(report,out,[])
 print(json.dumps({'passed':report['passed'],'output':str(out),'scope':report['scope']}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
