#!/usr/bin/env python3
"""Owned Source-only concurrent playback/import functional qualification.
Original Dispatcharr and dependency authors retain application/algorithm credit.
Integrated collector/Django controls only. No Native/default graph/resource acceptance.
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
RESOURCE_EXECUTION_BLOCKED=True
class ExpectedAbort(Exception):pass
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--mode',choices=['qualification','abort'],required=True);a=p.parse_args();root=a.repo.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);controller=out/'controller';shutil.copytree(HERE,controller,ignore=shutil.ignore_patterns('support','__pycache__','*.log'));(controller/'proc_snapshot.py').chmod(0o444);policy=json.loads((controller/'policy.json').read_text());report={'passed':False,'scope':__doc__,'performance_verified':False,'M0_complete':False,'Native_enabled':False,'project':'dispatcharr-m0-concurrent-'+uuid.uuid4().hex[:10]};save(out/'seal.json',{'policy_sha256':hashlib.sha256((controller/'policy.json').read_bytes()).hexdigest(),'controller_sha256':{v.name:hashlib.sha256(v.read_bytes()).hexdigest()for v in controller.iterdir()if v.is_file()}})
 tools=out/'support';tools.mkdir();support_archive=subprocess.check_output(['git','-C',str(root),'archive','21a152a2','scripts','docker']);subprocess.run(['tar','-x','--no-same-owner','-C',str(tools)],input=support_archive,check=True);save(out/'support-binding.json',{'git_revision':'21a152a2','sha256':hashlib.sha256(support_archive).hexdigest()});source=out/'original-source';source.mkdir(mode=0o755);source.chmod(0o755);raw=subprocess.check_output(['git','-C',str(root),'archive',BASE]);assert hashlib.sha256(raw).hexdigest()==ARCHIVE;subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=raw,check=True);(source/'media').mkdir(exist_ok=True)
 sys.path[:0]=[str(tools/'scripts/rust-integration'),str(tools/'scripts/rust-guide-perf')];ports=load(tools/'scripts/rust-integration/fixture_ports.py','concurrent_ports');work=load(controller/'workload.py','concurrent_work');observer=load(controller/'observer.py','concurrent_observer');database='rust_probe_backup_jobs_'+uuid.uuid4().hex[:10];bounded=load(controller/'bounded_process.py','integrated_bounded');report['mode']=a.mode;command_index=0
 env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='600',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')
 common={'DJANGO_SECRET_KEY':'playback-fixture-only','POSTGRES_DB':database,'POSTGRES_HOST':'baseline','POSTGRES_PORT':'5432','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','REDIS_HOST':'baseline','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','CELERY_BROKER_URL':'redis://:playback-fixture-only@baseline:6379/8','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@baseline:6379/9','JOB_DISPOSABLE_FIXTURE':'playback-fixture-only','DISPATCHARR_ENABLE_IP_LOOKUP':'false','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio','PYTHONDONTWRITEBYTECODE':'1','DISPATCHARR_LOG_LEVEL':'WARNING','PYTHONPATH':'/concurrent:/app'}
 services={'baseline':{'volumes':[str(controller)+':/concurrent:ro',str(tools/'scripts/rust-integration')+':/backup-tools:ro']},'origin':{}}
 for name in ['source','worker']:
  services[name]={'image':IMAGE,'working_dir':'/app','environment':common,'volumes':[str(source)+':/app:ro',str(controller)+':/concurrent:ro',str(tools/'scripts/rust-playback-baseline')+':/verification:ro',str(tools/'scripts/rust-m4-perf')+':/m4-perf:ro',str(out)+':/evidence','concurrent-media:/app/media'],'tmpfs':['/tmp:mode=1777,size=134217728','/data:mode=1777,size=67108864'],'stop_grace_period':'15s'}
 services['source']['networks']=['default','ingress'];services['source'].update(entrypoint=['/dispatcharrpy/bin/python','/m4-perf/source_web.py'],ports=['127.0.0.1::5656'])
 services['worker']['entrypoint']=['/dispatcharrpy/bin/celery','-A','concurrent_task_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-m0@fixture'];services['worker']['environment']={**common,'C_FORCE_ROOT':'true','PGAPPNAME':'m0-concurrent-worker'}
 for name,limits in policy['caps'].items():services[name].update(cpus=limits['nano_cpus']/1e9,mem_limit=limits['memory_bytes']);services[name].setdefault('volumes',[]).append(str(controller/'proc_snapshot.py')+':/m0-census.py:ro')
 overlay=out/'fixture.json';save(overlay,{'services':services,'volumes':{'concurrent-media':{}},'networks':{'default':{'internal':True},'ingress':{'internal':False}}});portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'origin':[8080],'source':[5656]});compose=['docker','compose','-p',report['project'],'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(portfile)]
 def stop_owned():
  stopped=subprocess.run(compose+['kill'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)
  if stopped.returncode:raise RuntimeError('OwnedStopUnacknowledged')
 def command(argv,body=None,timeout=180):
  nonlocal command_index
  command_index+=1
  return bounded.Collected(argv,env,8388608,stop_owned,stderr_path=out/('command-'+str(command_index)+'.stderr'),body=body).wait(timeout)

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
 before=None;job=None;joblog=None;holdjob=None
 try:
  execute('up','-d','baseline');deadline=time.monotonic()+180
  while True:
   try:
    execute('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200",timeout=5);break
   except (subprocess.SubprocessError,bounded.CollectionFailure):
    if time.monotonic()>deadline:raise TimeoutError('InternalBaselineReadiness')
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
  for name,limits in policy['caps'].items():
   cid=execute('ps','-q',name).decode().strip();record=json.loads(command(['docker','inspect',cid]))[0];assert record['HostConfig']['Memory']==limits['memory_bytes']and record['HostConfig']['NanoCpus']==limits['nano_cpus'];selected[name]={'id':cid,'limits':limits}
  report['observer_contract']=observer.Observer(selected,tools).smoke();report['observer_contract']['scope']='10s four-service capability check only; no workload resource acceptance';report['census']={}
  for name,value in selected.items():
   inventory=json.loads(command(['docker','exec',value['id'],'/dispatcharrpy/bin/python','/m0-census.py','--inventory']));parts=[json.loads(command(['docker','exec','-u',str(uid)+':'+str(gid),value['id'],'/dispatcharrpy/bin/python','/m0-census.py']))for uid,gid in inventory['identities']];end=json.loads(command(['docker','exec',value['id'],'/dispatcharrpy/bin/python','/m0-census.py','--inventory']));observed=[p for part in parts for p in part['process_ids']];report['census'][name]={'inventory_start':inventory,'inventory_end':end,'per_uid':parts,'complete':set(observed)==set(inventory['process_ids'])==set(end['process_ids'])and len(observed)==len(set(observed))and all(not(p['raced_processes']or p['raced_descriptors'])for p in parts)}
  def active(count):
   script="local k=redis.call('KEYS',ARGV[1]);local n=0;for _,v in ipairs(k)do if redis.call('TYPE',v).ok=='hash'then if redis.call('HGET',v,'user_id')~=ARGV[2]then return {-1,#k}end;n=n+1 end end;return {n,#k}"
   data=json.loads(execute('exec','-T','baseline','redis-cli','--json','EVAL',script,'0','live:channel:'+seed['channel_uuid']+':clients:*',str(seed['user_id'])));assert data==[count,count];slots=int(execute('exec','-T','baseline','redis-cli','GET','profile_connections:'+str(seed['profile_id'])).strip()or b'0');assert slots==count;return{'clients':count,'all_owned_user_id':True}
  def start():
   nonlocal job,joblog,holdjob
   if a.mode=='abort':
    lock_argv=compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/concurrent/hold_sql.py']
    holdjob=bounded.Collected(lock_argv,env,1048576,stop_owned,stdout_path=out/'hold-sql.stdout',stderr_path=out/'hold-sql.stderr')
    deadline=time.monotonic()+10
    while not(out/'held-relation-ready.json').exists():
     if time.monotonic()>deadline:raise TimeoutError('HeldRelationStartup')
     time.sleep(.05)
   save(out/'concurrent-window.json',{'begin':time.monotonic()});argv=compose+['exec','-T','-e','POSTGRES_DB='+database,'-e','JOB_DISPOSABLE_FIXTURE=playback-fixture-only','-e','CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8','-e','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9','baseline','/dispatcharrpy/bin/python','/concurrent/source_workflow.py'];job=bounded.Collected(argv,env,8388608,stop_owned,stdout_path=out/'workflow-controller-private.stdout',stderr_path=out/'workflow-controller-private.stderr')
   if a.mode=='abort':
    worker_inspected=json.loads(command(['docker','inspect',selected['worker']['id']]))[0];addresses=[v['IPAddress']for v in worker_inspected['NetworkSettings']['Networks'].values()if v['IPAddress']];assert len(addresses)==1;worker_ip=addresses[0];report['abort_worker_database_client_ip']=worker_ip
    deadline=time.monotonic()+20;waiting=[]
    while time.monotonic()<deadline:
     sql="SELECT coalesce(json_agg(json_build_object('pid',pid,'application_name',application_name,'wait_event_type',wait_event_type,'wait_event',wait_event,'state',state)),'[]'::json) FROM pg_stat_activity WHERE datname='"+database+"' AND client_addr='"+worker_ip+"'::inet AND wait_event_type='Lock'"
     waiting=json.loads(execute('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d',database,'-At','-c',sql))
     save(out/'pending-SQL-last-query-private.json',{'waiting':waiting,'worker_client_ip':worker_ip})
     if waiting and(out/'workflow-prerun.jsonl').exists():break
     time.sleep(.05)
    assert waiting,'No actual Original worker SQL wait observed'
    report['abort_worker_SQL_wait']=waiting
    report['abort_actual_task_prerun']=[json.loads(v)for v in(out/'workflow-prerun.jsonl').read_text().splitlines()]
    active(1);report['abort_playback_was_active']=True
    job.abort('ControllerAbort')
    for name,value in selected.items():
     inspected=json.loads(command(['docker','inspect',value['id']]))[0]['State'];assert inspected['Running']is False and inspected['Pid']==0
    report['abort_all_four_containers_stopped']=True
    for owned in [job,holdjob]:
     try:owned.wait(10)
     except bounded.CollectionFailure:pass
     assert owned.process.poll()is not None and not any(t.is_alive()for t in owned.threads)
    report['abort_host_exec_and_pipes_settled']=True
    raise ExpectedAbort()
   return None
  def finish(_state,begin,end):
   job.wait(timeout=5);report['normal_collector_witness']=load(controller/'normal_audit.py','normal_collector_audit').inspect(out,job);actual=json.loads((out/'source-refresh-workflow-private.json').read_text());assert actual['passed'];return{'scope':'No resource sampler during workload','workflow_passed':True}
  phase=work.run(base,seed['path'],seed['token'],1,15,120,out/'media',start,finish,active,prefix_limit=2097152);row=phase['viewers'][0];assert row['status']==200 and not row['errors']and not row['unexpected_eof']and row['post_window_barrier']and row['prefix_bytes']==2097152 and row['measured_bytes']>=120*100000 and row['first_byte_ms']<=15000;report['playback']=phase
  for label,file in [('startup','0.ts'),('import1','0-import1.ts'),('import2','0-import2.ts')]:
   payload=out/'media'/file;assert payload.stat().st_size==2097152;decoded=execute('exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/media/'+file,'-map','0:v:0','-map','0:a:0','-t','5','-f','null','-',timeout=20);frames=max([int(l.split(b'=')[1])for l in decoded.splitlines()if l.startswith(b'frame=')],default=0);assert frames>=25;report.setdefault('decodes',[]).append({'label':label,'frames':frames,'payload_sha256':hashlib.sha256(payload.read_bytes()).hexdigest()})
  tasks=[json.loads(l)for l in(out/'workflow-prerun.jsonl').read_text().splitlines()];assert len(tasks)==6;assert all(row['reader_start_monotonic']<=t['monotonic']<=row['reader_end_monotonic']for t in tasks);done={t['id']:t for t in[json.loads(l)for l in(out/'workflow-postrun.jsonl').read_text().splitlines()]}
  for task in tasks:
   earlier=[x for x in row['byte_trace']if x['monotonic']<=task['monotonic']];later=[x for x in row['byte_trace']if x['monotonic']>=done[task['id']]['monotonic']];assert earlier and later and later[0]['bytes']>earlier[-1]['bytes']
  report['resource_execution_hardblocked']=RESOURCE_EXECUTION_BLOCKED;report['real_worker_task_overlap']=True;report['task_executions']=6;report['durable_checks']=len(json.loads((out/'source-refresh-workflow-private.json').read_text())['checks'])
  until=time.monotonic()+45
  while json.loads(execute('exec','-T','baseline','/dispatcharrpy/bin/python','-c',"import urllib.request;print(urllib.request.urlopen('http://origin:8080/stats',timeout=3).read().decode())"))['active']:
   if time.monotonic()>until:raise TimeoutError('UpstreamCleanup')
   time.sleep(.25)
  active(0);report['default_database_unchanged']=helper('dispatcharr','baseline_manifest')==before;assert report['default_database_unchanged'];report['passed']=all(v['complete']for v in report['census'].values())
 except ExpectedAbort:
  report['passed']=True;report['scope']='Integrated Original SQL-blocked worker +active playback abort/physical-stop contract only';report['default_database_post_abort']='Unavailable after acknowledged all-service stop; default DB not mutated by copied-DB controls'
 except Exception as error:
  report['error_type']=type(error).__name__;report['error_message']=str(error)
  if isinstance(error,subprocess.CalledProcessError):(out/'failure-private.log').write_bytes(error.stderr or b'')
 finally:
  for owned in [job,holdjob]:
   if owned is not None:
    try:
     if owned.poll()is None:owned.abort('ControllerFinallyAbort')
     owned.wait(timeout=10)
    except bounded.CollectionFailure:pass
  if joblog:joblog.close()
  if before is not None and not report.get('abort_all_four_containers_stopped'):
   try:report['default_database_unchanged']=helper('dispatcharr','baseline_manifest')==before
   except Exception:report['default_database_unchanged']=False
   if not report['default_database_unchanged']:report['passed']=False
  (out/'compose-private.log').write_bytes(execute('logs','--no-color','--tail','200',timeout=30))
  execute('down','--volumes','--remove-orphans','--timeout','10',timeout=120);report['cleanup']={k:command(['docker']+args+['--filter','label=com.docker.compose.project='+report['project']]).decode().strip()for k,args in {'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}.items()};assert not any(report['cleanup'].values());save(out/'integration.json',report)
 print(json.dumps({'passed':report['passed'],'error_type':report.get('error_type'),'output':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
