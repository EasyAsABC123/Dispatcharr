"""Actual startup root/publication/terminal capture before the business observer.
Raw Redis/SQL/HTTP only; no Original application/task/API import or replay.
"""
import base64,datetime,hashlib,json,os,sys,threading,time,urllib.request,urllib.error,uuid
from pathlib import Path
import psycopg,redis
from journal import Journal
ROOT=Path('/evidence');MAX=16777216

def save(name,value):
 p=ROOT/name;t=p.with_suffix(p.suffix+'.pending');t.write_text(json.dumps(value,indent=2,allow_nan=False)+'\n');t.replace(p)
def need(v,label):
 if not v:raise ValueError(label)
def sql_snapshot():
 c=psycopg.connect(host='127.0.0.1',user='dispatch',password='playback-fixture-only',dbname=os.environ['POSTGRES_DB'],options='-c default_transaction_read_only=on -c statement_timeout=5000')
 tables={}
 try:
  with c.cursor()as q:
   for table in ['dispatcharr_channels_recording','django_celery_beat_periodictask','django_celery_beat_clockedschedule','core_systemnotification','core_coresettings']:
    q.execute('SELECT to_jsonb(t) FROM '+table+' t ORDER BY id LIMIT 4097');rows=[r[0]for r in q.fetchall()];need(len(rows)<=4096 and len(json.dumps(rows).encode())<=2097152,'BoundedStartupSQL');tables[table]=rows
 finally:c.close()
 return tables

def cache_snapshot(cache):
 keys=sorted(cache.scan_iter());need(len(keys)<=128,'StartupCache128Keys');rows=[];total=0
 for key in keys:
  raw=cache.dump(key);need(raw is None or len(raw)<=65536,'StartupCacheValue64KiB');total+=0 if raw is None else len(raw);need(total<=1048576,'StartupCache1MiB');rows.append({'key_base64':base64.b64encode(key).decode(),'type':cache.type(key).decode(),'dump_base64':None if raw is None else base64.b64encode(raw).decode(),'ttl_ms':cache.pttl(key)})
 return rows

def main():
 need(os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_'),'OwnedCloneOnly');began=time.time();deadline=time.monotonic()+70;broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis.from_url(os.environ['REDIS_URL']);journal=Journal(broker,'startup-monitor.jsonl','startup-terminal');rows=journal.rows;report={'passed':False,'Source_API_or_task_replay':False,'Original_application_imports':0,'began_epoch':began,'HTTP':[],'root_frames':{},'backend':{},'before_SQL':sql_snapshot(),'before_cache':cache_snapshot(cache),'before_backend_keys':[base64.b64encode(k).decode()for k in sorted(backend.scan_iter(match='celery-tasks:celery-task-meta-*'))]};save('startup-capture.json',report)
 pid=os.getpid();save('startup-observer-ready.json',{'PID':pid,'start_generation':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'PID_namespace':os.readlink('/proc/self/ns/pid'),'actual_monitor_subscribed':True,'Source_API_or_task_replay':False})
 try:
  wanted={'apps.channels.tasks.recover_recordings_on_startup','core.tasks.check_for_version_update'}
  while time.monotonic()<deadline:
   need(journal.invalid is None,'StartupMonitorNotFailed')
   frames={}
   for row in list(rows):
    command=row.get('command','')
    if command.upper().startswith('LPUSH CELERY '):
     value=json.loads(command.split(' ',2)[2]);task=value['headers']['task'];ident=value['headers']['id'];need(task in wanted,'UnexpectedStartupTaskExplicitFailure:'+task);frames[ident]={'raw_frame':value,'monitor_row':row}
   report['root_frames']=frames;report['backend']={}
   for ident in frames:
    raw=backend.get('celery-tasks:celery-task-meta-'+ident)
    if raw is not None:need(len(raw)<=1048576,'StartupBackend1MiB');report['backend'][ident]={'raw_base64':base64.b64encode(raw).decode(),'metadata':json.loads(raw),'ttl':backend.ttl('celery-tasks:celery-task-meta-'+ident)}
   try:
    try:response=urllib.request.urlopen('http://127.0.0.1:9192/health/ready',timeout=.4)
    except urllib.error.HTTPError as error:response=error
    with response:
     raw=response.read(65537);need(len(raw)<=65536,'StartupHealth64KiB');http={'status':response.status,'body_base64':base64.b64encode(raw).decode(),'body_sha256':hashlib.sha256(raw).hexdigest(),'body_bytes':len(raw)};report['HTTP'].append(http);health=json.loads(raw)
   except (ConnectionError,TimeoutError,OSError):health={}
   ready=health.get('selected_workers',{});terminal=len(frames)==2 and {v['raw_frame']['headers']['task']for v in frames.values()}==wanted and len(report['backend'])==2 and all(v['metadata'].get('status')in ['SUCCESS','FAILURE']for v in report['backend'].values())
   save('startup-capture.json',report)
   if terminal and ready.get('ready')is True and ready.get('startup')=='Started'and ready.get('clocked_scheduler_enabled')is True:break
   time.sleep(.025)
  else:raise ValueError('StartupRootsAndSelectedReadiness70s')
  need(all(v['metadata']['status']=='SUCCESS'for v in report['backend'].values()),'ActualStartupSuccessBackends')
  for ident,value in report['backend'].items():
   meta=value['metadata'];frame=frames[ident]['raw_frame'];need(meta['task_id']==ident and meta.get('parent_id')is None and meta['traceback']is None and meta['children']==[],'ActualStartupRootIdentityChildren')
   if frame['headers']['task']=='apps.channels.tasks.recover_recordings_on_startup':need(meta['result']=='Recovery complete','ActualRecoveryTerminalResult')
   else:need(meta['result']is None,'ActualStartupNoUpdateVersionResult')
  report['root_ids']=list(frames);report['selected_readiness']=ready;report['after_SQL']=sql_snapshot();report['after_cache']=cache_snapshot(cache);report['after_backend_keys']=[base64.b64encode(k).decode()for k in sorted(backend.scan_iter(match='celery-tasks:celery-task-meta-*'))];report['origin_requests_before_business']=[json.loads(line)for line in(ROOT/'origin-requests.jsonl').read_text().splitlines()];need(len(report['origin_requests_before_business'])==1 and report['origin_requests_before_business'][0]['status']==200 and report['origin_requests_before_business'][0]['completed'],'ActualStartupVersionNoUpdateContact');need(not broker.lrange('celery',0,-1)and not broker.lrange('dvr',0,-1),'ActualStartupQueuesSettled')
  report['passed']=True;report['completed_epoch']=time.time()
 except BaseException as error:report['error']={'type':type(error).__name__,'message':str(error)}
 finally:
  receipt=journal.close();report['monitor_receipt']=receipt;report['terminal_monitor_marker']=receipt['terminal_marker'];report['terminal_monitor_marker_observed']=receipt['terminal_barrier_observed'];report['monitor_thread_settled']=receipt['thread_settled'];report['monitor_error']=receipt['terminal_reader_error'];report['actual_monitor_rows']=receipt['actual_rows'];report['monitor_sha256']=hashlib.sha256((ROOT/'startup-monitor.jsonl').read_bytes()).hexdigest();broker.close();backend.close();cache.close();save('startup-capture.json',report)
 return 0 if report['passed']and report['terminal_monitor_marker_observed']and report['monitor_thread_settled']and report['monitor_error']is None else 2
if __name__=='__main__':raise SystemExit(main())
