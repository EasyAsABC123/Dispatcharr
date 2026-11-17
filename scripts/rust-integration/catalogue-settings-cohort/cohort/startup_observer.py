"""Owned metadata-only startup MONITOR using the existing bounded journal.
No application import, task publication or production function modification.
Original Dispatcharr/Celery/Kombu authors retain all algorithm credit.
"""
import json,os,signal,sys,time
from pathlib import Path
sys.path.insert(0,'/normal-dvr-tools/channel_tools')
import redis
from journal import Journal
root=Path(os.environ['CHANNEL_OBSERVER_ROOT']);deadline=float(os.environ['CHANNEL_FUNCTIONAL_DEADLINE']);stop=False;failure=None
TASKS={'apps.plugins.tasks.refresh_plugin_repos','apps.channels.tasks.recover_recordings_on_startup','core.tasks.check_for_version_update'}
def save(name,value):
 target=root/name;pending=target.with_suffix('.pending');pending.write_text(json.dumps(value));pending.replace(target)
def term(*unused):
 global stop
 stop=True
signal.signal(signal.SIGTERM,term)
client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=1)
database=os.environ.get('CHANNEL_OBSERVER_DATABASE')
if database is not None and database!='1':raise ValueError('OnlyExplicitBrokerDatabase1Supported')
journal=Journal(client,'private-startup-monitor.jsonl','plugin-startup-terminal',database=None if database is None else 1)
save('ready.json',{'actual_MONITOR_subscribed':True,'database_filter':journal.database,'all_databases_recorded':journal.database is None,'actual_euid':os.geteuid(),'actual_egid':os.getegid(),'pid':os.getpid(),'generation':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]})
try:
 while not stop:
  if journal.invalid:raise RuntimeError(journal.invalid)
  if time.monotonic()>=deadline:raise TimeoutError('Shared functional deadline')
  frames=[]
  for row in list(journal.rows):
   command=row.get('command','')
   parts=command.split(' ',2)
   if row.get('db')!=1 or len(parts)!=3 or parts[0].upper()not in('LPUSH','RPUSH')or parts[1].lower()not in('celery','dvr'):continue
   try:
    frame=json.loads(command.split(' ',2)[2]);frame=json.loads(frame)if isinstance(frame,str)else frame
   except(ValueError,TypeError)as error:raise RuntimeError('MalformedActualStartupBrokerFrame')from error
   frames.append({'frame':frame,'actual_redis_time':row['time'],'client_address':row.get('client_address'),'queue':parts[1]})
  if len(frames)>32:raise ValueError('Startup32Frames')
  save('snapshot.json',{'frames':frames,'actual_rows':len(journal.rows),'actual_monotonic':time.monotonic(),'journal_error':journal.invalid})
  time.sleep(min(.025,max(.001,deadline-time.monotonic())))
except BaseException as error:failure={'type':type(error).__name__,'message':str(error)}
finally:
 receipt=journal.close();receipt['observer_error']=failure;save('final.json',receipt);client.close()
raise SystemExit(0 if failure is None and receipt['thread_settled']and receipt['terminal_barrier_observed']and receipt['writer_closed']and not receipt['terminal_reader_error']else 1)
