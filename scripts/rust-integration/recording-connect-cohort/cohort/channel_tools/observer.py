"""Owned bounded Redis observer/advisory-lock peer; no Original app/task import."""
import json,os,signal,time
from pathlib import Path
import redis,psycopg
from journal import Journal
root=Path(os.environ['CHANNEL_OBSERVER_ROOT']);deadline=float(os.environ['CHANNEL_FUNCTIONAL_DEADLINE']);stop=False;sql=None;held=False;handled=set();failure=None

def save(name,value):
 p=root/name;t=p.with_suffix(p.suffix+'.pending');t.write_text(json.dumps(value,indent=2)+'\n');t.replace(p)
def term(*_):
 global stop
 stop=True
signal.signal(signal.SIGTERM,term)
client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=1)
journal=Journal(client,'redis-monitor-private.jsonl','channel-acceptance-terminal')
from channels_redis.core import RedisChannelLayer
from asgiref.sync import async_to_sync
layer=RedisChannelLayer(hosts=['redis://:playback-fixture-only@127.0.0.1:6379/0']);channel=os.environ['CHANNEL_RECEIVER'];async_to_sync(layer.group_add)('updates',channel)
save('observer-ready.json',{'actual_MONITOR_subscribed':True,'receiver':channel,'actual_group_add_completed':True,'Original_application_imported':False,'PID':os.getpid(),'generation':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]})
try:
 while not stop:
  if journal.invalid:raise RuntimeError(journal.invalid)
  if time.monotonic()>deadline:raise TimeoutError('Shared functional deadline')
  for command_path in sorted(root.glob('control-*.json')):
   if command_path.name in handled or command_path.stem.endswith('-receipt'):continue
   data=json.loads(command_path.read_text());action=data['action'];answer={'action':action,'actual':False}
   if action=='hold_sql':
    if sql is not None:raise RuntimeError('OnlyOneOwnedSQLGate')
    sql=psycopg.connect(dbname=os.environ['POSTGRES_DB'],user='dispatch',password='playback-fixture-only',host='127.0.0.1',autocommit=True)
    sql.execute('SELECT pg_advisory_lock(87310472)');held=True;answer.update(actual=True,PID=sql.info.backend_pid)
   elif action=='frame_mark':
    answer.update(actual=True,index=len(journal.rows))
   elif action=='frames_since':
    index=data['index']
    if type(index)is not int or not 0<=index<=len(journal.rows):raise ValueError('ActualJournalIndexRequired')
    marker='channel-live-publication-barrier-'+str(time.monotonic_ns());client.echo(marker);until=min(deadline,time.monotonic()+5)
    while not any(r.get('command')=='ECHO '+marker for r in journal.rows[index:]):
     if journal.invalid:raise RuntimeError(journal.invalid)
     if time.monotonic()>=until:raise TimeoutError('ActualPublicationMonitorBarrier')
     time.sleep(.001)
    frames=[]
    for row in list(journal.rows[index:]):
     monitor_command=row.get('command','')
     if row.get('db')!=1 or not monitor_command.upper().startswith('LPUSH CELERY '):continue
     frame=json.loads(monitor_command.split(' ',2)[2])
     if isinstance(frame,str):frame=json.loads(frame)
     if frame.get('headers',{}).get('task','').startswith('apps.channels.tasks.'):frames.append({'frame':frame,'monitor_row':row})
    if len(frames)>128 or len(json.dumps(frames))>1048576:raise ValueError('PublicationObserverBounds')
    answer.update(actual=True,frames=frames)
   elif action=='release_sql':
    if sql is None or not held:raise RuntimeError('NoActualSQLGateHeld')
    answer['actual']=sql.execute('SELECT pg_advisory_unlock(87310472)').fetchone()[0];held=False;sql.close();sql=None;answer['actual_connection_closed']=True
   else:raise ValueError('DeclaredOwnedObserverActionsOnly')
   save(command_path.stem+'-receipt.json',answer);handled.add(command_path.name)
  time.sleep(min(.025,max(.001,deadline-time.monotonic())))
except BaseException as error:failure={'type':type(error).__name__,'message':str(error)}
finally:
 if sql is not None:sql.close();sql=None
 receipt=journal.close();receipt.update(observer_error=failure,owned_SQL_connection_closed=sql is None,actual_finished_monotonic=time.monotonic());save('observer-final.json',receipt);client.close()
raise SystemExit(0 if failure is None and receipt['thread_settled']and receipt['terminal_barrier_observed']and receipt['writer_closed']and not receipt['terminal_reader_error']else 1)
