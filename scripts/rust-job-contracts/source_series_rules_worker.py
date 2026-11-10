"""Actual pinned Original SeriesRules child worker, no-recording cases only.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
No DVR/recording/network task is delegated or executed by this fixture.
"""
import base64,hashlib,inspect,json,os,pickle,signal,subprocess,sys,time,uuid
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
database=os.environ.get('POSTGRES_DB','')
if not database.startswith('rust_probe_backup_jobs_') or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):raise RuntimeError('Copied database required')
sys.path[:0]=['/app','/job-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from django.db import connection,transaction
from django.core.cache import cache as django_cache
from core.models import CoreSettings
from apps.channels.models import Recording
from apps.channels import tasks
from dispatcharr.celery import app
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=0)
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
task=tasks.evaluate_series_rules;lock='task_lock_evaluate_series_rules_all';worker=None
report={'core_settings_source':{name:inspect.getsource(getattr(CoreSettings,name)) for name in ['get_dvr_settings','get_dvr_series_rules','_get_group','_load_group_value']},'passed':False,'checks':[],'cases':[],'fixture_control':'Only absent case: bounded observer task_prerun timing gate precedes real blocked settings SQL; no application function replaced','scope':'Original worker no-recording SeriesRules settings/lock/return/envelope; no native consumer or recording execution','task':task.name,'attributes':{key:getattr(task,key) for key in ['acks_late','ignore_result','track_started','reject_on_worker_lost','max_retries']},'source':{name:inspect.getsource(getattr(tasks,name)) for name in ['evaluate_series_rules_impl','_evaluate_series_rules_locked']}}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def wait(fn,seconds=35):
 end=time.monotonic()+seconds
 while time.monotonic()<end:
  value=fn()
  if value:return value
  time.sleep(.01)
 raise TimeoutError('Owned SeriesRules observation deadline')
def rows(name):
 path=Path('/evidence')/name
 return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []
def stop():
 global worker
 if worker is not None and worker.poll() is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=12)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report.setdefault('forced_worker_cleanup',[]).append(True)
 worker=None
def launch(hold=False):
 global worker
 for raw in broker.lrange('celery',0,63):
  envelope=json.loads(raw)
  if envelope['headers']['task']!=task.name:
   report.setdefault('quarantined_startup_families',[]).append(envelope['headers']['task']);broker.lrem('celery',0,raw)
 with Path('/evidence/series-worker-private.log').open('ab') as log:
  worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','series_rules_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-series@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app',SERIES_RULES_PRERUN_HOLD='1' if hold else '0'),stdout=log,stderr=log,start_new_session=True)
 wait(lambda:broker.exists('_kombu.binding.celery.pidbox'))
try:
 before=list(Recording.objects.order_by('id').values())
 if len(before)>256:raise RuntimeError('Recording fixture bound')
 for label,group in [('absent',None),('empty-map',{}),('empty-list',{'series_rules':[]}),('nonlist',{'series_rules':'not-list'}),('nonmap-elements',{'series_rules':[0,None,'owned',[]]}),('busy',{'series_rules':[{'title':'Never execute this rule'}]}),('parented-empty',{'series_rules':[]})]:
  broker.flushdb();backend.flushdb();cache.delete(lock)
  CoreSettings.objects.filter(key='dvr_settings').delete()
  if group is not None:CoreSettings.objects.create(key='dvr_settings',name='Owned SeriesRules fixture',value=group)
  django_cache.delete(CoreSettings.group_cache_key('dvr_settings'))
  django_cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
  if label=='busy':cache.set(lock,'owned-busy',ex=300)
  gate=Path("/evidence/series-prerun-release");gate.unlink(missing_ok=True)
  options={}
  if label=='parented-empty':options={'root_id':str(uuid.uuid4()),'parent_id':str(uuid.uuid4())}
  early=None
  issued=task.apply_async(args=[],**options)
  envelope=json.loads(next(raw for raw in broker.lrange('celery',0,63) if json.loads(raw)['headers']['id']==issued.id))
  launch(hold=label=='absent')
  if label=='absent':
   wait(lambda:any(row['id']==issued.id for row in rows('series-rules-gate-entered.jsonl')))
   django_cache.delete(CoreSettings.group_cache_key('dvr_settings'))
   with transaction.atomic():
    with connection.cursor() as cursor:cursor.execute('LOCK TABLE core_coresettings IN ACCESS EXCLUSIVE MODE')
    gate.write_text('release owned Source observer timing gate')
    def blocked():
     with connection.cursor() as cursor:
      cursor.execute('SELECT pg_stat_clear_snapshot()')
      cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%core_coresettings%' AND pid<>pg_backend_pid()")
      return cursor.fetchone()[0]>0
    wait(blocked)
    early={'result_absent':backend.get(app.backend.get_key_for_task(issued.id)) is None,'unacked':broker.hlen('unacked'),'queue':broker.llen('celery'),'lock':cache.get(lock).decode() if cache.get(lock) else None}
    check('actual-earlyACK-blocked-settings-read',early['result_absent'] and early['unacked']==0 and early['queue']==0 and early['lock']=='locked')
  ident=issued.id
  post=wait(lambda:next((row for row in rows('series-rules-postrun.jsonl') if row['id']==ident),None));stop()
  raw=backend.get(app.backend.get_key_for_task(ident));check(label+'-bounded-result',raw is not None and len(raw)<=262144);metadata=json.loads(raw)
  expected={'scheduled':0,'details':[{'status':'skipped','reason':'concurrent evaluation in progress'}]} if label=='busy' else {'scheduled':0,'details':[]}
  check(label+'-actual-normal-return',post['state']=='SUCCESS' and metadata['status']=='SUCCESS' and app.AsyncResult(ident).get(timeout=2)==expected)
  check(label+'-result-shape',metadata['result']==expected and metadata['traceback'] is None and metadata['children']==[])
  check(label+'-expiry',3580<=backend.ttl(app.backend.get_key_for_task(ident))<=3600)
  check(label+'-lock-effect',cache.get(lock)==(b'owned-busy' if label=='busy' else None))
  check(label+'-no-recording-effects',list(Recording.objects.order_by('id').values())==before)
  check(label+'-no-children',broker.llen('celery')==0 and broker.hlen('unacked')==0)
  if options:check('parented-result-lineage',metadata.get('parent_id')==options['parent_id'])
  body=json.loads(base64.b64decode(envelope['body'],validate=True))
  check(label+'-actual-empty-call',body[0]==[] and body[1]=={} and envelope['headers']['task']==task.name)
  raw_group=cache.get(':1:'+CoreSettings.group_cache_key('dvr_settings'))
  check(label+'-bounded-shared-cache',raw_group is None or len(raw_group)<=65536)
  cached_group=pickle.loads(raw_group) if raw_group is not None else None
  if label=='busy':check('busy-skips-DVR-group-read',cached_group is None)
  else:
   check(label+'-shared-Django-group-fill',isinstance(cached_group,dict) and 285<=cache.ttl(':1:'+CoreSettings.group_cache_key('dvr_settings'))<=300)
   if label in ['absent','empty-map']:check(label+'-full-default-map',len(cached_group)==13 and cached_group['series_rules']==[] and cached_group['output_profile_id'] is None)
   else:check(label+'-no-default-overlay',cached_group==group)
  report['cases'].append({'name':label,'group':group,'cached_group':cached_group,'cache_pickle_base64':base64.b64encode(raw_group).decode() if raw_group is not None else None,'envelope':envelope,'result':metadata,'postrun':post,'early':early,'cache_group_ttl':cache.ttl(':1:'+CoreSettings.group_cache_key('dvr_settings'))})
 check('no-unexpected-executed-family',not rows('series-rules-unexpected-tasks.jsonl'))
 report['passed']=True
finally:
 stop();cache.delete(lock);broker.flushdb();backend.flushdb()
 report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-series-rules-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
