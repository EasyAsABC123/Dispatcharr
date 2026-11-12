"""Read-only three-family observer; Original Dispatcharr/Celery authors retain credit.
No task replacement or descendant routing override.
"""
import fcntl,json,os,time
from pathlib import Path
from celery.signals import task_prerun,task_postrun,after_task_publish,worker_ready
from dispatcharr.celery import app
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
ROOT=Path(os.environ.get('M0_OBSERVER_EVIDENCE_ROOT','/evidence'))
if str(ROOT)not in ['/evidence','/evidence/django-overflow']:raise RuntimeError('Owned observer root')
ROOT.mkdir(exist_ok=True)
ALLOWED={'apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules'}
def append(kind,value):
 value.update(monotonic=time.monotonic(),wall=time.time());data=(json.dumps(value,default=str)+'\n').encode()
 # All normal prefork children share one lock and one aggregate reservation.
 # A signal receiver exception alone is not a reliable failure witness in Celery.
 lock=os.open(str(ROOT/'worker-observer-budget.json'),os.O_RDWR|os.O_CREAT,0o600)
 try:
  fcntl.flock(lock,fcntl.LOCK_EX);raw=os.read(lock,256)
  budget=json.loads(raw)if raw else {'records':0,'bytes':0}
  if len(data)>65536 or budget['records']>=1024 or budget['bytes']+len(data)>8388608:
   (ROOT/'worker-observer-capacity.json').write_text(json.dumps({'capacity_exceeded':True}))
   raise RuntimeError('Observer bound')
  fd=os.open(str(ROOT/('workflow-'+kind+'.jsonl')),os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
  try:
   if os.write(fd,data)!=len(data):raise RuntimeError('Observer short write')
  finally:os.close(fd)
  budget['records']+=1;budget['bytes']+=len(data)
  os.lseek(lock,0,os.SEEK_SET);os.ftruncate(lock,0);os.write(lock,json.dumps(budget).encode())
 finally:os.close(lock)
@task_prerun.connect
def before(task_id=None,task=None,args=None,kwargs=None,**kw):
 append('prerun',{'task':task.name,'id':task_id,'args':args,'kwargs':kwargs,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'uid':os.getuid(),'cwd':os.getcwd()})
 if task.name not in ALLOWED:raise RuntimeError('Unrelated fixture task')
@task_postrun.connect
def after(task_id=None,task=None,state=None,retval=None,**kw):append('postrun',{'task':task.name,'id':task_id,'state':state,'result':retval})
@after_task_publish.connect
def published(sender=None,headers=None,body=None,**kw):append('published',{'task':sender,'headers':headers,'body':body})

@worker_ready.connect
def ready(**kwargs):
 Path("/evidence/concurrent-worker-ready.json").write_text(json.dumps({"ready":True,"pid":os.getpid()}))
