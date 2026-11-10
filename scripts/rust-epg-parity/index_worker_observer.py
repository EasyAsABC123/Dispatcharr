"""Fixture-only observer loaded by the real original Celery worker.
Original Dispatcharr authors retain all application/algorithm credit.
No original task/function is replaced. Raw task/result IDs are private evidence.
"""
import datetime as dt,json,os,time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_'):raise RuntimeError('Owned copied fixture required')
import redis
from celery.signals import task_prerun,task_postrun,after_task_publish
from dispatcharr.celery import app
TARGET='apps.epg.tasks.build_programme_index_task'
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])
if broker.connection_pool.connection_kwargs['db']!=8:raise RuntimeError('Owned brokerDB8 required')
ROOT=Path('/evidence')
def append(name,value):
    data=(json.dumps(value,default=str,separators=(',',':'))+'\n').encode()
    if len(data)>262144:raise RuntimeError('Observer record cap')
    fd=os.open(ROOT/name,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
    try:os.write(fd,data)
    finally:os.close(fd)
@task_prerun.connect
def prerun(task_id=None,task=None,args=None,kwargs=None,**unused):
    if task.name!=TARGET:append('index-unexpected-tasks.jsonl',{'task_name':task.name});return
    request=task.request
    append('index-prerun.jsonl',{'id':task_id,'args':args,'kwargs':kwargs,'pid':os.getpid(),'cwd':os.getcwd(),'uid':os.getuid(),'root_id':request.root_id,'parent_id':request.parent_id,'retries':request.retries,'delivery_info':request.delivery_info,'utc':dt.datetime.now(dt.timezone.utc).isoformat()})
@task_postrun.connect
def postrun(task_id=None,task=None,retval=None,state=None,**unused):
    if task.name==TARGET:append('index-postrun.jsonl',{'id':task_id,'state':state,'result':retval,'pid':os.getpid()})
@after_task_publish.connect
def published(sender=None,headers=None,**unused):
    if sender!=TARGET:return
    ident=headers['id'];deadline=time.monotonic()+2;found=None
    while time.monotonic()<deadline:
        queued=broker.lrange('celery',0,63);reserved=broker.hvals('unacked')
        if len(reserved)>64:raise RuntimeError('Owned reservation cap')
        for raw in queued:
            if len(raw)>65536:raise RuntimeError('Envelope cap')
            item=json.loads(raw)
            if item['headers']['id']==ident:found={'location':'queued','envelope':item};break
        if found is None:
            for raw in reserved:
                if len(raw)>131072:raise RuntimeError('Reservation cap')
                item=json.loads(raw)[0]
                if item['headers']['id']==ident:found={'location':'reserved','envelope':item};break
        if found is not None:break
        time.sleep(.005)
    append('index-child-envelopes.jsonl',{'id':ident,'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'observed':found})
