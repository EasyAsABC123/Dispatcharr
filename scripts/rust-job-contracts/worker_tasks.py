"""Neutral transport probes attached to the actual pinned Dispatcharr Celery app.
They never perform application writes/network I/O. Not production task ports.
Original Dispatcharr authors retain application credit.
"""
import os,time
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned job fixture required')
from dispatcharr.celery import app
import redis
control=redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])

def execute(task,kind,marker):
    if not marker.startswith('rust-probe-job:') or len(marker)>150:raise ValueError('Owned marker required')
    control.hset(marker,mapping={'started':time.time(),'pid':os.getpid(),'retries':task.request.retries})
    if kind=='retry'and task.request.retries==0:raise task.retry(exc=ValueError('fixture retry'),countdown=3,max_retries=1)
    if kind=='uncaught':raise ValueError('fixture uncaught')
    if kind=='scalar':return 'fixture scalar'
    if kind=='list':return [1,2]
    if kind=='null':return None
    if kind=='empty':return {}
    if kind=='failed-dict':return {'status':'failed','error':'fixture returned failure'}
    if kind=='null-error':return {'status':'failed','error':None}
    if kind=='block':
        deadline=time.monotonic()+40
        while not control.hget(marker,'release'):
            if time.monotonic()>deadline:raise TimeoutError('fixture release deadline')
            time.sleep(.05)
    return {'status':'completed','fixture':True}

@app.task(bind=True,name='rust_probe.job_default')
def default(self,kind,marker):return execute(self,kind,marker)

@app.task(bind=True,name='rust_probe.job_started',track_started=True)
def started(self,kind,marker):return execute(self,kind,marker)
