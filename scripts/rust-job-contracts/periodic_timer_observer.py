"""Read-only publication observer for the actual Original Beat process.
Original Dispatcharr/Celery authors retain credit/licenses.
"""
import json, os
from celery.signals import before_task_publish
from periodic_worker_observer import app
if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Owned fixture required')
@before_task_publish.connect
def publication(sender=None,body=None,headers=None,properties=None,exchange=None,routing_key=None,**kwargs):
    value={'task':sender,'body':body,'headers':headers,'properties':properties,'exchange':exchange,'routing_key':routing_key}
    raw=(json.dumps(value,default=str)+'\n').encode()
    if len(raw)>65536:raise RuntimeError('Publication observation bound')
    fd=os.open('/evidence/periodic-timer-publications.jsonl',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
    try:os.write(fd,raw)
    finally:os.close(fd)
