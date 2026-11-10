"""Fixture-only cached-client delete dependency, Original task/tracing unchanged.
Original Dispatcharr/Celery/redis-py authors retain application/framework credit.
"""
import json
from pathlib import Path
import index_worker_observer
from index_worker_observer import app,TARGET
from celery.signals import task_prerun,task_postrun
from core.utils import RedisClient
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
_saved=None
class DeleteOnly:
 def __init__(self,real,release):self.real=real;self.release=release
 def __getattr__(self,name):return getattr(self.real,name)
 def delete(self,*args,**kwargs):return self.release.delete(*args,**kwargs)
@task_prerun.connect
def install(task=None,args=None,**unused):
 global _saved
 if task.name!=TARGET:return
 plan=json.loads(Path('/evidence/double-failure-plan.json').read_text());mode=plan['sources'].get(str(args[0]))
 if mode is None:return
 _saved=RedisClient.get_client()
 if mode['release']=='network':
  release=redis.Redis(host='127.0.0.1',port=plan['closed_port'],db=0,socket_timeout=.2,socket_connect_timeout=.2,retry=Retry(NoBackoff(),0))
 elif mode['release']=='noperm':
  release=redis.Redis(host='127.0.0.1',port=6379,username=mode['acl_user'],password='playback-fixture-only',db=0,socket_timeout=.2,socket_connect_timeout=.2,retry=Retry(NoBackoff(),0))
 else:raise RuntimeError('Unknown owned release fault')
 RedisClient._client=DeleteOnly(_saved,release)
@task_postrun.connect
def restore(task=None,**unused):
 global _saved
 if task.name==TARGET and _saved is not None:RedisClient._client=_saved;_saved=None
