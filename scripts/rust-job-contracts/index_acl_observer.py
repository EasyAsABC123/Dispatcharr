"""Owned cached-client ACL dependency, Original tasks/tracing unchanged.
Original Dispatcharr/Redis/redis-py authors retain credit and original licenses.
"""
import json
from pathlib import Path
import index_worker_observer
from index_worker_observer import app,TARGET
from celery.signals import task_prerun,task_postrun
from core.utils import RedisClient
import redis
_saved=None
@task_prerun.connect
def install(task=None,args=None,**unused):
 global _saved
 if task.name!=TARGET:return
 plan=json.loads(Path('/evidence/cache-acl-plan.json').read_text());entry=plan['sources'].get(str(args[0]))
 if entry is None:return
 _saved=RedisClient.get_client();RedisClient._client=redis.Redis(host='127.0.0.1',port=6379,username=entry['username'],password='playback-fixture-only',db=0,socket_connect_timeout=.2,socket_timeout=.2)
@task_postrun.connect
def restore(task=None,**unused):
 global _saved
 if task.name==TARGET and _saved is not None:
  fault=RedisClient._client;RedisClient._client=_saved;_saved=None;fault.close()
