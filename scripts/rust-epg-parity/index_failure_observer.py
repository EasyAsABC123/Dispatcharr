"""Owned dependency faults only; original task, locks and Celery tracing unchanged.
Original Dispatcharr/Celery/Redis authors retain all application/transport credit.
"""
import json,os
from pathlib import Path
import index_worker_observer
from index_worker_observer import app,TARGET
from celery.signals import task_prerun,task_postrun
from core.utils import RedisClient
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
_saved=None
_saved_url=None
_saved_pool=None
class ReleaseNetworkFault:
 def __init__(self,real,closed):self.real=real;self.closed=closed
 def __getattr__(self,name):return getattr(self.real,name)
 def delete(self,*args,**kwargs):return self.closed.delete(*args,**kwargs)
@task_prerun.connect
def install_dependency_fault(task=None,args=None,**unused):
 global _saved,_saved_url,_saved_pool
 if task.name!=TARGET:return
 plan=json.loads(Path('/evidence/index-failure-plan.json').read_text());mode=plan['sources'].get(str(args[0]))
 if not mode:return
 _saved=RedisClient.get_client()
 if mode.startswith('cache-init-'):
  _saved_url=('REDIS_URL' in os.environ,os.environ.get('REDIS_URL'))
  os.environ['REDIS_URL']='redis://127.0.0.1:'+str(plan['closed_port'])+'/0'+('?malformed-query'if mode=='cache-init-url'else'')
  RedisClient._client=None
  return
 if mode=='busy-defer-publish':
  from kombu import Connection
  from kombu.pools import producers
  _saved_pool=(app.amqp._producer_pool,)
  app.amqp._producer_pool=producers[Connection('redis://127.0.0.1:'+str(plan['closed_port'])+'/8',connect_timeout=.2)]
  return
 if mode=='cache-auth':
  fault=redis.Redis(host='127.0.0.1',port=6379,password='owned-deliberately-wrong-password',db=0,socket_timeout=.2,socket_connect_timeout=.2,retry=Retry(NoBackoff(),0))
 else:
  fault=redis.Redis(host='127.0.0.1',port=plan['closed_port'],db=0,socket_timeout=.2,socket_connect_timeout=.2,retry=Retry(NoBackoff(),0))
 RedisClient._client=ReleaseNetworkFault(_saved,fault)if mode=='release-network'else fault
@task_postrun.connect
def restore_dependency(task=None,**unused):
 global _saved,_saved_url,_saved_pool
 if task.name==TARGET and _saved is not None:
  RedisClient._client=_saved;_saved=None
  if _saved_url is not None:
   present,value=_saved_url
   if present:os.environ['REDIS_URL']=value
   else:os.environ.pop('REDIS_URL',None)
   _saved_url=None
  if _saved_pool is not None:
   app.amqp._producer_pool=_saved_pool[0];_saved_pool=None
