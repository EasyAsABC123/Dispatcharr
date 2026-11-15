"""Pinned Celery RedisBackend and redis-py PubSub over native-owned I/O.
Actual dependency codecs/exception/chord/TTL/PUBLISH/handler behavior remain
pinned library code. One native shared subscriber owns the real socket; logical
read callbacks can block without blocking another callback or opening a socket.
Original Celery/redis-py/Dispatcharr authors retain all algorithm credit.
"""
import base64,importlib,math
from redis.client import PubSub as OriginalPubSub
from .redis_transport import NativeRedis,reply
from .wire import current,CompatibilityUnavailable

class NativePubSub(OriginalPubSub):
 def __init__(self,client,*args,**kwargs):
  self._native_client=client;self._native_id=None
  client._native_validate()
  super().__init__(client.connection_pool,*args,**kwargs)
 def _open(self):
  if self._native_id is None:
   result=current().call('jobs_backend','pubsub_open',{'ignore_subscribe_messages':bool(self.ignore_subscribe_messages)})
   ident=result.get('id')if isinstance(result,dict)else None
   if not isinstance(ident,str)or not ident or len(ident)>128:raise CompatibilityUnavailable('Actual logical PubSub receipt missing')
   self._native_id=ident
  return self._native_id
 def execute_command(self,*args):
  if not args:raise CompatibilityUnavailable('Missing PubSub command')
  command=self.encoder.encode(args[0]).upper()
  # Pattern/sharded/PING semantics require additional actual protocol frames.
  # They are refused before mutation, never fabricated as successful subscribe.
  if command not in(b'SUBSCRIBE',b'UNSUBSCRIBE'):raise CompatibilityUnavailable('Unsupported native PubSub operation')
  encoded=[base64.b64encode(self.encoder.encode(v)).decode('ascii')for v in args]
  return current().call('jobs_backend','pubsub_command',{'id':self._open(),'args':encoded})
 def parse_response(self,block=True,timeout=0):
  if self._native_id is None:raise RuntimeError('pubsub connection not set: did you forget to call subscribe()?')
  timeout=None if block else float(timeout or 0)
  if timeout is not None and(not math.isfinite(timeout)or timeout<0):raise ValueError('PubSub timeout must be finite and nonnegative')
  result=current().call('jobs_backend','pubsub_read',{'id':self._native_id,'timeout':timeout})
  if result is None:return None
  value=reply(result,self.encoder)
  if isinstance(value,Exception):raise value
  return value
 def reset(self):
  ident=self._native_id;self._native_id=None
  try:
   if ident is not None:current().call('jobs_backend','pubsub_close',{'id':ident})
  finally:super().reset()
 def close(self):self.reset()

class NativeBackendRedis(NativeRedis):
 _native_port='jobs_backend'
 def pubsub(self,*args,**kwargs):return NativePubSub(self,*args,**kwargs)

def install():
 module=importlib.import_module('celery.backends.redis');original=module.RedisBackend
 class NativeRedisBackend(original):
  def _get_client(self):return NativeBackendRedis
 module.RedisBackend=NativeRedisBackend
