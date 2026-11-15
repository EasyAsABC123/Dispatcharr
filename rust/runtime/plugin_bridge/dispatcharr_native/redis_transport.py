"""Actual redis-py6.4 command/response/codec API over shared native cache I/O.
No direct Redis socket is opened; original library response callbacks and Django
pickle/compression remain real code. Unsupported affinity/streaming is guarded.
"""
import base64,inspect
import redis
from redis._parsers.base import BaseParser
from redis.client import Redis as OriginalRedis,Pipeline as OriginalPipeline,NEVER_DECODE
from redis.exceptions import ConnectionError,TimeoutError
from .wire import current,CompatibilityUnavailable

def reply(value,encoder,decode=True):
 kind=value['kind']
 if kind=='nil':return None
 if kind=='int':return int(value['value'])
 if kind=='bytes':
  raw=base64.b64decode(value['value'],validate=True)
  return encoder.decode(raw)if decode else raw
 if kind=='array':return[reply(v,encoder,decode)for v in value['value']]
 if kind=='server_error':
  if value['value'].startswith('NATIVE_CAPACITY'):raise CompatibilityUnavailable('Native cache response capacity')
  return BaseParser.parse_error(value['value'])
 if kind=='authentication_error':return redis.exceptions.AuthenticationError(value['value'])
 if kind=='timeout':return TimeoutError(value['value'])
 if kind=='connection_error':return ConnectionError(value['value'])
 raise CompatibilityUnavailable('Native cache RESP variant unsupported')
class Response:
 def __init__(self,value,encoder):self.value=value;self.encoder=encoder
 def read_response(self,disable_decoding=False):
  value=reply(self.value,self.encoder,not disable_decoding)
  if isinstance(value,Exception):raise value
  return value

def commands(client,rows,transaction):
 client._native_validate()
 encoder=client.get_encoder();encoded=[]
 for args,_ in rows:
  values=[encoder.encode(v)for v in args]
  encoded.append([base64.b64encode(v).decode('ascii')for v in values])
 result=current().call(client._native_port,'commands',{'commands':encoded,'transaction':transaction})
 if'error'in result:
  error=reply(result['error'],encoder)
  if not isinstance(error,Exception):raise CompatibilityUnavailable('Native cache error missing actual error value')
  raise error
 return result

class NativeRedis(OriginalRedis):
 _native_port='cache'
 def __init__(self,*args,**kwargs):
  values=inspect.signature(OriginalRedis.__init__).bind_partial(self,*args,**kwargs).arguments
  if values.get('single_connection_client')or values.get('redis_connect_func')or values.get('credential_provider'):
   raise CompatibilityUnavailable('Native Redis custom/single-connection client is unimplemented')
  # This creates only the actual redis-py encoder/callback/pool DESCRIPTOR.
  # execute_command below never calls get_connection or the socket methods.
  super().__init__(*args,**kwargs);self._native_validated=False
 def _native_validate(self):
  if self._native_validated:return
  options=self.connection_pool.connection_kwargs;klass=self.connection_pool.connection_class
  if issubclass(klass,redis.connection.SSLConnection):transport='tls'
  elif issubclass(klass,redis.connection.UnixDomainSocketConnection):transport='unix'
  elif klass is redis.connection.Connection:transport='tcp'
  else:raise CompatibilityUnavailable('Native Redis custom connection class is unsupported')
  current().call(self._native_port,'validate_target',{'transport':transport,'host':options.get('host','localhost'),'port':int(options.get('port',6379)),'path':options.get('path'),'db':int(options.get('db',0)),'username':options.get('username'),'password':options.get('password'),'protocol':int(options.get('protocol',2))})
  self._native_validated=True
 def execute_command(self,*args,**options):
  if not args:raise redis.exceptions.RedisError('Missing Redis command')
  result=commands(self,[(args,options)],False)
  values=result.get('values')
  if not isinstance(values,list)or len(values)!=1:raise CompatibilityUnavailable('Native cache reply shape')
  return self.parse_response(Response(values[0],self.get_encoder()),args[0],**options)
 def pipeline(self,transaction=True,shard_hint=None):return NativePipeline(self,transaction,shard_hint)
 def pubsub(self,*args,**kwargs):raise CompatibilityUnavailable('Native plugin Redis PubSub ownership is not implemented')
 def monitor(self,*args,**kwargs):raise CompatibilityUnavailable('Native Redis MONITOR is not implemented')
class NativePipeline(OriginalPipeline):
 def __init__(self,client,transaction,shard_hint):
  super().__init__(client.connection_pool,client.response_callbacks,transaction,shard_hint);self._native_client=client
 def watch(self,*args):raise CompatibilityUnavailable('Native Redis WATCH connection affinity is not implemented')
 def immediate_execute_command(self,*args,**kwargs):return self._native_client.execute_command(*args,**kwargs)
 def execute(self,raise_on_error=True):
  stack=self.command_stack
  if not stack:return[]
  if self.watching:raise CompatibilityUnavailable('Native Redis watched pipeline ownership is unimplemented')
  try:
   if self.scripts:self.load_scripts()
   result=commands(self._native_client,stack,self.transaction or self.explicit_transaction)
   if result.get('queue_errors'):
    first=result['queue_errors'][0];error=reply(first['error'],self._native_client.get_encoder());self.annotate_exception(error,first['index']+1,stack[first['index']][0]);raise error
   values=result.get('values')
   if not isinstance(values,list)or len(values)!=len(stack):raise CompatibilityUnavailable('Native cache pipeline result count')
   decoded=[]
   for value,(args,options)in zip(values,stack):
    try:decoded.append(self._native_client.parse_response(Response(value,self._native_client.get_encoder()),args[0],**options))
    except redis.exceptions.ResponseError as error:decoded.append(error)
   if raise_on_error:self.raise_first_error(stack,decoded)
   return decoded
  finally:self.reset()

def install():
 # Install before Source clients import aliases; actual Source functions stay
 # unchanged. Low-level socket entrypoints fail rather than evade native quotas.
 redis.Redis=NativeRedis;redis.StrictRedis=NativeRedis;redis.client.Redis=NativeRedis
 def unsupported(*args,**kwargs):raise CompatibilityUnavailable('Direct Redis network transport is not owned by the native bridge')
 for klass in(redis.connection.Connection,redis.connection.SSLConnection,redis.connection.UnixDomainSocketConnection):
  klass.connect=unsupported
  if hasattr(klass,'connect_check_health'):klass.connect_check_health=unsupported
 # Async consumers/subscriptions need their own actual owned stream port.
 # Ordinary Source group_send already uses the shared native Channels adapter.
 import importlib
 async_redis=importlib.import_module("redis.asyncio");async_client=importlib.import_module("redis.asyncio.client")
 class UnsupportedAsync:
  def __init__(self,*args,**kwargs):unsupported()
  @classmethod
  def from_url(cls,*args,**kwargs):unsupported()
 async_redis.Redis=UnsupportedAsync;async_redis.StrictRedis=UnsupportedAsync;async_client.Redis=UnsupportedAsync
