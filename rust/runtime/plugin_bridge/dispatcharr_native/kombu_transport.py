"""Actual Kombu virtual framing over the SAME native Jobs broker/lease.
No Source worker/task body, network client, broad prefix admission or fake ACK.
Requires the native sealed registry/publication/size/binding implementation.
"""
import base64
from kombu.transport import virtual
from kombu.utils.json import dumps
from .wire import current,CompatibilityUnavailable
from .task_registry import seal

class Channel(virtual.Channel):
 def __init__(self,connection,**kwargs):
  super().__init__(connection,**kwargs)
  client=connection.client
  current().call('jobs','validate_broker',{'host':client.hostname,'port':client.port,'virtual_host':client.virtual_host,'username':client.userid,'password':client.password,'ssl':client.ssl})
 def _new_queue(self,queue,auto_delete=False,**kwargs):
  if auto_delete:raise CompatibilityUnavailable('Plugin auto-delete queue lifecycle is unsupported')
  current().call('jobs','validate_queue',{'queue':queue})
 def _queue_bind(self,exchange,routing_key,pattern,queue):
  current().call('jobs','bind_queue',{'exchange':exchange,'routing_key':routing_key,'pattern':pattern,'queue':queue})
 def _size(self,queue):
  value=current().call('jobs','queue_size',{'queue':queue})
  if type(value)is not int or value<0:raise CompatibilityUnavailable('Actual native queue length receipt missing')
  return value
 def _put(self,queue,message,**kwargs):
  witness,records=seal();name=message.get('headers',{}).get('task')
  if name not in records:raise CompatibilityUnavailable('Only an actual loaded plugin task may enter the dependency bridge')
  # Existing normal Jobs lane does not yet consume Source priority subqueues.
  priority=message.get('properties',{}).get('priority',0)
  if priority not in(None,0):raise CompatibilityUnavailable('Plugin priority subqueue is unsupported')
  # Preserve actual pinned Kombu/Redis JSON spelling and real virtual Channel
  # body encoding, delivery tag, headers and delivery_info augmentation.
  raw=dumps(message).encode('utf8')
  if len(raw)>1024*1024:raise CompatibilityUnavailable('Plugin raw task frame capacity')
  receipt=current().call('jobs','publish_registered',{'witness':witness,'name':name,'queue':queue,'raw_base64':base64.b64encode(raw).decode('ascii')})
  if not isinstance(receipt,dict)or receipt.get('settled')is not True:raise CompatibilityUnavailable('Actual native publication settlement missing')
 def _get(self,*args,**kwargs):raise CompatibilityUnavailable('Native owns FIFO reservation/ACK; dependency consumption is forbidden')
 def _purge(self,*args,**kwargs):raise CompatibilityUnavailable('Plugin queue purge lifecycle is unsupported')
 def _delete(self,*args,**kwargs):raise CompatibilityUnavailable('Plugin queue delete lifecycle is unsupported')
 def _put_fanout(self,*args,**kwargs):raise CompatibilityUnavailable('Plugin fanout queue lifecycle is unsupported')

class Transport(virtual.Transport):
 Channel=Channel
 driver_type='redis'
 driver_name='native-shared-jobs'

_installed=False
def install():
 global _installed
 if _installed:raise CompatibilityUnavailable('Native Kombu adapter was already installed')
 # Must be an actual capability check, not a wrapper-supplied readiness bool.
 required=['sealed_registry','source_frame_publish','source_binding','queue_size','claimed_trace','backend_commands','result_subscription']
 actual=current().call('jobs','require_capabilities',{'required':required})
 if actual!=required:raise CompatibilityUnavailable('Required actual native Jobs capabilities are missing')
 import kombu.transport
 from .backend_transport import install as install_backend
 # Plain Redis only. Other broker protocols retain an explicit missing scope;
 # readiness must not claim their unowned network transports are compatible.
 kombu.transport.TRANSPORT_ALIASES['redis']='dispatcharr_native.kombu_transport:Transport'
 kombu.transport._transport_cache.pop('redis',None)
 install_backend();_installed=True
