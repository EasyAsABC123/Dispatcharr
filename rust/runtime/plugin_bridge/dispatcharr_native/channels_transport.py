"""Actual Source group_send via the normal Native realtime publisher.
A receive/consumer/client is not silently fabricated for unsupported operations.
"""
import asyncio
from channels.layers import BaseChannelLayer
from .wire import current,CompatibilityUnavailable
class NativeChannelLayer(BaseChannelLayer):
 extensions=[]
 async def group_send(self,group,message):
  validator=getattr(self,'require_valid_group_name',None)
  if validator is not None:validator(group)
  else:
   validator=getattr(self,'valid_group_name',None)
   if validator is None:raise CompatibilityUnavailable('Pinned Channels name validator unavailable')
   validator(group)
  if not isinstance(message,dict):raise TypeError('message is not a dict')
  await asyncio.to_thread(current().call,'events','group_send',{'group':group,'message':message})
 async def send(self,*args,**kwargs):raise CompatibilityUnavailable('Native generic channel send is not implemented')
 async def receive(self,*args,**kwargs):raise CompatibilityUnavailable('Native generic channel receive is not implemented')
 async def new_channel(self,*args,**kwargs):raise CompatibilityUnavailable('Native plugin consumer registration is not implemented')
 async def group_add(self,*args,**kwargs):raise CompatibilityUnavailable('Native plugin consumer registration is not implemented')
 async def group_discard(self,*args,**kwargs):raise CompatibilityUnavailable('Native plugin consumer registration is not implemented')
def install():pass  # Actual layer chosen by settings; no connections on install.
