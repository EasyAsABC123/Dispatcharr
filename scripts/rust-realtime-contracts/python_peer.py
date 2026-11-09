"""Actual original ChannelsRedis and gevent-helper peer, isolated fixture only."""
import asyncio
import json
import os
import sys

sys.path.insert(0,'/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import channels_redis
import msgpack
from channels.layers import get_channel_layer
from core.utils import _gevent_ws_send


async def main():
    layer=get_channel_layer();channel=await layer.new_channel();await layer.group_add('updates',channel)
    print(json.dumps({'ready':True,'channel':channel,'channels_redis':channels_redis.__version__,'msgpack':msgpack.__version__}),flush=True)
    try:
        while True:
            line=await asyncio.to_thread(sys.stdin.readline)
            if not line:break
            if len(line)>1024*1024:raise ValueError('fixture request exceeds1MiB')
            request=json.loads(line);operation=request['op']
            if operation=='send':
                if request['path']=='sync':_gevent_ws_send('updates',request['message'])
                elif request['path']=='direct':await layer.send(request['channel'],request['message'])
                else:await layer.group_send('updates',request['message'])
                response={'sent':True}
            elif operation=='receive':
                try:
                    message=await asyncio.wait_for(layer.receive(channel),request.get('timeout_ms',3000)/1000)
                    await asyncio.sleep(.01)
                    response={'message':message}
                except asyncio.TimeoutError:response={'timeout':True}
            elif operation=='close':
                await layer.group_discard('updates',channel);print(json.dumps({'closed':True}),flush=True);break
            else:raise ValueError('unknown fixture operation')
            print(json.dumps(response),flush=True)
    finally:
        await layer.group_discard('updates',channel)
        await layer.close_pools()


if __name__=='__main__':asyncio.run(main())
