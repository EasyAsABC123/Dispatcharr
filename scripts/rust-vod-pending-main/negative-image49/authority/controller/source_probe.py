"""Actual pinned Django RedisClient+real reserve_profile_slot, no INCR method wrapper."""
import os,sys,json,time
from pathlib import Path
assert os.environ.get('PENDING_MAIN_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_pending_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.conf import settings
from django.db import connections
from core.utils import RedisClient
from apps.m3u.models import M3UAccountProfile
from apps.m3u.connection_pool import reserve_profile_slot
seed=json.loads(Path('/evidence/pending-main-private.json').read_text());profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);assert profile.max_streams==1
client=RedisClient.get_client();key='profile_connections:'+str(profile.pk);rows=[]
try:
 kwargs=client.connection_pool.connection_kwargs;assert kwargs['socket_timeout']==60 and kwargs['socket_connect_timeout']==5
 for case,initial,expected in[('success',0,(True,1,None)),('busy',1,(False,1,'profile_full'))]:
  client.set(key,initial);began=time.monotonic();value=reserve_profile_slot(profile,client);ended=time.monotonic();count=int(client.get(key));assert value==expected and count==1 and 2.4<=ended-began<4
  rows.append({'case':case,'passed':True,'result':value,'seconds':ended-began,'counter_after':count,'method_replaced':False})
 client.delete(key)
 Path('/evidence/Original-physical-authority.json').write_text(json.dumps({'passed':True,'scope':'genuine Original Django RedisClient/real reservation helper over TCP, not full HTTP','socket_timeout':kwargs['socket_timeout'],'connect_timeout':kwargs['socket_connect_timeout'],'retry_on_timeout':kwargs.get('retry_on_timeout'),'settings_socket_timeout':settings.REDIS_SOCKET_TIMEOUT,'rows':rows},indent=2)+'\n')
finally:connections.close_all()
