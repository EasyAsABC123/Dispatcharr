"""Read-only bounded local/module/broker observations; no Source replacements."""
import base64,hashlib,json,os
from pathlib import Path

def need(v,k):
 if not v:raise ValueError(k)
def local_state():
 from django.conf import settings
 from dispatcharr import display_timezone
 from apps.proxy.config import BaseConfig
 from capture_common import literal
 log=Path(settings.LOG_FILE_DIR);need(str(log).startswith('/shared-core-logs/'),'OwnedLogDirectoryOnly');files=[]
 if log.exists():
  for p in sorted(log.rglob('*')):
   need(not p.is_symlink(),'NoSymlinkLogObservation')
   if p.is_file():
    need(len(files)<32 and p.stat().st_size<=65536,'LogMetadataCapacity');raw=p.read_bytes();files.append({'relative':str(p.relative_to(log)),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'content_hex':raw.hex()})
 zone=display_timezone._cache['zone']
 return {'display_zone':None if zone is None else getattr(zone,'key',str(zone)),'display_checked_monotonic':display_timezone._cache['checked'],'process_proxy_cache':literal(BaseConfig._proxy_settings_cache),'process_proxy_cache_time':BaseConfig._proxy_settings_cache_time,'owned_log_files':files,'collector_process_started_by_fixture':False}
def brokers(raw):
 """Decode actual Redis list frames without LPOP/receive/ack/task execution."""
 out=[]
 for database in raw:
  for row in database['rows']:
   if row['type']!='list':continue
   for value in row['list_hex']:
    payload=bytes.fromhex(value);need(len(payload)<=65536,'BrokerFrameCapacity');message=json.loads(payload);need(type(message)is dict and 'headers'in message and 'body'in message,'ActualCeleryRedisListFrameSchema');body=base64.b64decode(message['body'],validate=True);need(len(body)<=65536,'BrokerDecodedBodyCapacity');decoded=json.loads(body);out.append({'database':database['database'],'queue_key_hex':row['key_hex'],'raw_frame_hex':value,'raw_frame_sha256':hashlib.sha256(payload).hexdigest(),'headers':message['headers'],'properties':message['properties'],'decoded_body':decoded})
 need(len(out)<=64,'AllBrokerFrameCapacity');return {'frames':out,'pop_receive_ack_calls':0,'worker_or_beat_started':False,'delay_replaced':False,'downstream_task_execution_qualified':False}
def bindings(expected_db):
 from django.conf import settings
 from django_redis import get_redis_connection
 from core.utils import RedisClient
 from celery import current_app
 from channels.layers import get_channel_layer
 from urllib.parse import urlsplit
 cache=get_redis_connection('default');native=RedisClient.get_client();layer=get_channel_layer();broker=current_app.conf.broker_url
 def db(client):return int(client.connection_pool.connection_kwargs['db'])
 need(db(cache)==db(native)==expected_db and int(urlsplit(layer.hosts[0]['address']).path.strip('/'))==expected_db and int(urlsplit(broker).path.strip('/'))==expected_db,'ActualCacheCoreChannelsBrokerDeclaredDB')
 return {'Django_cache_DB':db(cache),'core_RedisClient_DB':db(native),'Channels_DB':expected_db,'broker_DB':expected_db,'Celery_default_queue':current_app.conf.task_default_queue,'Celery_eager':current_app.conf.task_always_eager,'broker_transport_options':dict(current_app.conf.broker_transport_options),'normal_Source_config_no_driver_or_delay_patch':True}
