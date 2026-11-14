"""Genuine Source TS/Base helper context measured in the M235 Source cohort.
One APIClient PATCH, five direct helper reads plus six ConfigHelper accessor
reads (eleven classmethod calls total), one fixed 10.25-second wait.
No Source method replacement, time patch, SQL timestamp rewrite or task worker.
The global dispatcher owns bootstrap, fixed input validation and cleanup.
"""
import hashlib,json,os,sys,time
from pathlib import Path

def main():
 from django.db import connection,connections,transaction
 from django.conf import settings
 from django.core.cache import cache
 from django.contrib.auth import get_user_model
 from django.utils import timezone
 from django.urls import resolve
 from core.api_views import CoreSettingsViewSet
 from rest_framework.test import APIClient
 from core.models import CoreSettings
 from apps.proxy.config import BaseConfig,TSConfig
 from apps.proxy.live_proxy.config_helper import ConfigHelper
 from dispatcharr.app_initialization import should_skip_initialization
 from capture_common import audit,literal,save,connected_database,require,exception_record
 from public_manifest import manifest
 report={'passed':False,'capture_valid':False,'Source_TCP_or_Main':False,'actual_API_requests':0,'declared_direct_get_proxy_settings_calls':5,'context':'TS-cold-first-new-domain-not-Base-first-replay','declared_ConfigHelper_accessor_calls':6,'helper_calls':[],'accessor_calls':[],'records':[],'SQL_timestamp_rewrite':False,'class_or_clock_patch':False};output=Path('/evidence/ts-cache-source-private.json');schema=json.loads(Path('/special-root/inputs/schema62-authority.json').read_text());nonce=os.environ['HEALTH_SOURCE_NONCE'];relay=None;relay_thread=None;release_thread=None;gate=None;ledger=None;release_stop=__import__('threading').Event()
 def flush():save(output,report)
 def state():
  with transaction.atomic():
   with connection.cursor()as c:c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
   sql={'full6259':manifest(connection,schema),'named11':audit(connection),'settings':literal(list(CoreSettings.objects.order_by('id').values()))}
  return {'SQL':sql,'named_proxy_cache':{name:literal(cache.get(name))for name in [CoreSettings.group_cache_key('proxy_settings'),CoreSettings.group_cache_ver_key('proxy_settings')]},'Base_own_cache':literal(BaseConfig.__dict__.get('_proxy_settings_cache')),'Base_own_time':BaseConfig.__dict__.get('_proxy_settings_cache_time'),'TS_has_own_cache':'_proxy_settings_cache'in TSConfig.__dict__,'TS_own_cache':literal(TSConfig.__dict__.get('_proxy_settings_cache')),'TS_own_time':TSConfig.__dict__.get('_proxy_settings_cache_time'),'wall':time.time(),'monotonic':time.monotonic(),'observed_utc':timezone.now().isoformat()}
 def observed_call(kind,label,func):
  row={'label':label,'before':state(),'began_monotonic':time.monotonic(),'began_wall':time.time()};row['actual_call_attempted']=False;report[kind].append(row);flush()
  try:
   if label=='TS_initial':
    gate.arm('TS-class-cold');row['physical_reply_gate_armed_before_call']=True
   row['actual_call_attempted']=True;row['value']=literal(func());require(len(json.dumps(row['value']).encode())<=65536,'BoundedFullHelperValue');row['returned']=True
  except BaseException:row['exception']=exception_record(sys.exc_info())
  finally:row.update(completed_monotonic=time.monotonic(),completed_wall=time.time(),after=state());flush()
  require(row.get('returned')is True,'ActualHelperMustCompleteNoFallbackIdentityGuess');return row
 try:
  require(os.environ.get('HEALTH_SOURCE_AUTHORITY')=='one-new-TS-cache-only'and'shell'in sys.argv,'ExplicitOnceChildAuthority')
  require(should_skip_initialization()is True and connected_database(connection)=='rust_health_'+nonce,'ExactOwnedCloneAndSkip')
  require(int(settings.REDIS_DB)==1,'OwnedRedisOne')
  import threading
  from owned_resp_proxy import Gate,Proxy,Handler
  ledger=Path('/evidence/cache-reply-wire-private.jsonl').open('x')
  gate=Gate({'TS-class-cold':{'kind':'get','key_hex':b':1:coresettings:group:proxy_settings'.hex()}},ledger)
  relay=Proxy(('127.0.0.1',6380),Handler);relay.gate=gate;relay.backend_port=6379;relay_thread=threading.Thread(target=relay.serve_forever);relay_thread.start()
  def release_once():
   until=time.monotonic()+20
   while not release_stop.is_set() and time.monotonic()<until:
    rows=gate.snapshot()['rows'];held=next((x for x in rows if x['kind']=='backend-target-reply-read-held'),None)
    if held is not None:
     target=held['monotonic']+2.4
     release_stop.wait(max(0,target-time.monotonic()))
     with gate.lock:gate.armed=False
     gate.release.set();return
    release_stop.wait(.005)
   if not release_stop.is_set():
    with gate.lock:gate.invalid.append('NoActualSourceConsumerReplyHeld')
  release_thread=threading.Thread(target=release_once);release_thread.start()
  report['before_setup']=state();require(not report['before_setup']['TS_has_own_cache'],'FreshTSSubclassNoForcedCacheReset')
  User=get_user_model();require(User.objects.count()==0,'OwnedFreshUsers')
  with transaction.atomic():
   target,_=CoreSettings.objects.update_or_create(key='proxy_settings',defaults={'name':'Owned health helper baseline','value':{'stream_startup_timeout':8,'connection_timeout':10,'channel_init_grace_period':60}})
   # Definition-derived setup only; no measured request is replaced.
   CoreSettings.objects.update_or_create(key='network_access',defaults={'name':'Owned helper network baseline','value':{'UI':'127.0.0.1/32','STREAMS':'127.0.0.1/32'}})
   user=User.objects.create(username='owned-health-'+nonce,password='!',api_key='health-'+nonce,user_level=10,is_active=True)
  report['after_setup']=state()
  first=observed_call('helper_calls','TS_initial',TSConfig.get_proxy_settings)
  observed_call('helper_calls','Base_initial',BaseConfig.get_proxy_settings)
  # The actual ConfigHelper accessor is the live consumer's class binding.
  report['initial_ConfigHelper_scalars']={'startup':observed_call('accessor_calls','initial_startup',ConfigHelper.stream_startup_timeout)['value'],'connection':observed_call('accessor_calls','initial_connection',ConfigHelper.connection_timeout)['value'],'initialization':observed_call('accessor_calls','initial_initialization',ConfigHelper.channel_init_grace_period)['value']}
  client=APIClient(enforce_csrf_checks=True);client.raise_request_exception=False;client.credentials(HTTP_X_API_KEY=user.api_key)
  body=b'{"value":{"stream_startup_timeout":3,"connection_timeout":4,"channel_init_grace_period":5}}'
  row={'method':'PATCH','path':'/api/core/settings/'+str(target.id)+'/','body_hex':body.hex(),'body_sha256':hashlib.sha256(body).hexdigest(),'REMOTE_ADDR':'127.0.0.1','before':state(),'began_wall':time.time(),'began_monotonic':time.monotonic()};match=resolve(row['path']);require(match.func.cls is CoreSettingsViewSet and match.func.actions.get('patch')=='partial_update','ActualSourceRoutedPATCH');row.update(actual_view=match.func.cls.__module__+'.'+match.func.cls.__qualname__,actual_action='partial_update',private_header_hex={'HTTP_X_API_KEY':user.api_key.encode().hex()},actual_user_id=user.id);report['records'].append(row);report['actual_API_requests']+=1;flush()
  try:
   response=client.generic('PATCH',row['path'],data=body,content_type='application/json',HTTP_ACCEPT='application/json',REMOTE_ADDR='127.0.0.1');raw=bytes(response.content);require(len(raw)<=65536,'FullResponseCapacity');request=(getattr(response,'renderer_context',None)or{}).get('request');actor=getattr(request,'user',None);backend=getattr(request,'successful_authenticator',None);row.update(actual_auth={'user_id':getattr(actor,'pk',None),'is_authenticated':bool(getattr(actor,'is_authenticated',False)),'backend':None if backend is None else type(backend).__module__+'.'+type(backend).__qualname__},status=response.status_code,response_body_hex=raw.hex(),response_body_sha256=hashlib.sha256(raw).hexdigest(),headers=dict(response.items()));response.close()
  finally:row.update(completed_wall=time.time(),completed_monotonic=time.monotonic(),after=state());flush()
  observed_call('helper_calls','Base_after_write',BaseConfig.get_proxy_settings)
  observed_call('helper_calls','TS_after_write',TSConfig.get_proxy_settings)
  # Clock schedule only: a status/value never selects another request or wait.
  target_time=first['began_monotonic']+10.25;report['expiry_schedule']={'origin':first['began_monotonic'],'target':target_time,'before_sleep':time.monotonic()};time.sleep(max(0,target_time-time.monotonic()));report['expiry_schedule']['after_sleep']=time.monotonic()
  observed_call('helper_calls','TS_after_fixed_expiry',TSConfig.get_proxy_settings)
  report['final_ConfigHelper_scalars']={'startup':observed_call('accessor_calls','final_startup',ConfigHelper.stream_startup_timeout)['value'],'connection':observed_call('accessor_calls','final_connection',ConfigHelper.connection_timeout)['value'],'initialization':observed_call('accessor_calls','final_initialization',ConfigHelper.channel_init_grace_period)['value']}
  report['capture_valid']=report['actual_API_requests']==1 and len(report['helper_calls'])==5 and len(report['accessor_calls'])==6 and all(x.get('actual_call_attempted')is True and x.get('returned')is True for x in report['helper_calls']+report['accessor_calls']) and row['completed_monotonic']-row['began_monotonic']<=8;report['after']=state()
 except BaseException:report['exception']=exception_record(sys.exc_info())
 finally:
  if gate is not None:
   release_stop.set();gate.release.set()
   with gate.lock:gate.closed=True;sockets=list(gate.sockets)
   for sock in sockets:
    try:sock.shutdown(__import__('socket').SHUT_RDWR)
    except OSError:pass
   try:
    relay.shutdown();relay.server_close();relay_thread.join(3);release_thread.join(3)
    report['physical_cache_gate']=gate.snapshot();report['physical_cache_gate']['listener_joined']=not relay_thread.is_alive();report['physical_cache_gate']['release_thread_joined']=not release_thread.is_alive()
    if relay_thread.is_alive()or release_thread.is_alive()or gate.invalid:report['capture_valid']=False
   except BaseException:report['relay_cleanup_exception']=exception_record(sys.exc_info());report['capture_valid']=False
   finally:
    if ledger is not None:ledger.close()
  try:connections.close_all();report['connections_close_called']=True
  except BaseException:report['close_exception']=exception_record(sys.exc_info());report['capture_valid']=False
  flush()
 require(report['capture_valid'],'CompleteCaptureOnlyNoPredictionFilter')
if __name__=='__main__':main()
