"""Actual M3U API→SD guide workers through an owned HTTPS Schedules Direct Origin.
Use ONLY inside the existing milestone fixture/controller lease. No app starts
on import. Original Dispatcharr and dependency authors retain application credit.
"""
import base64,datetime,hashlib,json,time,uuid,traceback
from pathlib import Path
from budget import bounded_read
import urllib.request,urllib.error
class ActualNativeCeleryFault(Exception):
 pass

PORT=18097
CASE='schedules-direct'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def control(fixture,path,value=None):
 script="import json,urllib.request;data="+repr(None if value is None else json.dumps(value).encode())+";r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(urllib.request.Request('http://127.0.0.1:"+str(PORT)+"'+"+repr(path)+",data=data,headers={'Content-Type':'application/json'}),timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())"
 return json.loads(fixture.python(script,seconds=10,secret=True))
def prepare_context(fixture):
 if fixture.case!=CASE:return
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});print(json.dumps({'scope':'declared SD fixture schedule isolation only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 username='Owned SD';password=key;fixture._sd_credentials={'username':username,'password':password};config={'username_sha256':hashlib.sha256(username.encode()).hexdigest(),'password_sha1_sha256':hashlib.sha256(hashlib.sha1(password.encode()).hexdigest().encode()).hexdigest(),'ca_path':guest+'/sd-ca.pem','ready':guest+'/sd-origin.ready','final':guest+'/sd-origin-final.json'}
 # Only a key HASH is written into the /data bind. CA/leaf private keys are
 # generated in the Origin's private Source-side tmpdir, never exported.
 fixture.python("import json,os;from pathlib import Path;p=Path("+repr(guest+'/sd-origin-config.json')+");p.write_text("+repr(json.dumps(config))+");os.chown(p,65532,65532);p.chmod(0o444)")
 fixture.role('sd-guide-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/sd_guide_origin.py'],extra={'OWNED_SD_CONFIG':guest+'/sd-origin-config.json'})
 fixture.wait(lambda:(fixture.case_data/'sd-origin.ready').exists(),20,'actual-owned-provider-TLS-origin-ready')
 ready=json.loads((fixture.case_data/'sd-origin.ready').read_text());ca=fixture.case_data/'sd-ca.pem'
 if hashlib.sha256(ca.read_bytes()).hexdigest()!=ready['actual_CA_sha256']or ready['private_keys_exported']:raise AssertionError('ActualOwnedCAProof')
 ca_path='/data/sd-ca.pem'if fixture.backend=='Native'else guest+'/sd-ca.pem'
 fixture._provider_environment={'HTTPS_PROXY':'http://127.0.0.1:'+str(PORT),'https_proxy':'http://127.0.0.1:'+str(PORT),'HTTP_PROXY':'http://127.0.0.1:'+str(PORT),'http_proxy':'http://127.0.0.1:'+str(PORT),'ALL_PROXY':'http://127.0.0.1:'+str(PORT),'all_proxy':'http://127.0.0.1:'+str(PORT),'NO_PROXY':'127.0.0.1,localhost','no_proxy':'127.0.0.1,localhost','REQUESTS_CA_BUNDLE':ca_path}
 fixture._provider_origin_ready=ready
 # The SAME pinned Source Requests/Python stack must actually trust the new
 # Origin before any Source or Native business worker starts. This is a
 # transport-only probe, no Django/model/task/API invocation or verification bypass.
 proof_environment={key:value for key,value in fixture._provider_environment.items()if key not in('TMDB_API_KEY','OMDB_API_KEY')};proof_environment['REQUESTS_CA_BUNDLE']=guest+'/sd-ca.pem'
 proof_script="import os,json,hashlib,ssl,re;os.environ.update("+repr(proof_environment)+");import requests,urllib3;from urllib3.util.ssl_ import create_urllib3_context;s=requests.Session();receipt={'requests_version':requests.__version__,'urllib3_version':urllib3.__version__,'openssl_version':ssl.OPENSSL_VERSION,'verify_enabled':s.verify is True,'source_context_verify_flags':int(create_urllib3_context().verify_flags),'actual_CA_sha256':hashlib.sha256(open(os.environ['REQUESTS_CA_BUNDLE'],'rb').read()).hexdigest(),'status':None,'body_sha256':None,'errors':[]};\ntry:\n r=s.get('https://json.schedulesdirect.org/__owned_tls_probe',timeout=(5,5));body=r.content;receipt.update(status=r.status_code,body_sha256=hashlib.sha256(body).hexdigest(),actual_bytes=len(body));receipt['passed']=r.status_code==200 and body==b'owned-source-TLS-trust-proof'\nexcept BaseException as error:\n pending=[error];seen=set();receipt['passed']=False\n while pending and len(seen)<16:\n  item=pending.pop()\n  if id(item)in seen:continue\n  seen.add(id(item));entry={'type':type(item).__name__};reason=getattr(item,'reason',None);code=getattr(item,'verify_code',None);message=getattr(item,'verify_message',None)\n  if isinstance(reason,str)and re.fullmatch('[A-Z0-9_]{1,96}',reason):entry['actual_SSL_reason']=reason\n  if isinstance(code,int):entry['actual_verify_code']=code\n  if isinstance(message,str):entry['verify_message_sha256']=hashlib.sha256(message.encode()).hexdigest()\n  receipt['errors'].append(entry)\n  for value in [getattr(item,'__cause__',None),getattr(item,'__context__',None),reason,*getattr(item,'args',())]:\n   if isinstance(value,BaseException):pending.append(value)\nfinally:s.close()\nprint(json.dumps(receipt))"
 trust=json.loads(fixture.python(proof_script,seconds=15,secret=True));save(Path(fixture.output)/'actual-SD-Source-Requests-trust-preflight.json',trust)
 if trust.get('passed')is not True or not trust['verify_enabled']or trust['actual_CA_sha256']!=ready['actual_CA_sha256']:raise AssertionError('ActualPinnedSourceRequestsTrustPreflight')


 # DATA setup only: skip source post_save station-fetch callbacks, which are a
 # separate producer scope. Actual guide publication below is real API/Celery.
 username=fixture._sd_credentials['username'];password=fixture._sd_credentials['password']
 setup=fixture.sql("from apps.epg.models import EPGSource,EPGData;from apps.m3u.models import M3UAccount,M3UAccountProfile;from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount;s=EPGSource.objects.bulk_create([EPGSource(name='Owned SD guide',source_type='schedules_direct',username="+repr(' '+username+'\x1c')+",password="+repr(' '+password+'\x1f')+",custom_properties={'fetch_posters':False,'auto_apply_epg_logos':True})])[0];es=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id=str(10000+i),name='Owned SD station '+str(i),icon_url='https://json.schedulesdirect.org/20141201/image/channel-'+str(i))for i in range(1,4)]);a=M3UAccount.objects.bulk_create([M3UAccount(name='Owned SD playlist',account_type='STD',server_url='http://127.0.0.1:"+str(PORT)+"/owned.m3u',stale_stream_days=0,custom_properties={'hash_key':'url','auto_enable_new_groups_live':True,'orphan_channel_cleanup':'never'})])[0];M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=a,name='Default',is_default=True,is_active=True)]);g=ChannelGroup.objects.create(name='Owned SD');r=ChannelGroupM3UAccount.objects.create(channel_group=g,m3u_account=a,enabled=True,auto_channel_sync=True,auto_sync_channel_start=40000,custom_properties={'channel_numbering_mode':'fixed','skip_channel_profile_memberships':True});print(json.dumps({'source':s.pk,'epg':[e.pk for e in es],'account':a.pk,'group':g.pk,'relation':r.pk}))",secret=True)
 fixture._sd_guide=setup
 save(Path(fixture.output)/'SD-declared-inputs.json',{'source':setup['source'],'epg':setup['epg'],'account':setup['account'],'stations':3,'dates_per_MD5_request':20,'real_API_auto_batch_threshold':3,'actual_CA_sha256':ready['actual_CA_sha256'],'new_Source_characterization_required':True})

def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 setup=fixture._sd_guide;source=setup['source'];out=Path(fixture.output)/CASE;out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700);cells=[];seen=set()
 fixture.role('sd-guide-publications',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+CASE+'/sd-publications.jsonl'})
 fixture.wait(lambda:(fixture.case_data/'sd-publications.ready').exists(),10,'actual-SD-publication-observer')
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;password=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+password+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+password+'@localhost:6379/2';sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;from urllib.parse import urlsplit;assert urlsplit(current_app.conf.broker_url).path=='/1'and urlsplit(current_app.conf.result_backend).path=='/2';\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 calibration='00000000-0000-0000-0000-000000000000';key=guest("print(json.dumps(current_app.backend.get_key_for_task("+repr(calibration)+").decode()))");assert key.endswith(calibration);prefix=key[:-len(calibration)]
 def frames():
  p=fixture.case_data/'sd-publications.jsonl'
  if not p.exists():return []
  if p.stat().st_size>1048576:raise RuntimeError('DeclaredSDBrokerCaptureBudget')
  rows=[]
  for line in p.read_text().splitlines():
   try:rows.append(json.loads(line))
   except json.JSONDecodeError:continue
  return rows
 def terminal(ident):
  raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('declared SD result bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
  return None if raw is None else json.loads(raw)
 last_health=[0.0]
 def actual_common_fault(label):
  if fixture.backend!='Native' or time.monotonic()-last_health[0]<1:return
  last_health[0]=time.monotonic()
  try:
   request=urllib.request.Request(fixture.base_url+'/health/ready')
   try:response=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request,timeout=fixture.remaining(1))
   except urllib.error.HTTPError as error:response=error
   with response:
    raw=bounded_read(response,65536,lambda maximum:fixture.remaining(min(maximum,1)));status=response.status
   if len(raw)>65536:return
   health=json.loads(raw)
   workers=health.get('selected_workers',{})if isinstance(health,dict)else{}
   celery=workers.get('celery')if isinstance(workers,dict)else None
   # A bare503, transient startup, blocked/capacity state or malformed response
   # is NOT evidence of a failed common consumer. Only actual typed Fault is.
   if status==503 and workers.get('ready')is False and isinstance(celery,str)and celery.startswith('Fault('):
    path=private/(label+'-actual-common-celery-fault.json');save(path,{'status':status,'body_sha256':hashlib.sha256(raw).hexdigest(),'actual_health_body':health});path.chmod(0o600)
    raise ActualNativeCeleryFault('ActualNativeCommonCeleryFault')
  except (OSError,ValueError,RuntimeError):
   # Preserve the original bounded wait on an unavailable observer response.
   return
 def wait_task(ident,label,seconds=90):
  def observed():
   value=terminal(ident)
   if value is None:actual_common_fault(label)
   return value
  value=fixture.wait(observed,seconds,'actual-SD-task-terminal-'+label);save(private/(label+'-backend.json'),value)
  if value.get('status')!='SUCCESS':raise AssertionError('ActualSDBackendNotSuccess')
  return value
 def snapshot():
  value=fixture.sql("from apps.epg.models import EPGSource,ProgramData,SDScheduleMD5,SDProgramMD5;from apps.channels.models import Channel;from core.models import SystemEvent;s=EPGSource.objects.get(pk="+str(source)+");print(json.dumps({'status':s.status,'message':s.last_message,'updated_at':s.updated_at,'properties':s.custom_properties,'programmes':list(ProgramData.objects.filter(epg_id__in="+repr(setup['epg'])+").order_by('tvg_id','start_time').values('id','epg_id','tvg_id','program_id','start_time','end_time','title','sub_title','description','custom_properties')),'schedule_md5':list(SDScheduleMD5.objects.filter(epg_source=s).order_by('station_id','date').values('station_id','date','md5','last_modified')),'program_md5':list(SDProgramMD5.objects.filter(epg_source=s).order_by('program_id').values('program_id','md5')),'channels':list(Channel.objects.filter(channel_group_id="+str(setup['group'])+").order_by('id').values('name','epg_data_id','logo__url')),'events':list(SystemEvent.objects.filter(event_type__in=['epg_refresh','epg_error']).order_by('id').values('event_type','details'))},default=str))",secret=True)
  return value
 def publish(kind='mapped',force=False,defer=0,label='direct'):
  if kind not in('mapped','single','tvg'):raise ValueError('DeclaredSDTaskKind')
  module='apps.epg.sd_tasks'if kind!='tvg'else'apps.epg.tasks';name={'mapped':'fetch_sd_mapped_guide_batch','single':'fetch_sd_guide_for_epg','tvg':'parse_programs_for_tvg_id'}[kind];target=source if kind=='mapped'else setup['epg'][0]
  made=guest('from '+module+' import '+name+';t='+name+'.apply_async(args=['+str(target)+'],kwargs='+repr({'force':force,'_defer_retry':defer})+');print(json.dumps({"id":t.id,"kind":'+repr(kind)+'}))');save(private/(label+'-producer.json'),made)
  value=wait_task(made['id'],label);save(private/(label+'-SQL.json'),snapshot());return made['id'],value
 def patch(**fields):
  fixture.sql("from apps.epg.models import EPGSource;EPGSource.objects.filter(pk="+str(source)+").update(**"+repr(fields)+");print(json.dumps({'declared_source_input_change':True}))",secret=True)
 def properties(**changes):
  fixture.sql("from apps.epg.models import EPGSource;s=EPGSource.objects.get(pk="+str(source)+");v=dict(s.custom_properties or {});v.update("+repr(changes)+");EPGSource.objects.filter(pk=s.pk).update(custom_properties=v);print(json.dumps({'declared_source_properties_change':True}))",secret=True)
 def trace_since(before):return control(fixture,'/snapshot')['trace'][before:]
 def requests(rows):return [r for r in rows if r.get('actual_TLS')and r.get('path')!='/__owned_tls_probe']
 def cell(label,body):
  row={'name':label,'status':'running'};cells.append(row);save(out/'report.json',{'status':'running','cases':cells})
  try:row.update(status='passed',actual=body())
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(label+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
   if isinstance(error,ActualNativeCeleryFault):
    save(out/'report.json',{'status':'failed','cases':cells,'actual_common_celery_fault':True,'remaining_dependent_cells':'not_run; common consumer fault observed, no task replay or synthetic backend completion'});raise
  save(out/'report.json',{'status':'running','cases':cells})
 def initial():
  control(fixture,'/control',{'case':'base'});before=len(control(fixture,'/snapshot')['trace']);response=fixture.api('POST','/api/m3u/refresh/'+str(setup['account'])+'/',{});save(private/'initial-API.json',response);assert response['status']==202
  def parent():
   for f in frames():
    if(f.get('headers')or{}).get('task')=='apps.m3u.tasks.refresh_single_m3u_account'and f['headers']['id']not in seen:return f['headers']['id']
   return False
  ident=fixture.wait(parent,20,'actual-SD-auto-API-parent');seen.add(ident);parent_result=wait_task(ident,'initial-parent');child_ids=[x[0][0]for x in parent_result.get('children',[])];assert len(child_ids)==1
  guide=wait_task(child_ids[0],'initial-guide');assert guide['result']=='SD mapped guide batch complete'and guide.get('parent_id')==ident
  actual=[f for f in frames()if(f.get('headers')or{}).get('id')==child_ids[0]];assert len(actual)==1 and actual[0]['headers']['task']=='apps.epg.tasks.fetch_sd_mapped_guide_batch'and actual[0]['headers']['root_id']==ident
  rows=snapshot();save(private/'initial-SQL.json',rows);assert len(rows['programmes'])==3 and len(rows['schedule_md5'])==3 and len(rows['program_md5'])==3 and len(rows['channels'])==3
  assert rows['updated_at']is None
  for p in rows['programmes']:
   cp=p['custom_properties'];assert p['title']=='Owned SD '+p['program_id']and cp['rating']=='12'and cp['onscreen_episode']=='S2 E3'and cp['length']=={'value':'30','units':'minutes'}and cp['previously_shown_details']['start']=='2020-01-02'
   assert len(cp['credits']['actor'])==2 and cp['credits']['actor'][1]['guest']is True
  trace=trace_since(before);save(private/'initial-Origin.json',trace);paths=[r['path']for r in requests(trace)]
  assert all(p in paths for p in ['/20141201/token','/20141201/status','/20141201/lineups','/20141201/schedules/md5','/20141201/schedules','/20141201/programs'])
  return {'real_API_auto_to_one_SD_batch':True,'programmes':3,'real_parent_root':True,'country_rating':'GBR','Source_guest_duplicate_preserved':True,'actual_owned_TLS':True}
 def unchanged():
  before=snapshot();position=len(control(fixture,'/snapshot')['trace']);_,value=publish(label='unchanged');after=snapshot();trace=trace_since(position);save(private/'unchanged-Origin.json',trace)
  assert value['result']=='SD mapped guide batch complete'and before['programmes']==after['programmes']
  paths=[r['path']for r in requests(trace)];assert '/20141201/schedules/md5'in paths and not any(p in paths for p in ['/20141201/token','/20141201/schedules','/20141201/programs'])
  return {'actual_MD5_and_credential_cache_reuse':True,'unchanged_Programme_rows':True}
 def posters():
  properties(fetch_posters=True);position=len(control(fixture,'/snapshot')['trace']);publish(label='posters');after=snapshot();trace=trace_since(position);save(private/'posters-Origin.json',trace)
  assert any(r.get('path')=='/20141201/metadata/programs/'for r in trace)
  assert all(p['custom_properties']['sd_icon']=='https://json.schedulesdirect.org/20141201/image/owned-portrait'and p['custom_properties']['sd_poster_style']=='sd_recommended'for p in after['programmes'])
  return {'real_SD_metadata_request_and_programme_persistence':True}
 def delta_cached():
  control(fixture,'/control',{'case':'changed'});before=snapshot();position=len(control(fixture,'/snapshot')['trace']);publish(label='delta-cached');after=snapshot();trace=trace_since(position);save(private/'delta-cached-Origin.json',trace)
  paths=[r['path']for r in requests(trace)];assert '/20141201/schedules'in paths and '/20141201/programs'not in paths
  assert [{k:v for k,v in p.items()if k!='id'}for p in before['programmes']]==[{k:v for k,v in p.items()if k!='id'}for p in after['programmes']]
  return {'actual_surgical_replacement_with_Source_cached_fields':True}
 def metadata_error():
  control(fixture,'/control',{'case':'metadata-error'});before=snapshot();position=len(control(fixture,'/snapshot')['trace']);publish(label='metadata-error');after=snapshot();trace=trace_since(position);save(private/'metadata-error-Origin.json',trace)
  assert len([r for r in trace if r.get('path')=='/20141201/programs'and r.get('status')==503])==3
  assert [p['title']for p in before['programmes']]==[p['title']for p in after['programmes']]
  return {'actual_three_attempts_and_old_programme_fallback':True}
 def bad_credentials():
  control(fixture,'/control',{'case':'base'});patch(password='owned-wrong-'+uuid.uuid4().hex);position=len(control(fixture,'/snapshot')['trace']);publish(label='bad-credentials');a=snapshot();publish(label='persisted-lockout');b=snapshot();trace=trace_since(position);save(private/'bad-credentials-Origin.json',trace)
  assert a['status']=='error'and b['properties']['sd_auth_lockout_code']==4003 and len([r for r in trace if r.get('path')=='/20141201/token'])==1
  patch(password=' '+fixture._sd_credentials['password']+'\x1f');publish(label='credential-change-unlock');assert not snapshot()['properties'].get('sd_auth_lockout')
  return {'actual_persisted_lockout_and_credential_fingerprint_unlock':True}
 def debug_reauth():
  properties(sd_extra_debugging=True);fixture.redis(0,"return redis.call('DEL',ARGV[1])",':1:sd:token:'+str(source));control(fixture,'/control',{'case':'debug2055'});position=len(control(fixture,'/snapshot')['trace']);publish(label='debug-rejection');trace=trace_since(position);save(private/'debug-rejection-Origin.json',trace)
  tokens=[r for r in trace if r.get('path')=='/20141201/token'];assert len(tokens)==2 and tokens[0]['debug_header']and not tokens[1]['debug_header']and snapshot()['properties']['sd_extra_debugging']is False
  control(fixture,'/control',{'case':'expired'});position=len(control(fixture,'/snapshot')['trace']);publish(label='expired-reauth');trace=trace_since(position);save(private/'expired-reauth-Origin.json',trace);assert any(r.get('status')==403 for r in trace)and any(r.get('path')=='/20141201/token'for r in trace)
  return {'actual_debug_disable_and401_403_reauth':True}
 def single_and_tvg():
  control(fixture,'/control',{'case':'base'});position=len(control(fixture,'/snapshot')['trace']);_,early=publish('single',False,label='single-present');assert early['result']=='Guide data already present'and not requests(trace_since(position))
  _,direct=publish('single',True,label='single-force');_,delegate=publish('tvg',True,label='tvg-SD-delegation');assert direct['result']=='SD guide fetch complete'and delegate['result']=='SD guide fetch complete'
  return {'actual_single_skip_force_and_TVG_delegation':True}
 def eta_retry():
  token='owned-SD-lock-'+uuid.uuid4().hex;lock='task_lock_sd_mapped_guide_fetch_'+str(source)
  assert fixture.redis(0,"return redis.call('SET',ARGV[1],ARGV[2],'NX','EX',180)",lock,token)=='OK'
  try:
   ident,result=publish(label='held-batch-defer');assert result['result']=='Deferred - batch already in progress';children=[x[0][0]for x in result.get('children',[])];assert len(children)==1;child=children[0]
   original=fixture.wait(lambda:next((r for r in frames()if(r.get('headers')or{}).get('id')==child),None),15,'actual-SD-retry-publication');assert original['headers']['task']=='apps.epg.tasks.fetch_sd_mapped_guide_batch'and original['headers']['parent_id']==ident
   eta=datetime.datetime.fromisoformat(original['headers']['eta']);assert 60<(eta-datetime.datetime.now(datetime.timezone.utc)).total_seconds()<=95
   reserved=fixture.wait(lambda:fixture.redis(1,"local h=redis.call('HGETALL','unacked');for i=1,#h,2 do if string.find(h[i+1],ARGV[1],1,true)then return {h[i],h[i+1]}end end;return false",child),15,'actual-SD-durable-unacked');save(private/'SD-durable-receipt.json',reserved)
   assert fixture.redis(0,"if redis.call('GET',ARGV[1])==ARGV[2]then return redis.call('DEL',ARGV[1])end;return 0",lock,token)==1
   value=wait_task(child,'deferred-guide',110);assert value['result']=='SD mapped guide batch complete'and value.get('parent_id')==ident
   return {'actual_countdown90_publication_durable_reservation_and_due_execution':True,'child':child}
  finally:fixture.redis(0,"if redis.call('GET',ARGV[1])==ARGV[2]then return redis.call('DEL',ARGV[1])end;return 0",lock,token)
 for label,body in [('actual-API-auto3-to-owned-SD-guide',initial),('actual-cache-MD5-nochange',unchanged),('actual-poster-provider-precedence-persistence',posters),('actual-delta-surgical-oldmetadata',delta_cached),('actual-provider-error-retry-oldmetadata',metadata_error),('actual-auth-lockout-credential-unlock',bad_credentials),('actual-debug2055-token403-reauth',debug_reauth),('actual-single-and-TVG-SD-delegation',single_and_tvg),('actual-durable90-retry',eta_retry)]:cell(label,body)
 final={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'new_Source_characterization_required':True,'performance_verified':False,'unrun_required':['cold crash ETA visibility and warm restore','revoke-before-due and revoked deleted Source rows','held HTTP physical shutdown uncertainty','large5000-station/programme metadata matrices','genuinely malformed/nonfinite/alternate text encoding provider shapes','enabled plugin/Connect hooks','full Source SD station/lineup UI API']};save(out/'report.json',final);return final
