"""Actual recording HTTP→artwork workers through owned HTTPS provider Origins.
Use ONLY inside the existing milestone fixture/controller lease. No app starts
on import. Original Dispatcharr and dependency authors retain application credit.
"""
import base64,datetime,hashlib,json,time,uuid
from pathlib import Path
PORT=18095
CASE='recording-provider-artwork'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def control(fixture,path,value=None):
 script="import json,urllib.request;data="+repr(None if value is None else json.dumps(value).encode())+";r=urllib.request.build_opener(urllib.request.ProxyHandler({})).open(urllib.request.Request('http://127.0.0.1:"+str(PORT)+"'+"+repr(path)+",data=data,headers={'Content-Type':'application/json'}),timeout=5);raw=r.read(1048577);assert len(raw)<=1048576;print(raw.decode())"
 return json.loads(fixture.python(script,seconds=10,secret=True))
def prepare_context(fixture):
 if fixture.case!=CASE:return
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 config={'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json'}
 # Only a key HASH is written into the /data bind. CA/leaf private keys are
 # generated in the Origin's private Source-side tmpdir, never exported.
 fixture.python("import json,os;from pathlib import Path;p=Path("+repr(guest+'/provider-origin-config.json')+");p.write_text("+repr(json.dumps(config))+");os.chown(p,65532,65532);p.chmod(0o444)")
 fixture.role('recording-provider-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/provider_origin.py'],extra={'OWNED_PROVIDER_CONFIG':guest+'/provider-origin-config.json'})
 fixture.wait(lambda:(fixture.case_data/'provider-origin.ready').exists(),20,'actual-owned-provider-TLS-origin-ready')
 ready=json.loads((fixture.case_data/'provider-origin.ready').read_text());ca=fixture.case_data/'provider-ca.pem'
 if hashlib.sha256(ca.read_bytes()).hexdigest()!=ready['actual_CA_sha256']or ready['private_keys_exported']:raise AssertionError('ActualOwnedCAProof')
 ca_path='/data/provider-ca.pem'if fixture.backend=='Native'else guest+'/provider-ca.pem'
 fixture._provider_environment={'HTTPS_PROXY':'http://127.0.0.1:'+str(PORT),'https_proxy':'http://127.0.0.1:'+str(PORT),'HTTP_PROXY':'http://127.0.0.1:'+str(PORT),'http_proxy':'http://127.0.0.1:'+str(PORT),'ALL_PROXY':'http://127.0.0.1:'+str(PORT),'all_proxy':'http://127.0.0.1:'+str(PORT),'NO_PROXY':'127.0.0.1,localhost','no_proxy':'127.0.0.1,localhost','REQUESTS_CA_BUNDLE':ca_path,'TMDB_API_KEY':key,'OMDB_API_KEY':key}
 fixture._provider_origin_ready=ready
 # The SAME pinned Source Requests/Python stack must actually trust the new
 # Origin before any Source or Native business worker starts. This is a
 # transport-only probe, no Django/model/task/API invocation or verification bypass.
 proof_environment={key:value for key,value in fixture._provider_environment.items()if key not in('TMDB_API_KEY','OMDB_API_KEY')};proof_environment['REQUESTS_CA_BUNDLE']=guest+'/provider-ca.pem'
 proof_script="import os,json,hashlib,ssl,re;os.environ.update("+repr(proof_environment)+");import requests,urllib3;from urllib3.util.ssl_ import create_urllib3_context;s=requests.Session();receipt={'requests_version':requests.__version__,'urllib3_version':urllib3.__version__,'openssl_version':ssl.OPENSSL_VERSION,'verify_enabled':s.verify is True,'source_context_verify_flags':int(create_urllib3_context().verify_flags),'actual_CA_sha256':hashlib.sha256(open(os.environ['REQUESTS_CA_BUNDLE'],'rb').read()).hexdigest(),'status':None,'body_sha256':None,'errors':[]};\ntry:\n r=s.get('https://artwork-owned.invalid/__owned_tls_probe',timeout=(5,5));body=r.content;receipt.update(status=r.status_code,body_sha256=hashlib.sha256(body).hexdigest(),actual_bytes=len(body));receipt['passed']=r.status_code==200 and body==b'owned-source-TLS-trust-proof'\nexcept BaseException as error:\n pending=[error];seen=set();receipt['passed']=False\n while pending and len(seen)<16:\n  item=pending.pop()\n  if id(item)in seen:continue\n  seen.add(id(item));entry={'type':type(item).__name__};reason=getattr(item,'reason',None);code=getattr(item,'verify_code',None);message=getattr(item,'verify_message',None)\n  if isinstance(reason,str)and re.fullmatch('[A-Z0-9_]{1,96}',reason):entry['actual_SSL_reason']=reason\n  if isinstance(code,int):entry['actual_verify_code']=code\n  if isinstance(message,str):entry['verify_message_sha256']=hashlib.sha256(message.encode()).hexdigest()\n  receipt['errors'].append(entry)\n  for value in [getattr(item,'__cause__',None),getattr(item,'__context__',None),reason,*getattr(item,'args',())]:\n   if isinstance(value,BaseException):pending.append(value)\nfinally:s.close()\nprint(json.dumps(receipt))"
 trust=json.loads(fixture.python(proof_script,seconds=15,secret=True));save(Path(fixture.output)/'actual-provider-Source-Requests-trust-preflight.json',trust)
 if trust.get('passed')is not True or not trust['verify_enabled']or trust['actual_CA_sha256']!=ready['actual_CA_sha256']:raise AssertionError('ActualPinnedSourceRequestsTrustPreflight')


def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedProviderContextRequired')
 declared=fixture.binding.get('provider_required_cells',{}).get(fixture.backend)
 if declared is not None and not(fixture.backend=='Native'and declared==['direct','direct-cache']):raise ValueError('OnlyRootAuthorizedCausalNativeCachePairMayNarrowProviderCells')
 out=Path(fixture.output)/CASE;out.mkdir(exist_ok=False);cells=[]
 fixture.role('provider-broker-observer',['/dispatcharrpy/bin/python','/normal-dvr-tools/broker_monitor.py'],extra={'NORMAL_DVR_LEDGER':'/shared/'+CASE+'/provider-publications.jsonl'})
 fixture.wait(lambda:(fixture.case_data/'provider-publications.ready').exists(),10,'actual-provider-publication-observer')
 # Reuse the already-bound real Channels subscriber/decoder, never a fake sink.
 module=fixture.m6_module();subscription=module.subscribe(fixture)
 if fixture.backend=='Native':
  actual=json.loads(fixture.command([fixture.docker,'inspect',fixture.native],secret=True))[0];environment=dict(item.split('=',1)for item in actual['Config']['Env']);owner={'actual_CID':actual['Id'],'actual_PID':actual['State']['Pid']}
 else:
  launch=fixture.roles['celery'];script="import json,sys;from pathlib import Path;sys.path.insert(0,'/normal-dvr-tools');from role_owner import rpc;record="+repr(launch)+";actual=rpc(record,'status');assert actual['pid']==record['pid']and actual['starttime']==record['starttime']and actual['running'];config=json.loads(Path(record['socket']).with_suffix('.json').read_text());assert config['name']=='celery'and config['args']==record['actual_argv']and config['socket']==record['socket'];print(json.dumps({'environment':config['environment'],'actual_PID':actual['pid'],'actual_starttime':actual['starttime']}))"
  actual=json.loads(fixture.python(script,secret=True));environment=actual['environment'];owner={k:actual[k]for k in ('actual_PID','actual_starttime')}
 if any(environment.get(k)!=v for k,v in fixture._provider_environment.items()):raise AssertionError('ActualProviderProxyTrustBindingMismatch')
 save(out/'actual-proxy-CA-binding.json',{'backend':fixture.backend,'actual_owner':owner,'actual_proxy_port':PORT,'both_case_PROXY_variables_bound':True,'NO_PROXY_loopback_only':True,'actual_CA_sha256':fixture._provider_origin_ready['actual_CA_sha256'],'private_keys_exported':False,'API_keys_exported':False})

 def sql(code):return fixture.sql(code)
 def row(ident):return sql("r=Recording.objects.get(pk="+str(ident)+");print(json.dumps({'id':r.pk,'custom_properties':r.custom_properties,'task_id':r.task_id},default=str))")
 def logo_sequence():return sql("with connection.cursor()as c:\n c.execute(\"SELECT last_value FROM pg_sequences WHERE schemaname='public'AND sequencename=pg_get_serial_sequence('dispatcharr_channels_logo','id')::regclass::text\");v=c.fetchone();print(json.dumps(None if v is None else v[0]))")
 def case(label,expected_url=None,epg=None,direct=None,vod=False,existing=False,channel=False,reuse=False,scenario=None):
  required=fixture.binding.get('provider_required_cells',{}).get(fixture.backend)
  if required is not None and label not in required:return
  record={'name':label,'status':'running','began_epoch':time.time()};cells.append(record);save(out/(label+'.json'),record);ident=None
  try:
   control(fixture,'/control',{'case':scenario or label});before=control(fixture,'/snapshot');position=len(before['trace']);title='Owned provider '+label+' '+uuid.uuid4().hex[:8]
   if existing:title='Å'*270+uuid.uuid4().hex[:8]
   sql("from apps.channels.models import Channel;Channel.objects.filter(pk="+str(fixture.channel_id)+").update(logo_id=None,epg_data_id=None);print(json.dumps(True))")
   expected_id=None
   if channel:
    expected_id=sql("from apps.channels.models import Logo,Channel;r,_=Logo.objects.get_or_create(url='/data/provider-channel-logo',defaults={'name':'Owned channel fallback'});Channel.objects.filter(pk="+str(fixture.channel_id)+").update(logo_id=r.pk);print(json.dumps(r.pk))")
   if existing:
    expected_id=sql("from apps.channels.models import Logo;r=Logo.objects.create(name="+repr(title[:255])+",url='/data/owned-existing-logo');print(json.dumps(r.pk))");expected_url='/data/owned-existing-logo'
   if vod:
    expected_id=sql("from apps.vod.models import Movie,Series,VODLogo;a=VODLogo.objects.create(name='Owned Movie logo',url='/data/owned-movie-logo');b=VODLogo.objects.create(name='Owned Series logo',url='/data/owned-series-logo');Movie.objects.bulk_create([Movie(name="+repr(title)+",logo=a)]);Series.objects.bulk_create([Series(name="+repr(title)+",logo=b)]);print(json.dumps(a.pk))")
   if reuse:
    expected_id=sql("from apps.channels.models import Logo;r=Logo.objects.create(name='Different owned name',url='https://artwork-owned.invalid/reused.jpg');print(json.dumps(r.pk))")
   start=datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(hours=2);end=start+datetime.timedelta(minutes=10);program={'title':title};expected={}
   if epg is not None:
    program['id']=sql("from apps.epg.models import EPGData,ProgramData;from datetime import datetime;import uuid;e=EPGData.objects.create(tvg_id='provider-'+uuid.uuid4().hex,name='Owned provider EPG');p=ProgramData.objects.create(epg=e,tvg_id=e.tvg_id,start_time=datetime.fromisoformat("+repr(start.isoformat())+"),end_time=datetime.fromisoformat("+repr(end.isoformat())+"),title="+repr(title)+",custom_properties="+repr(epg)+");print(json.dumps(p.pk))")
    expected.update({k:v for k,v in epg.items()if k in ('rating','rating_system','season','episode')})
   if direct is not None:program['poster']={'url':direct}
   existing_url_rows=[]
   if expected_url is not None and not vod:
    existing_url_rows=sql("from apps.channels.models import Logo;print(json.dumps(list(Logo.objects.filter(url="+repr(expected_url)+").values('id','name','url'))))")
   record['actual_existing_URL_rows_before']=existing_url_rows
   sequence_before=logo_sequence()if reuse or existing_url_rows else None

   answer=fixture.api('POST','/api/channels/recordings/',{'channel':fixture.channel_id,'start_time':start.isoformat(),'end_time':end.isoformat(),'custom_properties':{'program':program,'provider_fixture_case':label}})
   record['actual_HTTP']=answer
   if answer['status']!=201:raise AssertionError('ActualProviderBookingHTTP201')
   ident=answer['body']['id']
   def publication():
    path=fixture.case_data/'provider-publications.jsonl'
    if not path.exists():return None
    if path.stat().st_size>1048576:raise RuntimeError('OwnedProviderPublicationBudget')
    for line in path.read_text().splitlines():
     frame=json.loads(line);value=json.loads(frame['raw_frame']);args=json.loads(base64.b64decode(value['body']))[0]
     if value['headers']['task']=='apps.channels.tasks.prefetch_recording_artwork'and args==[ident]:return frame
   published=fixture.wait(publication,20,'actual-provider-artwork-producer:'+label);record['actual_publication']=published;task=published['headers']['id']
   def terminal():
    raw=fixture.redis(2,"return redis.call('GET',ARGV[1])or false",'celery-tasks:celery-task-meta-'+task)
    value=json.loads(raw)if raw else None
    return value if value and value.get('status')in('SUCCESS','FAILURE','REVOKED')else None
   backend=fixture.wait(terminal,75,'actual-provider-worker-result:'+label);record['actual_backend']=backend
   if backend['status']!='SUCCESS'or backend.get('result')!='ok':raise AssertionError('ActualProviderWorkerResult')
   actual=row(ident);record['actual_recording']=actual;cp=actual['custom_properties']or{}
   if expected_id is not None:expected['poster_logo_id']=expected_id
   if expected_url is not None:expected['poster_url']=expected_url
   if any(cp.get(k)!=v for k,v in expected.items()):raise AssertionError('ActualProviderMetadataMismatch')
   if expected_url is None and cp.get('poster_url'):raise AssertionError('UnexpectedProviderURL')
   if expected_url is not None and not vod:
    logos=sql("from apps.channels.models import Logo;print(json.dumps(list(Logo.objects.filter(url="+repr(expected_url)+").values('id','name','url'))))");record['actual_Logo_rows']=logos
    if len(logos)!=1 or logos[0]['id']!=cp.get('poster_logo_id'):raise AssertionError('ActualProviderLogoPersistence')
    if existing_url_rows:
     if len(existing_url_rows)!=1 or logos[0]!=existing_url_rows[0]:raise AssertionError('ExistingSourceLogoURLMustPreserveIdNameAndURL')
    elif logos[0]['name']!=title[:255]:raise AssertionError('ActualSourceLogoNameClamp')
   if reuse or existing_url_rows:
    record['actual_sequence_before']=sequence_before;record['actual_sequence_after']=logo_sequence()
    if record['actual_sequence_after']!=sequence_before:raise AssertionError('ExistingLogoLookupMustNotAdvanceSequence')
   after=control(fixture,'/snapshot');trace=after['trace'][position:];record['actual_provider_HTTP_trace']=trace
   actual=[(r.get('host'),r.get('method'),r.get('path'))for r in trace if r.get('actual_TLS')]
   if not actual and label!='vod':raise AssertionError('ActualProviderNetworkUnmeasured')
   if label in ('epg','vod')and any(host in ('api.themoviedb.org','www.omdbapi.com','api.tvmaze.com','itunes.apple.com')for host,_,_ in actual):raise AssertionError('HigherPriorityArtworkDidNotSuppressProvider')
   required={'tmdb-find':('api.themoviedb.org','/3/find/ttowned123'),'tmdb-search':('api.themoviedb.org','/3/search/multi'),'omdb':('www.omdbapi.com','/'),'tvmaze':('api.tvmaze.com','/singlesearch/shows'),'itunes':('itunes.apple.com','/search')}
   if label in required and not any((h,p)==required[label]for h,_,p in actual):raise AssertionError('RequiredActualProviderStageMissing')
   if label=='direct' and [(m,p)for h,m,p in actual if h=='artwork-owned.invalid']!=[('HEAD','/range-only.jpg'),('GET','/range-only.jpg')]:raise AssertionError('ActualHEAD405RangedGETRequired')
   if label=='direct-cache'and any(h=='artwork-owned.invalid'for h,_,_ in actual):raise AssertionError('ActualValidationCacheHitMustSuppressHEADAndGET')
   if label in ('existing-logo','channel-logo','caught-errors')and not {'api.tvmaze.com','itunes.apple.com'}.issubset({h for h,_,_ in actual}):raise AssertionError('NoImageRequiresActualKeylessAttempts')
   emitted=module.events(fixture,subscription);updates=[e for e in emitted if e.get('data',{}).get('type')=='recording_updated'and e.get('data',{}).get('recording_id')==ident];record['actual_update_events']=updates
   if not updates:raise AssertionError('ActualProviderMetadataEventMissing')
   record['status']='passed'
  except Exception as error:
   private=out/'private';private.mkdir(mode=0o700,exist_ok=True);path=private/(label+'-controller-error.txt');path.write_text(repr(error));path.chmod(0o600);record.update(status='failed',error_type=type(error).__name__,private_error=str(path))
  finally:
   if ident is not None:
    try:
     result=fixture.api('DELETE',f'/api/channels/recordings/{ident}/');record['actual_cleanup_HTTP']=result
     if result['status']!=204:record['status']='failed'
    except Exception as error:record.update(status='failed',cleanup_error=type(error).__name__)
   record['ended_epoch']=time.time();save(out/(label+'.json'),record)
 try:
  case('epg','https://artwork-owned.invalid/best-poster',epg={'images':[{'type':'icon','size':'xxl','url':'https://artwork-owned.invalid/lower-priority'},{'type':'poster','size':'m','url':'https://artwork-owned.invalid/best-poster'}],'rating':'PG','season':0})
  case('vod',vod=True)
  case('tmdb-find','https://image.tmdb.org/t/p/w780/owned-find-first.jpg',epg={'date':'2024-01-01','imdb.com_id':'ttowned123'})
  case('tmdb-search','https://image.tmdb.org/t/p/w780/owned-search-first.jpg',epg={'date':'2024-01-01'})
  case('omdb','https://artwork-owned.invalid/omdb.jpg',epg={'imdb.com_id':'ttowned123'})
  case('tvmaze','https://artwork-owned.invalid/tvmaze-original.jpg')
  case('itunes','https://artwork-owned.invalid/600x600bb.jpg')
  case('direct','https://artwork-owned.invalid/range-only.jpg',direct='https://artwork-owned.invalid/range-only.jpg')
  case('direct-cache','https://artwork-owned.invalid/range-only.jpg',direct='https://artwork-owned.invalid/range-only.jpg',scenario='direct')
  case('existing-logo',existing=True)
  case('channel-logo',channel=True)
  case('caught-errors',channel=True)
  case('logo-reuse','https://artwork-owned.invalid/reused.jpg',reuse=True)
 finally:
  if 'provider-broker-observer'in fixture.roles:
   try:fixture.role('provider-broker-observer',[],stop=True)
   except Exception as error:cells.append({'name':'actual-observer-stop','status':'failed','error_type':type(error).__name__})
 expected_names=['direct','direct-cache']if declared is not None else ['epg','vod','tmdb-find','tmdb-search','omdb','tvmaze','itunes','direct','direct-cache','existing-logo','channel-logo','caught-errors','logo-reuse']
 measured_names=[c['name']for c in cells if c.get('name')in expected_names]
 report={'declared_required_cells':expected_names,'full_provider_matrix_executed':declared is None,'status':'passed'if measured_names==expected_names and all(c['status']=='passed'for c in cells)else'failed','cells':cells,'actual_owned_TLS_origin':fixture._provider_origin_ready,'new_Source_provider_cases':fixture.backend=='Original','Source_old_Core_replayed':False,'full_milestone_passed':False,'performance_verified':False,'unmeasured':['held provider HTTP/SQL physical resource controls','validation TTL expiry and multiple live distinct writer saturation','oversized body/cookie/CA boundary Source characterization','nonfinite/charset/unsupported provider shape domains']};save(out/'report.json',report);return report
