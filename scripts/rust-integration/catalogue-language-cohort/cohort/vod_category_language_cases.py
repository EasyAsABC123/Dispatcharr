"""Private Source category-language lifecycle11 characterization; not executed.
Original Dispatcharr and dependency authors retain all application/model credit.
Only actual HTTP/admin-tag API/claimed roots; the explicit descendant seed is fixture setup.
"""
import copy,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
import vod_dense_list_cases as dense
import vod_shared_metadata_cases as shared
from vod_series_advanced_cases import snapshot
from vod_series_info_cases import instant
from provider_cases import control
CASE='vod-category-language-lifecycle'
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
STAGES=(('language-A-initial','A'),('language-B-shared','B'),('language-A-en','A'),('language-A-fr','A'),('language-A-clear','A'),('language-A-en-again','A'),('language-B-fr','B'),('language-A-claim-fr','A'),('language-repeat','A'),('language-A-prune','A'),('language-B-prune','B'))
TAGS={'language-A-en':'EN','language-A-fr':'fr','language-A-clear':None,'language-A-en-again':'en','language-B-fr':'FR','language-A-claim-fr':'fr'}

def payloads():
 def content(empty=False):
  return {'get_vod_categories':[{'category_id':'11','category_name':'Owned Language Movies'}],'get_series_categories':[{'category_id':'21','category_name':'Owned Language Series'}],'get_vod_streams':[]if empty else[{'stream_id':9401,'name':'Owned Language Movie','year':2010,'category_id':'11','tmdb_id':'96001','description':'Owned movie baseline','container_extension':'mp4'}],'get_series':[]if empty else[{'series_id':9501,'name':'Owned Language Series','releaseDate':'2010-01-02','category_id':'21','tmdb':'97001','plot':'Owned series baseline'}]}
 values={label:content(label.endswith('prune'))for label,_ in STAGES}
 for label,owner in STAGES:
  if owner=='A'and label!='language-A-initial':
   for row in values[label]['get_vod_streams']:row.pop('description',None)
   for row in values[label]['get_series']:row.pop('plot',None)
 return values

def prepare_context(fixture):
 if fixture.case!=CASE:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_dense_list_cases':payloads(),'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json'}
 # Only a key HASH is written into the /data bind. CA/leaf private keys are
 # generated in the Origin's private Source-side tmpdir, never exported.
 fixture.python("import json,os;from pathlib import Path;p=Path("+repr(guest+'/provider-origin-config.json')+");p.write_text("+repr(json.dumps(config))+");os.chown(p,65532,65532);p.chmod(0o600)",secret=True)
 fixture.role('recording-provider-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/provider_origin.py'],extra={'OWNED_PROVIDER_CONFIG':guest+'/provider-origin-config.json'})
 fixture.wait(lambda:(fixture.case_data/'provider-origin.ready').exists(),20,'actual-owned-provider-TLS-origin-ready')
 ready=json.loads((fixture.case_data/'provider-origin.ready').read_text());ca=fixture.case_data/'provider-ca.pem'
 if hashlib.sha256(ca.read_bytes()).hexdigest()!=ready['actual_CA_sha256']or ready['private_keys_exported']:raise AssertionError('ActualOwnedCAProof')
 ca_path='/data/provider-ca.pem'if fixture.backend=='Native'else guest+'/provider-ca.pem'
 fixture._provider_environment={'HTTPS_PROXY':'http://127.0.0.1:'+str(PORT),'https_proxy':'http://127.0.0.1:'+str(PORT),'HTTP_PROXY':'http://127.0.0.1:'+str(PORT),'http_proxy':'http://127.0.0.1:'+str(PORT),'ALL_PROXY':'http://127.0.0.1:'+str(PORT),'all_proxy':'http://127.0.0.1:'+str(PORT),'NO_PROXY':'127.0.0.1,localhost','no_proxy':'127.0.0.1,localhost','REQUESTS_CA_BUNDLE':ca_path,'TMDB_API_KEY':key,'OMDB_API_KEY':key}
 fixture._provider_environment={name:value for name,value in fixture._provider_environment.items()if name not in('TMDB_API_KEY','OMDB_API_KEY')}
 fixture._provider_origin_ready=ready
 # The SAME pinned Source Requests/Python stack must actually trust the new
 # Origin before any Source or Native business worker starts. This is a
 # transport-only probe, no Django/model/task/API invocation or verification bypass.
 proof_environment={key:value for key,value in fixture._provider_environment.items()if key not in('TMDB_API_KEY','OMDB_API_KEY')};proof_environment['REQUESTS_CA_BUNDLE']=guest+'/provider-ca.pem'
 proof_script="import os,json,hashlib,ssl,re;os.environ.update("+repr(proof_environment)+");import requests,urllib3;from urllib3.util.ssl_ import create_urllib3_context;s=requests.Session();receipt={'requests_version':requests.__version__,'urllib3_version':urllib3.__version__,'openssl_version':ssl.OPENSSL_VERSION,'verify_enabled':s.verify is True,'source_context_verify_flags':int(create_urllib3_context().verify_flags),'actual_CA_sha256':hashlib.sha256(open(os.environ['REQUESTS_CA_BUNDLE'],'rb').read()).hexdigest(),'status':None,'body_sha256':None,'errors':[]};\ntry:\n r=s.get('https://artwork-owned.invalid/__owned_tls_probe',timeout=(5,5));body=r.content;receipt.update(status=r.status_code,body_sha256=hashlib.sha256(body).hexdigest(),actual_bytes=len(body));receipt['passed']=r.status_code==200 and body==b'owned-source-TLS-trust-proof'\nexcept BaseException as error:\n pending=[error];seen=set();receipt['passed']=False\n while pending and len(seen)<16:\n  item=pending.pop()\n  if id(item)in seen:continue\n  seen.add(id(item));entry={'type':type(item).__name__};reason=getattr(item,'reason',None);code=getattr(item,'verify_code',None);message=getattr(item,'verify_message',None)\n  if isinstance(reason,str)and re.fullmatch('[A-Z0-9_]{1,96}',reason):entry['actual_SSL_reason']=reason\n  if isinstance(code,int):entry['actual_verify_code']=code\n  if isinstance(message,str):entry['verify_message_sha256']=hashlib.sha256(message.encode()).hexdigest()\n  receipt['errors'].append(entry)\n  for value in [getattr(item,'__cause__',None),getattr(item,'__context__',None),reason,*getattr(item,'args',())]:\n   if isinstance(value,BaseException):pending.append(value)\nfinally:s.close()\nprint(json.dumps(receipt))"
 trust=json.loads(fixture.python(proof_script,seconds=15,secret=True));save(Path(fixture.output)/'actual-provider-Source-Requests-trust-preflight.json',trust)
 if trust.get('passed')is not True or not trust['verify_enabled']or trust['actual_CA_sha256']!=ready['actual_CA_sha256']:raise AssertionError('ActualPinnedSourceRequestsTrustPreflight')
 _prepare_rows(fixture)

def _prepare_rows(f):
 shared._prepare_rows(f)

def raw(value):return {k:v for k,v in value.items()if k!='observed_at'}

def tag_delta(f,private,label,role,language,previous):
 p=private/(label+'-tag-API');p.mkdir(mode=0o700);before=snapshot(f)
 assert raw(before)==raw(previous),'Previous root/seed to tag boundary exact'
 cache=aggregate.cache_state(f);provider=control(f,'/snapshot');broker=aggregate.broker(f);receiver=aggregate.receiver_begin(f,label+'-tag');import vod_category_language_compare as receiver_compare;save(p/'receiver-begin-proof.json',receiver_compare.capture_receiver(f,receiver));observer=journal.observer_start(f,label+'-tag',database=1)
 account=f._shared_setup[role];rows=[r for r in before['support']['vod_m3uvodcategoryrelation']if r['m3u_account_id']==account and r['category_id']in {r['category_id']for table in ['vod_m3umovierelation','vod_m3useriesrelation']for r in before[table]if r['m3u_account_id']==account}]
 assert len(rows)==2
 request={'category_settings':[{'id':r['category_id'],'enabled':True,'custom_properties':{'language':language}}for r in rows]}
 response=None
 try:response=f.api('PATCH','/api/m3u/accounts/'+str(account)+'/group-settings/',request)
 finally:
  after=snapshot(f);events=aggregate.receiver_read(f,receiver);publications=journal.producer_observer_stop(f,observer)
  for name,value in [('SQL-before',before),('SQL-after',after),('request',request),('HTTP',response),('WS',events),('journal',publications),('cache-before',cache),('cache-after',aggregate.cache_state(f)),('provider-before',provider),('provider-after',control(f,'/snapshot')),('broker-before',broker),('broker-after',aggregate.broker(f))]:save(p/(name+'.json'),value)
 assert response=={'context':f.case,'method':'PATCH','path':'/api/m3u/accounts/'+str(account)+'/group-settings/','credential_role':'admin','status':200,'body':{'message':'Group settings updated successfully'}}
 expected=copy.deepcopy(before);expected['observed_at']=after['observed_at'];ids={r['id']for r in rows}
 for r in expected['support']['vod_m3uvodcategoryrelation']:
  if r['id']in ids:r['custom_properties']={**(r['custom_properties']or{}),'language':language.lower()if isinstance(language,str)else None};r['enabled']=True
 seq='public.vod_m3uvodcategoryrelation_id_seq';old=expected['sequences'][seq];old['last_value']+=len(rows)if old['is_called']else len(rows)-1;old['is_called']=True
 assert expected==after,'Only category CP plus ordinary conflict-upsert sequence changes'
 assert cache==aggregate.cache_state(f)and provider==control(f,'/snapshot')and broker==aggregate.broker(f)and not publications['publications']
 return after

def seed_descendant(f,private,previous):
 p=private/'declared-after-B-seed';p.mkdir(mode=0o700);before=snapshot(f);assert raw(before)==raw(previous)
 cache=aggregate.cache_state(f);provider=control(f,'/snapshot');broker=aggregate.broker(f)
 code="""
from apps.vod.models import Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation
from django.db import transaction
from django.utils import timezone
with transaction.atomic():
 assert Movie.objects.count()==Series.objects.count()==1
 assert not Episode.objects.exists()and not M3UEpisodeRelation.objects.exists()
 relations=list(M3USeriesRelation.objects.order_by('m3u_account_id'));assert len(relations)==2 and relations[0].series_id==relations[1].series_id
 ep=Episode.objects.create(name='Owned Language Episode',description='Declared two-account language ownership',series_id=relations[0].series_id,season_number=1,episode_number=1,rating='7.0',duration_secs=60,tmdb_id=None,imdb_id=None,custom_properties={'fixture':'language-owned'})
 orphan=Episode.objects.create(name='Owned Language Empty Episode',series_id=relations[0].series_id,season_number=1,episode_number=2,tmdb_id=None,imdb_id=None,custom_properties={'fixture':'language-empty'})
 created=[]
 for rel in relations:
  obj=M3UEpisodeRelation.objects.create(m3u_account_id=rel.m3u_account_id,episode=ep,series_relation=rel,stream_id='owned-language-episode-'+str(rel.m3u_account_id),container_extension='mkv',last_seen=timezone.now(),custom_properties={'fixture':'language-owned'})
  created.append(obj.pk)
 for Model in [M3UMovieRelation,M3USeriesRelation]:
  for rel in Model.objects.all():
   cp={**(rel.custom_properties or {}),'detailed_fetched':True}
   if Model is M3USeriesRelation:cp['episodes_fetched']=True
   Model.objects.filter(pk=rel.pk).update(custom_properties=cp)
print(json.dumps({'fixture_only':True,'Episode':ep.pk,'OrphanEpisode':orphan.pk,'EpisodeRelations':created,'Series':ep.series_id,'declared_CP_flags':True}))
"""
 save(p/'seed-script.json',{'sha256':hashlib.sha256(code.encode()).hexdigest(),'scope':'Two Episodes (one empty),two EpisodeRelations and fetched flags on existing Movie/Series relations; only two automatic sequences; no task/reconcile calls'})
 result=f.sql(code,secret=True);after=snapshot(f)
 for name,value in [('SQL-before',before),('SQL-after',after),('setup-result',result),('cache-before',cache),('cache-after',aggregate.cache_state(f)),('provider-before',provider),('provider-after',control(f,'/snapshot')),('broker-before',broker),('broker-after',aggregate.broker(f))]:save(p/(name+'.json'),value)
 expected=copy.deepcopy(before);expected['observed_at']=after['observed_at']
 for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
  for rel in expected[table]:
   rel['custom_properties']={**(rel['custom_properties']or{}),'detailed_fetched':True}
   if table=='vod_m3useriesrelation':rel['custom_properties']['episodes_fetched']=True
 for table,count in [('vod_episode',2),('vod_m3uepisoderelation',2)]:
  expected[table]=after[table];assert len(after[table])==count
  for obj in after[table]:
   for field in ['created_at','updated_at']+(['last_seen']if table=='vod_m3uepisoderelation'else[]):assert instant(before['observed_at'])<=instant(obj[field])<=instant(after['observed_at'])
  key='public.'+table+'_id_seq';seq=expected['sequences'][key];seq['last_value']+=count if seq['is_called']else count-1;seq['is_called']=True
 for ep in after['vod_episode']:
  shared_episode=ep['id']==result['Episode'];assert ep['id']==result['Episode']if shared_episode else ep['id']==result['OrphanEpisode']
  business={k:v for k,v in ep.items()if k not in ['id','uuid','created_at','updated_at']}
  assert business=={'name':'Owned Language Episode'if shared_episode else'Owned Language Empty Episode','description':'Declared two-account language ownership'if shared_episode else None,'air_date':None,'rating':'7.0'if shared_episode else None,'duration_secs':60 if shared_episode else None,'series_id':before['vod_series'][0]['id'],'season_number':1,'episode_number':1 if shared_episode else 2,'tmdb_id':None,'imdb_id':None,'custom_properties':{'fixture':'language-owned'if shared_episode else'language-empty'}},'Every declared Episode business field exact'
 for er in after['vod_m3uepisoderelation']:
  sr=next(r for r in before['vod_m3useriesrelation']if r['m3u_account_id']==er['m3u_account_id'])
  assert {k:v for k,v in er.items()if k not in ['id','created_at','updated_at','last_seen']}=={'m3u_account_id':sr['m3u_account_id'],'episode_id':result['Episode'],'series_relation_id':sr['id'],'stream_id':'owned-language-episode-'+str(sr['m3u_account_id']),'container_extension':'mkv','custom_properties':{'fixture':'language-owned'}},'Every declared EpisodeRelation business field exact'
 assert expected==after,'Exact seed delta only'
 assert all(uuid.UUID(ep['uuid']).version==4 for ep in after['vod_episode'])
 assert {r['m3u_account_id']for r in after['vod_m3uepisoderelation']}=={f._shared_setup['A'],f._shared_setup['B']}
 for er in after['vod_m3uepisoderelation']:
  sr=next(r for r in after['vod_m3useriesrelation']if r['id']==er['series_relation_id']);assert sr['m3u_account_id']==er['m3u_account_id']and sr['series_id']==after['vod_episode'][0]['series_id']and er['episode_id']==result['Episode']
 assert cache==aggregate.cache_state(f)and provider==control(f,'/snapshot')and broker==aggregate.broker(f)
 f._language_seed=after;return after

def check_phase(f,label,selected,before,after):
 assert len(before['sequences'])==len(after['sequences'])==11 and before['m3u_m3uaccount']==after['m3u_m3uaccount']
 assert before['support']['core_systemevent']==after['support']['core_systemevent']and not after['support']['vod_vodlogo']
 for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
  for rel in after[table]:
   old=next((r for r in before[table]if r['id']==rel['id']),None)
   if old:
    for field in ['id','created_at','updated_at']:assert rel[field]==old[field]
    if rel['m3u_account_id']!=selected:assert rel['last_seen']==old['last_seen']
    field='movie_id'if table=='vod_m3umovierelation'else'series_id';expected=copy.deepcopy(old);expected['last_seen']=rel['last_seen'];expected[field]=rel[field]
    if rel['m3u_account_id']==selected:
     action='get_vod_streams'if table=='vod_m3umovierelation'else'get_series';basic=copy.deepcopy(payloads()[label][action][0]);basic['_provider_category_id']=basic['category_id'];basic['_category_id']=rel['category_id'];expected['custom_properties']={**(expected['custom_properties']or{}),'basic_data':basic}
    if old[field]!=rel[field]:
     expected['custom_properties']={**(expected['custom_properties']or{}),'detailed_fetched':False}
     if table=='vod_m3useriesrelation':expected['custom_properties']['episodes_fetched']=False
    assert expected==rel,'Only selected last_seen and reconciliation FK/fetched flags may change'
   if rel['m3u_account_id']==selected:assert instant(before['observed_at'])<=instant(rel['last_seen'])<=instant(after['observed_at'])
 for table in ['vod_movie','vod_series','vod_episode']:
  old={r['id']:r for r in before[table]}
  for obj in after[table]:
   if obj['id']in old:
    for field in ['uuid','created_at','updated_at']:assert obj[field]==old[obj['id']][field]
   else:
    assert uuid.UUID(obj['uuid']).version==4
    for field in ['created_at','updated_at']:assert instant(before['observed_at'])<=instant(obj[field])<=instant(after['observed_at'])
 for er in after['vod_m3uepisoderelation']:
  old=next((r for r in before['vod_m3uepisoderelation']if r['id']==er['id']),None)
  if old:assert er=={**old,'episode_id':er['episode_id']},'Existing EpisodeRelation only reassigns episode FK'
 if label not in ['language-A-initial','language-B-shared','language-A-prune','language-B-prune']:
  for table,rel,field in [('vod_movie','vod_m3umovierelation','movie_id'),('vod_series','vod_m3useriesrelation','series_id')]:
   old={owner:next(r for r in before[rel]if r['m3u_account_id']==f._shared_setup[owner])[field]for owner in ['A','B']};new={owner:next(r for r in after[rel]if r['m3u_account_id']==f._shared_setup[owner])[field]for owner in ['A','B']}
   if label in ['language-A-en','language-A-en-again']:assert new['B']==old['B']and new['A']!=new['B']and new['A']>new['B']
   elif label in ['language-A-clear','language-A-claim-fr']:
    assert new['A']==old['A']==new['B']and old['B']not in {r['id']for r in after[table]}
    oldA=next(r for r in before[table]if r['id']==old['A']);oldB=next(r for r in before[table]if r['id']==old['B']);survivor=next(r for r in after[table]if r['id']==new['A']);assert oldA['description']is None and oldB['description']and survivor['description']==oldB['description'],'Blank survivor metadata copied from actual owned donor'
   else:assert old==new
  if label=='language-A-en':
   for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
    for rel in after[table]:
     expected=rel['m3u_account_id']==f._shared_setup['B'];assert rel['custom_properties']['detailed_fetched']is expected
     if table=='vod_m3useriesrelation':assert rel['custom_properties']['episodes_fetched']is expected
   empty=f._language_seed['vod_episode'][1]['id'];assert all(ep['id']!=empty for ep in after['vod_episode'])
 if label=='language-B-prune':
  assert all(not after[t]for t in ['vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation']);return
 if label=='language-A-prune':
  for t in ['vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation']:assert len(after[t])==1 and after[t][0]['m3u_account_id']==f._shared_setup['B']
  assert len(after['vod_movie'])==len(after['vod_series'])==len(after['vod_episode'])==1;return
 langs={'language-A-initial':[''],'language-B-shared':[''],'language-A-en':['','en'],'language-A-fr':['','fr'],'language-A-clear':[''],'language-A-en-again':['','en'],'language-B-fr':['en','fr'],'language-A-claim-fr':['fr'],'language-repeat':['fr']}[label]
 for table in ['vod_movie','vod_series']:assert sorted(r['language']for r in after[table])==langs
 for er in after['vod_m3uepisoderelation']:
  sr=next(r for r in after['vod_m3useriesrelation']if r['id']==er['series_relation_id']);ep=next(r for r in after['vod_episode']if r['id']==er['episode_id']);assert sr['series_id']==ep['series_id']and sr['m3u_account_id']==er['m3u_account_id']
 if label in ['language-A-en','language-A-en-again']:assert len(after['vod_episode'])==2 and len(after['vod_m3uepisoderelation'])==2
 if label in ['language-A-en','language-A-en-again']:
  for table in ['vod_movie','vod_series']:
   new=next(r for r in after[table]if r['language']=='en');assert new['description']is None and new['rating']is None and new['genre']is None and new['custom_properties']is None,'Source language split creates minimal identity only'
 if label not in ['language-A-initial','language-B-shared']:
  original=f._language_seed['vod_episode'][0];retained=next(r for r in after['vod_episode']if r['id']==original['id']);assert retained['uuid']==original['uuid']
  if label in ['language-A-en','language-A-en-again']:
   ar=next(r for r in after['vod_m3useriesrelation']if r['m3u_account_id']==f._shared_setup['A']);assert retained['series_id']==ar['series_id'],'Episode equal-ownership tie follows higher new Series ID'
 if label in ['language-A-clear','language-A-claim-fr','language-repeat']:assert len(after['vod_episode'])==1 and len(after['vod_m3uepisoderelation'])==2
 if label=='language-repeat':
  for t in ['vod_movie','vod_series','vod_episode','vod_m3uepisoderelation']:assert before[t]==after[t]

def run(f):
 assert f.backend in('Original','Native')and f.case==CASE
 out=f.output/CASE;out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','Source_characterization':f.backend=='Original','Native_qualified':False,'cases':[],'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 previous=None;observer=None
 try:
  for label,role in STAGES:
   selected=f._shared_setup[role];account_name=f._shared_setup[role+'_name']
   row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
   p=private/label;p.mkdir(mode=0o700);f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
   if label in TAGS:previous=tag_delta(f,private,label,role,TAGS[label],previous)
   before=snapshot(f);cache=aggregate.cache_state(f);trace_start=len(control(f,'/snapshot')['trace']);control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,CASE+'-'+label,database=1)
   api=f.api('POST','/api/m3u/accounts/'+str(selected)+'/refresh-vod/',{});save(p/'HTTP.json',api)
   frame=None;backend=None;error=None
   try:
    if api['status']==202:
     def published():
      frames=[r for r in aggregate.frames(observer)if r['headers']['task']==TASK];assert len(frames)<=1
      return frames[0]if frames else False
     frame=f.wait(published,15,label+'-actual-root');backend=f.wait(lambda:aggregate.terminal(f,frame['headers']['id']),120,label+'-actual-terminal')
     f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-actual-drain')
   except BaseException as caught:error=caught
   finally:
    durable=journal.producer_observer_stop(f,observer);observer=None
    after=snapshot(f);events=aggregate.receiver_read(f,receiver);trace=control(f,'/snapshot')['trace'][trace_start:]
    catalogue={kind:f.api('GET','/api/vod/'+kind+'/?page_size=100')for kind in ['movies','series']}
    for name,value in [('SQL-before',before),('SQL-after',after),('WS',events),('Origin',trace),('journal',durable),('backend',backend),('frame',frame),('catalogue',catalogue),('cache-before',cache),('cache-after',aggregate.cache_state(f))]:save(p/(name+'.json'),value)
   if error is not None:raise error
   assert api['status']==202 and api['body']=={'message':'VOD refresh initiated for account '+account_name}
   if previous is not None:assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in previous.items()if k!='observed_at'},'Unchanged raw phase chain'
   ident=frame['headers']['id'];assert json.loads(__import__('base64').b64decode(frame['body']))==[[selected],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
   assert backend['status']=='SUCCESS'and backend['children']==[]
   match=re.fullmatch(re.escape('Batch VOD refresh completed for account '+account_name+' in ')+r'([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match
   assert [(r['task'],r['id'])for r in durable['publications']]==[(TASK,ident)]
   requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r];assert len(requests)==5
   for actual,action in zip(requests,[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']):
    expected={k:[hashlib.sha256(v.encode()).hexdigest()]for k,v in f._vod_login.items()}
    if action is not None:expected['action']=[hashlib.sha256(action.encode()).hexdigest()]
    assert actual['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==expected and actual['actual_TLS']and actual['status']==200 and actual['actual_user_agent']==f._vod_setup['actual_user_agent']
   updates=[r['data']for r in events['events']if isinstance(r.get('data'),dict)and r['data'].get('account')==selected]
   assert updates==[{'progress':0,'type':'m3u_refresh','account':selected,'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':selected,'action':'vod_refresh','status':'success','message':'VOD refresh completed in '+match[1]+' seconds'}]
   assert cache==aggregate.cache_state(f)
   for kind,table in [('movies','vod_movie'),('series','vod_series')]:assert catalogue[kind]['status']==200 and catalogue[kind]['body']['count']==len(after[table])
   row.update(actual_root=ident,actual_provider_requests=5,actual_queued_children=0,Source_observation_complete=True)
   try:
    check_phase(f,label,selected,before,after);row['status']='passed'
   except AssertionError as guessed:
    row.update(status='failed',semantic_expectation_failure=type(guessed).__name__);(p/'semantic-expectation-failure.txt').write_text(traceback.format_exc()[-16384:])
   previous=seed_descendant(f,private,after)if label=='language-B-shared'else after;save(out/'report.json',report)
 except BaseException as caught:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(caught).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except BaseException as caught:report['observer_cleanup_error']=type(caught).__name__
 report['all_eleven_context_observations_complete']=len(report['cases'])==len(STAGES) and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==len(STAGES) and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   from vod_category_language_compare import compare_retained_Source
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',Source_comparison_failure=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report
