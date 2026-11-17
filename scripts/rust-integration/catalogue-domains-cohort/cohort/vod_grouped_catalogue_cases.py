"""Private grouped international catalogue and ordered identity Source characterization; not executed.
Original Dispatcharr contributors and dependency authors retain all credit.
Two independent contexts on one bootstrap; actual HTTP/claimed workers only. International roots retain a continuous model chain; ordered identity roots declare resets.
"""
import copy,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
from vod_series_advanced_cases import snapshot
from vod_series_info_cases import instant
from provider_cases import control
CASES=('vod-international-catalogue','vod-ordered-title-identity')
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
STAGES={CASES[0]:('intl-fresh','intl-repeat','intl-replace','intl-sparse','intl-prune'),CASES[1]:('order-cross-key-forward','order-cross-key-reverse','order-overlapping-target-forward','order-overlapping-target-reverse','order-secondary-rebind-original','order-movie-conflict-series-success','order-series-conflict-movie-success')}
def _prepare_rows(f):base_series._prepare_rows(f)
def movie(stream,name,**extra):return {'stream_id':stream,'name':name,'year':2010,'category_id':'11','container_extension':'mp4',**extra}
def series(external,name,**extra):return {'series_id':external,'name':name,'releaseDate':'2010-01-02','category_id':'21',**extra}
def response(m,s):return {'get_vod_categories':[{'category_id':'11','category_name':'Owned Movies'}],'get_vod_streams':m,'get_series_categories':[{'category_id':'21','category_name':'Owned Series'}],'get_series':s}
def international_rows():
 m=[movie(5101,'\u2003Été 東京\u00a0',year=None,releaseDate='\u20032011-09-19\u00a0'),movie(5102,'Film (٢٠١٢)',year=None),movie(5103,'Cinéma\u20031990',year=None),movie(5104,'界'*255,year=None),movie(5105,'Café'),movie(5106,'Cafe\u0301')]
 s=[series(6101,'\u2003Été Série 東京\u00a0',releaseDate='\u20032011-09-19\u00a0'),series(6102,'Série (٢٠١٢)',releaseDate=''),series(6103,'Série 三',releaseDate='\u20032013-01-02\u00a0'),series(6104,'語'*255,releaseDate=''),series(6105,'Café'),series(6106,'Cafe\u0301')]
 for index,row in enumerate(m[:4]):row.update(tmdb_id=['5101','5102','5103','映画-5104'][index],description='映画の説明一',genre='ドラマ',director=['\u2003監督 一\u00a0','unused'],actors=['\u2003俳優 一\u00a0','俳優 二'])
 for index,row in enumerate(s[:4]):row.update(tmdb=['6101','6102','6103','連続-6104'][index],plot='連続の説明一',genre='ドラマ',director=['\u2003監督 一\u00a0','unused'],cast=['\u2003俳優 一\u00a0','俳優 二'])
 return m,s

def payloads(case):
 if case==CASES[0]:
  m,s=international_rows();fresh=response(m,s);replace=copy.deepcopy(fresh)
  for row in replace['get_vod_streams'][:4]:row.update(description='映画の説明二',director='監督 二',actors=['俳優 三'])
  for row in replace['get_series'][:4]:row.update(plot='連続の説明二',director='監督 二',cast=['俳優 三'])
  # Deliberately omit metadata/dates but retain exact identity/name. Source
  # sparse merge should preserve prior model values and stable model stamps.
  sparse=copy.deepcopy(replace)
  for action in ['get_vod_streams','get_series']:
   for row in sparse[action]:
    for key in ['description','plot','genre','director','actors','cast','releaseDate']:
     # The two no-ID Series retain their date identity; only the external-ID
     # rows omit dates in this sparse metadata-stability workflow.
     if action=='get_series'and key=='releaseDate'and not row.get('tmdb'):continue
     row.pop(key,None)
  prune=response([copy.deepcopy(sparse['get_vod_streams'][3])],[copy.deepcopy(sparse['get_series'][3])])
  return {'intl-fresh':fresh,'intl-repeat':copy.deepcopy(fresh),'intl-replace':replace,'intl-sparse':sparse,'intl-prune':prune}
 if case!=CASES[1]:raise ValueError('GroupedOwnedContextRequired')
 forward=response([movie(9701,'Movie First Key',tmdb_id='7101',description='first key'),movie(9701,'Movie Second Key',tmdb_id='7102',description='second key')],[series(9801,'Series First Key',tmdb='8101',plot='first key'),series(9801,'Series Second Key',tmdb='8102',plot='second key')]);reverse=copy.deepcopy(forward)
 reverse['get_vod_streams'].reverse();reverse['get_series'].reverse()
 overlap=response([movie(9701,'Movie Primary',tmdb_id='7101',description='primary first'),movie(9702,'Movie Secondary',imdb_id='tt7101',description='secondary second')],[series(9801,'Series Primary',tmdb='8101',plot='primary first'),series(9802,'Series Secondary',imdb='tt8101',plot='secondary second')]);overlap_reverse=copy.deepcopy(overlap)
 overlap_reverse['get_vod_streams'].reverse();overlap_reverse['get_series'].reverse()
 mc=[movie(9701,'Movie Conflict One',tmdb_id='7301',imdb_id='tt-conflict'),movie(9702,'Movie Conflict Two',tmdb_id='7302',imdb_id='tt-conflict')];sc=[series(9801,'Series Conflict One',tmdb='8301',imdb='tt-series-conflict'),series(9802,'Series Conflict Two',tmdb='8302',imdb='tt-series-conflict')]
 return {'order-cross-key-forward':forward,'order-cross-key-reverse':reverse,'order-overlapping-target-forward':overlap,'order-overlapping-target-reverse':overlap_reverse,'order-secondary-rebind-original':response([movie(9701,'Movie New Candidate',tmdb_id='7199',imdb_id='tt7101',description='candidate metadata')],[series(9801,'Series New Candidate',tmdb='8199',imdb='tt8101',plot='candidate metadata')]),'order-movie-conflict-series-success':response(mc,[series(9801,'Series Survives Movie Conflict',tmdb='8101')]),'order-series-conflict-movie-success':response([movie(9701,'Movie Survives Series Conflict',tmdb_id='7101')],sc)}

def prepare_context(fixture):
 if fixture.case not in CASES:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_dense_list_cases':payloads(fixture.case),'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json'}
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

def seed_spec(label):
 def row(name,tmdb=None,imdb=None,stream=None,year=2010):return {'name':name,'year':year,'tmdb_id':tmdb,'imdb_id':imdb,'stream':stream}
 if label in ['order-overlapping-target-forward','order-overlapping-target-reverse']:
  return {'movie':[row('Movie Persisted Target','7101','tt7101','9701')],'series':[row('Series Persisted Target','8101','tt8101','9801')]}
 if label=='order-secondary-rebind-original':return {'movie':[row('Movie IMDb Recheck Target',imdb='tt7101',stream='9701',year=2009)],'series':[row('Series IMDb Recheck Target',imdb='tt8101',stream='9801',year=2009)]}
 return {'movie':[],'series':[]}

def prepare_cell(f,private,label,previous):
 p=private/(label+'-setup');p.mkdir(mode=0o700);before=snapshot(f);cache=aggregate.cache_state(f);provider=control(f,'/snapshot');broker=aggregate.broker(f)
 save(p/'SQL-before.json',before)
 if previous is not None:assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in previous.items()if k!='observed_at'},'Prior actual root to declared setup boundary'
 spec=seed_spec(label)
 code="""
from apps.vod.models import Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation
from django.db import transaction
from django.utils.dateparse import parse_datetime
spec=json.loads(SPEC);account=ACCOUNT;created={'movie':[],'series':[]}
with transaction.atomic():
 M3UEpisodeRelation.objects.all().delete();M3UMovieRelation.objects.all().delete();M3USeriesRelation.objects.all().delete();Episode.objects.all().delete();Movie.objects.all().delete();Series.objects.all().delete()
 for kind,Model,Relation,field in [('movie',Movie,M3UMovieRelation,'movie'),('series',Series,M3USeriesRelation,'series')]:
  for item in spec[kind]:
   obj=Model.objects.create(name=item['name'],year=item['year'],tmdb_id=item['tmdb_id'],imdb_id=item['imdb_id'],language='',description='seed metadata',rating='6.0',genre='',custom_properties={'fixture_marker':'external-id-seed'})
   record={'id':obj.pk,'name':obj.name,'tmdb_id':obj.tmdb_id,'imdb_id':obj.imdb_id}
   if item['stream']is not None:
    values={'m3u_account_id':account,field:obj,'custom_properties':{'fixture_marker':'external-id-seed','detailed_fetched':True},'last_seen':parse_datetime('2020-01-01T00:00:00+00:00')}
    if kind=='movie':values.update(stream_id=item['stream'],container_extension='mp4',last_advanced_refresh=parse_datetime('2020-01-02T00:00:00+00:00'))
    else:values.update(external_series_id=item['stream'],last_episode_refresh=parse_datetime('2020-01-02T00:00:00+00:00'));values['custom_properties']['episodes_fetched']=True
    relation=Relation.objects.create(**values);record['relation_id']=relation.pk
   created[kind].append(record)
print(json.dumps({'declared_fixture_only':True,'created':created,'no_manual_sequence_reset':True}))
""".replace('SPEC',repr(json.dumps(spec))).replace('ACCOUNT',str(f._vod_setup['account']))
 save(p/'script.json',{'sha256':hashlib.sha256(code.encode()).hexdigest(),'seed_spec':spec,'scope':'Only six VOD entity/relation tables and their automatic sequences; accounts/support tables untouched'})
 observed=f.sql(code,secret=True);after=snapshot(f)
 for name,value in [('SQL-before',before),('SQL-after',after),('setup-result',observed),('cache-before',cache),('cache-after',aggregate.cache_state(f)),('provider-before',provider),('provider-after',control(f,'/snapshot')),('broker-before',broker),('broker-after',aggregate.broker(f))]:save(p/(name+'.json'),value)
 left=copy.deepcopy(before);right=copy.deepcopy(after)
 for value in (left,right):
  value.pop('observed_at')
  for table in ['vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation']:value.pop(table);value['sequences'].pop('public.'+table+'_id_seq')
 assert left==right,'Unrelated account/support/sequence state exact during setup'
 assert cache==aggregate.cache_state(f)and provider==control(f,'/snapshot')and broker==aggregate.broker(f),'Fixture setup emits no task/API/provider/cache effect'
 return after,observed

def assert_cell(label,before,after,setup):
 assert len(before['sequences'])==len(after['sequences'])==11
 assert before['m3u_m3uaccount']==after['m3u_m3uaccount']and before['support']['core_systemevent']==after['support']['core_systemevent']
 assert not after['vod_episode']and not after['vod_m3uepisoderelation']and not after['support']['vod_vodlogo']
 for table in ['vod_movie','vod_series','vod_m3umovierelation','vod_m3useriesrelation']:
  old={r['id']:r for r in before[table]}
  for row in after[table]:
   if row['id']in old:
    for key in ['created_at','updated_at']:assert row[key]==old[row['id']][key]
    if 'uuid'in row:assert row['uuid']==old[row['id']]['uuid']
   else:
    for key in ['created_at','updated_at']:assert instant(before['observed_at'])<=instant(row[key])<=instant(after['observed_at'])
    if 'uuid'in row:assert uuid.UUID(row['uuid']).version==4
   if 'last_seen'in row:assert instant(before['observed_at'])<=instant(row['last_seen'])<=instant(after['observed_at'])
 if label.startswith('intl-'):
  count=1 if label=='intl-prune'else 6
  assert len(after['vod_movie'])==len(after['vod_series'])==len(after['vod_m3umovierelation'])==len(after['vod_m3useriesrelation'])==count
  for table in ['vod_movie','vod_series']:
   assert all(r['language']==''for r in after[table])
   if label!='intl-prune':assert {'Café','Cafe\u0301'}<=set(r['name']for r in after[table]),'No implicit Unicode normalization'
  if label in ['intl-fresh','intl-repeat']:
   movies={r['tmdb_id']:r for r in after['vod_movie']if r['tmdb_id']is not None};series={r['tmdb_id']:r for r in after['vod_series']if r['tmdb_id']is not None}
   assert {key:r['year']for key,r in movies.items()}=={'5101':2011,'5102':2012,'5103':1990,'映画-5104':None}
   assert {key:r['year']for key,r in series.items()}=={'6101':2011,'6102':None,'6103':2013,'連続-6104':None}
   assert movies['5101']['name']=='Été 東京'and series['6101']['name']=='Été Série 東京'
   assert len(movies['映画-5104']['name'])==len(series['連続-6104']['name'])==255
   assert movies['5101']['custom_properties']['actors']=='俳優 一, 俳優 二'and series['6101']['custom_properties']['cast']=='俳優 一, 俳優 二'
  if label=='intl-repeat':
   assert before['vod_movie']==after['vod_movie']and before['vod_series']==after['vod_series']
  if label=='intl-replace':
   assert all(r['description']=='映画の説明二'for r in after['vod_movie']if r['tmdb_id']is not None)
   assert all(r['description']=='連続の説明二'for r in after['vod_series']if r['tmdb_id']is not None)
   assert next(r for r in after['vod_movie']if r['tmdb_id']=='5101')['custom_properties']['director']=='監督 一'
   assert next(r for r in after['vod_series']if r['tmdb_id']=='6101')['custom_properties']['director']=='監督 二'
  if label=='intl-sparse':assert before['vod_movie']==after['vod_movie']and before['vod_series']==after['vod_series']
  if label=='intl-prune':assert after['vod_movie'][0]['tmdb_id']=='映画-5104'and after['vod_series'][0]['tmdb_id']=='連続-6104'
  return
 for kind,table,rel in [('movie','vod_movie','vod_m3umovierelation'),('series','vod_series','vod_m3useriesrelation')]:
  rows=after[table];relations=after[rel];prefix='7'if kind=='movie'else'8'
  if label in ['order-cross-key-forward','order-cross-key-reverse']:
   assert len(rows)==len(relations)==1
   assert rows[0]['tmdb_id']==prefix+('101'if label.endswith('forward')else'102')
  if label.startswith('order-overlapping-target-'):
   assert len(rows)==1 and len(relations)==2
   assert rows[0]['id']==before[table][0]['id']and rows[0]['name']==before[table][0]['name']
   assert rows[0]['description']==('primary first'if label.endswith('forward')else'secondary second')
   idfield='stream_id'if kind=='movie'else'external_series_id';old=next(r for r in relations if r[idfield]==('9701'if kind=='movie'else'9801'))
   assert old['custom_properties']['detailed_fetched']is True
  if label=='order-secondary-rebind-original':
   assert rows==before[table]and len(relations)==1 and relations[0]['id']==before[rel][0]['id']
   assert relations[0]['custom_properties']['detailed_fetched']is False
   if kind=='series':assert relations[0]['custom_properties']['episodes_fetched']is False
  if label in ['order-movie-conflict-series-success','order-series-conflict-movie-success']:
   failed=kind==('movie'if label.startswith('order-movie-')else'series')
   assert len(rows)==len(relations)==(0 if failed else 1)


def run(f):
 assert f.backend in('Original','Native')and f.case in CASES
 phases=STAGES[f.case]
 out=f.output/f.case;out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','Source_characterization':f.backend=='Original','Native_qualified':False,'cases':[],'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 previous=None;observer=None
 try:
  for label in phases:
   row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
   p=private/label;p.mkdir(mode=0o700);f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
   if f.case==CASES[0]and previous is not None:prepared,setup=previous,None
   else:prepared,setup=prepare_cell(f,private,label,previous)
   before=snapshot(f);assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in prepared.items()if k!='observed_at'},'Declared setup to root before exact';cache=aggregate.cache_state(f);trace_start=len(control(f,'/snapshot')['trace']);control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,f.case+'-'+label,database=1)
   api=f.api('POST','/api/m3u/accounts/'+str(f._vod_setup['account'])+'/refresh-vod/',{});save(p/'HTTP.json',api)
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
   if api['status']!=202 or frame is None or backend is None:raise AssertionError('ActualRootCaptureIncomplete')
   row.update(actual_root=frame['headers']['id'],actual_provider_requests=len([r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r]),actual_queued_children=len(backend.get('children',[])),Source_observation_complete=True)
   try:
    assert api['status']==202 and api['body']=={'message':'VOD refresh initiated for account Owned VOD ingest'}
    # Each cell's explicit reset/seed delta is recorded separately; no hidden reset.
    ident=frame['headers']['id'];assert json.loads(__import__('base64').b64decode(frame['body']))==[[f._vod_setup['account']],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
    assert backend['status']=='SUCCESS'and backend['children']==[]
    match=re.fullmatch(r'Batch VOD refresh completed for account Owned VOD ingest in ([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match
    assert [(r['task'],r['id'])for r in durable['publications']]==[(TASK,ident)]
    requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r];assert len(requests)==5
    for actual,action in zip(requests,[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']):
     expected={k:[hashlib.sha256(v.encode()).hexdigest()]for k,v in f._vod_login.items()}
     if action is not None:expected['action']=[hashlib.sha256(action.encode()).hexdigest()]
     assert actual['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==expected and actual['actual_TLS']and actual['status']==200 and actual['actual_user_agent']==f._vod_setup['actual_user_agent']
    updates=[r['data']for r in events['events']if isinstance(r.get('data'),dict)and r['data'].get('account')==f._vod_setup['account']]
    assert updates==[{'progress':0,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'success','message':'VOD refresh completed in '+match[1]+' seconds'}]
    assert cache==aggregate.cache_state(f)
    for kind,table in [('movies','vod_movie'),('series','vod_series')]:assert catalogue[kind]['status']==200 and catalogue[kind]['body']['count']==len(after[table])
    assert_cell(label,before,after,setup);row['status']='passed'
   except Exception as guessed:
    row.update(status='failed',semantic_expectation_failure=type(guessed).__name__);(p/'semantic-expectation-failure.txt').write_text(traceback.format_exc()[-16384:])
   previous=after;save(out/'report.json',report)
 except BaseException as caught:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(caught).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except BaseException as caught:report['observer_cleanup_error']=type(caught).__name__
 report['all_grouped_Source_observations_complete']=len(report['cases'])==len(phases) and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==len(phases) and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   from vod_catalogue_domains_compare import compare_retained_Source
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',Source_comparison_failure=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report
