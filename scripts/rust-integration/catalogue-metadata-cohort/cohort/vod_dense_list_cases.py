"""Bounded ordinary provider-list metadata characterization, not yet executed.
Original Dispatcharr contributors and dependency authors retain all credit.
Existing Fixture/HTTP/claimed worker only; no Source task-body calls.
"""
import copy,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
from vod_series_advanced_cases import snapshot
from provider_cases import control
from vod_series_info_cases import instant

CASE='vod-dense-list'
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
LOGO_A='https://owned.invalid/dense-logo-A.png'
LOGO_B='https://owned.invalid/dense-logo-B.png'

def payloads():
 movie={'stream_id':9401,'name':'Owned Dense Movie (2010)','year':2010,'category_id':'11','container_extension':'mp4','description':'Movie description one','plot':'fallback plot unused','rating':'8,5','vote_average':'7.0','genre':'Adventure','category_name':'fallback category unused','duration_secs':5400,'duration':'95','stream_icon':LOGO_A,'director':[' Director One ','Director Two'],'actors':[' Actor One ',' Actor Two '],'trailer':['',' trailer-one '],'release_date':'2010-01-02','is_adult':True}
 series={'series_id':9501,'name':'Owned Dense Series','category_id':'21','releaseDate':'2010-01-02','plot':'Series description one','rating':'7.5','genre':'Drama','cover':LOGO_A,'director':[' Series Director ','Ignored Director'],'cast':[' Series Actor One ',' Series Actor Two '],'youtube_trailer':' series-trailer-one '}
 def response(m,s):return {'get_vod_categories':[{'category_id':'11','category_name':'Owned Movies'}],'get_vod_streams':m,'get_series_categories':[{'category_id':'21','category_name':'Owned Series'}],'get_series':s}
 result={name:response([copy.deepcopy(movie)],[copy.deepcopy(series)])for name in ['fresh-dense','identical-repeat','nonblank-metadata-update','sparse-blank-and-logo-clear','duplicate-representative-and-reuse']}
 changed=result['nonblank-metadata-update'];changed['get_vod_streams'][0].update(description='Movie description two',rating=8,genre='Mystery',duration_secs=0,duration='01:02:03',stream_icon=LOGO_B,director=[' Replacement Director '],actors=[' Replacement Actor '],trailer='trailer-two',release_date='2011-03-04')
 changed['get_series'][0].update(plot='Series description two',rating='8,0',genre='Comedy',cover=LOGO_B,director='Replacement Series Director',cast=[' Replacement Series Actor '],youtube_trailer='series-trailer-two',releaseDate='2010-02-03')
 sparse=result['sparse-blank-and-logo-clear'];sparse['get_vod_streams'][0]={k:v for k,v in movie.items()if k in ['stream_id','name','year','category_id','container_extension']};sparse['get_vod_streams'][0].update(description='',plot='',rating='',genre='',director='',actors=[],trailer='',release_date='')
 sparse['get_series'][0]={'series_id':9501,'name':series['name'],'category_id':'21','release_date':'2010-02-03','plot':'','rating':'','genre':'','director':'','cast':[],'youtube_trailer':'','cover':'x'*501}
 duplicate=result['duplicate-representative-and-reuse'];duplicate['get_vod_streams'][0]['description']='';duplicate['get_vod_streams'][0]['plot']='';duplicate['get_series'][0]['plot']=''
 m2=copy.deepcopy(movie);m2.update(stream_id=9402,description='Complementary Movie description',genre='Ignored Later Genre');s2=copy.deepcopy(series);s2.update(series_id=9502,plot='Complementary Series description',genre='Ignored Later Genre')
 duplicate['get_vod_streams'].append(m2);duplicate['get_series'].append(s2)
 result['last-owner-prune']=response([],[])
 return result

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


def _prepare_rows(f):base_series._prepare_rows(f)

def core_rows(after):
 assert len(after['vod_movie'])==len(after['vod_series'])==1
 return after['vod_movie'][0],after['vod_series'][0]

def assert_values(label,before,after):
 assert len(before['sequences'])==len(after['sequences'])==11
 assert before['m3u_m3uaccount']==after['m3u_m3uaccount']
 assert before['support']['core_systemevent']==after['support']['core_systemevent']
 assert not after['vod_episode']and not after['vod_m3uepisoderelation']
 for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
  for relation in after[table]:assert instant(before['observed_at'])<=instant(relation['last_seen'])<=instant(after['observed_at']),(label,table,'last_seen')
 logo_sequence='public.vod_vodlogo_id_seq'
 expected_logo=1 if label in ['fresh-dense','identical-repeat']else 2
 assert after['sequences'][logo_sequence]=={'last_value':expected_logo,'is_called':True},(label,'logo sequence')
 if label=='last-owner-prune':
  assert all(not after[t]for t in ['vod_movie','vod_series','vod_m3umovierelation','vod_m3useriesrelation'])
  assert after['support']['vod_vodlogo']==before['support']['vod_vodlogo']
  return
 movie,series=core_rows(after)
 assert movie['name']=='Owned Dense Movie (2010)'and movie['year']==2010
 assert series['name']=='Owned Dense Series'and series['year']==2010
 assert all(r['tmdb_id']is None and r['imdb_id']is None and r['language']==''for r in [movie,series])
 logos={r['id']:r for r in after['support']['vod_vodlogo']}
 if label=='sparse-blank-and-logo-clear':
  assert movie['logo_id']is None and series['logo_id']is None
  for table in ['vod_movie','vod_series']:
   old=before[table][0];new=after[table][0]
   assert {k:v for k,v in old.items()if k!='logo_id'}=={k:v for k,v in new.items()if k!='logo_id'},table
 else:
  assert movie['logo_id']==series['logo_id']
  assert logos[movie['logo_id']]['url']==(LOGO_B if label=='nonblank-metadata-update'else LOGO_A)
 if label in ['fresh-dense','identical-repeat']:
  assert {k:movie[k]for k in ['description','rating','genre','duration_secs','is_adult','custom_properties']}=={'description':'Movie description one','rating':'8.5','genre':'Adventure','duration_secs':5400,'is_adult':True,'custom_properties':{'youtube_trailer':'trailer-one','director':'Director One','actors':'Actor One, Actor Two','release_date':'2010-01-02'}}
  assert {k:series[k]for k in ['description','rating','genre','custom_properties']}=={'description':'Series description one','rating':'7.5','genre':'Drama','custom_properties':{'director':'Series Director','cast':'Series Actor One, Series Actor Two','releaseDate':'2010-01-02','youtube_trailer':'series-trailer-one','category_id':'21'}}
 if label=='identical-repeat':
  assert before['vod_movie']==after['vod_movie']and before['vod_series']==after['vod_series']and before['support']['vod_vodlogo']==after['support']['vod_vodlogo']
 if label=='nonblank-metadata-update':
  assert [movie[k]for k in ['description','rating','genre','duration_secs']]==['Movie description two','8.0','Mystery',3723]
  assert movie['custom_properties']=={'youtube_trailer':'trailer-two','director':'Director One','actors':'Actor One, Actor Two','release_date':'2010-01-02'}
  assert [series[k]for k in ['description','rating','genre']]==['Series description two','8.0','Comedy']
  assert series['custom_properties']=={'director':'Replacement Series Director','cast':'Replacement Series Actor','releaseDate':'2010-02-03','youtube_trailer':'series-trailer-two','category_id':'21'}
 if label=='duplicate-representative-and-reuse':
  assert len(after['vod_m3umovierelation'])==len(after['vod_m3useriesrelation'])==2
  assert movie['description']=='Complementary Movie description'and series['description']=='Complementary Series description'
  assert movie['genre']=='Adventure'and series['genre']=='Drama'
  assert after['support']['vod_vodlogo']==before['support']['vod_vodlogo']
 if label=='fresh-dense':
  for table in ['vod_movie','vod_series']:
   for entity in after[table]:
    assert uuid.UUID(entity['uuid']).version==4
    for key in ['created_at','updated_at']:assert instant(before['observed_at'])<=instant(entity[key])<=instant(after['observed_at']),(table,key)
 if label!='fresh-dense':
  for table in ['vod_movie','vod_series']:
   for key in ['id','uuid','created_at','updated_at']:assert before[table][0][key]==after[table][0][key],(label,table,key)

def run(f):
 assert f.backend in('Original','Native')and f.case==CASE
 out=f.output/CASE;out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','Source_characterization':f.backend=='Original','Native_qualified':False,'cases':[],'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 previous=None;observer=None
 try:
  for label in payloads():
   row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
   p=private/label;p.mkdir(mode=0o700);f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
   before=snapshot(f);cache=aggregate.cache_state(f);trace_start=len(control(f,'/snapshot')['trace']);control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,CASE+'-'+label,database=1)
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
   assert api['status']==202 and api['body']=={'message':'VOD refresh initiated for account Owned VOD ingest'}
   if previous is not None:assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in previous.items()if k!='observed_at'},'Unchanged raw phase chain'
   ident=frame['headers']['id'];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
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
   row.update(actual_root=ident,actual_provider_requests=5,actual_queued_children=0,Source_observation_complete=True)
   try:
    assert_values(label,before,after);row['status']='passed'
   except AssertionError as guessed:
    row.update(status='failed',semantic_expectation_failure=type(guessed).__name__);(p/'semantic-expectation-failure.txt').write_text(traceback.format_exc()[-16384:])
   previous=after;save(out/'report.json',report)
 except BaseException as caught:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(caught).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except BaseException as caught:report['observer_cleanup_error']=type(caught).__name__
 report['all_six_context_observations_complete']=len(report['cases'])==6 and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==6 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as caught:
   report.update(status='failed',Source_comparison_failure=type(caught).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report


def compare_retained_Source(f,private):
 """Fail closed until actual Source6 creates the exact full projection contract."""
 refs=f.binding['retained_source_vod_dense_list'];values={}
 for name,ref in refs.items():
  path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],name;values[name]=json.loads(path.read_text())
 assert set(values)=={'qualification','projection','report','execution','cleanup','Root_cleanup','identity'}
 assert values['qualification']['actual_Source_cases_passed']==6 and values['qualification']['Source_dense_list_context_capture_qualified']is True
 assert values['report']['status']=='passed'and len(values['report']['cases'])==6 and all(row['status']=='passed'for row in values['report']['cases'])
 terminal=values['execution'];assert terminal['actual_exit']==0 and terminal['stdout_EOF']and terminal['stderr_EOF']and terminal['process_group_settled']
 assert values['cleanup']['exact_owned_absence']and values['Root_cleanup']['passed']
 identity=values['identity'];assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
 assert identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
 assert identity['Source_contexts']==1 and identity['Native_contexts']==0 and identity['actual_natural_root_count']==6
 from vod_dense_list_compare import PHASES,compare_public,source_http_origin,native_http_origin
 sql_refs=f.binding['retained_source_vod_dense_list_SQL'];source_sql={}
 assert set(sql_refs)==set(PHASES)
 for label,ends in sql_refs.items():
  assert set(ends)=={'before','after'};source_sql[label]={}
  for end,ref in ends.items():
   path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'];source_sql[label][end]=json.loads(path.read_text())
 source_origin=source_http_origin(f.binding['retained_source_vod_dense_list_http_origin'],identity,values['cleanup'])
 native_origin,native_proof=native_http_origin(f)
 save(Path(private).parent/'HTTP-origin-binding.json',{'Source_actual_origin':source_origin,'Native_actual_context':native_proof,'projection_scope':'Only Native movies/series results[].logo.cache_url request origin maps to verified Source origin; exact SQL-bound logoID/providerURL/path/query/hash, no fragments/userinfo or other URL rewriting'})
 actual=compare_public(values['projection'],values['report'],source_sql,private,source_origin,native_origin);assert actual['status']=='passed'
 save(Path(private).parent/'Source-comparison.json',actual)
