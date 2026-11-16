"""Natural VOD ingestion through actual HTTP, Source Requests and existing HTTPS Origin.
Original Dispatcharr and dependency authors retain all application credit.
No Source body invocation, new server, scheduler or worker for inline helpers.
"""
import base64,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
PORT=18095
CASE='vod-repeat'
TASK='apps.vod.tasks.refresh_vod_content'
from provider_cases import control

def prepare_context(fixture):
 if fixture.case!=CASE:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json'}
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
 if f.backend=='Native':f._provider_environment['RUST_EXPERIMENTAL_SELECTED_BEAT']='0'
 login=f._vod_login
 code="""
from apps.m3u.models import M3UAccount
from apps.vod.models import Movie,Series,Episode,VODCategory,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.plugins.models import PluginConfig
from core.models import UserAgent
assert not PluginConfig.objects.exists()
assert all(not model.objects.exists()for model in(Movie,Series,Episode,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation))
assert VODCategory.objects.count()<=128
agent=UserAgent.objects.create(name='Owned VOD agent',user_agent='dispatcharr-owned-vod-ingestion')
rows=M3UAccount.objects.bulk_create([
 M3UAccount(name='Owned VOD ingest',account_type='XC',server_url='https://xc-vod-owned.invalid/xc/player_api.php?discard=1#fragment',username=USER,password=PASSWORD,user_agent=agent,is_active=True,status='success',last_message='Owned VOD account baseline',custom_properties={'enable_vod':True}),
 M3UAccount(name='Owned VOD disabled',account_type='XC',server_url='https://xc-vod-owned.invalid/xc',username=USER,password=PASSWORD,user_agent=agent,is_active=True,custom_properties={'enable_vod':False}),
 M3UAccount(name='Owned VOD non-XC',account_type='STD',is_active=True,custom_properties={'enable_vod':True})])
print(json.dumps({'account':rows[0].pk,'disabled':rows[1].pk,'non_XC':rows[2].pk,'actual_user_agent':agent.user_agent,'fresh_global_catalogue':True}))
""".replace('USER',repr(login['username'])).replace('PASSWORD',repr(login['password']))
 f._vod_setup=f.sql(code,secret=True)

def snapshot(f):
 return f.sql("""
from apps.m3u.models import M3UAccount
from apps.vod.models import Movie,Series,Episode,VODCategory,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import SystemEvent
models=(Movie,Series,Episode,VODCategory,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,SystemEvent)
result={}
for model in models:
 assert model.objects.count()<=128
 result[model.__name__]=list(model.objects.order_by('pk').values())
result['account']=M3UAccount.objects.values('id','name','status','last_message','custom_properties','is_active','account_type').get(pk=ACCOUNT)
print(json.dumps(result,default=str))
""".replace('ACCOUNT',str(f._vod_setup['account'])),secret=True)

def _source_contract(f,out,actual):
 # Store actual unnormalized authority locally. A later Native binding must
 # supply the exact qualified Source run/execution/cleanup, not a claim flag.
 save(out/'actual-contract.json',actual)
 if f.backend=='Original':return
 refs=f.binding['retained_source_vod_ingestion']
 for name,ref in refs.items():assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256'],name
 source=json.loads(Path(refs['contract']['path']).read_text())
 assert json.loads(Path(refs['report']['path']).read_text())['status']=='passed'
 execution=json.loads(Path(refs['execution']['path']).read_text());assert execution['actual_exit']==0 and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']
 assert json.loads(Path(refs['cleanup']['path']).read_text())['all_owned_resources_absent']
 assert actual['semantic_projection']==source['semantic_projection']

def _warmup_run(f):
 assert f.case==CASE
 out=f.output/(CASE+'-warmup');out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'actual-Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 f.wait(lambda:not any(aggregate.broker(f).values()),20,'VOD-initial-real-broker-drained')
 report={'status':'running','cases':[],'full_milestone_passed':False,'performance_verified':False,'metadata_update_conflict_language_and_nonempty_Series_parity':False}
 observer=None
 def cell(name,body):
  row={'name':name,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
  try:row['actual']=body();row['status']='passed'
  except BaseException as error:row.update(status='failed',failure_type=type(error).__name__);(private/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
  save(out/'report.json',report)
 def refusal(label,ident,body):
  observed=journal.observer_start(f,'vod-'+label,database=1);before=snapshot(f);cache=aggregate.cache_state(f)
  try:response=f.api('POST','/api/m3u/accounts/'+str(ident)+'/refresh-vod/',{});after=snapshot(f)
  finally:durable=journal.producer_observer_stop(f,observed)
  save(private/(label+'-HTTP-and-journal.json'),{'response':response,'before':before,'after':after,'journal':durable})
  assert response['status']==400 and response['body']=={'error':body}
  assert before==after and cache==aggregate.cache_state(f)and not durable['publications']and not any(aggregate.broker(f).values())
  return {'actual_HTTP':400,'actual_publications':0,'SQL_and_owned_cache_unchanged':True}
 def ingest():
  nonlocal observer
  before=snapshot(f);cache_before=aggregate.cache_state(f);trace_before=control(f,'/snapshot');save(private/'before.json',before);save(private/'cache-before.json',cache_before)
  receiver=aggregate.receiver_begin(f,'VOD');observer=journal.observer_start(f,'vod-ingestion',database=1)
  response=f.api('POST','/api/m3u/accounts/'+str(f._vod_setup['account'])+'/refresh-vod/',{});save(private/'API.json',response)
  assert response['status']==202 and response['body']=={'message':'VOD refresh initiated for account Owned VOD ingest'}
  def publication():
   observed=[row for row in aggregate.frames(observer)if row['headers']['task']==TASK];assert len(observed)<=1
   return observed[0]if observed else False
  frame=f.wait(publication,15,'actual-VOD-HTTP-task-frame');ident=frame['headers']['id'];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
  assert json.loads(base64.b64decode(frame['body']))==[[f._vod_setup['account']],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]
  backend=f.wait(lambda:aggregate.terminal(f,ident),120,'actual-VOD-claimed-parent-terminal');save(private/'backend.json',backend)
  assert backend['status']=='SUCCESS'and backend['children']==[]
  match=re.fullmatch(r'Batch VOD refresh completed for account Owned VOD ingest in ([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match
  f.wait(lambda:not any(aggregate.broker(f).values()),20,'actual-VOD-no-helper-task-broker-drain')
  durable=journal.producer_observer_stop(f,observer);observer=None;save(private/'durable-journal.json',durable)
  assert [(row['task'],row['id'])for row in durable['publications']]==[(TASK,ident)]
  after=snapshot(f);save(private/'after.json',after);cache_after=aggregate.cache_state(f);save(private/'cache-after.json',cache_after)
  events=aggregate.receiver_read(f,receiver);save(private/'actual-WS.json',events)
  trace=control(f,'/snapshot')['trace'][len(trace_before['trace']):];save(private/'actual-Origin.json',trace)
  requests=[row for row in trace if row.get('host')=='xc-vod-owned.invalid'and'path'in row]
  action_names=[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']
  assert len(requests)==len(action_names)
  for row,action in zip(requests,action_names):
   assert row['actual_TLS']and row['method']=='GET'and row['path']=='/xc/player_api.php'and row['status']==200 and row['actual_user_agent']==f._vod_setup['actual_user_agent']
   hashes={k:[hashlib.sha256(v.encode()).hexdigest()]for k,v in f._vod_login.items()}
   if action is not None:hashes['action']=[hashlib.sha256(action.encode()).hexdigest()]
   assert row['query_sha256']==hashes
  assert after['account']==before['account']and after['SystemEvent']==before['SystemEvent']and cache_before==cache_after
  assert len(after['Movie'])==2 and not after['Series']and not after['Episode']and not after['VODLogo']and not after['M3USeriesRelation']and not after['M3UEpisodeRelation']
  movie_fields=['name','year','language','tmdb_id','imdb_id','description','rating','genre','duration_secs','custom_properties','is_adult','logo_id']
  movies=sorted([{k:row[k]for k in movie_fields}for row in after['Movie']],key=lambda row:row['name'])
  expected=[{'name':name,'year':year,'language':'','tmdb_id':None,'imdb_id':None,'description':'','rating':None,'genre':'','duration_secs':None,'custom_properties':None,'is_adult':False,'logo_id':None}for name,year in[('Owned Movie (2020)',2020),('Owned Second Movie (2021)',2021)]];assert movies==expected
  categories={row['id']:row for row in after['VODCategory']};links=after['M3UVODCategoryRelation'];assert len(links)==3 and all(row['enabled']and row['custom_properties']=={}and row['m3u_account_id']==f._vod_setup['account']for row in links)
  assert sorted((categories[row['category_id']]['name'],categories[row['category_id']]['category_type'])for row in links)==[('Owned Films','movie'),('Uncategorized','movie'),('Uncategorized','series')]
  movie_by_id={row['id']:row for row in after['Movie']};relations=after['M3UMovieRelation'];assert len(relations)==2
  relation_projection=[]
  for row in sorted(relations,key=lambda row:row['stream_id']):
   category=categories[row['category_id']];assert category['name']=='Owned Films'and row['m3u_account_id']==f._vod_setup['account']and row['last_advanced_refresh']is None
   data=row['custom_properties'];assert data['detailed_fetched']is False and set(data)=={'basic_data','detailed_fetched'}
   stream=int(row['stream_id']);ordinal=stream-9100;name=['Owned Movie (2020)','Owned Second Movie (2021)'][ordinal-1];extension=['mp4','mkv'][ordinal-1]
   assert data['basic_data']=={'num':ordinal,'stream_id':stream,'name':name,'category_id':'4','container_extension':extension,'is_adult':'0','_provider_category_id':'4','_category_id':row['category_id']}
   assert row['container_extension']==extension and movie_by_id[row['movie_id']]['name']==name
   relation_projection.append({'stream_id':row['stream_id'],'movie':name,'category':'Owned Films','container_extension':extension,'basic_data':{**data['basic_data'],'_category_id':'<owned-movie-category>'},'detailed_fetched':False})
  updates=[row['data']for row in events['events']if isinstance(row.get('data'),dict)and row['data'].get('account')==f._vod_setup['account']]
  assert updates==[{'progress':0,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'success','message':'VOD refresh completed in '+match[1]+' seconds'}]
  catalogue=f.api('GET','/api/vod/movies/?page_size=100');save(private/'actual-catalogue-HTTP.json',catalogue);assert catalogue['status']==200 and catalogue['body']['count']==2 and sorted(row['name']for row in catalogue['body']['results'])==[row['name']for row in expected]
  projection={'movies':movies,'relations':relation_projection,'category_links':[{'name':name,'type':kind,'enabled':True,'custom_properties':{}}for name,kind in[('Owned Films','movie'),('Uncategorized','movie'),('Uncategorized','series')]],'Series_count':0,'Episode_count':0,'queued_inline_helpers':0,'actual_provider_actions':action_names,'actual_VOD_updates':[dict(row,account='<owned-account>',message='VOD refresh completed in <duration> seconds')if row['progress']==100 else dict(row,account='<owned-account>')for row in updates],'account_unchanged':True,'SystemEvents_unchanged':True,'owned_Core_cache_unchanged':True,'catalogue_count':2,'task_status':'SUCCESS','completion_template':'Batch VOD refresh completed for account Owned VOD ingest in <duration> seconds'}
  _source_contract(f,private,{'actual_root':ident,'actual_backend':backend,'semantic_projection':projection})
  return {'actual_HTTP202':True,'actual_task':ident,'actual_claimed_backend_SUCCESS':True,'actual_movies':2,'actual_movie_relations':2,'actual_category_relations':3,'actual_series':0,'actual_children':0,'actual_TLS_provider_requests':5,'actual_WS_updates':2,'actual_catalogue_HTTP200':True,'Source_task_bodies_or_inline_helpers_called_by_observer':False}
 try:cell('natural-HTTP202-fresh-Movies-emptySeries-inline-helpers',ingest)
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer));observer=None
   except Exception as error:report['observer_cleanup_error']=type(error).__name__
 report['status']='passed'if all(row['status']=='passed'for row in report['cases'])and not report.get('observer_cleanup_error')else'failed';save(out/'report.json',report);return report


def run(f):
 assert f.case==CASE and f.backend in('Original','Native')
 out=f.output/CASE;out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
 report={'status':'running','Source_only':f.backend=='Original','fresh_first_ingest_is_causal_warmup':True,'cases':[],'full_milestone_passed':False,'performance_verified':False}
 save(out/'report.json',report)
 warmup=_warmup_run(f);save(private/'causal-fresh-ingest.json',warmup)
 if warmup['status']!='passed':
  report.update(status='failed',failure_stage='causal_fresh_ingest',cases=[{'name':'causal-fresh-ingest','status':'failed'}]);save(out/'report.json',report);return report
 account=f._vod_setup['account'];initial=snapshot(f)
 movie_ids={row['name']:row['id']for row in initial['Movie']}
 stable={row['name']:{k:row[k]for k in('id','uuid','created_at','updated_at')}for row in initial['Movie']}
 observer=None
 def phase(label,provider_case,expect_error=False):
  nonlocal observer
  row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
  observed=private/label;observed.mkdir(mode=0o700)
  before=snapshot(f);cache=aggregate.cache_state(f);trace_before=control(f,'/snapshot')['trace'];control(f,'/control',{'case':provider_case})
  receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,'vod-repeat-'+label,database=1)
  response=f.api('POST','/api/m3u/accounts/'+str(account)+'/refresh-vod/',{});save(observed/'API.json',response)
  assert response['status']==202
  def publication():
   frames=[v for v in aggregate.frames(observer)if v['headers']['task']==TASK];assert len(frames)<=1
   return frames[0]if frames else False
  frame=f.wait(publication,15,'repeat-actual-root-'+label);ident=frame['headers']['id'];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
  backend=f.wait(lambda:aggregate.terminal(f,ident),120,'repeat-actual-terminal-'+label);save(observed/'backend.json',backend)
  assert backend['status']=='SUCCESS'and backend['children']==[]
  duration=None
  if expect_error:
   message='Provider returned no VOD categories for account Owned VOD ingest; aborting VOD refresh to preserve existing category selections'
   assert backend['result']=='VOD refresh failed: '+message
  else:
   match=re.fullmatch(r'Batch VOD refresh completed for account Owned VOD ingest in ([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match;duration=match[1]
  f.wait(lambda:not any(aggregate.broker(f).values()),20,'repeat-broker-drained-'+label)
  durable=journal.producer_observer_stop(f,observer);observer=None;save(observed/'durable-journal.json',durable)
  assert [(v['task'],v['id'])for v in durable['publications']]==[(TASK,ident)]
  after=snapshot(f);events=aggregate.receiver_read(f,receiver);trace=control(f,'/snapshot')['trace'][len(trace_before):]
  save(observed/'SQL-before.json',before);save(observed/'SQL-after.json',after);save(observed/'WS.json',events);save(observed/'Origin.json',trace)
  assert before['account']==after['account']and before['SystemEvent']==after['SystemEvent']and cache==aggregate.cache_state(f)
  requests=[v for v in trace if v.get('host')=='xc-vod-owned.invalid'and'path'in v]
  actions=[None,'get_vod_categories']if expect_error else[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']
  assert len(requests)==len(actions)
  for actual,action in zip(requests,actions):
   expected={k:[hashlib.sha256(v.encode()).hexdigest()]for k,v in f._vod_login.items()}
   if action is not None:expected['action']=[hashlib.sha256(action.encode()).hexdigest()]
   assert actual['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==expected and actual['actual_TLS']and actual['actual_user_agent']==f._vod_setup['actual_user_agent']and actual['status']==200
  updates=[v['data']for v in events['events']if isinstance(v.get('data'),dict)and v['data'].get('account')==account]
  end_message='VOD refresh failed: '+message if expect_error else'VOD refresh completed in '+duration+' seconds'
  assert updates==[{'progress':0,'type':'m3u_refresh','account':account,'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':account,'action':'vod_refresh','status':'error'if expect_error else'success','message':end_message}]
  row.update(status='observed',actual_root=ident,provider_actions=len(actions));save(out/'report.json',report)
  return row,before,after
 try:
  row,before,after=phase('unchanged','empty')
  assert before['Movie']==after['Movie']
  for old,new in zip(before['M3UMovieRelation'],after['M3UMovieRelation']):
   assert {k:v for k,v in old.items()if k!='last_seen'}=={k:v for k,v in new.items()if k!='last_seen'}and new['last_seen']>=old['last_seen']
  row['status']='passed';save(out/'report.json',report)
  # Ordinary model setup only: preserved JSON detail flag is not a task/body patch.
  f.sql("from apps.vod.models import M3UMovieRelation;from django.utils import timezone;rows=list(M3UMovieRelation.objects.filter(m3u_account_id="+str(account)+"));[setattr(r,'custom_properties',{**(r.custom_properties or {}),'detailed_fetched':True,'owned_note':'preserve'})for r in rows];[setattr(r,'last_advanced_refresh',timezone.now())for r in rows];M3UMovieRelation.objects.bulk_update(rows,['custom_properties','last_advanced_refresh']);print(json.dumps({'ordinary_setup_rows':len(rows)}))")
  row,before,after=phase('update-reassign-deduplicate-category-replace','vod-repeat-update')
  movies={v['name']:v for v in after['Movie']};assert set(movies)=={'Owned Movie (2020)','Owned Replacement (2022)'}
  assert {k:movies['Owned Movie (2020)'][k]for k in stable['Owned Movie (2020)']}==stable['Owned Movie (2020)']and movies['Owned Movie (2020)']['is_adult']is True
  rels={v['stream_id']:v for v in after['M3UMovieRelation']if v['m3u_account_id']==account};old={v['stream_id']:v for v in before['M3UMovieRelation']}
  assert set(rels)=={'9101','9102','9103'}and rels['9101']['movie_id']==rels['9103']['movie_id']==movie_ids['Owned Movie (2020)']
  for stream in('9101','9102'):
   assert rels[stream]['id']==old[stream]['id']and rels[stream]['created_at']==old[stream]['created_at']and rels[stream]['updated_at']==old[stream]['updated_at']and rels[stream]['last_advanced_refresh']==old[stream]['last_advanced_refresh']
   assert rels[stream]['custom_properties']['owned_note']=='preserve'
  assert rels['9101']['custom_properties']['detailed_fetched']is True and rels['9102']['custom_properties']['detailed_fetched']is False
  assert rels['9102']['movie_id']==movies['Owned Replacement (2022)']['id']and rels['9102']['movie_id']!=movie_ids['Owned Second Movie (2021)']
  assert not any(v['name']=='Owned Films'for v in after['VODCategory'])and sorted((v['name'],v['category_type'])for v in after['VODCategory'])==[('Uncategorized','movie'),('Uncategorized','series'),('Updated Films','movie')]
  row['status']='passed';save(out/'report.json',report)
  replacement=movies['Owned Replacement (2022)']['id']
  shared=f.sql("from apps.m3u.models import M3UAccount;from apps.vod.models import M3UMovieRelation;from django.utils import timezone;a=M3UAccount.objects.bulk_create([M3UAccount(name='Owned shared Movie reference',account_type='STD',is_active=False)])[0];r=M3UMovieRelation.objects.bulk_create([M3UMovieRelation(m3u_account=a,movie_id="+str(replacement)+",stream_id='owned-shared',container_extension='mp4',custom_properties={})])[0];print(json.dumps({'other_account':a.pk,'relation':r.pk}))")
  save(private/'ordinary-shared-reference-setup.json',shared)
  row,before,after=phase('prune-stale-preserve-other-account','vod-repeat-prune')
  own=[v for v in after['M3UMovieRelation']if v['m3u_account_id']==account];assert len(own)==1 and own[0]['stream_id']=='9101'
  assert len(after['Movie'])==2 and any(v['id']==replacement for v in after['Movie'])and any(v['id']==shared['relation']for v in after['M3UMovieRelation'])
  assert next(v for v in after['Movie']if v['id']==movie_ids['Owned Movie (2020)'])['is_adult']is True
  row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('empty-categories-preserve-selected-catalogue','vod-repeat-empty-categories',True);assert before==after;row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('empty-Movies-clean-only-owned-stale-orphans','vod-repeat-empty-movies')
  assert not any(v['m3u_account_id']==account for v in after['M3UMovieRelation'])and len(after['M3UMovieRelation'])==1 and after['M3UMovieRelation'][0]['id']==shared['relation']
  assert len(after['Movie'])==1 and after['Movie'][0]['id']==replacement
  catalogue=f.api('GET','/api/vod/movies/?page_size=100');save(private/'final-catalogue.json',catalogue);assert catalogue['status']==200 and catalogue['body']=={'count':0,'next':None,'previous':None,'results':[]}
  row['status']='passed';save(out/'report.json',report)
 except BaseException as error:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(error).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except Exception as error:report['observer_cleanup_error']=type(error).__name__
 report['status']='passed'if len(report['cases'])==5 and all(v['status']=='passed'for v in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if report['status']=='passed':
  from vod_repeat_projection import project
  actual=project(private);save(out/'semantic-projection.json',actual)
  if f.backend=='Native':
   refs=f.binding['retained_source_vod_repeat']
   for name,ref in refs.items():assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256'],name
   authority=json.loads(Path(refs['offline_correction']['path']).read_text());assert authority['offline_corrected_capture_qualified']is True and authority['historical_controller_actual_exit']==1 and authority['historical_workflow_status']=='failed'
   execution=json.loads(Path(refs['execution']['path']).read_text());assert execution['actual_exit']==1 and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']
   cleanup=json.loads(Path(refs['cleanup']['path']).read_text());assert cleanup['exact_owned_absence']and cleanup['all_original_process_generations_absent']and cleanup['host_CLI_groups_settled']
   expected=json.loads(Path(refs['projection']['path']).read_text());assert actual==expected
 save(out/'report.json',report);return report
