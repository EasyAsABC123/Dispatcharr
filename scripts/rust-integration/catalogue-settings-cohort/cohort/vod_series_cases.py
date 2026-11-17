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
CASE='vod-series'
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


def run(f):
 assert f.case==CASE and f.backend in('Original','Native')
 out=f.output/CASE;out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','Source_only':f.backend=='Original','cases':[],'full_milestone_passed':False,'performance_verified':False,'episodes_and_metadata_qualified':False};save(out/'report.json',report)
 account=f._vod_setup['account'];observer=None
 def phase(label,provider_case):
  nonlocal observer
  row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
  p=private/label;p.mkdir(mode=0o700);f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
  before=snapshot(f);cache=aggregate.cache_state(f);trace_before=control(f,'/snapshot')['trace'];control(f,'/control',{'case':provider_case});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,'vod-series-'+label,database=1)
  api=f.api('POST','/api/m3u/accounts/'+str(account)+'/refresh-vod/',{});save(p/'HTTP.json',api);assert api['status']==202 and api['body']=={'message':'VOD refresh initiated for account Owned VOD ingest'}
  def publication():
   frames=[r for r in aggregate.frames(observer)if r['headers']['task']==TASK];assert len(frames)<=1
   return frames[0]if frames else False
  frame=f.wait(publication,15,label+'-actual-root');ident=frame['headers']['id'];assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
  backend=f.wait(lambda:aggregate.terminal(f,ident),120,label+'-actual-terminal');save(p/'backend.json',backend);assert backend['status']=='SUCCESS'and backend['children']==[];match=re.fullmatch(r'Batch VOD refresh completed for account Owned VOD ingest in ([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match
  f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-real-drain');durable=journal.producer_observer_stop(f,observer);observer=None;save(p/'journal.json',durable);assert [(r['task'],r['id'])for r in durable['publications']]==[(TASK,ident)]
  after=snapshot(f);events=aggregate.receiver_read(f,receiver);trace=control(f,'/snapshot')['trace'][len(trace_before):]
  for name,value in [('SQL-before',before),('SQL-after',after),('WS',events),('Origin',trace)]:save(p/(name+'.json'),value)
  assert after['account']==before['account']and after['SystemEvent']==before['SystemEvent']and cache==aggregate.cache_state(f)
  assert all(not after[name]for name in ['Movie','M3UMovieRelation','Episode','M3UEpisodeRelation','VODLogo'])
  requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r];actions=[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series'];assert len(requests)==5
  for actual,action in zip(requests,actions):
   expected={k:[hashlib.sha256(v.encode()).hexdigest()]for k,v in f._vod_login.items()}
   if action is not None:expected['action']=[hashlib.sha256(action.encode()).hexdigest()]
   assert actual['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==expected and actual['actual_TLS']and actual['status']==200 and actual['actual_user_agent']==f._vod_setup['actual_user_agent']
  updates=[r['data']for r in events['events']if isinstance(r.get('data'),dict)and r['data'].get('account')==account]
  assert updates==[{'progress':0,'type':'m3u_refresh','account':account,'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':account,'action':'vod_refresh','status':'success','message':'VOD refresh completed in '+match[1]+' seconds'}]
  catalogue=f.api('GET','/api/vod/series/?page_size=100');save(p/'catalogue.json',catalogue);assert catalogue['status']==200 and catalogue['body']['count']==len(after['Series'])
  row.update(actual_root=ident,provider_actions=5,actual_Series=len(after['Series']),actual_relations=len(after['M3USeriesRelation']),actual_queued_children=0,status='observed');save(out/'report.json',report);return row,before,after
 try:
  row,before,after=phase('fresh-nonempty-Series-no-Movies','vod-series-initial')
  assert len(after['Series'])==2 and len(after['M3USeriesRelation'])==2
  series={r['id']:r for r in after['Series']};categories={r['id']:r for r in after['VODCategory']}
  for r in after['Series']:
   assert r['year']is None and r['language']==''and r['description']==''and r['rating']is None and r['genre']==''and r['logo_id']is None and r['tmdb_id']is None and r['imdb_id']is None and r['custom_properties']=={'category_id':'11'}
  for r in after['M3USeriesRelation']:
   assert r['m3u_account_id']==account and r['last_episode_refresh']is None and categories[r['category_id']]['name']=='Owned Series'
   cp=r['custom_properties'];assert set(cp)=={'basic_data','detailed_fetched','episodes_fetched'}and cp['detailed_fetched']is False and cp['episodes_fetched']is False
   assert cp['basic_data']['series_id']==int(r['external_series_id'])and cp['basic_data']['name']==series[r['series_id']]['name']and cp['basic_data']['_category_id']==r['category_id']and cp['basic_data']['_provider_category_id']=='11'
  initial=after;row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('unchanged-Series-repeat','vod-series-initial');assert before['Series']==after['Series']
  for a,b in zip(before['M3USeriesRelation'],after['M3USeriesRelation']):assert {k:v for k,v in a.items()if k!='last_seen'}=={k:v for k,v in b.items()if k!='last_seen'}and b['last_seen']>=a['last_seen']
  row['status']='passed';save(out/'report.json',report)
  f.sql("from apps.vod.models import M3USeriesRelation;from django.utils import timezone;rows=list(M3USeriesRelation.objects.filter(m3u_account_id="+str(account)+"));[setattr(r,'custom_properties',{**(r.custom_properties or {}),'detailed_fetched':True,'episodes_fetched':True,'owned_note':'preserve'})for r in rows];[setattr(r,'last_episode_refresh',timezone.now())for r in rows];M3USeriesRelation.objects.bulk_update(rows,['custom_properties','last_episode_refresh']);print(json.dumps({'setup_rows':len(rows)}))")
  row,before,after=phase('Series-reassign-globaldedup-category-replace','vod-series-update');assert len(after['Series'])==2 and len(after['M3USeriesRelation'])==3
  previous_series={r['name']:r for r in before['Series']};current_series={r['name']:r for r in after['Series']}
  assert all(previous_series['Owned Series (2020)'][field]==current_series['Owned Series (2020)'][field]for field in ['id','uuid','created_at','updated_at'])
  rels={r['external_series_id']:r for r in after['M3USeriesRelation']};old={r['external_series_id']:r for r in before['M3USeriesRelation']};assert rels['9201']['series_id']==rels['9203']['series_id']and rels['9202']['series_id']!=old['9202']['series_id']
  for key in ['9201','9202']:
   assert all(rels[key][field]==old[key][field]for field in ['id','created_at','updated_at','last_episode_refresh'])and rels[key]['custom_properties']['owned_note']=='preserve'
  assert rels['9201']['custom_properties']['detailed_fetched']and rels['9201']['custom_properties']['episodes_fetched']and not rels['9202']['custom_properties']['detailed_fetched']and not rels['9202']['custom_properties']['episodes_fetched']
  assert all(r['custom_properties']=={'category_id':'15'}for r in after['Series'])and not any(r['name']=='Owned Series'for r in after['VODCategory'])
  row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('empty-Series-prunes-only-stale-Series','vod-series-empty');assert not after['Series']and not after['M3USeriesRelation'];row['status']='passed';save(out/'report.json',report)
 except BaseException as error:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(error).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except Exception as error:report['observer_cleanup_error']=type(error).__name__
 report['status']='passed'if len(report['cases'])==4 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if report['status']=='passed':
  from vod_series_projection import project
  actual=project(private);save(out/'semantic-projection.json',actual)
  if f.backend=='Native':
   try:
    refs=f.binding['retained_source_vod_series']
    for name,ref in refs.items():assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256'],name
    qualification=json.loads(Path(refs['qualification']['path']).read_text());assert qualification['Source_Series_context_capture_qualified']is True and qualification['Source_series_actual_cases_passed']==4 and qualification['historical_whole_controller_actual_exit']==1
    source=json.loads(Path(refs['report']['path']).read_text());assert source['status']=='passed'and len(source['cases'])==4 and all(row['status']=='passed'for row in source['cases'])
    execution=json.loads(Path(refs['execution']['path']).read_text());assert execution['actual_exit']==1 and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']
    cleanup=json.loads(Path(refs['cleanup']['path']).read_text());assert cleanup['exact_owned_absence']and cleanup['all_original_process_generations_absent']and cleanup['host_CLI_groups_settled']
    root=json.loads(Path(refs['Root_cleanup']['path']).read_text());assert root['passed']
    expected=json.loads(Path(refs['projection']['path']).read_text());assert json.dumps(actual,sort_keys=True,separators=(',',':'),allow_nan=False)==json.dumps(expected,sort_keys=True,separators=(',',':'),allow_nan=False)
    report['exact_retained_Source_Series_semantics_equal']=True
   except BaseException as error:
    report.update(status='failed',semantic_comparison_failure=type(error).__name__);(private/'semantic-failure.txt').write_text(traceback.format_exc()[-8192:])
 save(out/'report.json',report);return report

