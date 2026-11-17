"""Private Source shared-account4 characterization; not executed.
Original Dispatcharr and dependency authors retain all application/model credit.
Only actual HTTP/claimed roots; the explicit descendant seed is fixture setup.
"""
import copy,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
import vod_dense_list_cases as dense
from vod_series_advanced_cases import snapshot
from vod_series_info_cases import instant
from provider_cases import control
CASE='vod-shared-metadata'
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
STAGES=(('shared-A-initial','A'),('shared-B-update','B'),('shared-A-prune','A'),('shared-B-prune','B'))
def payloads():
 values=dense.payloads()
 return {'shared-A-initial':copy.deepcopy(values['fresh-dense']),'shared-B-update':copy.deepcopy(values['nonblank-metadata-update']),'shared-A-prune':copy.deepcopy(values['last-owner-prune']),'shared-B-prune':copy.deepcopy(values['last-owner-prune'])}

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
 base_series._prepare_rows(f)
 code="""
from apps.m3u.models import M3UAccount
a=M3UAccount.objects.get(pk=ACCOUNT)
fields={field.attname:getattr(a,field.attname)for field in M3UAccount._meta.concrete_fields if field.attname not in('id','created_at','updated_at')}
fields['name']='Owned VOD shared B'
assert fields['account_type']=='XC'and fields['is_active']and fields['custom_properties'].get('enable_vod')is True
b=M3UAccount.objects.bulk_create([M3UAccount(**fields)])[0]
assert b.pk!=a.pk
print(json.dumps({'A':a.pk,'B':b.pk,'A_name':a.name,'B_name':b.name,'same_owned_transport_login_UserAgent':True,'creation_before_first_root':True}))
""".replace('ACCOUNT',str(f._vod_setup['account']))
 f._shared_setup=f.sql(code,secret=True)

def seed_descendant(f,private,after_A):
 p=private/'declared-after-A-seed';p.mkdir(mode=0o700)
 before=snapshot(f);cache=aggregate.cache_state(f);provider=control(f,'/snapshot');broker=aggregate.broker(f)
 assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in after_A.items()if k!='observed_at'},'A after to seed before exact'
 code="""
from apps.vod.models import Episode,M3UEpisodeRelation,M3USeriesRelation
from django.db import transaction
from django.utils import timezone
assert not Episode.objects.exists()and not M3UEpisodeRelation.objects.exists()
with transaction.atomic():
 relation=M3USeriesRelation.objects.get(m3u_account_id=ACCOUNT,external_series_id='9501')
 assert relation.series.name=='Owned Dense Series'and relation.series.year==2010
 episode=Episode.objects.create(name='Owned Shared Episode A',description='Explicit shared-owner fixture descendant',air_date=None,rating='7.0',duration_secs=60,series_id=relation.series_id,season_number=1,episode_number=1,tmdb_id=None,imdb_id=None,custom_properties={'fixture':'shared-owner-A'})
 stream=M3UEpisodeRelation.objects.create(m3u_account_id=ACCOUNT,episode=episode,series_relation=relation,stream_id='owned-shared-episode-A',container_extension='mkv',custom_properties={'fixture':'shared-owner-A'},last_seen=timezone.now())
print(json.dumps({'declared_fixture_setup_only':True,'Episode':episode.pk,'EpisodeRelationA':stream.pk,'Series':relation.series_id,'SeriesRelationA':relation.pk}))
""".replace('ACCOUNT',str(f._shared_setup['A']))
 save(p/'seed-script.json',{'sha256':hashlib.sha256(code.encode()).hexdigest(),'fixture_only':True,'ordinary_ORM_two_rows_no_HTTP_or_task_body':True})
 result=f.sql(code,secret=True);after=snapshot(f)
 for name,value in [('SQL-before',before),('SQL-after',after),('setup-result',result),('cache-before',cache),('cache-after',aggregate.cache_state(f)),('provider-before',provider),('provider-after',control(f,'/snapshot')),('broker-before',broker),('broker-after',aggregate.broker(f))]:save(p/(name+'.json'),value)
 left=copy.deepcopy(before);right=copy.deepcopy(after)
 for value in [left,right]:
  value.pop('observed_at')
  for table in ['vod_episode','vod_m3uepisoderelation']:value.pop(table);value['sequences'].pop('public.'+table+'_id_seq')
 assert left==right,'Only declared two tables/two sequences change'
 assert not before['vod_episode']and not before['vod_m3uepisoderelation']
 assert len(after['vod_episode'])==len(after['vod_m3uepisoderelation'])==1
 episode=after['vod_episode'][0];stream=after['vod_m3uepisoderelation'][0];assert uuid.UUID(episode['uuid']).version==4
 assert episode['id']==result['Episode']and stream['id']==result['EpisodeRelationA']and episode['series_id']==result['Series']and stream['episode_id']==episode['id']and stream['series_relation_id']==result['SeriesRelationA']and stream['m3u_account_id']==f._shared_setup['A']
 for table in ['vod_episode','vod_m3uepisoderelation']:
  key='public.'+table+'_id_seq';old=before['sequences'][key]
  assert after['sequences'][key]=={'last_value':old['last_value']+(1 if old['is_called']else 0),'is_called':True}
  for row in after[table]:
   for field in ['created_at','updated_at']+(['last_seen']if table=='vod_m3uepisoderelation'else[]):assert instant(before['observed_at'])<=instant(row[field])<=instant(after['observed_at'])
 assert cache==aggregate.cache_state(f)and provider==control(f,'/snapshot')and broker==aggregate.broker(f),'No source/API/task/cache/provider effect during fixture seed'
 f._shared_seed=after;return after

def check_phase(f,label,selected,before,after):
 assert len(before['sequences'])==len(after['sequences'])==11
 assert before['m3u_m3uaccount']==after['m3u_m3uaccount']and before['support']['core_systemevent']==after['support']['core_systemevent']
 for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
  others=lambda value:[r for r in value[table]if r['m3u_account_id']!=selected]
  assert others(before)==others(after),(label,table,'unrelated owner rows raw stable')
  for row in after[table]:
   if row['m3u_account_id']==selected:assert instant(before['observed_at'])<=instant(row['last_seen'])<=instant(after['observed_at'])
 if label=='shared-A-initial':
  assert len(after['vod_movie'])==len(after['vod_series'])==len(after['vod_m3umovierelation'])==len(after['vod_m3useriesrelation'])==1
  assert not after['vod_episode']and not after['vod_m3uepisoderelation']
  assert len(after['support']['vod_vodlogo'])==1
 if label=='shared-B-update':
  assert len(after['vod_movie'])==len(after['vod_series'])==1 and len(after['vod_m3umovierelation'])==len(after['vod_m3useriesrelation'])==2
  for table in ['vod_movie','vod_series']:
   for field in ['id','uuid','created_at','updated_at']:assert before[table][0][field]==after[table][0][field]
  movie=after['vod_movie'][0];series=after['vod_series'][0]
  assert movie['description']=='Movie description two'and movie['duration_secs']==3723 and movie['custom_properties']['director']=='Director One'and movie['custom_properties']['youtube_trailer']=='trailer-two'
  assert series['description']=='Series description two'and series['custom_properties']['director']=='Replacement Series Director'
  assert before['vod_episode']==after['vod_episode']and before['vod_m3uepisoderelation']==after['vod_m3uepisoderelation']
  assert len(after['support']['vod_vodlogo'])==2
 if label=='shared-A-prune':
  assert len(after['vod_m3umovierelation'])==len(after['vod_m3useriesrelation'])==1
  assert after['vod_m3useriesrelation'][0]['m3u_account_id']==f._shared_setup['B']
  assert before['vod_movie']==after['vod_movie']and before['vod_series']==after['vod_series']and before['vod_episode']==after['vod_episode']
  assert len(after['vod_episode'])==1 and not after['vod_m3uepisoderelation']
  assert before['support']['vod_vodlogo']==after['support']['vod_vodlogo']
 if label=='shared-B-prune':
  assert all(not after[t]for t in ['vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation'])
  assert len(after['support']['vod_vodlogo'])==2 and before['support']['vod_vodlogo']==after['support']['vod_vodlogo']



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
   previous=seed_descendant(f,private,after)if label=='shared-A-initial'else after;save(out/'report.json',report)
 except BaseException as caught:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(caught).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except BaseException as caught:report['observer_cleanup_error']=type(caught).__name__
 report['all_four_context_observations_complete']=len(report['cases'])==4 and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==4 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',Source_comparison_failure=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report


def compare_retained_Source(f,private):
 refs=f.binding['retained_source_vod_shared_metadata'];assert set(refs)=={'qualification','projection','report','execution','cleanup','Root_cleanup','identity'};values={}
 for name,ref in refs.items():
  path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],name;values[name]=json.loads(path.read_text())
 q=values['qualification'];assert q['Source_shared_metadata_context_capture_qualified']and q['actual_Source_cases_passed']==4 and q['actual_controller_exit']==0
 assert values['report']['status']=='passed'and values['report']['all_four_Source_observations_complete']and len(values['report']['cases'])==4 and all(r['status']=='passed'for r in values['report']['cases'])
 e=values['execution'];assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert values['cleanup']['exact_owned_absence']and values['Root_cleanup']['passed']
 identity=values['identity'];assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'and identity['actual_natural_root_count']==4
 from vod_shared_metadata_compare import PHASES,SEED,compare_public,source_http_origin
 refs=f.binding['retained_source_vod_shared_metadata_SQL'];assert set(refs)==set(PHASES)|{SEED};sql={}
 for label,ends in refs.items():
  assert set(ends)=={'before','after'};sql[label]={}
  for end,ref in ends.items():
   path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'];sql[label][end]=json.loads(path.read_text())
 source_origin=source_http_origin(f.binding['retained_source_vod_shared_metadata_http_origin'],identity,values['cleanup'])
 from vod_dense_list_compare import native_http_origin
 native_origin,proof=native_http_origin(f)
 save(Path(private).parent/'HTTP-origin-binding.json',{'Source_actual_origin':source_origin,'Native_actual_context':proof,'projection_scope':'Only exact SQL-bound nested logo.cache_url request origin maps to verified Source origin'})
 actual=compare_public(values['projection'],values['report'],sql,private,source_origin,native_origin);assert actual['status']=='passed';save(Path(private).parent/'Source-comparison.json',actual)
