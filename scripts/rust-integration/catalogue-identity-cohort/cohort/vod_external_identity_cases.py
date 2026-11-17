"""Actual-Source-qualified bounded external-ID list identity9 observer.
Original Dispatcharr contributors and dependency authors retain all credit.
Nine independent bounded pre-root graphs; actual HTTP/claimed workers only.
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
CASE='vod-external-identity'
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
PHASES=('fresh-priority','duplicate-ID-occurrences','same-provider-ID-repeat','external-ID-name-year-change','alias-and-zero-precedence','changed-primary-existing-target','sparse-ID-omission','secondary-ID-recheck','duplicate-secondary-conflict')
def _prepare_rows(f):base_series._prepare_rows(f)
def movie(stream,name,**extra):return {'stream_id':stream,'name':name,'year':2010,'category_id':'11','container_extension':'mp4',**extra}
def series(external,name,**extra):return {'series_id':external,'name':name,'releaseDate':'2010-01-02','category_id':'21',**extra}
def payloads():
 def response(m,s):return {'get_vod_categories':[{'category_id':'11','category_name':'Owned Movies'}],'get_vod_streams':m,'get_series_categories':[{'category_id':'21','category_name':'Owned Series'}],'get_series':s}
 return {
 'fresh-priority':response([movie(9701,'Movie TMDB',tmdb_id='7101',imdb_id='tt7101'),movie(9702,'Movie IMDB',imdb_id='tt7102'),movie(9703,'Movie Plain')],[series(9801,'Series TMDB',tmdb='8101',imdb='tt8101'),series(9802,'Series IMDB',imdb='tt8102'),series(9803,'Series Plain')]),
 'duplicate-ID-occurrences':response([movie(9701,'Movie First',tmdb_id='7101',imdb_id='tt7101',description=''),movie(9702,'Movie Later',tmdb_id='7101',imdb_id='tt-ignored',description='filled')],[series(9801,'Series First',tmdb='8101',imdb='tt8101',plot=''),series(9802,'Series Later',tmdb='8101',imdb='tt-ignored',plot='filled')]),
 'same-provider-ID-repeat':response([movie(9701,'Movie First',tmdb_id='7101',description=''),movie(9701,'Movie Later',tmdb_id='7101',description='filled')],[series(9801,'Series First',tmdb='8101',plot=''),series(9801,'Series Later',tmdb='8101',plot='filled')]),
 'external-ID-name-year-change':response([movie(9701,'Movie Incoming New Name',tmdb_id='7101',description='updated')],[series(9801,'Series Incoming New Name',tmdb='8101',plot='updated')]),
 'alias-and-zero-precedence':response([movie(9701,'Movie Alias',tmdb_id='7101',tmdb='7102',imdb_id='tt7101',imdb='tt-ignored'),movie(9702,'Movie Numeric Zero',tmdb_id=0,tmdb='7103'),movie(9703,'Movie Text Zero',tmdb_id='0',tmdb='7104',imdb_id='tt7104')],[series(9801,'Series Alias',tmdb='8101',tmdb_id='8102',imdb='tt8101',imdb_id='tt-ignored'),series(9802,'Series Numeric Zero',tmdb=0,tmdb_id='8103'),series(9803,'Series Text Zero',tmdb='0',tmdb_id='8104',imdb='tt8104')]),
 'changed-primary-existing-target':response([movie(9701,'Movie Incoming Target Name',tmdb_id='7102',description='updated target')],[series(9801,'Series Incoming Target Name',tmdb='8102',plot='updated target')]),
 'sparse-ID-omission':response([movie(9701,'Movie Sparse New'),movie(9702,'Movie Sparse Existing')],[series(9801,'Series Sparse New'),series(9802,'Series Sparse Existing')]),
 'secondary-ID-recheck':response([movie(9701,'Movie New Candidate',tmdb_id='7199',imdb_id='tt7101',description='candidate metadata')],[series(9801,'Series New Candidate',tmdb='8199',imdb='tt8101',plot='candidate metadata')]),
 'duplicate-secondary-conflict':response([movie(9701,'Movie Conflict One',tmdb_id='7301',imdb_id='tt-conflict'),movie(9702,'Movie Conflict Two',tmdb_id='7302',imdb_id='tt-conflict')],[series(9801,'Series Conflict One',tmdb='8301',imdb='tt-series-conflict'),series(9802,'Series Conflict Two',tmdb='8302',imdb='tt-series-conflict')])}

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

def seed_spec(label):
 def row(name,tmdb=None,imdb=None,stream=None,year=2010):return {'name':name,'year':year,'tmdb_id':tmdb,'imdb_id':imdb,'stream':stream}
 result={'movie':[],'series':[]}
 if label=='external-ID-name-year-change':
  result={'movie':[row('Movie Persisted Old Name','7101',stream='9701',year=2009)],'series':[row('Series Persisted Old Name','8101',stream='9801',year=2009)]}
 if label=='changed-primary-existing-target':
  result={'movie':[row('Movie Old Target','7101',stream='9701'),row('Movie Existing Target','7102')],'series':[row('Series Old Target','8101',stream='9801'),row('Series Existing Target','8102')]}
 if label=='sparse-ID-omission':
  result={'movie':[row('Movie Sparse New','7101',stream='9701'),row('Movie Sparse Existing','7102',stream='9702'),row('Movie Sparse Existing')],'series':[row('Series Sparse New','8101',stream='9801'),row('Series Sparse Existing','8102',stream='9802'),row('Series Sparse Existing')]}
 if label=='secondary-ID-recheck':
  result={'movie':[row('Movie IMDb Recheck Target',imdb='tt7101',stream='9700',year=2009)],'series':[row('Series IMDb Recheck Target',imdb='tt8101',stream='9800',year=2009)]}
 return result

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
 for table in ['vod_m3umovierelation','vod_m3useriesrelation']:
  for relation in after[table]:assert instant(before['observed_at'])<=instant(relation['last_seen'])<=instant(after['observed_at'])
 for kind,table,relation_table in [('movie','vod_movie','vod_m3umovierelation'),('series','vod_series','vod_m3useriesrelation')]:
  rows=after[table];relations=after[relation_table];prefix='7'if kind=='movie'else'8';idfield='stream_id'if kind=='movie'else'external_series_id'
  if label=='fresh-priority':
   assert len(rows)==len(relations)==3
   assert {(r['tmdb_id'],r['imdb_id'])for r in rows}=={(prefix+'101','tt'+prefix+'101'),(None,'tt'+prefix+'102'),(None,None)}
  if label in ['duplicate-ID-occurrences','same-provider-ID-repeat']:
   assert len(rows)==1 and len(relations)==(2 if label=='duplicate-ID-occurrences'else 1)
   assert rows[0]['name']==('Movie First'if kind=='movie'else'Series First')and rows[0]['description']=='filled'
   assert rows[0]['tmdb_id']==prefix+'101'
   assert all(r['custom_properties']['basic_data']['name'] in ['Movie First','Movie Later','Series First','Series Later']for r in relations)
   if label=='same-provider-ID-repeat':assert relations[0]['custom_properties']['basic_data']['name'].endswith('First')
  if label=='external-ID-name-year-change':
   assert len(rows)==len(relations)==1;old=before[table][0];new=rows[0]
   assert new['id']==old['id']and new['uuid']==old['uuid']and new['name']==old['name']and new['year']==2010 and new['description']=='updated'
   assert new['created_at']==old['created_at']and new['updated_at']==old['updated_at']
   assert relations[0]['custom_properties']['detailed_fetched']is True
  if label=='alias-and-zero-precedence':
   assert len(rows)==len(relations)==3
   assert {(r['tmdb_id'],r['imdb_id'])for r in rows}=={(prefix+'101','tt'+prefix+'101'),(prefix+'103',None),(None,'tt'+prefix+'104')}
  if label=='changed-primary-existing-target':
   target=next(r for r in before[table]if r['tmdb_id']==prefix+'102');old_relation=before[relation_table][0]
   assert len(rows)==len(relations)==1 and rows[0]['id']==target['id']and rows[0]['name']==target['name']and rows[0]['description']=='updated target'
   assert relations[0]['id']==old_relation['id']and relations[0]['custom_properties']['detailed_fetched']is False
  if label=='sparse-ID-omission':
   assert len(rows)==len(relations)==2 and all(r['tmdb_id']is None and r['imdb_id']is None for r in rows)
   existing=next(r for r in before[table]if r['tmdb_id']is None and r['imdb_id']is None)
   assert existing['id']in {r['id']for r in rows}and all(r['custom_properties']['detailed_fetched']is False for r in relations)
  if label=='secondary-ID-recheck':
   assert len(rows)==len(relations)==1;old=before[table][0]
   assert rows==before[table]and rows[0]['tmdb_id']is None and rows[0]['imdb_id']==old['imdb_id']and rows[0]['description']=='seed metadata'
  if label=='duplicate-secondary-conflict':assert not rows and not relations,'Caught batch unique conflict retains literal empty entity outcome'
  if kind=='series'and label in ['changed-primary-existing-target','sparse-ID-omission']:assert all(r['custom_properties']['episodes_fetched']is False for r in relations)
  # Every preexisting provider relation that survives keeps its PK/clocks and
  # refresh marker even when its global identity is reassigned.
  old={r['id']:r for r in before[relation_table]}
  for relation in relations:
   if relation['id']in old:
    for key in ['created_at','updated_at']+(['last_advanced_refresh']if kind=='movie'else['last_episode_refresh']):assert relation[key]==old[relation['id']][key]



def run(f):
 assert f.backend in('Original','Native')and f.case==CASE
 out=f.output/CASE;out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','Source_characterization':f.backend=='Original','Native_qualified':False,'cases':[],'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 previous=None;observer=None
 try:
  for label in PHASES:
   row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report)
   p=private/label;p.mkdir(mode=0o700);f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
   prepared,setup=prepare_cell(f,private,label,previous)
   before=snapshot(f);assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in prepared.items()if k!='observed_at'},'Declared setup to root before exact';cache=aggregate.cache_state(f);trace_start=len(control(f,'/snapshot')['trace']);control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,CASE+'-'+label,database=1)
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
   row.update(actual_root=ident,actual_provider_requests=5,actual_queued_children=0,Source_observation_complete=True)
   try:
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
 report['all_nine_context_observations_complete']=len(report['cases'])==9 and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==9 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',Source_comparison_failure=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report


def compare_retained_Source(f,private):
 refs=f.binding['retained_source_vod_external_identity'];assert set(refs)=={'qualification','projection','report','execution','cleanup','Root_cleanup','identity'};values={}
 for name,ref in refs.items():
  path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],name;values[name]=json.loads(path.read_text())
 q=values['qualification'];assert q['Source_external_identity_context_capture_qualified']and q['actual_Source_cases_passed']==9 and q['actual_controller_exit']==0
 assert values['report']['status']=='passed'and values['report']['all_nine_Source_observations_complete']and len(values['report']['cases'])==9 and all(r['status']=='passed'for r in values['report']['cases'])
 e=values['execution'];assert e['actual_exit']==0 and e['stdout_EOF']and e['stderr_EOF']and e['process_group_settled']
 assert values['cleanup']['exact_owned_absence']and values['Root_cleanup']['passed']
 identity=values['identity'];assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'and identity['actual_natural_root_count']==9
 from vod_external_identity_compare import PHASES,compare_public,source_http_origin
 refs=f.binding['retained_source_vod_external_identity_SQL'];labels=tuple(PHASES)+tuple(label+'-setup'for label in PHASES);assert set(refs)==set(labels);sql={}
 for label,ends in refs.items():
  assert set(ends)=={'before','after'};sql[label]={}
  for end,ref in ends.items():
   path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'];sql[label][end]=json.loads(path.read_text())
 source_origin=source_http_origin(f.binding['retained_source_vod_external_identity_http_origin'],identity,values['cleanup'])
 from vod_dense_list_compare import native_http_origin
 native_origin,proof=native_http_origin(f)
 save(Path(private).parent/'HTTP-origin-binding.json',{'Source_actual_origin':source_origin,'Native_actual_context':proof,'projection_scope':'Only exact SQL-bound nested logo.cache_url request origin maps to verified Source origin'})
 actual=compare_public(values['projection'],values['report'],sql,private,source_origin,native_origin);assert actual['status']=='passed';save(Path(private).parent/'Source-comparison.json',actual)
