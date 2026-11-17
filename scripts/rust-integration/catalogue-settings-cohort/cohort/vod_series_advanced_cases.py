"""Unexecuted Source characterization of catalogue refresh after provider details.
Original Dispatcharr and dependency authors retain all application credit.
A retained Source AFTER row is explicit fixture setup, never generated evidence.
"""
import base64,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
from vod_series_info_cases import load_cases,instant,MODEL_IMPORT
from provider_cases import control
PORT=18095
CASE='vod-series-advanced'
TASK='apps.vod.tasks.refresh_vod_content'

def prepare_context(fixture):
 if fixture.case!=CASE:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json','vod_series_advanced_cases':{'vod-advanced-matching':[{'num':1,'series_id':3000,'name':'rust-probe-vod-rest-Series0','category_id':'11','releaseDate':'2010-01-01'}],'vod-advanced-empty':[],'vod-advanced-yearless':[{'num':1,'series_id':3000,'name':'rust-probe-vod-rest-Series0','category_id':'11'}]}}
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
def snapshot(f):
 return f.sql(MODEL_IMPORT+"from core.models import SystemEvent;from apps.vod.models import VODLogo,VODCategory,M3UVODCategoryRelation;assert all(m.objects.count()<=128 for m in models);result={m._meta.db_table:list(m.objects.order_by('id').values())for m in models};assert all(m.objects.count()<=256 for m in [VODLogo,VODCategory,M3UVODCategoryRelation,SystemEvent]);result['support']={m._meta.db_table:list(m.objects.order_by('id').values())for m in [VODLogo,VODCategory,M3UVODCategoryRelation,SystemEvent]};result['observed_at']=__import__('django.utils.timezone',fromlist=['now']).now();result['sequences']={};\nwith connection.cursor()as cursor:\n for m in [*models,VODLogo,VODCategory,M3UVODCategoryRelation,SystemEvent]:\n  cursor.execute(\"SELECT pg_get_serial_sequence(%s,'id')\",[m._meta.db_table]);name=cursor.fetchone()[0]\n  if name:\n   assert name in {'public.'+x._meta.db_table+'_id_seq'for x in [*models,VODLogo,VODCategory,M3UVODCategoryRelation,SystemEvent]}\n   cursor.execute('SELECT last_value,is_called FROM '+name);value=cursor.fetchone();result['sequences'][name]={'last_value':value[0],'is_called':value[1]}\nprint(json.dumps(result,cls=SourceExactEncoder))",secret=True)

def _prepare_rows(f):
 assert f.backend in('Original','Native')
 base_series._prepare_rows(f)
 f._advanced_missing=snapshot(f)
 f._advanced_source=next(r for r in load_cases(f)if r['label']=='series_force')
 _seed(f,initial=True)

def _seed(f,initial=False):
 record=f._advanced_source
 # This is explicitly fixture setup from a retained actual Source AFTER row,
 # never a write pretending to be the output of the following natural root.
 selected=record['after']['vod_series'][0]
 assert selected['id']==1 and selected['name']=='rust-probe-vod-rest-Series0'
 rows={'vod_series':[selected],
  'vod_episode':[r for r in record['after']['vod_episode']if r['series_id']==1],
  'vod_m3useriesrelation':[r for r in record['after']['vod_m3useriesrelation']if r['series_id']==1],
  'vod_m3uepisoderelation':[r for r in record['after']['vod_m3uepisoderelation']if r['series_relation_id']==1]}
 assert len(rows['vod_episode'])==4 and len(rows['vod_m3useriesrelation'])==1 and len(rows['vod_m3uepisoderelation'])==1
 rows=json.loads(json.dumps(rows))
 for relation in rows['vod_m3useriesrelation']+rows['vod_m3uepisoderelation']:
  assert relation['m3u_account_id']==2
  relation['m3u_account_id']=f._vod_setup['account']
 code="from apps.vod.models import Movie,Series,Episode,VODCategory,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation;from django.db import transaction;from django.core.serializers.json import DjangoJSONEncoder;rows=json.loads("+repr(json.dumps(rows))+");\nwith transaction.atomic():\n assert not Movie.objects.exists()and not M3UMovieRelation.objects.exists()\n M3UEpisodeRelation.objects.all().delete();M3USeriesRelation.objects.all().delete();Episode.objects.all().delete();Series.objects.all().delete()\n"
 if initial:
  code+=" assert not VODCategory.objects.exists()and not VODLogo.objects.exists()\n VODCategory.objects.bulk_create([VODCategory(id=2,name='rust-probe-vod-rest-Drama',category_type='series')]);VODLogo.objects.bulk_create([VODLogo(id=1,name='rust-probe-vod-rest-shared-logo',url='https://fixture.invalid/logo.jpg')])\n with connection.cursor()as cursor:cursor.execute(\"SELECT setval(pg_get_serial_sequence('vod_vodcategory','id'),3,true)\");cursor.execute(\"SELECT setval(pg_get_serial_sequence('vod_vodlogo','id'),1,true)\")\n"
 code+=" for m in [Series,Episode,M3USeriesRelation,M3UEpisodeRelation]:\n  for row in rows[m._meta.db_table]:\n   m.objects.bulk_create([m(**row)]);m.objects.filter(pk=row['id']).update(**{k:v for k,v in row.items()if k!='id'})\n with connection.cursor()as cursor:\n  for name,state in json.loads("+repr(json.dumps(record['sequence_after']))+").items():\n   if name in {'public.'+m._meta.db_table+'_id_seq'for m in [Series,Episode,M3USeriesRelation,M3UEpisodeRelation]}:cursor.execute('SELECT setval(%s::regclass,%s,%s)',[name,state['last_value'],state['is_called']])\nprint(json.dumps({'declared_fixture_setup':'selected actual Source series_force AFTER rows','selected_existing_Series':1,'selected_existing_Episodes':4,'selected_existing_episode_streams':1,'initial':"+repr(initial)+"}))"
 result=f.sql(code,secret=True)
 seeded=snapshot(f)
 for table,expected in rows.items():
  assert json.dumps(seeded[table],sort_keys=True)==json.dumps(expected,sort_keys=True),table
 f._advanced_seed=seeded
 return result

def _stable_rows(before,after,table,except_fields=()):
 assert [r['id']for r in before[table]]==[r['id']for r in after[table]],table
 for a,b in zip(before[table],after[table]):
  assert {k:v for k,v in a.items()if k not in except_fields}=={k:v for k,v in b.items()if k not in except_fields},table

def run(f):
 assert f.backend in('Original','Native')and f.case==CASE
 out=f.output/CASE;out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
 if f.backend=='Original':save(private/'Beat-stop.json',f.role('beat',[],stop=True))
 else:
  status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['clocked_scheduler_enabled']is False
 report={'status':'running','cases':[],'Source_characterization':f.backend=='Original','retained_provider_info_replayed':False,'advanced_seed_from_retained_actual_Source':True,'Native_qualified':False,'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 observer=None
 def phase(label,provider_case):
  nonlocal observer
  row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report);p=private/label;p.mkdir(mode=0o700)
  f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain')
  before=snapshot(f);cache=aggregate.cache_state(f);trace_before=control(f,'/snapshot')['trace'];control(f,'/control',{'case':provider_case});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,CASE+'-'+label,database=1)
  api=f.api('POST','/api/m3u/accounts/'+str(f._vod_setup['account'])+'/refresh-vod/',{});save(p/'HTTP.json',api)
  assert api['status']==202 and api['body']=={'message':'VOD refresh initiated for account Owned VOD ingest'}
  def publication():
   frames=[r for r in aggregate.frames(observer)if r['headers']['task']==TASK];assert len(frames)<=1
   return frames[0]if frames else False
  frame=f.wait(publication,15,label+'-actual-root');ident=frame['headers']['id']
  backend=f.wait(lambda:aggregate.terminal(f,ident),120,label+'-actual-terminal');save(p/'backend.json',backend)
  f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-actual-drain');durable=journal.producer_observer_stop(f,observer);observer=None
  after=snapshot(f);catalogue=f.api('GET','/api/vod/series/?page_size=100');save(p/'catalogue.json',catalogue);events=aggregate.receiver_read(f,receiver);trace=control(f,'/snapshot')['trace'][len(trace_before):]
  for name,value in [('SQL-before',before),('SQL-after',after),('WS',events),('Origin',trace),('journal',durable),('cache-before',cache),('cache-after',aggregate.cache_state(f))]:save(p/(name+'.json'),value)
  # Capture observations before contract assertions. Detection of unexpected
  # Source behavior remains a literal failed characterization, not a rewrite.
  assert frame['headers']['root_id']==ident and frame['headers']['parent_id']is None
  assert backend['status']=='SUCCESS'and backend['children']==[]
  match=re.fullmatch(r'Batch VOD refresh completed for account Owned VOD ingest in ([0-9]+\.[0-9]{2}) seconds',backend['result']);assert match
  assert [(r['task'],r['id'])for r in durable['publications']]==[(TASK,ident)]
  requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r]
  assert len(requests)==5
  for actual,action in zip(requests,[None,'get_vod_categories','get_series_categories','get_vod_streams','get_series']):
   expected={key:[hashlib.sha256(value.encode()).hexdigest()]for key,value in f._vod_login.items()}
   if action is not None:expected['action']=[hashlib.sha256(action.encode()).hexdigest()]
   assert actual['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==expected and actual['actual_TLS']and actual['status']==200 and actual['actual_user_agent']==f._vod_setup['actual_user_agent']
  updates=[r['data']for r in events['events']if isinstance(r.get('data'),dict)and r['data'].get('account')==f._vod_setup['account']]
  assert updates==[{'progress':0,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'processing','message':'Owned VOD account baseline'},{'progress':100,'type':'m3u_refresh','account':f._vod_setup['account'],'action':'vod_refresh','status':'success','message':'VOD refresh completed in '+match[1]+' seconds'}]
  assert before['m3u_m3uaccount']==after['m3u_m3uaccount']
  assert before['support']['core_systemevent']==after['support']['core_systemevent']
  assert not after['vod_movie']and not after['vod_m3umovierelation']
  assert catalogue['status']==200 and catalogue['body']['count']==len(after['vod_series'])
  row.update(actual_root=ident,actual_provider_requests=5,actual_queued_children=0,status='observed');save(out/'report.json',report)
  return row,before,after
 try:
  save(private/'retained-Source-seed-namespace.json',{'Source_account_id':2,'actual_owned_account_id':f._vod_setup['account'],'only_selected_VOD_FKs_translated':True,'bootstrap_account_rows_not_overwritten':True})
  save(private/'missing-catalogue-before-qualified-seed.json',f._advanced_missing)
  save(private/'details-qualified-initial-seed.json',f._advanced_seed)
  row,before,after=phase('sparse-list-preserves-detail-and-Episodes','vod-advanced-matching')
  assert len(after['vod_series'])==1 and len(after['vod_episode'])==4 and len(after['vod_m3uepisoderelation'])==1
  _stable_rows(before,after,'vod_episode');_stable_rows(before,after,'vod_m3uepisoderelation')
  _stable_rows(before,after,'vod_series',('logo_id','custom_properties'))
  series=after['vod_series'][0];assert series['logo_id']is None and series['custom_properties']=={'fixture':0,'releaseDate':'2010-01-01','category_id':'11'}
  relation=after['vod_m3useriesrelation'][0];old=before['vod_m3useriesrelation'][0]
  basic={'num':1,'series_id':3000,'name':'rust-probe-vod-rest-Series0','category_id':'11','releaseDate':'2010-01-01','_provider_category_id':'11','_category_id':2}
  expected={**old,'category_id':2,'last_seen':relation['last_seen'],'custom_properties':{**old['custom_properties'],'basic_data':basic}}
  assert relation==expected and relation['category_id']==2
  assert instant(before['observed_at'])<=instant(relation['last_seen'])<=instant(after['observed_at']);row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('advanced-catalogue-repeat','vod-advanced-matching')
  for table in ['vod_series','vod_episode','vod_m3uepisoderelation']:_stable_rows(before,after,table)
  _stable_rows(before,after,'vod_m3useriesrelation',('last_seen',));assert instant(before['observed_at'])<=instant(after['vod_m3useriesrelation'][0]['last_seen'])<=instant(after['observed_at']);row['status']='passed';save(out/'report.json',report)
  row,before,after=phase('prune-cascades-detail-Episodes-and-streams','vod-advanced-empty')
  assert all(not after[table]for table in ['vod_series','vod_episode','vod_m3useriesrelation','vod_m3uepisoderelation'])
  assert after['support']['vod_vodlogo']==before['support']['vod_vodlogo'];row['status']='passed';save(out/'report.json',report)
  save(private/'independent-yearless-branch-setup.json',_seed(f))
  row,before,after=phase('yearless-list-reassigns-and-cascades-old-details','vod-advanced-yearless')
  assert len(after['vod_series'])==1 and len(after['vod_m3useriesrelation'])==1 and not after['vod_episode']and not after['vod_m3uepisoderelation']
  new=after['vod_series'][0];old_series=before['vod_series'][0];relation=after['vod_m3useriesrelation'][0];old=before['vod_m3useriesrelation'][0]
  assert new['id']!=old_series['id']and new['uuid']!=old_series['uuid']and new['year']is None and new['description']==''and new['rating']is None and new['genre']==''and new['logo_id']is None
  assert new['name']==old_series['name']and new['custom_properties']=={'category_id':'11'}
  basic={'num':1,'series_id':3000,'name':'rust-probe-vod-rest-Series0','category_id':'11','_provider_category_id':'11','_category_id':2}
  expected={**old,'series_id':new['id'],'category_id':2,'last_seen':relation['last_seen'],'custom_properties':{**old['custom_properties'],'basic_data':basic,'detailed_fetched':False,'episodes_fetched':False}}
  assert relation==expected and relation['category_id']==2
  assert instant(before['observed_at'])<=instant(relation['last_seen'])<=instant(after['observed_at'])
  assert new['tmdb_id']is None and new['imdb_id']is None and new['language']==''and uuid.UUID(new['uuid']).version==4
  assert all(instant(before['observed_at'])<=instant(new[key])<=instant(after['observed_at'])for key in ['created_at','updated_at'])
  assert after['support']['vod_vodlogo']==before['support']['vod_vodlogo'];row['status']='passed';save(out/'report.json',report)
 except BaseException as error:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(error).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except Exception as error:report['observer_cleanup_error']=type(error).__name__
 report['status']='passed'if len(report['cases'])==4 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if report['status']=='passed':
  from vod_series_advanced_projection import project
  actual=project(private);save(out/'semantic-projection.json',actual)
  if f.backend=='Native':
   try:
    refs=f.binding['retained_source_vod_series_advanced']
    values={}
    for name,ref in refs.items():
     path=Path(ref['path']);assert hashlib.sha256(path.read_bytes()).hexdigest()==ref['sha256'],name;values[name]=json.loads(path.read_text())
    qualification=values['qualification'];assert qualification['Source_advanced_Series_context_capture_qualified']is True and qualification['actual_Source_cases_passed']==4 and qualification['actual_controller_exit']==0
    source=values['report'];assert source['status']=='passed'and len(source['cases'])==4 and all(row['status']=='passed'for row in source['cases'])
    execution=values['execution'];assert execution['actual_exit']==0 and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']
    cleanup=values['cleanup'];assert cleanup['exact_owned_absence']and cleanup['all_original_process_generations_absent']and cleanup['host_CLI_groups_settled']and not cleanup['errors']
    assert values['Root_cleanup']['passed']
    assert json.dumps(actual,sort_keys=True,separators=(',',':'),allow_nan=False)==json.dumps(values['projection'],sort_keys=True,separators=(',',':'),allow_nan=False)
    report.update(Native_qualified=True,exact_retained_Source_full_rows_and11sequences_equal=True)
   except BaseException as error:
    report.update(status='failed',semantic_comparison_failure=type(error).__name__);(private/'semantic-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report
