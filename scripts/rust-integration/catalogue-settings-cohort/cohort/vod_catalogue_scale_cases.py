"""Private catalogue-scale Source characterization; not executed.
Original Dispatcharr contributors and dependency authors retain all credit.
One additive scale context on the existing bootstrap; five continuous roots across Source1000-row transaction boundaries.
"""
import copy,hashlib,json,re,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base_series
from vod_series_info_cases import MODEL_IMPORT
from vod_series_info_cases import instant
from provider_cases import control
CASE='vod-catalogue-scale'
TASK='apps.vod.tasks.refresh_vod_content'
PORT=18095
PHASES=('scale-threshold-1000','scale-multi-2001','scale-repeat-2001','scale-middle-batch-conflict','scale-prune')
SNAPSHOT_BYTES=24*1024*1024
SCALE_PAGES=64

def _prepare_rows(f):base_series._prepare_rows(f)

def payloads():
 def rows(kind,count,description):
  movie=kind=='movie';result=[]
  for number in range(1,count+1):
   row={'name':('Scale Movie 'if movie else'Scale Series ')+str(number).zfill(5),'category_id':'11'if movie else'21'}
   if movie:row.update(stream_id=500000+number,year=2010,container_extension='mp4',tmdb_id='scale-m-'+str(number),description=description)
   else:row.update(series_id=600000+number,releaseDate='2010-01-02',tmdb='scale-s-'+str(number),plot=description)
   result.append(row)
  return result
 def response(count,description):return {'get_vod_categories':[{'category_id':'11','category_name':'Owned Movies'}],'get_series_categories':[{'category_id':'21','category_name':'Owned Series'}],'get_vod_streams':rows('movie',count,description),'get_series':rows('series',count,description)}
 threshold=response(1000,'scale initial');multi=response(2001,'scale full');failure=response(2001,'scale committed around failed middle batch')
 for action,primary,secondary,prefix in [('get_vod_streams','tmdb_id','imdb_id','m'),('get_series','tmdb','imdb','s')]:
  for index in [1000,1001]:failure[action][index].update({primary:'scale-conflict-'+prefix+'-'+str(index),secondary:'scale-conflict-secondary-'+prefix})
 prune=copy.deepcopy(multi)
 for action in ['get_vod_streams','get_series']:prune[action]=[prune[action][index]for index in [0,999,2000]]
 return {'scale-threshold-1000':threshold,'scale-multi-2001':multi,'scale-repeat-2001':copy.deepcopy(multi),'scale-middle-batch-conflict':failure,'scale-prune':prune}

def snapshot(f,target):
 """One read-only full SQL observation via a bounded private transfer file.
 The existing1MiB command pipe remains unchanged and carries only a SHA receipt.
 The host owns the precreated0600 file; the ordinary root SQL executor writes it
 without chown/replacement. No Source app/model/task output is manufactured.
 """
 import os,stat
 target=Path(target);target.parent.mkdir(parents=True,exist_ok=True)
 assert f.data.stat().st_uid==os.getuid(),'Host-owned transfer root required'
 host=f.data/('.owned-scale-snapshot-'+uuid.uuid4().hex+'.json');guest='/shared/'+host.name
 fd=os.open(host,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.close(fd);initial=host.stat()
 code=MODEL_IMPORT+"""
import hashlib,os
from pathlib import Path
from core.models import SystemEvent
from apps.vod.models import VODLogo,VODCategory,M3UVODCategoryRelation
support=[VODLogo,VODCategory,M3UVODCategoryRelation,SystemEvent];allmodels=[*models,*support]
assert os.geteuid()==0,'Existing baseline root SQL executor required'
counts={m._meta.db_table:m.objects.count()for m in allmodels}
assert counts['m3u_m3uaccount']<=64 and all(counts[m._meta.db_table]<=8192 for m in models[1:]) and all(counts[m._meta.db_table]<=512 for m in support)
with connection.cursor()as cursor:
 total=0
 for model in allmodels:
  assert model._meta.db_table in {m._meta.db_table for m in allmodels}
  cursor.execute('SELECT COALESCE(sum(octet_length(row_to_json(t)::text)),0) FROM '+model._meta.db_table+' t');total+=cursor.fetchone()[0]
 assert total<=16*1024*1024,'Bound decoded SQL observation before materialization'
result={m._meta.db_table:list(m.objects.order_by('id').values())for m in models};result['support']={m._meta.db_table:list(m.objects.order_by('id').values())for m in support};result['sequences']={}
with connection.cursor()as cursor:
 for model in allmodels:
  cursor.execute("SELECT pg_get_serial_sequence(%s,'id')",[model._meta.db_table]);name=cursor.fetchone()[0]
  if name:
   assert name in {'public.'+m._meta.db_table+'_id_seq'for m in allmodels}
   cursor.execute('SELECT last_value,is_called FROM '+name);value=cursor.fetchone();result['sequences'][name]={'last_value':value[0],'is_called':value[1]}
result['observed_at']=__import__('django.utils.timezone',fromlist=['now']).now()
raw=json.dumps(result,cls=SourceExactEncoder,allow_nan=False).encode();assert len(raw)<=24*1024*1024
path=Path(GUEST);assert path.is_file()and path.stat().st_size==0
with path.open('wb')as stream:stream.write(raw);stream.flush();os.fsync(stream.fileno())
print(json.dumps({'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'full_table_counts':counts,'all11sequences':len(result['sequences'])==11,'SQL_executor_UID':os.geteuid(),'root_guest_private_file_only':True}))
""".replace('GUEST',repr(guest))
 try:
  receipt=f.sql(code,secret=True);now=host.lstat();assert stat.S_ISREG(now.st_mode)and now.st_ino==initial.st_ino and now.st_uid==os.getuid()and stat.S_IMODE(now.st_mode)==0o600
  assert receipt['all11sequences']and receipt['SQL_executor_UID']==0 and 0<receipt['bytes']<=SNAPSHOT_BYTES and now.st_size==receipt['bytes']
  with host.open('rb')as stream:raw=stream.read(SNAPSHOT_BYTES+1)
  assert len(raw)==receipt['bytes']and hashlib.sha256(raw).hexdigest()==receipt['sha256']
  target.write_bytes(raw);target.chmod(0o600);save(target.with_suffix('.staging.json'),receipt)
  return json.loads(raw)
 finally:host.unlink(missing_ok=True)

def page_number(url,base,kind):
 from urllib.parse import urlsplit,parse_qs
 actual=urlsplit(url);origin=urlsplit(base);query=parse_qs(actual.query,keep_blank_values=True)
 assert actual.scheme==origin.scheme and actual.netloc==origin.netloc and actual.username is None and actual.password is None and not actual.fragment
 assert actual.path=='/api/vod/'+kind+'/'and set(query)<=set(['page','page_size','ordering'])
 assert query.get('page_size')==['100']and query.get('ordering')==['name']
 page=query.get('page',['1']);assert len(page)==1 and page[0].isdigit()and str(int(page[0]))==page[0]and 1<=int(page[0])<=SCALE_PAGES
 return int(page[0]),actual.path+'?'+actual.query

def collect_catalogue(f,directory):
 Path(directory).mkdir(mode=0o700,exist_ok=True)
 result={}
 for kind in ['movies','series']:
  url=f.base_url+'/api/vod/'+kind+'/?page_size=100&ordering=name';pages=[];seen=set();rows=[];count=None
  while url is not None:
   number,path=page_number(url,f.base_url,kind);assert number==len(pages)+1 and number not in seen;seen.add(number)
   reply=f.api('GET',path);save(Path(directory)/(kind+'-page-'+str(number).zfill(3)+'.json'),{'request_path':path,'response':reply});assert reply['status']==200;body=reply['body'];assert type(body['count'])is int and 0<=body['count']<=8192 and len(body['results'])<=100
   if count is None:count=body['count']
   assert body['count']==count
   if body['previous']is not None:assert page_number(body['previous'],f.base_url,kind)[0]==number-1
   else:assert number==1
   pages.append({'request_path':path,'page_number':number,'response':reply});rows.extend(body['results']);url=body['next']
  assert len(rows)==count and len({row['id']for row in rows})==count
  result[kind]={'actual_base_url':f.base_url,'count':count,'pages':pages,'all_pages_collected':True}
 return result


def assert_cell(label,before,after,catalogue_pages):
 expected=1000 if label=='scale-threshold-1000'else 1001 if label=='scale-middle-batch-conflict'else 3 if label=='scale-prune'else 2001
 assert len(before['sequences'])==len(after['sequences'])==11
 assert before['m3u_m3uaccount']==after['m3u_m3uaccount']and before['support']['core_systemevent']==after['support']['core_systemevent']
 assert not after['vod_episode']and not after['vod_m3uepisoderelation']and not after['support']['vod_vodlogo']
 for kind,table,relation,idfield,prefix in [('movies','vod_movie','vod_m3umovierelation','stream_id','m'),('series','vod_series','vod_m3useriesrelation','external_series_id','s')]:
  rows=after[table];relations=after[relation];assert len(rows)==len(relations)==expected
  assert catalogue_pages[kind]['count']==expected and catalogue_pages[kind]['all_pages_collected']
  api_rows=[r for page in catalogue_pages[kind]['pages']for r in page['response']['body']['results']];assert {r['id']for r in api_rows}=={r['id']for r in rows}
  models={row['id']:row for row in rows};old={row['id']:row for row in before[table]}
  for row in rows:
   assert row['language']==''and row['tmdb_id'].startswith('scale-'+prefix+'-')and row['imdb_id']is None
   if row['id']in old:
    for key in ['uuid','created_at','updated_at','name']:assert row[key]==old[row['id']][key]
   else:
    assert uuid.UUID(row['uuid']).version==4
    for key in ['created_at','updated_at']:assert instant(before['observed_at'])<=instant(row[key])<=instant(after['observed_at'])
  old_rel={r['id']:r for r in before[relation]}
  for rel in relations:
   assert instant(before['observed_at'])<=instant(rel['last_seen'])<=instant(after['observed_at'])
   if rel['id']in old_rel:
    for key in ['created_at','updated_at']:assert rel[key]==old_rel[rel['id']][key]
  if label=='scale-threshold-1000':assert not before[table]and not before[relation]
  if label in ['scale-multi-2001','scale-repeat-2001']:assert all(row['description']=='scale full'for row in rows)
  if label=='scale-repeat-2001':assert before[table]==after[table]
  if label=='scale-middle-batch-conflict':
   numbers={int(row['tmdb_id'].removeprefix('scale-'+prefix+'-'))for row in rows};assert numbers==set(range(1,1001))|{2001}
   assert all(row['description']=='scale committed around failed middle batch'for row in rows)
   seq='public.'+table+'_id_seq';assert after['sequences'][seq]=={'last_value':before['sequences'][seq]['last_value']+2,'is_called':True}
   assert before['sequences']['public.'+relation+'_id_seq']==after['sequences']['public.'+relation+'_id_seq']
  if label=='scale-prune':assert {int(row['tmdb_id'].removeprefix('scale-'+prefix+'-'))for row in rows}=={1,1000,2001}


def prepare_context(fixture):
 if fixture.case!=CASE:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_dense_list_cases':payloads(),'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json'}
 # Only a key HASH is written into the /data bind. CA/leaf private keys are
 # generated in the Origin's private Source-side tmpdir, never exported.
 # Large controlled payloads travel as a bounded private file; never put
 # megabytes into exec argv or widen the existing command-pipe budget.
 host=Path(fixture.output)/'private'/'scale-provider-config.json';host.parent.mkdir(mode=0o700,exist_ok=True)
 raw=json.dumps(config,separators=(',',':'),allow_nan=False).encode();assert len(raw)<=8*1024*1024
 host.write_bytes(raw);host.chmod(0o600)
 fixture.command(fixture.compose+['cp',str(host),'baseline:'+guest+'/provider-origin-config.json'],seconds=40,secret=True)
 fixture.python("import os;path="+repr(guest+'/provider-origin-config.json')+";os.chown(path,65532,65532);os.chmod(path,0o600)",secret=True)
 save(Path(fixture.output)/'private'/'scale-provider-config-staging.json',{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'configured_phases':list(PHASES),'file_transfer_instead_of_inline_argv':True})
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



def run(f):
 assert f.backend in('Original','Native')and f.case==CASE
 phases=PHASES
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
   before=snapshot(f,p/'SQL-before.json')
   if previous is not None:assert {k:v for k,v in before.items()if k!='observed_at'}=={k:v for k,v in previous.items()if k!='observed_at'},'Continuous scale raw phase chain'
   cache=aggregate.cache_state(f);trace_start=len(control(f,'/snapshot')['trace']);control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,f.case+'-'+label,database=1)
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
    after=snapshot(f,p/'SQL-after.json');events=aggregate.receiver_read(f,receiver);trace=control(f,'/snapshot')['trace'][trace_start:]
    for name,value in [('WS',events),('Origin',trace),('journal',durable),('backend',backend),('frame',frame),('cache-before',cache),('cache-after',aggregate.cache_state(f))]:save(p/(name+'.json'),value)
    catalogue=collect_catalogue(f,p/'catalogue-pages');save(p/'catalogue.json',catalogue)
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
    for kind,table in [('movies','vod_movie'),('series','vod_series')]:assert catalogue[kind]['all_pages_collected']and catalogue[kind]['count']==len(after[table])
    assert_cell(label,before,after,catalogue);row['status']='passed'
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
 report['all_five_Source_scale_observations_complete']=len(report['cases'])==len(phases) and all(r.get('Source_observation_complete')for r in report['cases'])
 report['status']='passed'if len(report['cases'])==len(phases) and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 if f.backend=='Native'and report['status']=='passed':
  try:
   from vod_catalogue_domains_compare import compare_retained_Source
   compare_retained_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',Source_comparison_failure=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16384:])
 save(out/'report.json',report);return report
