"""Current Main provider-info replay of five retained actual Source TCP/SQL seeds.
Original Dispatcharr and dependency authors retain all application credit.
Native only; no natural-import chain, Source replay or performance claim.
"""
import base64,datetime,hashlib,json,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
from provider_cases import control
PORT=18095
CASE='vod-series-info'
def load_cases(f):
 refs=f.binding['retained_source_provider_info']
 for ref in refs.values():assert hashlib.sha256(Path(ref['path']).read_bytes()).hexdigest()==ref['sha256']
 proof=json.loads(Path(refs['authority']['path']).read_text());assert proof['capture_passed']and proof['restored18']and proof['sequences_restored']
 rows=json.loads(Path(refs['cases']['path']).read_text());assert len(rows)==5 and [r['label']for r in rows]==['series_missing_flags','series_fresh','series_force','series_empty_episodes','series_no_stored_empty']
 assert proof['excerpt_sha256']==refs['cases']['sha256']and all(r['historical_native_HTTP_control_passed']for r in proof['cases'])
 return rows

def prepare_context(fixture):
 if fixture.case!=CASE:raise ValueError('VODOwnedContextRequired')
 fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();r,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'schedule_enabled':False});r,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'name':'DVR Settings','value':{}});CoreSettings.objects.filter(pk=r.pk).update(value={**(r.value or {}),'pre_offset_minutes':0,'post_offset_minutes':0});print(json.dumps({'scope':'declared provider fixture defaults only'}))")
 key=uuid.uuid4().hex+uuid.uuid4().hex;guest='/shared/'+fixture.case
 user=uuid.uuid4().hex;password=uuid.uuid4().hex
 fixture._vod_login={'username':user,'password':password}
 config={'vod_login':fixture._vod_login,'key_sha256':hashlib.sha256(key.encode()).hexdigest(),'ca_path':guest+'/provider-ca.pem','ready':guest+'/provider-origin.ready','final':guest+'/provider-origin-final.json','vod_series_info_cases':{r['label']:r['provider_scenario']['payload']for r in load_cases(fixture)}}
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
 assert f.backend=='Native'
 f._provider_environment['RUST_EXPERIMENTAL_SELECTED_BEAT']='0'
 f.sql("from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation;from apps.m3u.models import M3UAccount;assert all(not m.objects.exists()for m in [Movie,Series,Episode,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation]);assert not VODCategory.objects.exists();print(json.dumps({'owned_fresh_catalogue':True}))")
 # These are the exact Source bootstrap support rows, not successful refresh
 # output. Explicit IDs preserve the retained API/SQL expectation identities.
 f.sql("from apps.vod.models import VODLogo,VODCategory;VODLogo.objects.bulk_create([VODLogo(id=1,name='rust-probe-vod-rest-shared-logo',url='https://fixture.invalid/logo.jpg')]);VODCategory.objects.bulk_create([VODCategory(id=i,name='rust-probe-vod-rest-'+name,category_type=kind)for i,name,kind in [(1,'Drama','movie'),(2,'Drama','series'),(3,'News|Live','movie')]]);from apps.accounts.models import User;from rest_framework_simplejwt.tokens import AccessToken;u=User.objects.create(username='owned-provider-info-standard',password='!',user_level=1,is_active=True,custom_properties={});print(json.dumps({'id':u.pk,'access':str(AccessToken.for_user(u))}))",secret=True)
 # Fetch the token separately so only private command receipts contain it.
 f.accounts['provider_info_standard']=f.sql("from apps.accounts.models import User;from rest_framework_simplejwt.tokens import AccessToken;u=User.objects.get(username='owned-provider-info-standard');print(json.dumps({'id':u.pk,'access':str(AccessToken.for_user(u))}))",secret=True)
 allocate_owned_accounts(f)
 prepare_default_agent(f)

TABLES=['m3u_m3uaccount','vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation']
MODEL_IMPORT="from apps.m3u.models import M3UAccount;from apps.vod.models import Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation;from django.core.serializers.json import DjangoJSONEncoder;import datetime;SourceExactEncoder=type('SourceExactEncoder',(DjangoJSONEncoder,),{'default':lambda self,value:value.isoformat()if isinstance(value,(datetime.datetime,datetime.date,datetime.time))else DjangoJSONEncoder.default(self,value)});models=[M3UAccount,Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation];"

DEFAULT_AGENT_KEYS=[':1:coresettings:group:stream_settings',':1:coresettings:groupver:stream_settings',':1:coresettings:default_user_agent',':1:coresettings:default_user_agent:ver']
SCOPED_CACHE_SNAPSHOT="local out={};for _,key in ipairs(ARGV)do local dump=redis.call('DUMP',key);local expiry=redis.call('PEXPIRETIME',key);if dump and #dump>16384 then return redis.error_reply('Owned cache snapshot bound')end;out[#out+1]={key,dump or false,expiry};end;return out"
SCOPED_CACHE_RESTORE="""local out={};for i=1,#ARGV,4 do
 local key=ARGV[i];local present=ARGV[i+1];local dump=ARGV[i+2];local expiry=tonumber(ARGV[i+3]);local status='absent';
 if present=='0' then redis.call('DEL',key);if redis.call('DUMP',key)then return redis.error_reply('Expected absent named key')end
 elseif expiry==-1 then redis.call('RESTORE',key,0,dump,'REPLACE');if redis.call('DUMP',key)~=dump or redis.call('PEXPIRETIME',key)~=-1 then return redis.error_reply('Persistent named key mismatch')end;status='restored_persistent'
 elseif expiry>=0 then
  local t=redis.call('TIME');local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000);
  if expiry<=now then redis.call('DEL',key);status='naturally_expired'
  else
   redis.call('RESTORE',key,expiry,dump,'REPLACE','ABSTTL');local got=redis.call('DUMP',key);local actual=redis.call('PEXPIRETIME',key);
   if not got or actual==-2 then
    t=redis.call('TIME');now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000);if now<expiry then return redis.error_reply('Named key disappeared before expiry')end;status='naturally_expired'
   else if got~=dump or actual~=expiry then return redis.error_reply('Expiring named key mismatch')end;status='restored_original_expiry' end
  end
 else return redis.error_reply('Invalid named cache expiry state') end
 out[#out+1]={key,status};end;return out"""
def prepare_default_agent(f):
 code=MODEL_IMPORT+"from core.models import CoreSettings,UserAgent,STREAM_SETTINGS_KEY;import redis,base64;r=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0);row=CoreSettings.objects.values().get(key=STREAM_SETTINGS_KEY);assert isinstance(row['value'],dict);assert UserAgent.objects.count()<=128;cached=[];\nfor key,dump,expiry in r.eval("+repr(SCOPED_CACHE_SNAPSHOT)+",0,*"+repr(DEFAULT_AGENT_KEYS)+"):\n assert (dump is None and expiry==-2)or(dump is not None and(expiry==-1 or expiry>=0));cached.append({'key':key.decode(),'dump_base64':None if dump is None else base64.b64encode(dump).decode(),'expiry_ms':expiry})\nprint(json.dumps({'setting':row,'UserAgent_rows':list(UserAgent.objects.order_by('pk').values()),'cache':cached},cls=SourceExactEncoder));r.close()"
 prior=f.sql(code,secret=True);f._provider_default_agent_prior=prior;f._provider_default_agent_restored=False
 save(f.output/'private/provider-info-default-agent-prior.json',prior)
 requests=[request for record in load_cases(f)for request in record['provider_requests']]
 expected=requests[0]['headers']['User-Agent'];assert all(r['headers']['User-Agent']==expected for r in requests)
 script=MODEL_IMPORT+"from core.models import CoreSettings,UserAgent,STREAM_SETTINGS_KEY;from apps.m3u.models import M3UAccount;prior=json.loads("+repr(json.dumps(prior['setting']))+");expected="+repr(expected)+";before=CoreSettings.get_default_user_agent();row=CoreSettings.objects.get(pk=prior['id']);assert row.key==STREAM_SETTINGS_KEY;row.value={**row.value,'default_user_agent':None};row.save(update_fields=['value']);effective=CoreSettings.get_default_user_agent();accounts=list(M3UAccount.objects.filter(pk__in="+repr(f._provider_owned_ids)+").order_by('pk'));assert len(accounts)==3 and all(a.user_agent_id is None for a in accounts);resolved=[a.get_user_agent_string()for a in accounts];assert effective==expected and resolved==[expected]*3;assert json.loads(json.dumps(list(UserAgent.objects.order_by('pk').values()),cls=SourceExactEncoder))==json.loads("+repr(json.dumps(prior['UserAgent_rows']))+");after=json.loads(json.dumps(CoreSettings.objects.values().get(pk=prior['id']),cls=SourceExactEncoder));assert after=={**prior,'value':{**prior['value'],'default_user_agent':None}};CoreSettings.invalidate_group_cache(STREAM_SETTINGS_KEY);print(json.dumps({'prior_stream_settings':prior,'Source_effective_before':before,'Source_effective_after':effective,'Source_owned_account_effective':resolved,'account_UA_FKs_all_null':True,'all_UserAgent_rows_unchanged':True,'Source_cache_invalidator_after_probe':True,'Native_cold_default_path_required':True,'expected_actual_Native_provider_wire_UA':expected},cls=SourceExactEncoder))"
 assert len(script.encode())<98304
 f._provider_default_agent_preflight=f.sql(script,secret=True)
 save(f.output/'private/provider-info-default-agent-preflight.json',f._provider_default_agent_preflight)

def stop_roles_for_default_restore(f):
 f._provider_info_roles_stopped=False
 f.stop_roles()
 f._provider_info_roles_stopped=True

def restore_context(f):
 prior=getattr(f,'_provider_default_agent_prior',None)
 if prior is None:return {'scoped_default_agent_setup_started':False}
 if getattr(f,'_provider_default_agent_restored',False):return {'already_restored':True}
 assert f.native is None and getattr(f,'_provider_info_roles_stopped',False),'ActualMainAndRoleStopRequiredBeforeCacheVersionRestore'
 script=MODEL_IMPORT+"from core.models import CoreSettings,UserAgent,STREAM_SETTINGS_KEY;from apps.m3u.models import M3UAccount;import redis,base64;prior=json.loads("+repr(json.dumps(prior))+");row=CoreSettings.objects.get(pk=prior['setting']['id']);assert row.key==STREAM_SETTINGS_KEY;row.value=prior['setting']['value'];row.save(update_fields=['value']);r=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=0);args=[];\nfor state in prior['cache']:\n payload=state['dump_base64'];expiry=state['expiry_ms'];assert(payload is None and expiry==-2)or(payload is not None and(expiry==-1 or expiry>=0));args.extend([state['key'],'0'if payload is None else'1',b''if payload is None else base64.b64decode(payload),str(expiry)])\nobserved=r.eval("+repr(SCOPED_CACHE_RESTORE)+",0,*args);assert json.loads(json.dumps(CoreSettings.objects.values().get(pk=row.pk),cls=SourceExactEncoder))==prior['setting'];assert json.loads(json.dumps(list(UserAgent.objects.order_by('pk').values()),cls=SourceExactEncoder))==prior['UserAgent_rows'];assert not M3UAccount.objects.filter(pk__in="+repr(f._provider_owned_ids)+").exclude(user_agent_id=None).exists();print(json.dumps({'prior_Core_setting_row_restored_exactly':True,'all_UserAgent_rows_unchanged':True,'account_UA_FKs_all_null':True,'named_cache_keys_only':"+repr(DEFAULT_AGENT_KEYS)+",'atomic_cache_restore_status':[[key.decode(),status.decode()]for key,status in observed],'original_absolute_expiry_preserved':True,'no_live_Main_or_provider_role_during_restore':True}));r.close()"
 assert len(script.encode())<98304
 result=f.sql(script,secret=True);f._provider_default_agent_restored=True
 save(f.output/'provider-info-default-agent-restoration.json',result)
 return result

def allocate_owned_accounts(f):
 # The real shared bootstrap already owns provider/profile/stream fixtures.
 # Keep all their identities/FKs intact and allocate only this context's rows.
 baseline=snapshot(f);existing=baseline['m3u_m3uaccount']
 f._provider_bootstrap_ids=[r['id']for r in existing]
 source=[r for r in load_cases(f)[0]['before']['m3u_m3uaccount']if r['id']!=1]
 assert len(source)==3
 code=MODEL_IMPORT+"source=json.loads("+repr(json.dumps(source))+");login=json.loads("+repr(json.dumps(f._vod_login))+");assert not M3UAccount.objects.filter(name__in=[r['name']for r in source]).exists();objects=[];\nfor row in source:\n fields={k:v for k,v in row.items()if k!='id'};fields.update(server_url='https://xc-vod-owned.invalid/xc',username=login['username'],password=login['password']);objects.append(M3UAccount(**fields))\ncreated=M3UAccount.objects.bulk_create(objects)\nwith connection.cursor()as cursor:\n cursor.execute(\"SELECT last_value,is_called FROM public.m3u_m3uaccount_id_seq\");value=cursor.fetchone()\nprint(json.dumps({'map':[[old['id'],new.pk]for old,new in zip(source,created)],'sequence':{'last_value':value[0],'is_called':value[1]}}))"
 receipt=f.sql(code,secret=True)
 f._provider_account_map={int(old):int(new)for old,new in receipt['map']}
 f._provider_owned_ids=list(f._provider_account_map.values())
 f._provider_account_sequence=receipt['sequence']
 assert len(set(f._provider_owned_ids))==3 and not set(f._provider_owned_ids)&set(f._provider_bootstrap_ids)
 f._provider_namespace={'Source_to_owned_account_ids':receipt['map'],'preserved_bootstrap_account_ids':f._provider_bootstrap_ids,'owned_account_sequence':receipt['sequence'],'no_bootstrap_account_rows_or_FKs_changed':True}
 after=snapshot(f)
 assert [r for r in after['m3u_m3uaccount']if r['id']in f._provider_bootstrap_ids]==existing

def owned_record(f,original):
 # Retained Source bytes stay immutable. This transparent fixture namespace
 # translation preserves the existing shared bootstrap and its foreign keys.
 record=json.loads(json.dumps(original));mapping=f._provider_account_map
 actual=snapshot(f)
 bootstrap=[r for r in actual['m3u_m3uaccount']if r['id']in f._provider_bootstrap_ids]
 assert len(bootstrap)==len(f._provider_bootstrap_ids)
 for phase in ['before','after']:
  accounts=[r for r in record[phase]['m3u_m3uaccount']if r['id']!=1]
  for account in accounts:account['id']=mapping[account['id']]
  record[phase]['m3u_m3uaccount']=sorted([*bootstrap,*accounts],key=lambda row:row['id'])
  for table in ['vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation']:
   for row in record[phase][table]:row['m3u_account_id']=mapping[row['m3u_account_id']]
 assert original['sequence_before']['public.m3u_m3uaccount_id_seq']==original['sequence_after']['public.m3u_m3uaccount_id_seq']
 for phase in ['sequence_before','sequence_after']:
  record[phase]['public.m3u_m3uaccount_id_seq']=dict(f._provider_account_sequence)
 account=record['expected']['json']['m3u_account'];account['id']=mapping[account['id']]
 record['_owned_account_ids']=list(f._provider_owned_ids)
 return record

def restore(f,record):
 # Independent seeds exactly mirror the Source oracle's restore-per-case
 # method. No fixture sets refreshed flags/episode output after execution.
 values=json.loads(json.dumps(record['before']))
 for a in values['m3u_m3uaccount']:
  if a['id']in f._provider_owned_ids:
   a['server_url']='https://xc-vod-owned.invalid/xc'
   a['username']=f._vod_login['username'];a['password']=f._vod_login['password']
 code=MODEL_IMPORT+"from django.db import transaction;from django.utils import timezone;import datetime;values=json.loads("+repr(json.dumps(values))+ ");sequences=json.loads("+repr(json.dumps(record['sequence_before']))+");bootstrap_ids="+repr(f._provider_bootstrap_ids)+";\nwith transaction.atomic():\n for m in reversed(models[1:]):m.objects.all().delete()\n for m in models:\n  for row in values[m._meta.db_table]:\n   if m is M3UAccount and row['id']in bootstrap_ids:continue\n   fields={k:v for k,v in row.items()if k!='id'}\n   if not m.objects.filter(pk=row['id']).update(**fields):m.objects.bulk_create([m(**row)]);m.objects.filter(pk=row['id']).update(**fields)\n # Source freshness is causal, relative to its actual request. Rebase ONLY\n # the selected relation clock to the same1h/25h policy here.\n fresh="+repr(record['label']in ['series_missing_flags','series_fresh','series_force'])+"\n M3USeriesRelation.objects.filter(pk=1).update(last_episode_refresh=timezone.now()-datetime.timedelta(hours=1 if fresh else 25))\n with connection.cursor()as cursor:\n  for name,value in sequences.items():\n   assert name in {m._meta.db_table+'_id_seq'for m in models}or name in {'public.'+m._meta.db_table+'_id_seq'for m in models}\n   cursor.execute('SELECT setval(%s::regclass,%s,%s)',[name,value['last_value'],value['is_called']])\nprint(json.dumps({'restored_independent_Source_seed':True,'label':"+repr(record['label'])+"}))"
 # Stay below Linux's per-argument limit; the existing fixture command is used.
 assert len(code.encode())<98304
 return f.sql(code,secret=True)

def snapshot(f):
 return f.sql(MODEL_IMPORT+"from core.models import SystemEvent;from apps.vod.models import VODLogo,VODCategory;assert all(m.objects.count()<=128 for m in models);result={m._meta.db_table:list(m.objects.order_by('id').values())for m in models};result['support']={m._meta.db_table:list(m.objects.order_by('id').values())for m in [VODLogo,VODCategory,SystemEvent]};result['observed_at']=__import__('django.utils.timezone',fromlist=['now']).now();result['sequences']={};\nwith connection.cursor()as cursor:\n for m in models:\n  cursor.execute(\"SELECT pg_get_serial_sequence(%s,'id')\",[m._meta.db_table]);name=cursor.fetchone()[0]\n  if name:\n   assert name in {'public.'+x._meta.db_table+'_id_seq'for x in models}\n   cursor.execute('SELECT last_value,is_called FROM '+name);value=cursor.fetchone();result['sequences'][name]={'last_value':value[0],'is_called':value[1]}\nprint(json.dumps(result,cls=SourceExactEncoder))",secret=True)

def instant(value):
 return datetime.datetime.fromisoformat(value.replace('Z','+00:00'))

def compare_seed(record,before):
 assert before['sequences']==record['sequence_before'],'Source exact before sequences'
 for table in TABLES:
  expected=record['before'][table];actual=before[table]
  assert [r['id']for r in actual]==[r['id']for r in expected],(table,'Source seed IDs')
  for e,a in zip(expected,actual):
   assert set(e)==set(a)
   for key,value in e.items():
    if table=='m3u_m3uaccount'and e['id']in record['_owned_account_ids']and key in ['server_url','username','password']:continue
    if table=='vod_m3useriesrelation'and e['id']==1 and key=='last_episode_refresh':
     expected_hours=1 if record['label']in ['series_missing_flags','series_fresh','series_force']else 25
     age=(instant(before['observed_at'])-instant(a[key])).total_seconds()
     assert expected_hours*3600<=age<=expected_hours*3600+30
    elif key in ['created_at','updated_at','last_advanced_refresh','last_episode_refresh','last_seen']and value is not None:
     assert instant(a[key])==instant(value),(table,e['id'],key,'Source seed clock')
    else:assert json.dumps(a[key],sort_keys=True,separators=(',',':'))==json.dumps(value,sort_keys=True,separators=(',',':')),(table,e['id'],key,'Source seed value')

def compare_sql(record,before,after):
 assert after['sequences']==record['sequence_after'],'Source exact after sequences'
 changed=[]
 for table in TABLES:
  if table=='m3u_m3uaccount':
   assert after[table]==before[table]
   continue
  expected=record['after'][table];actual=after[table]
  assert [r['id']for r in actual]==[r['id']for r in expected],table
  old={r['id']:r for r in record['before'][table]}
  actual_old={r['id']:r for r in before[table]}
  for e,a in zip(expected,actual):
   assert set(e)==set(a),(table,'fields')
   for key,value in e.items():
    previous=old.get(e['id'],{}).get(key)
    if key in ['created_at','updated_at','last_advanced_refresh','last_episode_refresh','last_seen']and value!=previous:
     observed=instant(a[key]);assert instant(before['observed_at'])<=observed<=instant(after['observed_at']),(table,key,'causal clock')
     assert e['id']not in actual_old or a[key]!=actual_old[e['id']][key]
     changed.append({'table':table,'id':e['id'],'field':key,'own_actual_interval':True})
    elif key=='last_episode_refresh'and table=='vod_m3useriesrelation'and e['id']==1:
     assert a[key]==actual_old[e['id']][key]
    elif key=='uuid'and table=='vod_episode'and e['id']not in old:
     assert uuid.UUID(value).version==4 and uuid.UUID(a[key]).version==4
     changed.append({'table':table,'id':e['id'],'field':'uuid','new_episode_UUIDv4':True})
    elif key in ['created_at','updated_at','last_seen','last_episode_refresh']and value is not None:
     assert instant(a[key])==instant(value),(table,e['id'],key)
    else:assert json.dumps(a[key],sort_keys=True,separators=(',',':'))==json.dumps(value,sort_keys=True,separators=(',',':')),(table,e['id'],key)
 assert after['support']==before['support']
 return changed

def compare_body(record,actual,after):
 expected=json.loads(json.dumps(record['expected']['json']))
 # Every newly generated episode UUID is bound back to the independently
 # captured actual SQL row. Existing UUIDs and every other body value stay exact.
 old={r['id']for r in record['before']['vod_episode']}
 rows={r['id']:r for r in after['vod_episode']}
 for episodes in expected.get('episodes',{}).values():
  for ep in episodes:
   if ep['id']not in old:ep['uuid']=rows[ep['id']]['uuid']
 assert json.dumps(actual,sort_keys=True,separators=(',',':'))==json.dumps(expected,sort_keys=True,separators=(',',':'))

def run(f):
 assert f.backend=='Native'and f.case==CASE
 out=f.output/CASE;out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700)
 report={'status':'running','cases':[],'Source_capture_replayed':False,'natural_import_chain_Source_qualified':False,'current_Main_acceptance':False,'cache_equivalence_Source_qualified':False,'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 observer=None
 try:
  save(private/'owned-account-namespace.json',f._provider_namespace)
  for original in load_cases(f):
   record=owned_record(f,original)
   label=record['label'];row={'name':label,'status':'running'};report['cases'].append(row);save(out/'report.json',report);p=private/label;p.mkdir(mode=0o700)
   save(p/'restore.json',restore(f,record));f.wait(lambda:not any(aggregate.broker(f).values()),15,label+'-prior-drain')
   before=snapshot(f);save(p/'SQL-before.json',before);compare_seed(record,before);cache=aggregate.cache_state(f);trace_before=control(f,'/snapshot')['trace'];control(f,'/control',{'case':label});receiver=aggregate.receiver_begin(f,label);observer=journal.observer_start(f,CASE+'-'+label,database=1)
   api=f.api('GET',record['path'],token='provider_info_standard',headers={'Host':'localhost'});save(p/'HTTP.json',api)
   after=snapshot(f);trace=control(f,'/snapshot')['trace'][len(trace_before):];events=aggregate.receiver_read(f,receiver);durable=journal.producer_observer_stop(f,observer);observer=None
   for name,value in [('SQL-before',before),('SQL-after',after),('Origin',trace),('WS',events),('journal',durable),('cache-before',cache),('cache-after',aggregate.cache_state(f))]:save(p/(name+'.json'),value)
   assert api['status']==record['expected']['status']==200
   clocks=compare_sql(record,before,after);save(p/'SQL-comparison.json',{'passed':True,'declared_entropy':clocks});compare_body(record,api['body'],after)
   requests=[r for r in trace if r.get('host')=='xc-vod-owned.invalid'and'path'in r];assert len(requests)==len(record['provider_requests'])
   for actual,source in zip(requests,record['provider_requests']):
    query={k:[hashlib.sha256(v.encode()).hexdigest()for v in values]for k,values in source['query'].items()}
    for key,value in f._vod_login.items():query[key]=[hashlib.sha256(value.encode()).hexdigest()]
    headers={k:source['headers'].get(k)for k in ['User-Agent','Accept','Accept-Encoding','Connection']}
    assert actual['method']==source['method']=='GET'and actual['path']=='/xc/player_api.php'and actual['query_sha256']==query and actual['actual_TLS']and actual['status']==source['response']['status']==200 and actual['actual_request_headers']==headers
    assert actual['response_sha256']==hashlib.sha256(base64.b64decode(source['response']['body_base64'])).hexdigest()
   assert not durable['publications']and not any(aggregate.broker(f).values())
   # Retained205 did not capture Redis cache baselines. Default-agent lookup
   # can lazily populate settings cache; retain raw diagnostics without
   # inventing Source-equivalence from an unchanged-cache assertion.
   # No task/WS producer is called by this Source provider-info path. Retain
   # raw receiver observations, reject any application event in this context.
   assert not events['events']
   row.update(status='passed',actual_provider_requests=len(requests),exact_Source_HTTP_SQL=True,actual_queued_tasks=0);save(out/'report.json',report)
 except BaseException as error:
  if report['cases']:report['cases'][-1].update(status='failed',failure_type=type(error).__name__)
  (private/'failure.txt').write_text(traceback.format_exc()[-16384:])
 finally:
  if observer is not None:
   try:save(private/'failed-observer-final.json',journal.producer_observer_stop(f,observer))
   except Exception as error:report['observer_cleanup_error']=type(error).__name__
 report['status']='passed'if len(report['cases'])==5 and all(r['status']=='passed'for r in report['cases'])and not report.get('observer_cleanup_error')else'failed'
 report['current_Main_acceptance']=report['status']=='passed';save(out/'report.json',report);return report
