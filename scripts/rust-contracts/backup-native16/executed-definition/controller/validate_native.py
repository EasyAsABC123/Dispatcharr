"""Finite Native16 observation qualification; raw recorderfalse remains capture-only."""
import base64,datetime,hashlib,hmac,io,json,pickle,pickletools,re,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
SOURCE_SHA='5f0ce76f0d308da14cce1c0f57899577dd14732e1bc818ee916864d2d1902760'
ORIGINAL_SHA='daa2835e76df10e6dece88473d4b08d2ce1e4ad82680de3296de99604b54a288'
BASE={':1:coresettings:groupver:network_access','core:version_check_dispatch_lock','dvr:recover_dispatch_lock'}
GROUPS={':1:coresettings:group:network_access',':1:coresettings:group:system_settings'}
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal'}
def require(v,label):
 if not v:raise ValueError(label)
def clock(v):
 d=datetime.datetime.fromisoformat(v.replace('Z','+00:00'));require(d.utcoffset()==datetime.timedelta(0),'UTCClock');return d
def token(v):return hmac.new(b'backup-job-fixture-only',v.encode(),hashlib.sha256).hexdigest()[:32]
def literal_equal(left,right):
 if type(left)is not type(right):return False
 if isinstance(left,dict):return set(left)==set(right)and all(literal_equal(left[k],right[k])for k in left)
 if isinstance(left,list):return len(left)==len(right)and all(literal_equal(a,b)for a,b in zip(left,right))
 if isinstance(left,float):return left.hex()==right.hex()
 return left==right

def raw_guard_redis(before,after):
 for index in(1,2):
  left,right=before['redis'][index],after['redis'][index]
  require(set(left)==set(right)and left['database']==right['database']==index+1 and type(left['capture_bytes'])is type(right['capture_bytes'])is int and left['capture_bytes']==right['capture_bytes'],'GuardRawRedisScopeAndByteCount')
  a={r['key_hex']:r for r in left['rows']};b={r['key_hex']:r for r in right['rows']};require(len(a)==len(left['rows'])and len(b)==len(right['rows'])and set(a)==set(b),'GuardRawRedisExactKeysNoDrop')
  for key,row in a.items():
   other=b[key];require(set(row)==set(other)and literal_equal({k:v for k,v in row.items()if k!='pttl'},{k:v for k,v in other.items()if k!='pttl'}),'GuardRawRedisAllTypesDumpValuesListBytes')
   before_ttl,after_ttl=row['pttl'],other['pttl'];require(type(before_ttl)is type(after_ttl)is int and((before_ttl==after_ttl==-1)or(0<after_ttl<=before_ttl)),'GuardRawRedisPersistentOrMonotonicFiniteTTL')
 return True

def safe_cache(raw):
 require(len(raw)<=65536,'BoundedCacheValue')
 if re.fullmatch(rb'-?[0-9]+',raw):return int(raw)
 allowed={'PROTO','FRAME','EMPTY_DICT','MEMOIZE','SHORT_BINUNICODE','BINUNICODE','BINUNICODE8','MARK','SETITEMS','SETITEM','EMPTY_LIST','APPENDS','APPEND','BININT1','BININT2','BININT','LONG1','LONG4','NEWTRUE','NEWFALSE','NONE','BINFLOAT','FLOAT','INT','STOP','BINPUT','LONG_BINPUT','BINGET','LONG_BINGET','UNICODE','STRING','EMPTY_TUPLE','TUPLE','TUPLE1','TUPLE2','TUPLE3'}
 ops=list(pickletools.genops(raw));require(bool(ops)and all(op.name in allowed for op,arg,pos in ops)and ops[-1][0].name=='STOP'and ops[-1][2]==len(raw)-1,'OnlyCompleteLiteralPickleOpcodes');value=pickle.loads(raw);require(len(json.dumps(value).encode())<=65536,'BoundedDecodedLiteralCache');return value

def cache(state):
 scopes=state['redis'];require([x['database']for x in scopes]==[1,2,3],'ThreeRawRedisScopes');values={};ttls={}
 for row in scopes[0]['rows']:
  key=bytes.fromhex(row['key_hex']).decode();require(key not in values and row['type']=='string','UniqueCacheString');require(type(row['value_hex'])is str and type(row['dump_hex'])is str,'LiteralCacheBytes');values[key]=safe_cache(bytes.fromhex(row['value_hex']));ttls[key]=row['pttl']
 return values,ttls

def check_cache(source,native):
 expected,_=cache(source);actual,ttls=cache(native);require(literal_equal(actual,expected),'ExactTypedDecodedNamedCacheValuesNoDropOrExtraCredentialBudget')
 for key,ttl in ttls.items():
  require(ttl==-1 if key.endswith('groupver:network_access')else 0<ttl<=300000,'PersistentOrFiniteTTLFormNoExactCrossRunTTL')
 return True

def sql(state):
 v=state['SQL'];require(set(v)=={'tables','sequences'}and set(v['tables'])==set(v['sequences'])==TABLES,'Full11SQLScope')
 for rows in v['tables'].values():require(type(rows)is list and len(rows)<=10000,'BoundedRawRows')
 for row in v['sequences'].values():require(set(row)=={'last_value','is_called'}and type(row['last_value'])is int and type(row['is_called'])is bool,'LiteralSQLSequences')
 require(v['tables']['accounts_user']==state['users'],'RawUsersSQLBinding');return v

def queued(state):
 rows=[]
 for x in state['redis'][1]['rows']:
  if x['type']=='list':
   for value in x['list_hex']:rows.append((bytes.fromhex(x['key_hex']).decode(),json.loads(bytes.fromhex(value))))
 return rows

def expected_path(spec,aliases):
 path=spec['path_template']
 for k,v in aliases.items():path=path.replace('{'+k+'}',v)
 require('{'not in path,'BoundObservedAlias')
 selector=spec['token_selector']
 if selector is not None:path+='?token='+('invalid-fixture-token'if selector=='invalid_literal'else token(aliases['unknown_task']if selector=='unknown_task_token'else aliases['created_task']))
 return path

def compare(native,source,plan):
 require(native['capture_complete']is True and native['passed']is False and native['capture_only_passed_field']is True,'CompleteCaptureOnlyRecorderNotRelabeled');require(native['actual_request_count']==len(native['requests'])==16 and native['Source_application_runs']==native['Main_application_runs']==0,'FiniteNativeOnly16');require(native['Source_reference_mode']=='projected-backup-source16-reference-v1'and native['Source_reference_sha256']==SOURCE_SHA and native['Original_Source_raw_sha256']==ORIGINAL_SHA,'StrictSingleProjectedSourceReference')
 db=native['database'];nonce=db.removeprefix('rust_probe_backupjob_native_');require(db=='rust_probe_backupjob_native_'+nonce and re.fullmatch('[a-f0-9]{10}',nonce)is not None,'OwnedNativeDatabase');require(native['native_actual_queue']=='celery'and native['Source_fixture_queue_override']==source['before_Celery_configuration']['owned_queue'],'ExplicitDifferentFixtureRouteContext')
 seed=native['seed'];seed_sql=sql(seed);users={};require(len(seed['users'])==3,'ExactThreeActors')
 for role,s,n in zip(plan['roles'],source['seed_users'],seed['users']):
  alias=role['alias'];wanted=dict(s,username='rust-probe-backupjob-native-'+nonce+'-'+alias,api_key='backup-native-'+nonce+'-'+alias,password='!',date_joined=native['seed_clock']);wanted['date_joined']=n['date_joined'];require(n==wanted and abs(clock(n['date_joined'])-clock(native['seed_clock']))<=datetime.timedelta(microseconds=1),'SeedFourOverridesAndPGMicrosecondQuantization:'+alias);users[alias]=n
 require(seed_sql['sequences']['accounts_user']=={'last_value':3,'is_called':True},'UserSeedSequence');require(native['Source_cache_context_seed']['before_owned_cache_keys']==0 and native['Source_cache_context_seed']['strict_three_key_allowlist']is True and native['Source_cache_context_seed']['auth_history_or_events_reset']is False and len(native['Source_cache_context_seed']['rows'])==3,'ImportedThreeInitialContextNotFiveOrHistoryReset');check_cache(source['seed_snapshot'],seed)
 require(cache(native['initial'])[0]=={},'NaturalEmptyNativeCacheRetainedBeforeImport');context=native['Source_cache_context_seed'];require(context['before_any_request_or_auth_worker']is True,'ContextImportedBeforeRequests');expected_context={r['key_hex']:r for r in source['seed_snapshot']['redis'][0]['rows']};actual_context={r['key_hex']:r for r in seed['redis'][0]['rows']};receipts={r['key_hex']:r for r in context['rows']};require(set(receipts)==set(actual_context)==set(expected_context),'ExactThreeContextReceipts')
 for key,row in expected_context.items():
  receipt=receipts[key];digest=hashlib.sha256(bytes.fromhex(row['dump_hex'])).hexdigest();require(receipt['Source_dump_sha256']==receipt['restored_dump_sha256']==digest and actual_context[key]['dump_hex']==row['dump_hex']and receipt['Source_seed_pttl']==row['pttl'],'ExactContextDumpReadbackAndSourceTTL');require((receipt['RESTORE_ttl']==0 and receipt['actual_readback_pttl']==-1)if row['pttl']==-1 else(receipt['RESTORE_ttl']==row['pttl']and 0<receipt['actual_readback_pttl']<=row['pttl']),'PersistentVersusFiniteRestoreTTL')
 aliases={'unknown_task':'never-created-'+nonce};previous=seed;results=[];worker_begin=clock(native['worker_began_utc']);worker_end=clock(native['worker_completed_utc']);require(worker_begin<=worker_end,'OwnWorkerClockOrder')
 for row,src,spec in zip(native['requests'],source['requests'],plan['cases']):
  number=spec['ordinal'];require(all(row[k]==spec[k]for k in('ordinal','name','method'))and row['auth_selectors']==spec['auth'],'Fixed16Identity');require(row['path']==expected_path(spec,aliases),'OwnExactRoleTaskTokenPath')
  headers={}
  if 'x_api_key'in spec['auth']:headers['x-api-key']=users[spec['auth']['x_api_key']]['api_key'].encode().hex()
  if 'bearer_literal'in spec['auth']:headers['authorization']=('Bearer '+spec['auth']['bearer_literal']).encode().hex()
  require(row['raw_headers_private']==headers,'RealNativeCredentialMaterial');require(row['status']==src['status']==spec['predicted_status_UNMEASURED'],'ActualMeasuredSourceStatus');raw=bytes.fromhex(row['body_hex']);require(len(raw)==row['body_bytes']and len(raw)<=(8388608 if number==16 else 65536)and hashlib.sha256(raw).hexdigest()==row['body_sha256'],'LiteralRawResponseBinding');require(clock(row['began_utc'])<=clock(row['completed_utc']),'RequestClockOrder')
  require(sql(row['before'])==sql(row['after'])==seed_sql and row['before']['users']==row['after']['users']==seed['users']and row['before']['events']==row['after']['events']==seed['events'],'All11RowsSequencesFullUsersEventsUnchanged');require(sql(previous)==sql(row['before']),'CrossRequestSQLContinuity');check_cache(src['before'],row['before']);check_cache(src['after'],row['after']);previous=row['after']
  source_challenge={k.lower():v for k,v in src['headers'].items()}.get('www-authenticate');actual_challenge=row['headers_hex'].get('www-authenticate');require(source_challenge==(None if actual_challenge is None else bytes.fromhex(actual_challenge).decode()),'Manual401AndAuthenticationChallengeBoundary')
  if number!=16:require(json.loads(raw)==row['json'],'ExactJSONBytes')
  if number==4:
   value=row['json'];require(set(value)=={'detail','task_id','task_token'}and value['detail']=='Backup started'and re.fullmatch('[a-f0-9]{8}-[a-f0-9]{4}-4[a-f0-9]{3}-[89ab][a-f0-9]{3}-[a-f0-9]{12}',value['task_id'])is not None and value['task_token']==token(value['task_id']),'NaturalUUID4AndOwnHMAC');aliases['created_task']=value['task_id'];frames=queued(row['after']);require(len(frames)==1 and frames[0][0]=='celery','OneActualDefaultQueueFrame');frame=frames[0][1];require(frame['headers']['id']==value['task_id']and frame['headers']['task']=='apps.backups.tasks.create_backup_task'and frame['properties']['delivery_info']=={'exchange':'','routing_key':'celery'},'ActualEnvelopeRouteAndIdentityNotLPUSHOnly');body=json.loads(base64.b64decode(frame['body'],validate=True));require(body[0]==[]and body[1]=={},'NoInjectedTaskArguments')
  elif number<=3:require(queued(row['before'])==queued(row['after'])==[],'RefusalsNoJobEffects')
  elif number<=12:require(queued(row['before'])==queued(row['after'])and len(queued(row['after']))==1,'PendingNoConsumeOrEnqueue')
  if number<=12:require(row['before']['files']==row['after']['files']==[],'NoArchiveBeforeWorker')
  for side in('before','after'):
   result_rows=row[side]['redis'][2]['rows']
   if number<=12:require(result_rows==[],'NoResultBeforeWorker')
   else:
    require(len(result_rows)==1 and result_rows[0]['type']=='string'and bytes.fromhex(result_rows[0]['key_hex']).decode()=='celery-tasks:celery-task-meta-'+aliases['created_task'],'OnlyOwnBackendResultKey');literal=json.loads(bytes.fromhex(result_rows[0]['value_hex']));require(literal['status']=='SUCCESS'and literal['task_id']==aliases['created_task']and literal['result']==native['actual_result_metadata']['result']and literal['date_done']==native['actual_result_metadata']['date_done']and literal['children']==[]and literal['traceback']is None,'LiteralActualBackendMetadata');require(0<result_rows[0]['pttl']<=3600000,'OwnResultTTLFinite3600Seconds')
  if number==13:
   value=row['json'];require(set(value)=={'state','result'}and value['state']=='completed'and set(value['result'])=={'status','filename','size'}and value['result']['status']=='completed'and type(value['result']['size'])is int and value['result']['size']>0,'OwnCompletedSchema');name=value['result']['filename'];require(re.fullmatch('dispatcharr-backup-[0-9]{4}(?:\\.[0-9]{2}){5}\\.zip',name)is not None,'GeneratedFilenameShape');aliases['completed_filename']=name;instant=datetime.datetime.strptime(name.removeprefix('dispatcharr-backup-').removesuffix('.zip'),'%Y.%m.%d.%H.%M.%S').replace(tzinfo=datetime.timezone.utc);require(worker_begin.replace(microsecond=0)<=instant<=worker_end,'OwnFilenameClock');meta=native['actual_result_metadata'];require(meta['task_id']==aliases['created_task']and meta['result']==value['result']and worker_begin<=clock(meta['date_done'])<=worker_end and aliases['created_task']in native['worker_run']and native['worker_run'].startswith('Completed('),'ActualWorkerResultIdentityClock');require(queued(row['before'])==queued(row['after'])==[],'WorkerActuallyConsumedSingleFrame')
  elif number==14:require(row['json']==native['requests'][12]['json'],'AdminSameCompletedResult')
  elif number==15:
   require(len(row['json'])==1 and row['json'][0]['name']==aliases['completed_filename']and row['json'][0]['size']==native['requests'][12]['json']['result']['size']and worker_begin<=clock(row['json'][0]['created'])<=worker_end,'OwnListedArchiveAndClock')
  elif number==16:
   require(row['after']['files']==[{'name':aliases['completed_filename'],'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}]and len(raw)==native['requests'][12]['json']['result']['size'],'OwnActualFileResultAndStreamMatch')
   with zipfile.ZipFile(io.BytesIO(raw))as z:
    require(z.namelist()==['database.dump','metadata.json']and sum(i.file_size for i in z.infolist())<=67108864 and z.read('database.dump')[:5]==b'PGDMP','OwnBoundedActualPGDMPArchive');m=json.loads(z.read('metadata.json'));require(set(m)=={'format','version','database_type','database_file','created_at'}and m['format']=='dispatcharr-backup'and m['version']==2 and m['database_type']=='postgresql'and m['database_file']=='database.dump'and worker_begin<=clock(m['created_at'])<=worker_end,'OwnV2MetadataClock')
  elif number!=4:require(row['json']==src['json'],'StableSourceJSON')
  results.append({'ordinal':number,'status':row['status'],'SQL_users_events_cache_body_qualified':True})
 guards=native['native_only_family_guards'];require(guards['controls']==5 and guards['all_UnsupportedTask']is True and sql(guards['before'])==sql(guards['after'])==seed_sql,'FiveAdditionalNativeOnlyAuthorityRefusals');before_cache,_=cache(guards['before']);after_cache,_=cache(guards['after']);require(literal_equal(before_cache,after_cache) and guards['before']['events']==guards['after']['events']==seed['events']and guards['before']['files']==guards['after']['files'],'GuardCacheEventsFilesNoChange');raw_guard_redis(native['requests'][-1]['after'],guards['before']);raw_guard_redis(guards['before'],guards['after']);raw_guard_redis(guards['after'],native['final']);require(native['HTTP_listener_joined']is True and native['backups_shutdown']is True and sql(native['final'])==seed_sql,'NormalCaptureHTTPAndServiceDrain');return {'passed':True,'actual_Native_HTTP_requests':16,'retained_Source_requests':16,'additional_Native_only_family_controls':5,'raw_recorder_passed_false_is_capture_only':True,'request1_cache_keys_before':3,'request1_cache_keys_after':5,'credential_events_and_budget_not_normalized':True,'caller_loss_physical_cancel_Main_packaging_restore_schedule_SQLite_parity':False,'notification_physical2_4_6_negatives_waived':False,'requests':results}

def validate(native):
 binding=json.loads((ROOT/'inputs/Source-binding.json').read_text());path=ROOT/'inputs/Source16-projection.json';require(hashlib.sha256(path.read_bytes()).hexdigest()==binding['Source16_projection_sha256']==SOURCE_SHA and binding['Original_raw_sha256']==ORIGINAL_SHA,'ExactSingleSourceReference');return compare(native,json.loads(path.read_text()),json.loads((ROOT/'inputs/plan.json').read_text()))
