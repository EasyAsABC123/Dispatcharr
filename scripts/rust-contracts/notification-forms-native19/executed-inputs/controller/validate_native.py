"""Finite Native12 response/SQL/history +7 legacy regressions; full cache gap stays open."""
import copy,datetime,hashlib,importlib.util,json,math,pickle,pickletools,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
AUTHORITY=ROOT/'inputs/source-authority'
for directory in(AUTHORITY,AUTHORITY/'validation'):
 if str(directory)not in sys.path:sys.path.insert(0,str(directory))
_spec=importlib.util.spec_from_file_location('forms_source12_public_authority',AUTHORITY/'verify.py');_source=importlib.util.module_from_spec(_spec);_spec.loader.exec_module(_source)
SOURCE=_source.load_source_projection();PLAN=_source.load_plan()
TABLES=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
N='core_systemnotification';D='core_notificationdismissal';GROUP=':1:coresettings:group:network_access';SYS=':1:coresettings:group:system_settings';VERSION=':1:coresettings:groupver:network_access'
def require(ok,label):
 if not ok:raise ValueError(label)
def equal(a,b):return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if isinstance(a,dict)else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if isinstance(a,list)else a==b)
def clock(text):
 value=datetime.datetime.fromisoformat(text.replace('Z','+00:00'));require(value.utcoffset()==datetime.timedelta(0),'UTCClock');return value
class Clocks:
 def __init__(self,source):
  self.mapping={};self.generated=set()
  for user in source['seed']['users']:self.generated.add(clock(user['date_joined']))
  for row in source['requests']:
   if row['index']in(1,7,2,4,8):
    target=row['json']['id']
    notice=next(n for n in row['after']['notifications']['notifications']if n['id']==target)
    self.generated.add(clock(notice['updated_at']))
    if row['index']in(1,7):self.generated.add(clock(notice['created_at']))
 def compare(self,source,native):
  a,b=clock(source),clock(native)
  # Existing mapping ALWAYS precedes equality; generated coincidence registers.
  if a in self.mapping:require(self.mapping[a]==b,'ExistingGeneratedClockMappingNoRewind');return
  if a in self.generated:self.mapping[a]=b;return
  require(a==b,'UnchangedSeedInstantNoClockNormalization')
def own(row,value):require(clock(row['began_utc'])<=clock(value)<=clock(row['completed_utc']),'OwnRequestGeneratedClock')
def sql_snapshot(value):
 require(set(value)=={'tables','sequences'}and set(value['tables'])==set(value['sequences'])==set(TABLES),'ExactlyNative11SQLScopes')
 for name,rows in value['tables'].items():require(type(rows)is list and len(rows)<=10000 and all(type(r)is dict for r in rows),'RawNativeSQLRows')
 for seq in value['sequences'].values():require(set(seq)=={'last_value','is_called'}and type(seq['last_value'])is int and seq['last_value']>=1 and type(seq['is_called'])is bool,'LiteralNativeSequenceTypes')
def notice(a,b,clocks):
 require(set(a)==set(b),'ExactNoticeFields')
 for key,value in a.items():
  if key in('created_at','updated_at')or key=='expires_at'and value is not None:clocks.compare(value,b[key])
  else:require(equal(value,b[key]),'NoticeField:'+key)
def graph(source,native,clocks):
 sql_snapshot(native)
 for scope,table in(('notifications',N),('dismissals',D)):
  a=source['notifications'][scope];b=native['tables'][table];require(len(a)==len(b),'NotificationGraphRowCounts')
  for left,right in zip(a,b):
   if table==N:notice(left,right,clocks)
   else:
    require(set(left)==set(right),'DismissalFields')
    for key,value in left.items():
     if key=='dismissed_at':clocks.compare(value,right[key])
     else:require(equal(value,right[key]),'DismissalLiteralField:'+key)
 for table in(N,D):require(equal(source['audit']['sequences']['public.'+table+'_id_seq'],native['sequences'][table]),'SourceNoticeDismissalSequencesExact')
def users(value,source,nonce,clocks,seed_start,seed_end):
 require(len(value)==3,'ExactlyThreeActorRows')
 for native,original,role in zip(value,source['seed']['users'],PLAN['roles']):
  require(set(native)==set(original),'ExactActorFieldSet');alias=role['alias']
  for key,item in original.items():
   if key=='api_key':require(native[key]=='native-forms-api-'+nonce+'-'+alias,'OwnNativeAPIKeyNoProjectedCredentialUse')
   elif key=='username':require(native[key]=='native-forms-'+nonce+'-'+alias,'OwnNativeUsername')
   elif key=='date_joined':clocks.compare(item,native[key]);require(clock(seed_start)<=clock(native[key])<=clock(seed_end),'OwnSeedActorClock')
   else:require(equal(item,native[key]),'UnchangedActorField:'+key)
def dump_string(data):
 require(12<=len(data)<=65536 and data[0]==0,'OwnedRedisStringDump')
 if data[1]<64:
  n=data[1];require(len(data)==n+12,'PlainRedisDumpLength');return data[2:2+n]
 require(data[1]==0xc3 and data[2]<64 and data[3]<64,'BoundedSmallLZFStringDump');count,size=data[2:4];require(size<=128 and len(data)==count+14,'LZFSize');data=data[4:4+count];out=bytearray();at=0
 while at<len(data):
  control=data[at];at+=1
  if control<32:n=control+1;require(at+n<=len(data),'LZFLiteral');out.extend(data[at:at+n]);at+=n
  else:
   n=control>>5;offset=(control&31)<<8
   if n==7:require(at<len(data),'LZFLength');n+=data[at];at+=1
   require(at<len(data),'LZFOffset');offset+=data[at];at+=1;position=len(out)-offset-1;require(position>=0,'LZFBackReference')
   for _ in range(n+2):require(position<len(out),'LZFRead');out.append(out[position]);position+=1
  require(len(out)<=size,'LZFDeclaredCapacity')
 require(len(out)==size,'LZFDecodedLength');return bytes(out)
def group_pickle(raw):
 require(len(raw)<=4096,'GroupedSettingsPickleCapacity');ops=list(pickletools.genops(raw));allowed={'PROTO','FRAME','EMPTY_DICT','MEMOIZE','BINPUT','SHORT_BINUNICODE','BINUNICODE','SETITEM','SETITEMS','MARK','STOP'}
 require(len(ops)<=128 and ops[-1][0].name=='STOP'and ops[-1][2]==len(raw)-1 and all(op.name in allowed for op,arg,at in ops),'LiteralNetworkDictionaryPickleOpcodesOnly')
 result=pickle.loads(raw);require(equal(result,{'UI':'127.0.0.0/8'}),'ExactOwnedNetworkGroupValue');return result
def cache(value,warm):
 require(value['api_key_failed_auth_history']is None and value['api_key_failed_auth_raw_hex']is None,'NoValidOrMissingKeyHistoryResetOrAddition')
 rows={bytes.fromhex(r['key_hex']).decode():r for r in value['rows']};require(len(rows)==len(value['rows'])and set(rows)==({GROUP}if warm else set()),'NaturalNativeColdThenNetworkOnlyCacheScope')
 named=value['named_decoded'];require(set(named)=={GROUP,SYS,VERSION},'AllThreeNamedGroupsRetained')
 for key in(SYS,VERSION):require(named[key]=={'raw_GET_hex':None,'decoded':None},'ExistingCallbackAndVersionGapVisibleNoImport')
 if warm:
  row=rows[GROUP];require(0<row['pttl']<=300000,'NativeGroupFiniteTTL');raw=bytes.fromhex(named[GROUP]['raw_GET_hex']);require(raw==dump_string(bytes.fromhex(row['dump_hex'])),'ActualGETAndDUMPMustBind');decoded=group_pickle(raw);require(equal(named[GROUP]['decoded'],decoded),'DecodedNamedGroupBoundToActualBytes')
 else:require(named[GROUP]=={'raw_GET_hex':None,'decoded':None},'NoWarmNativeSeedCache')
def raw_redis(value):
 require(type(value)is list and [r['database']for r in value]==[0,1,2,3],'FullNativeRawRedisFourDatabases')
 for db in value:
  require(len(db['rows'])<=1000 and len({r['key_hex']for r in db['rows']})==len(db['rows']),'UniqueBoundedRawKeys')
  for row in db['rows']:require(type(row['pttl'])is int and row['pttl']>=-1 and type(row['type'])is str and len(bytes.fromhex(row['key_hex']))<=1024 and row['dump_hex']is not None,'ActualRawRedisTypeDUMPAndTTL')
def redis_unchanged(a,b):
 require(a['database']==b['database'],'RedisDatabaseBinding');left={r['key_hex']:r for r in a['rows']};right={r['key_hex']:r for r in b['rows']};require(set(left)==set(right),'AllRawRedisKeysPreserved')
 for key,row in left.items():
  other=right[key];require(equal({k:v for k,v in row.items()if k!='pttl'},{k:v for k,v in other.items()if k!='pttl'}),'RawRedisValueDUMPTypeMembersPreserved');x,y=row['pttl'],other['pttl'];require(x==y==-1 or 0<y<=x,'NativeTTLMonotonicWithoutNormalization')
def bind_cache_raw(cache_value,raw_scopes):
 rows={r['key_hex']:r for r in cache_value['rows']};actual={r['key_hex']:r for r in raw_scopes[1]['rows']};require(set(rows)==set(actual),'FullCacheAndRawRedisDB1KeyBinding')
 for key,row in rows.items():
  observed=actual[key];require(observed['type']=='string'and observed['dump_hex']==row['dump_hex'],'CacheDUMPBoundToIndependentRawSnapshot');require(0<observed['pttl']<=row['pttl'],'SequentialCacheSnapshotTTLOrder')
  name=bytes.fromhex(key).decode();require(observed['value_hex']==cache_value['named_decoded'][name]['raw_GET_hex'],'ActualRawRedisGETByteBinding')
def raw_body(row):
 data=bytes.fromhex(row['response_body_hex']);require(len(data)==row['response_body_bytes']and hashlib.sha256(data).hexdigest()==row['response_body_sha256'],'RawNativeBodyHashAndLength');require(equal(json.loads(data)if data else None,row['json']),'RawNativeJSONBodyBinding');return data

def validate(native):
 _source.validate_observations();binding=json.loads((ROOT/'inputs/Source-binding.json').read_text());require(binding['mode']=='projected-notification-forms-source12-reference-v1'and binding['projection_sha256']==native['Source_projection_sha256']=='1c3f5dcce14ffe9a773cb29833c4caef52ed15c813cf9473f6c397dc34ca240f','StrictOneProjectedReferenceOnly');require(native['Source_reference_mode']==binding['mode']and native['Original_raw_sha256']==binding['Original_raw_sha256'],'NativeReferenceIdentityFields')
 require(native['capture_complete']is True and native['passed']is False and native['capture_only_passed_field']is True and native['actual_request_count']==19 and len(native['requests'])==12 and len(native['Native_transport_diagnostics'])==7,'CompleteNative12Plus7CaptureRecorderFalse')
 require(native['full_cache_parity_claimed']is False and native['actual_auth_backend_introspection']is False and native['Source_application_runs']==native['Main_application_runs']==0,'NoFabricatedWholeStackOrBackendClaim');database=native['database'];nonce=database.removeprefix('rust_probe_notifforms_native_');require(len(nonce)==10 and all(c in'0123456789abcdef'for c in nonce),'OwnedNativeDatabase')
 clocks=Clocks(SOURCE);seed=native['seed_SQL'];sql_snapshot(seed);graph(SOURCE['seed'],seed,clocks);users(native['seed_users'],SOURCE,nonce,clocks,native['seed_started_utc'],native['seed_completed_utc']);require(equal(seed['tables']['accounts_user'],native['seed_users']),'SeedUserSQLBinding');cache(native['cache_after_seed'],False);previous=seed;previous_raw=None;cache_gaps=[]
 for actual,source,spec in zip(native['requests'],SOURCE['requests'],PLAN['cases']):
  i=spec['index'];require(actual['index']==i and actual['actor']==spec['actor']and actual['method']==spec['method']and actual['path']==source['path']and actual['target_alias']==spec['target_alias']and actual['body_sha256']==spec['body_sha256']and actual['content_type']==spec['content_type']and actual['actual_request_invoked']is True,'ExactNativeCaseRoleMethodRouteBodyAndInvocation');require(actual['status']==source['status'],'ActualSourceStatus');data=raw_body(actual);require(type(actual['headers'])is dict and all(type(k)is type(v)is str for k,v in actual['headers'].items()),'RawResponseHeadersRetained');require(i==12 or actual['headers'].get('content-type')=='application/json','NativeJSONContentTypeForJSONBody')
  expected_headers={'HTTP_X_API_KEY':('native-forms-api-'+nonce+'-'+spec['actor']).encode().hex()};require(equal(actual['raw_headers_private'],expected_headers),'ActualOwnPersistedHeaderBytes')
  before,after=actual['before_SQL'],actual['after_SQL'];require(equal(before,previous),'CrossRequestFull11SQLContinuity');graph(source['before'],before,clocks);graph(source['after'],after,clocks)
  for table in TABLES:
   if table not in(N,D):require(equal(before['tables'][table],after['tables'][table])and equal(after['tables'][table],seed['tables'][table])and equal(before['sequences'][table],after['sequences'][table])and equal(after['sequences'][table],seed['sequences'][table]),'OtherNineTablesAndSequencesUntouched:'+table)
  require(equal(actual['before_events'],native['seed_events'])and equal(actual['after_events'],native['seed_events']),'NoCredentialEventMaskingOrMutation')
  if i in(1,2,4,7,8):
   a,b=source['json'],actual['json'];require(set(a)==set(b),'ExactSavedSerializerFields')
   for name,value in a.items():
    if name=='created_at':clocks.compare(value,b[name])
    else:require(equal(value,b[name]),'SavedResponseField:'+name)
   saved=next(v for v in after['tables'][N]if v['id']==b['id']);own(actual,saved['updated_at'])
   if i in(1,7):own(actual,saved['created_at'])
  else:require(equal(source['json'],actual['json']),'LiteralErrorOr204SourceBody');require(i!=12 or data==b'','Empty204')
  cache(actual['before_cache'],i!=1);cache(actual['after_cache'],True);raw_redis(actual['before_raw_Redis']);raw_redis(actual['after_raw_Redis']);bind_cache_raw(actual['before_cache'],actual['before_raw_Redis']);bind_cache_raw(actual['after_cache'],actual['after_raw_Redis'])
  if previous_raw is not None:
   for a,b in zip(previous_raw,actual['before_raw_Redis']):redis_unchanged(a,b)
  for db in(0,2,3):redis_unchanged(actual['before_raw_Redis'][db],actual['after_raw_Redis'][db])
  if i!=1:redis_unchanged(actual['before_raw_Redis'][1],actual['after_raw_Redis'][1])
  source_keys={bytes.fromhex(r['key_hex']).decode()for r in source['after']['cache']['rows']};native_keys={bytes.fromhex(r['key_hex']).decode()for r in actual['after_cache']['rows']};cache_gaps.append({'index':i,'Source_cache_keys':sorted(source_keys),'Native_cache_keys':sorted(native_keys),'Source_only_keys':sorted(source_keys-native_keys),'Native_only_keys':sorted(native_keys-source_keys),'full_cache_parity':False,'raw_Sources_and_Native_snapshots_retained':True})
  previous=after;previous_raw=actual['after_raw_Redis']
 predictions=[('anonymous',False,'application/json',401),('anonymous',True,'application/json',401),('standard',False,'application/json',403),('standard',True,'application/json',403),('admin',False,'application/json',400),('admin',True,'application/json',400),('admin',False,'text/plain',400)]
 for offset,(row,predicted)in enumerate(zip(native['Native_transport_diagnostics'],predictions),13):
  actor,oversize,media,status=predicted;require(row['index']==offset and row['actor']==actor and row['oversize']is oversize and row['content_type']==media and row['predicted_status_predeclared']==row['status']==status and row['actual_request_invoked']is True,'PredeclaredNativeOnlyTransportRegression');raw_body(row);expected_body={'detail':'Authentication credentials were not provided.'if actor=='anonymous'else'You do not have permission to perform this action.'if actor=='standard'else'JSON parse error'};require(equal(row['json'],expected_body),'ExistingNativeCreateRejectionBody')
  require(row['body_bytes']==(2097153 if oversize else 1),'BoundedNativeDiagnosticBody');require(row['body_sha256']==hashlib.sha256(b'x'*2097153 if oversize else b'{').hexdigest(),'ExactNativeDiagnosticInputHash');require(clock(row['began_utc'])<=clock(row['completed_utc']),'DiagnosticOwnClockOrder');require(equal(row['before_SQL'],previous)and equal(row['after_SQL'],previous),'NativeOnlyRefusalsNoSQLOrSequenceEffects');require(equal(row['before_events'],native['seed_events'])and equal(row['after_events'],native['seed_events']),'NativeOnlyNoCredentialEvents');cache(row['before_cache'],True);cache(row['after_cache'],True)
  raw_redis(row['before_raw_Redis']);raw_redis(row['after_raw_Redis']);bind_cache_raw(row['before_cache'],row['before_raw_Redis']);bind_cache_raw(row['after_cache'],row['after_raw_Redis'])
  for a,b in zip(previous_raw,row['before_raw_Redis']):redis_unchanged(a,b)
  for a,b in zip(row['before_raw_Redis'],row['after_raw_Redis']):redis_unchanged(a,b)
  if actor!='anonymous':require(row['raw_headers_private']=={'HTTP_X_API_KEY':('native-forms-api-'+nonce+'-'+actor).encode().hex()},'ActualDiagnosticHeaderBytes')
  else:require(row['raw_headers_private']=={},'ActualAnonymousNoHeader')
  previous_raw=row['after_raw_Redis']
 require(equal(native['final_SQL'],previous)and equal(native['final_events'],native['seed_events'])and native['server_graceful_wait0']is True,'FinalSQLAndServerWait0');cache(native['final_cache'],True);raw_redis(native['final_raw_Redis']);bind_cache_raw(native['final_cache'],native['final_raw_Redis'])
 for a,b in zip(previous_raw,native['final_raw_Redis']):redis_unchanged(a,b)
 return {'passed':True,'input_kind':'synthetic-only-not-actual-observations'if native.get('_synthetic_only')is True else'retained-native-capture','new_application_runs_by_validator':0,'retained_Source_requests':12,'actual_Native_Source12_requests':12,'Native_only_transport_regressions':7,'generated_clock_maps':len(clocks.mapping),'response_SQL_auth_history_qualified_only':True,'Native_capture_only_passed_field':False,'Source_executed_controller_exit_preserved':1,'Source_offline21_qualification_only':True,'full_cache_parity_passed':False,'cache_mismatch_observations':cache_gaps,'existing_notification_request_started_system_group_gap_open':True,'physical_caller_loss_or_Main_default_graph_acceptance':False}
