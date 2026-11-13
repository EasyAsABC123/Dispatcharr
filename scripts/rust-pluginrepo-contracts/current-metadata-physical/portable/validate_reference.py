"""Unexecuted Native24 JSON/SQL/cache-effect qualification; explicit renderer gap."""
import datetime,hashlib,json,math,pickle,pickletools,io
R='plugins_pluginrepo';C='plugins_pluginconfig'
def need(x,label):
 if not x:raise ValueError(label)
def equal(a,b):return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if type(a)is dict else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if type(a)is list else a==b)
def clock(s):
 v=datetime.datetime.fromisoformat(s.replace('Z','+00:00'));need(v.utcoffset()==datetime.timedelta(0),'UTCClock');return v
class Clocks:
 def __init__(self,source):
  self.generated={clock(next(r for r in q['after']['repos']if r['id']==801)['updated_at'])for q in source['requests']if q['index']in(9,10,11,12)};self.mapping={}
 def compare(self,a,b):
  a,b=clock(a),clock(b)
  if a in self.mapping:need(self.mapping[a]==b,'ExistingClockMappingNoRewind');return
  if a in self.generated:self.mapping[a]=b;return
  need(a==b,'ImportedExistingClockExactInstant')
def record(source,native,clocks):
 need(type(source)is dict and type(native)is dict and set(source)==set(native),'ExactRowFields')
 for k,v in source.items():
  if isinstance(v,str)and(k.endswith('_at')or k in('last_fetched','last_login','date_joined')):clocks.compare(v,native[k])
  else:need(equal(v,native[k]),'ExactStableRowField:'+k)
def selected(source,native,clocks):
 for key in ['repos','configs','settings','users','events','system_notifications','notification_dismissals']:
  need(len(source[key])==len(native[key]),'ExactSelectedRowCounts:'+key)
  for a,b in zip(source[key],native[key]):record(a,b,clocks)
 need(equal(source['public_manifest']['sequences'],native['public_manifest']['sequences']),'All59SequencesExactNoConsumption')
def cache_literal(row):
 need(row['type']=='string'and type(row['value_hex'])is str,'RawGroupString');raw=bytes.fromhex(row['value_hex']);need(len(raw)<=65536,'Group64KiB')
 if raw.isdigit():return int(raw)
 ops=list(pickletools.genops(raw));need(len(ops)<=4096 and all(op.name not in{'GLOBAL','STACK_GLOBAL','REDUCE','BUILD','NEWOBJ','NEWOBJ_EX','OBJ','INST','PERSID','BINPERSID','EXT1','EXT2','EXT4'}for op,arg,at in ops),'LiteralNonExecutableCacheOnly');
 class Literal(pickle.Unpickler):
  def find_class(self,module,name):raise ValueError('NoCacheGlobals')
 return Literal(io.BytesIO(raw)).load()
def cache(source,native):
 def rows(items):
  need([x['database']for x in items]==[0,1,2,3]and all(x['complete']is True and x['destructive_commands_or_consumption']is False for x in items),'CompleteRedisAll4');return{(d['database'],r['key_hex']):r for d in items for r in d['rows']}
 a,b=rows(source),rows(native);need(set(a)==set(b),'ExactSourceRedisKeySetNoWarmImportOrMissingVersions');groups={b':1:coresettings:group:system_settings'.hex(),b':1:coresettings:group:network_access'.hex()};serialization=True
 for key,left in a.items():
  right=b[key];need(left['type']==right['type'],'ExactRedisKeyTypes')
  if key[0]==1 and key[1]in groups:need(equal(cache_literal(left),cache_literal(right)),'ExactDecodedSourceSettingGroup');serialization&=left['value_hex']==right['value_hex']and left['dump_hex']==right['dump_hex']
  else:need(equal({k:v for k,v in left.items()if k!='pttl'},{k:v for k,v in right.items()if k!='pttl'}),'LiteralImportedOrUnrelatedRedisBytesIncludingChannels')
  for row in [left,right]:need(type(row['pttl'])is int and row['pttl']>=-1,'LiteralTTLTypeAndMeaning')
  need((left['pttl']==-1)==(right['pttl']==-1),'PersistentVersusFiniteTTLMeaning')
 return serialization
def own_cache(before,after):
 a={(d['database'],r['key_hex']):r for d in before for r in d['rows']};b={(d['database'],r['key_hex']):r for d in after for r in d['rows']};allowed={(1,b':1:coresettings:group:network_access'.hex()),(1,b':1:coresettings:group:system_settings'.hex())};need(set(a)<=set(b)and set(b)-set(a)<=allowed,'OnlyNamedRequestGroupFills')
 for k,v in a.items():
  other=b[k];need(equal({n:x for n,x in v.items()if n!='pttl'},{n:x for n,x in other.items()if n!='pttl'}),'ExistingRawRedisKeyUnchanged');need(type(other['pttl'])is int and(v['pttl']==other['pttl']==-1 or 0<=other['pttl']<=v['pttl']),'OwnFiniteTTLMonotonicNoReset')
def response(source,native,clocks):
 need(type(source)is type(native),'ExactResponseJSONPrimitiveType')
 if type(source)is list:need(len(source)==len(native),'ExactJSONListLength');[response(a,b,clocks)for a,b in zip(source,native)]
 elif type(source)is dict:
  need(set(source)==set(native),'ExactJSONFields')
  for k,v in source.items():
   if k in('created_at','updated_at','last_fetched')and v is not None:clocks.compare(v,native[k])
   else:response(v,native[k],clocks)
 else:need(equal(source,native),'ExactJSONScalar')
def validate(source,native):
 need(source['capture_complete']is True and source['actual_request_count']==24 and source['Source_framework_APIClient_only']is True,'MeasuredSource24');need(native['capture_only']is True and native['passed']is False and native['capture_complete']is True and native['actual_request_count']==native['completed_capture_count']==23,'NativeCaptureOnlyComplete23NotRecorderPass');need(native['raw_body_byte_parity_claimed']is False and native['Source24_first23_only']is True and native['Source_case24_requested']is False and native['Main_or_fullM8']is False,'ExplicitParsedOnlyScopeAndFrameworkGap')
 clocks=Clocks(source);selected(source['seed'],native['before_requests'],clocks);serialized_groups=[];raw_body_equal=[]
 for a,b in zip(source['requests'][:23],native['requests']):
  for k in ['index','name','actor','method','path','body_hex','body_sha256','body_bytes']:need(equal(a[k],b[k]),'ExactFixedInput:'+k)
  need(b['actual_request_invoked']is True and b['capture_complete']is True,'ActualEachRequest');need(b['physical_close']['fd_after']==-1 and b['physical_close']['original_inode_absent']is True and b['physical_close']['timer_joined']is True,'EachActualHTTPSocketPhysicallyClosed')
  body=bytes.fromhex(b['response_body_hex']);need(len(body)==b['response_body_bytes']and hashlib.sha256(body).hexdigest()==b['response_body_sha256']and equal(json.loads(body),b['json']),'LiteralNativeBodyRetainedNotNormalized')
  for edge in ['before','after']:selected(a[edge],b[edge],clocks);serialized_groups.append(cache(a[edge]['raw_Redis'],b[edge]['raw_Redis']))
  own_cache(b['before']['raw_Redis'],b['after']['raw_Redis'])
  before=b['before']['public_manifest'];after=b['after']['public_manifest'];allowed={'plugins_pluginrepo'}if a['index']in(9,10,11,12)else{'plugins_pluginrepo','plugins_pluginconfig'}if a['index']==18 else set();need(equal(before['sequences'],after['sequences']),'OwnAll59SequenceValuesUnchanged')
  for k in ['columns','indexes','constraints']:need(equal(before[k],after[k]),'OwnFullSchemaMetadataUnchanged')
  for table,row in before['tables'].items():
   if table not in allowed:need(equal(row,after['tables'][table]),'EveryOtherTableUnchanged:'+table)
  if a['index']in(9,10,11,12):
   own=next(r for r in b['after']['repos']if r['id']==801)['updated_at'];need(clock(b['began_utc'])<=clock(own)<=clock(b['completed_utc']),'OwnActualPUTSaveClock')
  need(a['status']==b['status'],'Exact23HTTPStatuses');ah={k.lower():v for k,v in(a['headers'].items()if type(a['headers'])is dict else a['headers'])};bh={k.lower():v for k,v in b['headers']};need(ah.get('www-authenticate')==bh.get('www-authenticate'),'ExactAuthenticationChallengeNoOmission');response(a['json'],b['json'],clocks);raw_body_equal.append(a['response_body_hex']==b['response_body_hex']);need(dict((k.lower(),v)for k,v in b['headers']).get('content-type','').split(';')[0]=='application/json','ActualNativeJSONContentType')
 need(len(native['requests'])==23 and all(type(x)is bool for x in serialized_groups),'Finite23Outcomes')
 return {'passed':True,'Source_scoped_HTTP_JSON_cases':23,'authentication_challenge_semantics':True,'full_header_byte_parity':False,'actual_Native_HTTP_cases':23,'Source24_case24_unexecuted_in_this_run':True,'SQL_effect_cases':23,'actual_raw_body_byte_equal_by_case':raw_body_equal,'group_serialization_byte_equal_all':all(serialized_groups),'decoded_group_and_unrelated_raw_Redis_scope_qualified':True,'Source_APIClient_is_not_TCP_authority':True,'retained_User_read_limits_scope':True,'whole_ordinary_ingress_or_RSS_qualified':False,'Main_or_fullM8_or_physical_ownership_acceptance':False}
