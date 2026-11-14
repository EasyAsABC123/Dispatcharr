"""Unexecuted Native24 JSON/SQL/cache-effect qualification; explicit renderer gap."""
import datetime,hashlib,json,math,pickle,pickletools,io
R='plugins_pluginrepo';C='plugins_pluginconfig'
def need(x,label):
 if not x:raise ValueError(label)
def equal(a,b):return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if type(a)is dict else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if type(a)is list else a==b)
def clock(s):
 v=datetime.datetime.fromisoformat(s.replace('Z','+00:00'));need(v.utcoffset()==datetime.timedelta(0),'UTCClock');return v
SQL_DATES={'created_at','updated_at','last_fetched','last_login','date_joined'}
def dates(tree):
 if type(tree)is dict:
  for key,value in tree.items():
   if key in SQL_DATES and type(value)is str:
    try:yield clock(value)
    except ValueError:pass
   elif type(value)in(dict,list):yield from dates(value)
 elif type(tree)is list:
  for value in tree:yield from dates(value)
class Clocks:
 def __init__(self,source):
  self.generated=set(dates(source['requests']))-set(dates(source['seed']));self.mapping={};self.window=None
 def compare(self,a,b):
  need(type(a)is str and type(b)is str,'TimestampStrings');a,b=clock(a),clock(b)
  if a in self.mapping:need(self.mapping[a]==b,'ExistingClockMappingNoRewind');return
  if a in self.generated:
   need(self.window is not None and self.window[0]<=b<=self.window[1],'NewNativeGeneratedClockInActualRequest');self.mapping[a]=b;return
  need(a==b,'ImportedExistingClockExactUTCInstant')

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
def own_cache(source_before,source_after,before,after):
 def rows(items):return{(d['database'],r['key_hex']):r for d in items for r in d['rows']}
 sa,sb,a,b=map(rows,[source_before,source_after,before,after]);need(set(a)-set(b)==set(sa)-set(sb)and set(b)-set(a)==set(sb)-set(sa),'ExactRequestInducedKeyAddsDeletesNotAllCacheClear')
 for k in set(a)&set(b):
  left,right=a[k],b[k];need(equal({n:v for n,v in left.items()if n!='pttl'},{n:v for n,v in right.items()if n!='pttl'}),'ExistingRawRedisKeyUnchanged');need(type(right['pttl'])is int and(left['pttl']==right['pttl']==-1 or 0<=right['pttl']<=left['pttl']),'OwnFiniteTTLNoReset')

def response(source,native,clocks):
 need(type(source)is type(native),'ExactResponseJSONPrimitiveType')
 if type(source)is list:need(len(source)==len(native),'ExactJSONListLength');[response(a,b,clocks)for a,b in zip(source,native)]
 elif type(source)is dict:
  need(set(source)==set(native),'ExactJSONFields')
  for k,v in source.items():
   if k in('created_at','updated_at','last_fetched')and v is not None:clocks.compare(v,native[k])
   else:response(v,native[k],clocks)
 else:need(equal(source,native),'ExactJSONScalar')
def validate_import(source,native):
 clocks=Clocks(source);selected(source['seed'],native,clocks);cache(source['seed']['raw_Redis'],native['raw_Redis'])
 a={(d['database'],r['key_hex']):r for d in source['seed']['raw_Redis']for r in d['rows']};b={(d['database'],r['key_hex']):r for d in native['raw_Redis']for r in d['rows']};need(len(a)==len(b)==9,'ExactlyNineActualExistingSeedKeys')
 for key,row in a.items():
  other=b[key];need(type(other['pttl'])is int and(row['pttl']==other['pttl']==-1 or 0<=other['pttl']<=row['pttl']),'ActualImportedRemainingTTLNoSyntheticRenewal')
 return {'passed':True,'exact_selected_SQL_existing_state_and_all59seq':True,'exact9existing_Redis_keys_no_request_group_warm':True,'Native_model_save_or_bootstrap_claim':False}

def validate(source,native):
 count=source['actual_request_count'];need(count in(24,1,16)and source['capture_complete']is True and source['Source_framework_APIClient_only']is True and source['client_raise_request_exception']is False,'MeasuredExactSourceEpoch');need(native['capture_only']is True and native['passed']is False and native['capture_complete']is True and native['actual_request_count']==native['completed_capture_count']==len(native['requests'])==count,'NativeCompleteCaptureOnlyExactEpoch');need(native['raw_body_byte_parity_claimed']is False and native['production_Main_exercised']is True and native['full_M8_accepted']is False,'LimitedScope');clocks=Clocks(source);selected(source['seed'],native['before_requests'],clocks);serialized=[];byte_equal=[];order_equal=[]
 for a,b in zip(source['requests'],native['requests']):
  for k in ['index','name','actor','method','path','body_hex','body_sha256','body_bytes']:need(equal(a[k],b[k]),'ExactInput:'+k)
  need(b['actual_request_invoked']is True and b['capture_complete']is True,'ActualRequest');need(b['physical_close']['fd_after']==-1 and b['physical_close']['original_inode_absent']is True and b['physical_close']['timer_joined']is True,'ActualFrontendHTTPReaderClosed');body=bytes.fromhex(b['response_body_hex']);need(len(body)==b['response_body_bytes']and hashlib.sha256(body).hexdigest()==b['response_body_sha256']and equal(json.loads(body),b['json']),'LiteralNativeBodySHA');clocks.window=(clock(b['began_utc']),clock(b['completed_utc']));need(clocks.window[0]<=clocks.window[1],'ActualNativeWindow')
  for edge in ['before','after']:selected(a[edge],b[edge],clocks);serialized.append(cache(a[edge]['raw_Redis'],b[edge]['raw_Redis']))
  own_cache(a['before']['raw_Redis'],a['after']['raw_Redis'],b['before']['raw_Redis'],b['after']['raw_Redis']);before=b['before']['public_manifest'];after=b['after']['public_manifest'];source_before=a['before']['public_manifest'];source_after=a['after']['public_manifest'];allowed={name for name,value in source_before['tables'].items()if not equal(value,source_after['tables'][name])};need(allowed<={'plugins_pluginrepo','plugins_pluginconfig'},'SourceChangedOnlyExpectedDomainTables')
  for key in ['columns','indexes','constraints']:need(equal(before[key],after[key]),'FullSchemaUnchanged')
  for name,value in before['tables'].items():
   if name not in allowed:need(equal(value,after['tables'][name]),'EveryOtherNativeTableUnchanged:'+name)
  need(equal(source_before['sequences'],before['sequences'])and equal(source_after['sequences'],after['sequences']),'All59ExactSequenceEffectsIncludingTempCreates');need(a['status']==b['status'],'ExactHTTPStatus');ah={k.lower():v for k,v in a['headers'].items()};bh={k.lower():v for k,v in b['headers']};need(ah.get('www-authenticate')==bh.get('www-authenticate'),'ExactAuthChallenge');need(ah.get('content-type')==bh.get('content-type')=='application/json','ExactJSONContentType');response(a['json'],b['json'],clocks);byte_equal.append(a['response_body_hex']==b['response_body_hex']);order_equal.append(type(a['json'])is not dict or list(a['json'])==list(b['json']))
 return {'passed':True,'Source_scoped_actual_HTTP_JSON_SQL_cases':count,'actual_Native_TCP_cases':count,'Source_APIClient_is_not_TCP_authority':True,'Source_original_network_host1_limitations_unchanged':True,'actual_raw_body_equal_by_case':byte_equal,'actual_top_level_JSON_field_order_equal_by_case':order_equal,'all_decoded_group_effects_and_unrelatedRedis_qualified':True,'group_serialization_byte_equal_all':all(serialized),'exact59sequence_and_foreign_table_effects':True,'own_generated_clock_mapping':{a.isoformat():b.isoformat()for a,b in clocks.mapping.items()},'full_header_byte_parity':False,'Main_or_fullM8_or_HTTPphysical_acceptance':False}

def validate_http_sql(source,native):
 """Separate API/SQL scope; never removes or accepts strict cache differences."""
 count=source['actual_request_count'];need(count in(16,24,1)and source['capture_complete']is True,'Complete retained Source context');need(native['capture_complete']is True and native['actual_request_count']==native['completed_capture_count']==len(native['requests'])==count,'Complete actual Main context');need(native['production_Main_exercised']is True and native['full_M8_accepted']is False,'Bounded Main scope')
 clocks=Clocks(source);selected(source['seed'],native['before_requests'],clocks);bodies=[];orders=[]
 for a,b in zip(source['requests'],native['requests']):
  for key in ['index','name','actor','method','path','body_hex','body_sha256','body_bytes']:need(equal(a[key],b[key]),'Exact literal input:'+key)
  need(b['actual_request_invoked']is True and b['capture_complete']is True,'Actual completed request');need(b['physical_close']['fd_after']==-1 and b['physical_close']['original_inode_absent']is True and b['physical_close']['timer_joined']is True,'Actual HTTP reader retirement')
  raw=bytes.fromhex(b['response_body_hex']);need(len(raw)==b['response_body_bytes']and hashlib.sha256(raw).hexdigest()==b['response_body_sha256']and equal(json.loads(raw),b['json']),'Actual response bytes/hash/JSON')
  clocks.window=(clock(b['began_utc']),clock(b['completed_utc']));need(clocks.window[0]<=clocks.window[1],'Actual request clock window')
  for edge in ['before','after']:selected(a[edge],b[edge],clocks)
  before=b['before']['public_manifest'];after=b['after']['public_manifest'];sb=a['before']['public_manifest'];sa=a['after']['public_manifest'];allowed={key for key,value in sb['tables'].items()if not equal(value,sa['tables'][key])};need(allowed<={'plugins_pluginrepo','plugins_pluginconfig'},'Observed Source mutation domain')
  for key in ['columns','indexes','constraints']:need(equal(before[key],after[key]),'Native schema unchanged')
  for key,value in before['tables'].items():
   if key not in allowed:need(equal(value,after['tables'][key]),'Foreign Native table unchanged:'+key)
  need(equal(sb['sequences'],before['sequences'])and equal(sa['sequences'],after['sequences']),'All59 sequence effects exact')
  need(a['status']==b['status'],'Exact actual HTTP status');left={k.lower():v for k,v in a['headers'].items()};right={k.lower():v for k,v in b['headers']};need(left.get('content-type')==right.get('content-type')=='application/json','Exact JSON media type');need(left.get('www-authenticate')==right.get('www-authenticate'),'Exact authentication challenge');response(a['json'],b['json'],clocks);bodies.append(a['response_body_hex']==b['response_body_hex']);orders.append(type(a['json'])is not dict or list(a['json'])==list(b['json']))
 return {'passed':True,'actual_Main_API_cases':count,'selected_SQL_tables':7,'all59_sequence_effects':True,'foreign_Native_tables_and_schema_unchanged':True,'initial_all_foreign_Source_rows_equivalent':False,'cache_parity_qualified':False,'literal_API_body_equal_by_case':bodies,'top_level_JSON_order_equal_by_case':orders,'Source_original_TCP_authority':False,'full_M8_accepted':False,'performance_measured':False}
