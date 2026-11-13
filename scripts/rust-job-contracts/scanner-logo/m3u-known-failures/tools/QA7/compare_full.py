"""Full-field Source4 comparison; no field stripping or post-diff sorting.
Original author credit retained. Every raw value remains in capture JSON.
"""
import base64
from datetime import datetime,timezone
from uuid import UUID
TABLES=('accounts','profiles','sources','indices','epg_data','programmes','streams','all_owned_streams','groups','group_links','system_events')
TIME_FIELDS={'created_at','updated_at','last_seen','timestamp'}
GROUPS='apps.m3u.tasks.refresh_m3u_groups';SINGLE='apps.m3u.tasks.refresh_single_m3u_account'
def require(value,label):
 if not value:raise AssertionError(label)
def instant(value):
 require(type(value)is str,'AwareTimeString')
 date=datetime.fromisoformat(value);require(date.tzinfo is not None,'AwareTimeOffset')
 return date.astimezone(timezone.utc)
def equal(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(equal(a[k],b[k])for k in a)
 if isinstance(a,list):return len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))
 return a==b
def native_owner_token(raw_base64):
 # Actual frozen coordination token is rust:<canonical RFC4122 UUIDv4>.
 # This validates a decoded COPY; observers retain the original GET bytes.
 try:
  token=base64.b64decode(raw_base64,validate=True).decode('ascii')
  if len(token)!=41 or not token.startswith('rust:'):return False
  owner=UUID(token[5:])
  return str(owner)==token[5:]and owner.version==4
 except (ValueError,TypeError,UnicodeDecodeError):return False
class Clocks:
 def __init__(self):self.bindings={};self.raw=[]
 def bind(self,source,native,path):
  s,n=instant(source),instant(native)
  self.raw.append({'path':path,'Source_raw':source,'Native_raw':native,'existing_binding':None if s not in self.bindings else self.bindings[s].isoformat()})
  if s in self.bindings:require(self.bindings[s]==n,'ExistingClockBinding:'+path)
  else:self.bindings[s]=n
 def rows(self,source,native,path,window=None,before_source=None,before_native=None):
  require(len(source)==len(native),'RowCount:'+path)
  old_s={r['id']:r for r in before_source or[]};old_n={r['id']:r for r in before_native or[]}
  for i,(s,n)in enumerate(zip(source,native)):
   require(set(s)==set(n),'FieldPresence:'+path+str(i));require(s['id']==n['id'],'SeededRowOrderIdentity:'+path)
   for key in s:
    p=path+'/'+str(s['id'])+'/'+key
    if key not in TIME_FIELDS or s[key]is None:
     require(equal(s[key],n[key]),'ExactField:'+p);continue
    self.bind(s[key],n[key],p)
    previous_s=old_s.get(s['id'],{}).get(key);previous_n=old_n.get(n['id'],{}).get(key)
    if key in old_s.get(s['id'],{})and key in old_n.get(n['id'],{}):
     changed=s[key]!=previous_s;require((n[key]!=previous_n)==changed,'ChangedUnchanged:'+p)
     if not changed:require(n[key]==previous_n,'UnchangedLiteral:'+p);continue
    if window:
     require(instant(window['began_utc'])<=instant(n[key])<=instant(window['finished_utc']),'ActualNativeCommandWindow:'+p)

def compare_case(reference,actual,clocks,residual_keys):
 require(reference['name']==actual['name'],'CaseName');require(actual['source_fallback']is False,'NoSourceFallback')
 for phase in ('before','after'):
  for table in TABLES:
   clocks.rows(reference[phase][table],actual[phase][table],phase+'/'+table,
    actual['case_window']if phase=='after'else None,
    reference['before'][table]if phase=='after'else None,actual['before'][table]if phase=='after'else None)
  for key in ('events','files','cache_files'):
   require(equal(reference[phase][key],actual[phase][key]),'Exact'+phase+':'+key)
  source_cache=reference[phase]['output_cache'];native_cache=actual[phase]['output_cache'];require(set(source_cache)==set(native_cache),'CacheFieldPresence')
  for key in source_cache:
   if key.endswith('_ttl'):require(type(native_cache[key])is int and 1<=native_cache[key]<=600,'OwnedCacheTTL:'+key)
   else:require(equal(source_cache[key],native_cache[key]),'OutputCache:'+key)
  source_locks=reference[phase]['locks'];native_locks=actual[phase]['locks'];require(set(source_locks)==set(native_locks),'ExactLockKeys:'+phase)
  for key in source_locks:
   s,n=source_locks[key],native_locks[key];require(set(s)==set(n),'LockFieldPresence')
   require(type(n['ttl'])is int and 1<=n['ttl']<=300,'FiniteTaskLockTTL')
   if key in residual_keys:
    require(native_owner_token(n['raw_base64']),'NativeUniqueOwnerToken')
    require(n['raw_base64']!=s['raw_base64'],'ExplicitRawTokenDifference')
    if phase=='after'and key in actual['before']['locks']:require(n['raw_base64']==actual['before']['locks'][key]['raw_base64'],'NoResidualTokenReplacement')
   else:require(s['raw_base64']==n['raw_base64'],'ForeignLockRawPreserved')
 parent_source=reference['results'][reference['parent_id']]['metadata'];parent_native=actual['results'][actual['parent_id']]['metadata']
 require(set(parent_source)==set(parent_native),'ParentMetadataFields')
 for key in ('status','result','traceback'):require(equal(parent_source[key],parent_native[key]),'ParentTypedMetadata:'+key)
 require(parent_native['task_id']==actual['parent_id'],'ParentResultIdentity')
 source_children={v['task']:v['id']for v in reference['postruns']if v['task']in(GROUPS,SINGLE)}
 native_children={v['task']:v['headers']['id']for v in actual['publications']if v['task']in(GROUPS,SINGLE)}
 translated=[]
 for value in parent_source['children']:
  family=next(k for k,ident in source_children.items()if ident==value[0][0]);translated.append([[native_children[family],value[0][1]],value[1]])
 require(equal(translated,parent_native['children']),'ExactParentChildrenTupleOrder')
 clocks.bind(parent_source['date_done'],parent_native['date_done'],'parent/date_done')
 window=actual['producer_window'];require(instant(window['began_utc'])<=instant(parent_native['date_done'])<=instant(window['finished_utc']),'ActualSourceProducerUTCWindow')
 metadata_checks=[]
 require(len(actual['publications'])==len(reference['publications']),'ExactPublicationMultiplicity')
 for family in (GROUPS,SINGLE):
  if family not in source_children:continue
  s_task=next(v for v in reference['postruns']if v['task']==family);s=reference['results'][s_task['id']]['metadata']
  pubs=[v for v in actual['publications']if v['task']==family];require(len(pubs)==1,'ExactlyOneChild:'+family)
  pub=pubs[0];ident=pub['headers']['id'];require(ident in actual['native_ids'],'BothNativeChildren')
  row=actual['results'][ident];n=row['metadata'];require(set(s)==set(n),'ExactMetadataFields:'+family)
  require(n['task_id']==ident and n.get('parent_id')==actual['parent_id'],'OwnedResultIdentity')
  require(pub['headers']['root_id']==actual['parent_id']and pub['headers']['parent_id']==actual['parent_id'],'ActualParentRoot')
  original_pub=next(v for v in reference['publications']if v['task']==family)
  for field in ('task','timelimit','eta','expires','retries','ignore_result'):
   require((field in original_pub['headers'])==(field in pub['headers']),'ProtocolFieldPresence:'+field)
   if field in original_pub['headers']:require(equal(original_pub['headers'][field],pub['headers'][field]),'ActualProtocolField:'+field)
  require(equal(original_pub['body'],pub['body']),'ActualProtocolArgsKwargsAndCallbacks')
  for key in ('status','result','children'):require(equal(s[key],n[key]),'TypedMetadata:'+family+':'+key)
  require(3500<=row['ttl']<=3600,'ResultTTL')
  original_get=reference['results'][s_task['id']]
  require(row['Original_get_type']==original_get['Original_get_type'],'ActualOriginalAsyncResultType:'+family)
  if s['status']=='FAILURE':require(equal(row['Original_exception_args'],original_get['Original_exception_args']),'ActualOriginalExceptionArgs:'+family)
  else:require(equal(row['Original_get_value'],n['result']),'ActualOriginalNormalValue:'+family)
  clocks.bind(s['date_done'],n['date_done'],'metadata/'+family+'/date_done')
  window=actual['command_windows'][ident];require(instant(window['began_utc'])<=instant(n['date_done'])<=instant(window['finished_utc']),'ActualMetadataCommandWindow')
  if s['status']=='FAILURE':
   require(type(n['traceback'])is str and n['traceback'].startswith('Native Dispatcharr M3U groups diagnostic'),'TruthfulNativeDiagnostic')
   require('Traceback (most recent call last)'not in n['traceback']and n['traceback']!=s['traceback'],'NoPythonTraceEquality')
  else:require(s['traceback']is None and n['traceback']is None,'NormalTraceNull')
  actual_id=ident;s_id=s_task['id']
  n_files=next(v['files']for v in actual['native_fileposts']if v['id']==actual_id)
  s_files=next(v['files']for v in reference['fileposts']if v['id']==s_id)
  require(equal(s_files,n_files),'ExactIntermediateCacheBytes:'+family)
  metadata_checks.append({'task':family,'actual_id':ident,'Source_id':s_id,'status':n['status'],'traceback_equal':s['traceback']==n['traceback']})
 actual['temporal_comparison']={'all_fields_retained':True,'raw_bindings':list(clocks.raw),'Source_command_intervals':None,'Native_SQL_clock_injected':False,'Native_case_window':actual['case_window']}
 actual['lease_scope']={'raw_unique_token_difference_explicit':True,'native_renewal_stopped':True,'Source_renewal_after120s':'OPEN','full_lease_parity':False}
 return metadata_checks
