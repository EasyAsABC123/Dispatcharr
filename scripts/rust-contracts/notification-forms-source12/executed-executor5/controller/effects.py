"""Unexecuted Source12 predictions, exact seed/target/body checks and bounded raw-effect validation."""
import copy,datetime,hashlib,json
from definition import ROOT,validate,validate_row,equal
from fixture_guards import TABLES,dataset,require
ACTIONS=['create','partial_update','partial_update','update','retrieve','partial_update','create','partial_update','partial_update','partial_update','partial_update','destroy']
def clock(value):
 x=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));require(x.utcoffset()==datetime.timedelta(0),'UTCClock');return x

def normalized(row):
 return {k:clock(v)if k in('created_at','updated_at','dismissed_at','date_joined','last_login')and v is not None else v for k,v in row.items()}
def graph(state):return state['notifications']
SCHEMA=json.loads((ROOT/'inputs/schema62-authority.json').read_text())
def full_manifest(value):
 require(set(value)=={'tables','sequences','columns','indexes','constraints'}and set(value['tables'])==set(SCHEMA['tables'])and len(value['tables'])==62 and set(value['sequences'])==set(SCHEMA['sequences'])and len(value['sequences'])==59,'Full62Table59SequenceScope')
 require(all(equal(value[k],SCHEMA[k])for k in('columns','constraints','indexes')),'ExactPublicSchemaMetadata')
 for row in value['tables'].values():require(set(row)=={'count','sha_like_md5'}and type(row['count'])is int and 0<=row['count']<=10000 and type(row['sha_like_md5'])is str and len(row['sha_like_md5'])==32 and all(c in'0123456789abcdef'for c in row['sha_like_md5']),'LegacyManifestExactCountMD5Shape')
 for row in value['sequences'].values():require(type(row)is list and len(row)==2 and type(row[0])is int and row[0]>=1 and type(row[1])is bool,'Full59LiteralSequenceValues')
 return True

def raw_redis(value):
 require(type(value)is list and [r['database']for r in value]==[0,1,2,3],'FullOwnedRedis0_1_2_3')
 for scope in value:
  require(scope['complete']is True and scope['destructive_commands_or_consumption']is False and len(scope['rows'])<=1000 and len({r['key_hex']for r in scope['rows']})==len(scope['rows']),'CompleteUniqueNonconsumingRawRedis')
  for row in scope['rows']:require(type(row['pttl'])is int and row['pttl']>=-1 and type(row['type'])is str and len(bytes.fromhex(row['dump_hex']))<=65536,'RawRedisDUMPTypeTTL')
 return True

def redis_unchanged(before,after,ignore_cache=False):
 left={r['key_hex']:r for r in before['rows']if not(ignore_cache and bytes.fromhex(r['key_hex']).startswith(b':1:'))};right={r['key_hex']:r for r in after['rows']if not(ignore_cache and bytes.fromhex(r['key_hex']).startswith(b':1:'))};require(set(left)==set(right),'NoRawRedisKeyDropOrExtra')
 for key,row in left.items():
  other=right[key];require(equal({k:v for k,v in row.items()if k!='pttl'},{k:v for k,v in other.items()if k!='pttl'}),'AllRawRedisTypesDUMPValueMembersScoresUnchanged');a,b=row['pttl'],other['pttl'];require((a==b==-1)or(0<b<=a),'RawRedisFiniteTTLMonotonicOrPersistent')
 return True

def channels(value):
 require(value['group']=='updates'and value['Redis_database']==1 and value['receive_pop_or_ack_calls']==0 and value['unknown_or_setup_frames_consumed']is False,'NonconsumingActualChannelsScope')
 require(value['mailbox_type']in('none','zset')and len(value['frames'])<=64,'ChannelsMailboxShape')
 for row in value['frames']:
  raw=bytes.fromhex(row['raw_hex']);require(len(raw)==row['raw_bytes']and hashlib.sha256(raw).hexdigest()==row['raw_sha256'],'ActualRawChannelsFrameHash')
 return True

def snapshot(state,database):
 dataset(state['audit'],database);full_manifest(state['public_manifest']);raw_redis(state['raw_Redis']);channels(state['Channels'])
 for table in TABLES:
  require(state['public_manifest']['tables'][table]['count']==state['audit']['tables'][table]['rows']and equal(state['public_manifest']['sequences'][table+'_id_seq'],[state['audit']['sequences']['public.'+table+'_id_seq']['last_value'],state['audit']['sequences']['public.'+table+'_id_seq']['is_called']]),'Full62Named11CrossBinding')
 for table,v in state['audit']['tables'].items():
  rows=v['raw_rows'];require(type(rows)is list and len(rows)==v['rows'],'Full11RawRowCount')
  require(all(type(r)is dict for r in rows),'Full11RawLiteralRows');require(type(v['raw_json_text'])is list and len(v['raw_json_text'])==len(rows)and all(equal(json.loads(t),r)for t,r in zip(v['raw_json_text'],rows))and hashlib.sha256(b'['+b','.join(t.encode()for t in v['raw_json_text'])+b']').hexdigest()==v['sha256'],'ExactPostgresRawJSONTextFingerprintBinding')
 require(equal([normalized(r)for r in state['audit']['tables']['accounts_user']['raw_rows']],[normalized(r)for r in state['users']])and equal([normalized(r)for r in state['audit']['tables']['core_systemnotification']['raw_rows']],[normalized(r)for r in graph(state)['notifications']])and equal([normalized(r)for r in state['audit']['tables']['core_notificationdismissal']['raw_rows']],[normalized(r)for r in graph(state)['dismissals']]),'RawSQLORMProjectionBinding')
 for table in TABLES:
  text='['+', '.join(sorted(state['audit']['tables'][table]['raw_json_text']))+']';require(hashlib.md5(text.encode()).hexdigest()==state['public_manifest']['tables'][table]['sha_like_md5'],'Full62Named11MD5RawTextCrossBinding')
 require(type(state['users'])is type(state['events'])is type(state['media'])is list,'RawUsersEventsMedia');cache=state['cache'];require(type(cache['rows'])is list and len(cache['rows'])<=1000 and len({r['key_hex']for r in cache['rows']})==len(cache['rows']),'RawUniqueCacheKeys')
 for row in cache['rows']:require(type(row['pttl'])is int and row['pttl']>=-1 and len(bytes.fromhex(row['key_hex']))<=1024 and len(bytes.fromhex(row['dump_hex']))<=65536,'LiteralRawCacheDumpTTL')
 return True

CACHE_VERSION=':1:coresettings:groupver:network_access'
CACHE_GROUPS={':1:coresettings:group:system_settings',':1:coresettings:group:network_access'}
def cache_scope(state,warm):
 rows=state['cache']['rows'];keys={bytes.fromhex(r['key_hex']).decode()for r in rows};require(keys==({CACHE_VERSION}|CACHE_GROUPS if warm else{CACHE_VERSION}),'PredictedExactNamedCacheScopeNoDropOrExtra');require(state['cache']['api_key_failed_auth_history']is None,'ValidAPIKeyPermissionDenialDoesNotResetOrAddFailedAuthHistory')
 for row in rows:
  key=bytes.fromhex(row['key_hex']).decode();require(row['type']=='string'and type(row['value_hex'])is str and len(bytes.fromhex(row['value_hex']))<=65536,'RawStringGETAndDUMP');require(row['pttl']==-1 if key==CACHE_VERSION else 0<row['pttl']<=300000,'PersistentVersionAndFiniteGroupTTLForms')
 return True

def metadata_policy(row,number):
 meta=row['parsed_request_metadata'];require(meta['request_available']is True and meta['forced_parse']is False,'ActualCachedMetadataNoForcedParsing')
 if number in(5,6,10,11,12):
  require(all(meta['cached'][k].get('class_value')=='rest_framework.request.Empty'and'fields'not in meta['cached'][k]for k in('_data','_files','_full_data')),'PredictedLookupPermissionUnsupportedOrDeleteBeforeBodyDataLoaded')
 if number==7:
  require(meta['cached']['_files']['fields']==[{'name':'unknown_upload','values':[{'kind':'file_metadata','class':'django.core.files.uploadedfile.InMemoryUploadedFile','name':'owned-small.txt','size':20,'content_type':'text/plain','charset':None,'file_content_read':False}]}],'ActualUnknownSmallMemoryFileMetadataNotStoredOrReadByObserver')
 if number==8:
  title=next(x for x in meta['cached']['_data']['fields']if x['name']=='title');require(title['values']==[{'kind':'literal','value':'first'},{'kind':'literal','value':'second'}],'ActualDuplicateInputOrderRetainedBeforeLastValueSerialization')
 return True

def validate_seed(report,plan):
 context=report['Channels_probe_context'];require(context['registered']is True and context['group']=='updates'and context['Redis_database']==1 and context['non_consuming']is True and context['Source_methods_replaced']is False and context['group_registration_is_fixture_context_not_Default_graph']is True,'ActualOwnedChannelsProbeContextDeclared')
 database=report['database'];seed=report['seed'];snapshot(seed,database);cache_scope(seed,False);nonce=database.removeprefix('rust_probe_notifforms_');require([r['id']for r in seed['users']]==[1,2,3],'ExactOwnedThreeUserIDs')
 for user,role in zip(seed['users'],plan['roles']):
  alias=role['alias'];wanted={'id':role['id'],'username':'rust-probe-notifforms-'+nonce+'-'+alias,'password':'!','api_key':'fixture-forms-key-'+nonce+'-'+alias,'is_active':role['is_active'],'user_level':role['user_level'],'custom_properties':role['custom_properties'],'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'stream_limit':0,'avatar_config':{}}
  require(set(user)==set(wanted)|{'date_joined'}and all(equal(user[k],v)for k,v in wanted.items()),'ExactActorFields:'+alias);require(clock(report['seed_started_utc'])<=clock(user['date_joined'])<=clock(report['seed_completed_utc']),'SeedOwnActorClock')
 require(equal([normalized(r)for r in graph(seed)['notifications']],[normalized(r)for r in plan['seed_notifications']])and equal([normalized(r)for r in graph(seed)['dismissals']],[normalized(r)for r in plan['seed_dismissals']]),'ExactSeedRowsAndDismissalPairs')
 for table,row in plan['seed_sequences'].items():require(equal(seed['audit']['sequences']['public.'+table+'_id_seq'],row),'ExactThreeSeedSequences')
 settings=[r for r in seed['audit']['tables']['core_coresettings']['raw_rows']if r['key']=='network_access'];require(len(settings)==1 and settings[0]['name']=='Network Access'and equal(settings[0]['value'],plan['global_network_settings']),'ExactSourceNetworkSettingSeed')
 require(report['aliases']==plan['seed_aliases'],'ExactPreRequestSeedAliases')
 return {'passed':True,'roles':3,'notifications':2,'dismissals':3,'exact_seed_aliases_and_fields':True}

def response_notice(row,notice):
 expected={k:v for k,v in notice.items()if k!='updated_at'};expected['is_dismissed']=False;actual=row['json'];require(set(actual)==set(expected),'ExactSavedNoticeSerializerFields')
 for key,value in expected.items():require(clock(actual[key])==clock(value)if key=='created_at'else equal(actual[key],value),'SavedSnapshotBody:'+key)

def compare(report,plan):
 validate(plan);require(report['capture_valid']is True and report['actual_request_count']==report['completed_capture_count']==len(report['requests'])==12 and report['force_authenticate_used']is False and report['Source_TCP_or_Native']is False,'FiniteRealResolverSource12Capture')
 aliases=dict(plan['seed_aliases']);seed_report=copy.deepcopy(report);seed_report['aliases']=aliases;validate_seed(seed_report,plan);seed=report['seed'];previous=seed;checks=[]
 for row,spec in zip(report['requests'],plan['cases']):
  n=spec['index'];validate_row(row,spec,aliases);require(row['resolver']['class']=='core.api_views.SystemNotificationViewSet'and row['resolver']['action']==ACTIONS[n-1],'CorrectActualResolvedAction');require(row['resolver']['kwargs']==({}if ACTIONS[n-1]=='create'else{'pk':str(aliases[spec['target_alias']])})and row['resolver']['actions'].get(spec['method'].lower())==ACTIONS[n-1]and row['resolver']['url_name']==('systemnotification-list'if ACTIONS[n-1]=='create'else'systemnotification-detail'),'ExactActualResolvedMethodTargetAndURLName');require(row['transport']=='http_response'and row['capture_complete']is True and row['status']==spec['predicted_status_UNMEASURED'],'UnmeasuredPredictionMatchesActualStatusOrNegativePreserved');require(type(row['headers'])is dict and all(type(k)is type(v)is str for k,v in row['headers'].items()),'LiteralResponseHeaders');body=bytes.fromhex(row['response_body_hex']);require(len(body)==row['response_body_bytes']and hashlib.sha256(body).hexdigest()==row['response_body_sha256'],'RawResponseBodyHash')
  if body:require(equal(json.loads(body),row['json']),'ActualJSONRawBodyBinding')
  require(clock(row['began_utc'])<=clock(row['completed_utc']),'OwnRequestClock');metadata_policy(row,n);before=row['before'];after=row['after'];snapshot(before,report['database']);snapshot(after,report['database']);cache_scope(before,n!=1);cache_scope(after,True);require(equal(before['notifications'],previous['notifications'])and equal(before['audit'],previous['audit']),'CrossRequestSQLGraphContinuity')
  for index in range(4):redis_unchanged(previous['raw_Redis'][index],before['raw_Redis'][index])
  require(equal({k:v for k,v in previous['Channels'].items()if k!='mailbox_pttl'},{k:v for k,v in before['Channels'].items()if k!='mailbox_pttl'}),'CrossRequestChannelsNoDroppedSetupOrUnknownFrames')
  require(equal(before['users'],after['users'])and equal(after['users'],seed['users'])and equal(before['events'],after['events'])and equal(after['events'],seed['events']),'LiteralUsersEventsNoOmission');require(before['media']==after['media']==seed['media']==[],'NoOwnedMediaFileEffect')
  actors={r['id']:r for r in seed['users']};uid=next(r['id']for r in plan['roles']if r['alias']==spec['actor']);require(row['raw_headers_private']=={'HTTP_X_API_KEY':actors[uid]['api_key'].encode().hex()},'ActualRawPersistedAPIKey');require(row['actual_auth_observation']=={'user_id':uid,'is_authenticated':True,'backend_class':'apps.accounts.authentication.ApiKeyAuthentication'},'ActualRoleBackend')
  for table in SCHEMA['tables']:
   if table not in('core_systemnotification','core_notificationdismissal'):require(equal(before['public_manifest']['tables'][table],after['public_manifest']['tables'][table])and equal(after['public_manifest']['tables'][table],seed['public_manifest']['tables'][table]),'Other60PublicTablesNoEffects:'+table)
  for index in range(4):redis_unchanged(before['raw_Redis'][index],after['raw_Redis'][index],ignore_cache=index==1)
  require(equal({k:v for k,v in before['Channels'].items()if k!='mailbox_pttl'},{k:v for k,v in after['Channels'].items()if k!='mailbox_pttl'})and equal(after['Channels']['frames'],seed['Channels']['frames']),'AllSetupOrUnknownChannelsFramesRetainedNoNewPredictionEffect')
  for table in TABLES:
   if table not in('core_systemnotification','core_notificationdismissal'):require(equal(before['audit']['tables'][table],after['audit']['tables'][table])and equal(after['audit']['tables'][table],seed['audit']['tables'][table]),'OtherNineTablesExact:'+table)
  wanted=copy.deepcopy(graph(before));sequences=copy.deepcopy(before['audit']['sequences']);byid={r['id']:r for r in wanted['notifications']}
  if n in(1,7):
   value=row['json'];identifier=value['id'];require(type(identifier)is int and identifier not in byid,'NewActualNaturalID');alias='FormA'if n==1 else'FormB';aliases[alias]=identifier
   require(identifier==sequences['public.core_systemnotification_id_seq']['last_value']+1,'SingleCreateSequenceIncrement');created=next(r for r in graph(after)['notifications']if r['id']==identifier);expected={'id':identifier,'notification_key':'owned_forms_a'if n==1 else'owned_forms_b','title':'Form A'if n==1 else'Form B','message':'First form notification'if n==1 else'Multipart notification','notification_type':'info','priority':'normal','source':'system','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':created['created_at'],'updated_at':created['updated_at']};require(equal(created,expected),'ActualFormCreateDefaultsUnknownFileIgnored');require(clock(row['began_utc'])<=clock(created['created_at'])<=clock(created['updated_at'])<=clock(row['completed_utc']),'OwnCreateClocks');wanted['notifications'].append(created);wanted['notifications'].sort(key=lambda r:r['id']);sequences['public.core_systemnotification_id_seq']={'last_value':identifier,'is_called':True};response_notice(row,created)
  elif n in(2,4,8):
   current=byid[aliases[spec['target_alias']]];actual=next(r for r in graph(after)['notifications']if r['id']==current['id']);require(clock(row['began_utc'])<=clock(actual['updated_at'])<=clock(row['completed_utc'])and clock(actual['updated_at'])>clock(current['updated_at']),'ActualSaveTouchesOwnUpdatedClock');current['updated_at']=actual['updated_at']
   if n==2:current['action_data']={'flag':True,'count':1,'nested':[False,0]}
   if n==4:current.update(title='Form A replaced',message='Boolean fields omitted',is_active=False,admin_only=False)
   if n==8:current.update(title='second',priority='high',action_data={'flag':True,'count':1})
   response_notice(row,actual)
  elif n==12:
   require(body==b''and row['json']is None,'Actual204EmptyBody');wanted['notifications']=[r for r in wanted['notifications']if r['id']!=aliases['aliasDelete']];wanted['dismissals']=[r for r in wanted['dismissals']if r['notification_id']!=aliases['aliasDelete']]
  elif n in(3,9):require(row['json']=={'action_data':['Value must be valid JSON.']},'LiteralJSONFieldValidationError')
  elif n==10:require(row['json']=={'detail':'You do not have permission to perform this action.'},'ActualAdminPermissionRefusalBeforeParse')
  elif n==11:require(row['json']=={'detail':'Unsupported media type "text/plain" in request.'},'ActualUnsupportedMediaType')
  require(equal(graph(after),wanted),'OnlyPredeclaredNotificationGraphEffect');require(equal(after['audit']['sequences'],sequences),'ExactAll11SequenceEffects');
  for name,entry in after['public_manifest']['sequences'].items():
   expected=[sequences['public.'+name]['last_value'],sequences['public.'+name]['is_called']]if'public.'+name in sequences else seed['public_manifest']['sequences'][name];require(equal(entry,expected),'All59SequenceEffects:'+name)
  previous=after;checks.append({'index':n,'status':row['status'],'seed_target_body_roles_SQL_users_events_media_and_sequences':True,'raw_cache_capture_retained_without_history_reset_or_value_normalization':True})
 final=report['final_before_probe_cleanup'];snapshot(final,report['database']);require(equal(final['audit'],previous['audit'])and equal(final['public_manifest'],previous['public_manifest'])and equal(final['users'],previous['users'])and equal(final['events'],previous['events'])and equal(final['media'],previous['media']),'FinalFull62SQLRolesEventsMediaContinuity');cache_scope(final,True)
 for index in range(4):redis_unchanged(previous['raw_Redis'][index],final['raw_Redis'][index])
 require(equal(final['Channels']['frames'],previous['Channels']['frames'])and report['Channels_probe_cleanup']['frames_consumed']==0 and report['Channels_probe_cleanup']['mailbox_deleted_by_fixture']is False,'FinalNonconsumingChannelsContinuityAndCleanup')
 require(report['aliases']==aliases and report['connections_close_called']is True,'FinalAliasAndConnectionClosure');return {'passed':True,'actual_Source_requests':12,'predictions_were_unmeasured_before_capture':True,'checks':checks,'full_public_schema_tables':62,'full_public_schema_sequences':59,'raw_Redis_databases':[0,1,2,3],'Channels_frames_consumed':0,'TCP_Native_fullendpoint_or_performance_acceptance':False}
