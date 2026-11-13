"""Unexecuted Source24 metadata effects oracle; no app imports/network."""
import copy,datetime,hashlib,json,re,io,pickle,pickletools,math
R='plugins_pluginrepo';C='plugins_pluginconfig'
PLAN_SHA256='89583a4c4cec44a8755c5d3c76bf0fa9c6e409b5710bd3f4c1460694dd35a16b'
FIELDS=['id','name','url','is_official','enabled','public_key','signature_verified','registry_url','plugin_count','last_fetched','last_fetch_status','created_at','updated_at']
def need(ok,label):
 if not ok:raise ValueError(label)
def equal(a,b):return type(a)is type(b)and(set(a)==set(b)and all(equal(a[k],b[k])for k in a)if type(a)is dict else len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))if type(a)is list else a==b)
def clock(text):
 value=datetime.datetime.fromisoformat(text.replace('Z','+00:00'));need(value.utcoffset()==datetime.timedelta(0),'UTCClock');return value
def own(value,row):need(clock(row['began_utc'])<=clock(value)<=clock(row['completed_utc']),'OwnRequestSaveClock')
def response_repo(body,row):
 need(type(body)is dict and set(body)==set(FIELDS),'ExactSerializerFields')
 for key in FIELDS:
  if key in ['created_at','updated_at','last_fetched']:
   need(body[key]is None and row[key]is None or type(body[key])is str and type(row[key])is str and clock(body[key])==clock(row[key]),'RetainedSerializerClock')
  elif key=='registry_url':
   manifest=row['cached_manifest']or{};manifest=manifest.get('manifest',manifest);need(body[key]==(manifest.get('registry_url','')or''),'DerivedRegistryURL')
  elif key=='plugin_count':
   manifest=row['cached_manifest']or{};manifest=manifest.get('manifest',manifest);plugins=manifest.get('plugins',[]);need(type(body[key])is int and body[key]==(len(plugins)if type(plugins)is list else 0),'DerivedPluginCount')
  else:need(equal(body[key],row[key]),'ExactRepoSerializerField:'+key)
def manifest_pair(before,after,changed,schema):
 for value in [before,after]:
  need(sorted(value['tables'])==schema['tables']and sorted(value['sequences'])==schema['sequences'],'ExactPinnedTableAndSequenceSets')
  for key in ['columns','indexes','constraints']:need(equal(value[key],schema[key]),'ExactPinnedSchemaMetadata')
  for sequence in value['sequences'].values():need(type(sequence)is list and len(sequence)==2 and type(sequence[0])is int and type(sequence[1])is bool,'ExactSequencePrimitiveTypes')
 need(set(before)==set(after),'FullManifestFields')
 for key in ['columns','indexes','constraints','sequences']:need(equal(before[key],after[key]),'NoSchemaOrSequenceChanges:'+key)
 need(len(before['tables'])==len(after['tables'])==62 and set(before['tables'])==set(after['tables'])and len(before['sequences'])==59,'Full6259')
 for name,row in before['tables'].items():
  other=after['tables'][name]
  for item in [row,other]:need(set(item)=={'count','sha_like_md5'}and type(item['count'])is int and 0<=item['count']<=10000 and re.fullmatch('[a-f0-9]{32}',item['sha_like_md5'])is not None,'TypedWholeTableDigest')
  if name not in changed:need(equal(row,other),'OtherTableUnchanged:'+name)
def literal_cache(raw):
 need(type(raw)is bytes and len(raw)<=65536,'BoundedCacheLiteral')
 if raw.isdigit():return int(raw)
 ops=list(pickletools.genops(raw));need(len(ops)<=4096 and all(op.name not in {'GLOBAL','STACK_GLOBAL','REDUCE','BUILD','NEWOBJ','NEWOBJ_EX','OBJ','INST','PERSID','BINPERSID','EXT1','EXT2','EXT4'}for op,arg,at in ops),'NoExecutableCachePickle')
 class Safe(pickle.Unpickler):
  def find_class(self,module,name):raise ValueError('NoCacheGlobals')
 return Safe(io.BytesIO(raw)).load()
def stable_redis(before,after,allow_fill):
 for inventory in before+after:need(type(inventory['database'])is int and inventory['complete']is True and inventory['destructive_commands_or_consumption']is False and type(inventory['capture_bytes'])is int and 0<=inventory['capture_bytes']<=2097152 and type(inventory['rows'])is list and len(inventory['rows'])<=1000,'CompleteBoundedNonDestructiveRawRedisInventory')
 a={(db['database'],row['key_hex']):row for db in before for row in db['rows']};b={(db['database'],row['key_hex']):row for db in after for row in db['rows']};need([db['database']for db in before]==[0,1,2,3]==[db['database']for db in after],'AllFourRawRedisScopes');allowed={b':1:coresettings:group:system_settings'.hex():{'time_zone':'UTC'},b':1:coresettings:group:network_access'.hex():{'UI':'127.0.0.0/8'}}
 need(set(a)<=set(b)and all(db==1 and key in allowed for db,key in set(b)-set(a))and(allow_fill or set(a)==set(b)),'OnlyDeclaredCacheFillNoKeyLoss')
 for key,row in a.items():
  other=b[key];left={k:v for k,v in row.items()if k!='pttl'};right={k:v for k,v in other.items()if k!='pttl'};need(equal(left,right),'NoExistingRedisValueTypeDumpOrFrameMutation');x,y=row['pttl'],other['pttl'];need(type(x)is type(y)is int and(x==y==-1 or 0<y<=x),'FiniteTTLMonotonic')
 for (db,key),row in b.items():
  if db==1 and key in allowed:need(row['type']=='string'and equal(literal_cache(bytes.fromhex(row['value_hex'])),allowed[key])and type(row['pttl'])is int and 0<row['pttl']<=300000,'ExactStoredSettingsGroupAndTTL')
def channels(value,nonce):
 expected='specific.rust_owned_forms_'+nonce+'!receiver';need(value['group']=='updates'and value['receiver']==expected and type(value['Redis_database'])is int and value['Redis_database']==1 and value['receive_pop_or_ack_calls']==0 and type(value['receive_pop_or_ack_calls'])is int and value['unknown_or_setup_frames_consumed']is False,'ActualOwnedNonConsumingChannelsScope');need(type(value['frames'])is list and len(value['frames'])<=64,'BoundedOwnedChannelFrames')
 for row in value['frames']:
  raw=bytes.fromhex(row['raw_hex']);need(len(raw)==row['raw_bytes']and len(raw)<=65536 and hashlib.sha256(raw).hexdigest()==row['raw_sha256'],'LiteralChannelFrameBytes');need(type(row['score'])is float and math.isfinite(row['score'])and row['score'].hex()==row['score_hex'],'ActualChannelScore');need(type(row['actual_layer_decoded'])is dict,'ActualLayerDecodedFrameShape')
def state_continuity(before,after):
 for key in ['public_manifest','repos','configs','settings','users','events','owned_log_and_media_files','system_notifications','notification_dismissals']:need(equal(before[key],after[key]),'CrossRequestState:'+key)
 need(equal(before['Channels']['frames'],after['Channels']['frames']),'CrossRequestOwnedChannelsFrames');stable_redis(before['raw_Redis'],after['raw_Redis'],False)
def validate_seed(report,plan):
 seed=report['seed'];prior=report['before_owned_seed'];need([r['id']for r in seed['repos']]==[1,801,802]and len(prior['repos'])==1 and prior['repos'][0]['id']==1,'ExactInitialRepositoryIDs');need(equal(seed['repos'][0],prior['repos'][0]),'PinnedOfficialRowUnchangedBySeed');official=seed['repos'][0]
 for key,item in {'id':1,'name':'Dispatcharr Official','url':'https://dispatcharr.github.io/Plugins/manifest.json','is_official':True,'enabled':True,'cached_manifest':{},'public_key':'','signature_verified':None,'last_fetched':None,'last_fetch_status':''}.items():need(equal(official[key],item),'ExactActualMigrationOfficialField:'+key)
 for actual,spec in zip(seed['repos'][1:],plan['seed']['custom_repos']):
  for key,item in spec.items():need(equal(actual[key],item),'ExactCustomSeedField:'+key)
  need(set(actual)==set(spec)|{'last_fetched','last_fetch_status','created_at','updated_at'}and actual['last_fetched']is None and actual['last_fetch_status']=='','ExactRepoDefaultFieldSet')
  need(clock(report['seed_started_utc'])<=clock(actual['created_at'])<=clock(actual['updated_at'])<=clock(report['seed_completed_utc']),'OwnCustomRepoSeedClock')
 for spec in plan['seed']['CoreSettings']:
  actual=next(row for row in seed['settings']if row['key']==spec['key']);need(equal(actual['value'],spec['value'])and actual['name']==spec['name'],'ExactSourceCoreSeedGroup')
 need(len(seed['users'])==4 and len(seed['configs'])==2,'ExactActorAndConfigSeedCounts')
 for actual,spec in zip(seed['users'],plan['roles']):
  need(set(actual)=={'id','username','password','first_name','last_name','email','is_staff','is_active','is_superuser','last_login','date_joined','avatar_config','user_level','custom_properties','api_key','stream_limit'},'ExactRawActorFieldSet')
  for key,item in {'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'avatar_config':{},'stream_limit':0}.items():need(equal(actual[key],item),'ExactActorModelDefault:'+key)
  for key in ['id','user_level','custom_properties']:need(equal(actual[key],spec[key]),'ExactSeedRoleField')
  nonce=report['database'].removeprefix('rust_probe_pluginrepo_');need(actual['username']=='owned-pluginrepo-'+nonce+'-'+spec['alias']and actual['api_key']=='owned-pluginrepo-api-'+nonce+'-'+spec['alias']and actual['password']=='!'and actual['is_active']is True,'ExactOwnedSeedCredentialRole');need(clock(report['seed_started_utc'])<=clock(actual['date_joined'])<=clock(report['seed_completed_utc']),'OwnSeedUserDateJoined')
 for actual,spec in zip(seed['configs'],plan['seed']['managed_config_pairs']):
  for key,item in spec.items():need(equal(actual[key],item),'ExactManagedConfigSeedField')
  need(set(actual)==set(spec)|{'name','slug','version','description','settings','owned_tasks','suspended_schedules','installed_version_is_prerelease','deprecated','created_at','updated_at'},'ExactConfigFieldSet')
  for key,item in {'name':spec['key'],'slug':spec['key'],'version':'','description':'','settings':{},'owned_tasks':[],'suspended_schedules':[],'installed_version_is_prerelease':False,'deprecated':False}.items():need(equal(actual[key],item),'ExactInertConfigDefault:'+key)
  need(clock(report['seed_started_utc'])<=clock(actual['created_at'])<=clock(actual['updated_at'])<=clock(report['seed_completed_utc']),'OwnConfigSeedClock')
 for name,last in plan['seed']['sequence_last_values_called'].items():need(equal(seed['public_manifest']['sequences'][name],[last,True]),'ExactDeclaredInitialSequence')
 cache={bytes.fromhex(row['key_hex']).decode():row for row in seed['cache']['rows']}
 def epoch_ns(text):
  delta=clock(text)-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc);return(delta.days*86400+delta.seconds)*1000000000+delta.microseconds*1000
 for name in ['system_settings','network_access']:
  row=cache[':1:coresettings:groupver:'+name];value=literal_cache(bytes.fromhex(row['value_hex']));need(row['type']=='string'and row['pttl']==-1 and type(value)is int and epoch_ns(report['seed_started_utc'])<=value<=epoch_ns(report['seed_completed_utc'])+999,'ActualSourceSeedPersistentGroupVersionOwnClockWindow')
 return True

def validate(report,plan,schema):
 need(report['definition_sha256']==PLAN_SHA256,'ExactDefinition2PlanReference');need(report['capture_complete']is True and report['passed']is False and report['capture_only']is True and report['actual_request_count']==report['completed_capture_count']==24 and len(report['requests'])==24,'Actual24CaptureRecorderFalse');need(report['Source_framework_APIClient_only']is True and report['Source_TCP_or_Native']is False and report['force_authenticate_used']is False and report['client_raise_request_exception']is True,'ExactFrameworkAuthExceptionScope');need(report.get('outer_fixture_catch')is None and report.get('probe_cleanup_exception')is None and report['connections_close_called']is True,'NoCollectorFailureAndDjangoConnectionsClosed');need(equal(report['probe_cleanup'],{'own_membership_discarded':True,'frames_consumed':0,'mailbox_deleted_by_fixture':False}),'ActualOwnedProbeDiscardNoFrameConsumption');validate_seed(report,plan);need(0<=(clock(report['phase_completed_utc'])-clock(report['phase_started_utc'])).total_seconds()<=120 and type(report['phase_elapsed_seconds'])in(int,float)and 0<=report['phase_elapsed_seconds']<=120,'FiniteSourcePhase120');previous=report['seed'];checks=0
 for row,spec in zip(report['requests'],plan['cases']):
  need((row['index'],row['name'],row['method'],row['actor'],row['target_alias'])==(spec['index'],spec['name'],spec['method'],spec['actor'],spec['target']),'ExactCaseOrderIdentity');path=plan['method_route_bindings'][spec['target']];need(row['path']==path,'ExactPlannedResolvedTarget');need(row['resolver']['class']=='apps.plugins.api_views.'+('PluginRepoListCreateAPIView'if spec['target']=='collection'else'PluginRepoDetailAPIView')and row['resolver']['kwargs']==({}if spec['target']=='collection'else{'pk':int(path.rstrip('/').split('/')[-1])}),'ExactResolverClassAndPK')
  raw=bytes.fromhex(row['body_hex']);need(raw==bytes.fromhex(spec['body_hex'])and len(raw)==spec['body_bytes']==row['body_bytes']and hashlib.sha256(raw).hexdigest()==row['body_sha256']==spec['body_sha256']and row['content_type']==spec['content_type'],'ExactLiteralBodyHash');nonce=report['database'].removeprefix('rust_probe_pluginrepo_');expected_headers={}if spec['actor']=='anonymous'else{'HTTP_X_API_KEY':('owned-pluginrepo-api-'+nonce+'-'+spec['actor']).encode().hex()};need(equal(row['raw_headers_private'],expected_headers),'ActualDeclaredPersistedAPIKeyHeader');need(row['actual_request_invoked']is True and row['capture_complete']is True and 0<=(clock(row['completed_utc'])-clock(row['began_utc'])).total_seconds()<=8,'ActualInvocationAndClock');before,after=row['before'],row['after'];nonce=report['database'].removeprefix('rust_probe_pluginrepo_');channels(before['Channels'],nonce);channels(after['Channels'],nonce);state_continuity(previous,before);previous=after
  # TTL drift is retained and checked separately from exact payload continuity.
  stable_redis(before['raw_Redis'],after['raw_Redis'],True);need(equal(before['Channels']['frames'],after['Channels']['frames']),'NoMetadataRequestChannelsFrames');need(before['cache']['api_key_failed_auth_history']is after['cache']['api_key_failed_auth_history']is None,'NoValidOrMissingKeyFailureHistoryMutation')
  changed=[]
  if spec['predicted_status']is None:
   need(row['transport']=='framework_exception'and row['framework_exception']['complete']is True and row['framework_exception']['truncated']is False,'LiteralCompleteUnmeasuredFrameworkException');need('status'not in row and 'json'not in row,'NoInventedHTTPResponseForException');exc=row['framework_exception'];need(type(exc['type'])is str and '.'in exc['type'],'LiteralFrameworkExceptionType')
   for field,cap in [('message',65536),('traceback',4194304)]:
    data=bytes.fromhex(exc[field+'_hex']);need(type(exc[field+'_bytes'])is int and len(data)==exc[field+'_bytes']and len(data)<=cap and hashlib.sha256(data).hexdigest()==exc[field+'_sha256']and exc[field+'_original_bytes']==len(data)and exc[field+'_original_sha256']==exc[field+'_sha256']and exc[field+'_truncated']is False,'CompleteLiteralExceptionBytesAndHash')
  else:
   need(row['transport']=='http_response'and row['status']==spec['predicted_status'],'PredictedFiniteHTTPStatus');auth=row['actual_auth_observation']
   if spec['actor']=='anonymous':need(equal(auth,{'user_id':None,'is_authenticated':False,'backend_class':None}),'ActualAnonymousAuthObservation')
   else:
    role=next(item for item in plan['roles']if item['alias']==spec['actor']);need(equal(auth,{'user_id':role['id'],'is_authenticated':True,'backend_class':'apps.accounts.authentication.ApiKeyAuthentication'}),'ActualPersistedAPIKeyBackendAndRole')
   headers=row['headers'];need(type(headers)is dict and all(type(k)is type(v)is str for k,v in headers.items())and sum(len(k.encode())+len(v.encode())for k,v in headers.items())<=65536 and[v for k,v in headers.items()if k.lower()=='content-type']==['application/json'],'ActualJSONContentTypeAndBoundedHeaders');body=bytes.fromhex(row['response_body_hex']);need(len(body)==row['response_body_bytes']and hashlib.sha256(body).hexdigest()==row['response_body_sha256']and equal(json.loads(body),row['json']),'ExactRawHTTPJSONBinding')
   if row['method']=='PUT'and row['status']==200:
    changed=[R];left=copy.deepcopy(before['repos']);target=next(r for r in left if r['id']==801);actual=next(r for r in after['repos']if r['id']==801);data=json.loads(raw)
    if 'enabled'in data:target['enabled']=data['enabled']
    if 'public_key'in data:target['public_key']=data['public_key'].strip()
    target['updated_at']=actual['updated_at'];own(actual['updated_at'],row);need(equal(left,after['repos']),'OnlyWhitelistedTargetWriteAndOwnClock');response_repo(row['json'],actual)
   elif row['method']=='DELETE'and row['status']==200:
    changed=[R,C];need(equal([r for r in before['repos']if r['id']!=801],after['repos']),'OnlyTargetRepoDeleted');configs=copy.deepcopy(before['configs'])
    for config in configs:
     if config['source_repo_id']==801:config['source_repo_id']=None
    need(equal(configs,after['configs'])and row['json']=={'success':True},'OnlyFKSetNullNoConfigClockOrSequenceChange')
   elif row['method']=='GET'and row['status']==200:
    expected=sorted(after['repos'],key=lambda r:(not r['is_official'],r['name']));need(type(row['json'])is list and len(row['json'])==len(expected),'ExactRepositoryListOrdering')
    for body,source in zip(row['json'],expected):response_repo(body,source)
  if row['method']=='DELETE'and row.get('transport')=='http_response':
   metadata=row['parsed_request_metadata'];need(metadata['request_available']is True and metadata['forced_parse']is False and set(metadata['cached'])=={'_data','_files','_full_data'}and all(equal(x,{'present':True,'class':'builtins.type','class_value':'rest_framework.request.Empty'})for x in metadata['cached'].values()),'DeleteDRFParserNotMaterializedByView')
  if R not in changed:need(equal(before['repos'],after['repos']),'NoOtherRepoChange')
  if C not in changed:need(equal(before['configs'],after['configs']),'NoOtherConfigChange')
  need(equal(before['users'],after['users'])and equal(before['events'],after['events']),'NoActorOrCredentialEventMutation');need(equal(before['settings'],after['settings'])and equal(before['owned_log_and_media_files'],after['owned_log_and_media_files']),'NoSettingOrOwnedFileChangesByMetadataRequests');need(equal(before['system_notifications'],after['system_notifications'])and equal(before['notification_dismissals'],after['notification_dismissals']),'NoSetupNoticeGraphMutation');manifest_pair(before['public_manifest'],after['public_manifest'],changed,schema)
  for table,key in [(R,'repos'),(C,'configs')]:
   for value in [before,after]:need(value['public_manifest']['tables'][table]['count']==len(value[key]),'RawTargetRowCountFingerprintBinding')
  checks+=1
 state_continuity(previous,report['final_before_probe_cleanup'])
 return {'passed':True,'Source_requests':checks,'framework_APIkey_only':True,'Source_TCP_Native_Main_or_fullM8':False,'cache_and_Channels_scope':'decoded owned groups, unrelated raw keys/frames preserved, TTL monotonic; unexecuted predictions'}
