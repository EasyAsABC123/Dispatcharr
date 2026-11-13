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
