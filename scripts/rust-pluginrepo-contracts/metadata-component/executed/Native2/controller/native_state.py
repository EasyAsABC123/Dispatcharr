"""Unexecuted raw owned PG/Redis Native24 observer/importer. No Django imports."""
import datetime,hashlib,json,os,re,types
from pathlib import Path
from public_manifest import manifest
from redis_channels import inventory
NAMED11=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
TABLES={'repos':'plugins_pluginrepo','configs':'plugins_pluginconfig','settings':'core_coresettings','users':'accounts_user','events':'core_systemevent','system_notifications':'core_systemnotification','notification_dismissals':'core_notificationdismissal'}
def need(x,label):
 if not x:raise ValueError(label)
def owned(name):need(type(name)is str and re.fullmatch(r'rust_probe_pluginrepo_[a-f0-9]{10}',name)is not None,'OwnedPluginRepoDatabase');return name
def quote(name):return '"'+name.replace('"','""')+'"'
def connect(database,readonly=True):
 import psycopg
 need(database=='dispatcharr'and readonly or owned(database)==database,'DefaultReadOnlyOrOwnedOnly')
 connection=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname=database,connect_timeout=3,options='-c statement_timeout=5000'+(' -c default_transaction_read_only=on'if readonly else''),autocommit=False)
 with connection.cursor()as cur:
  cur.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ'+(' READ ONLY'if readonly else''));cur.execute('SELECT current_database()');need(cur.fetchone()==(database,),'ActualDatabaseIdentity')
  if readonly:cur.execute('SHOW transaction_read_only');need(cur.fetchone()==('on',),'ActualReadOnlyOn')
 return connection
def adapter(connection):
 class Adapter:
  ops=types.SimpleNamespace(quote_name=quote)
  def cursor(self):return connection.cursor()
 return Adapter()
def rows(connection,table):
 need(table in TABLES.values(),'PinnedRawTableOnly')
 with connection.cursor()as cur:
  cur.execute('SELECT count(*),COALESCE(sum(octet_length(to_jsonb(t)::text)),0),COALESCE(max(octet_length(to_jsonb(t)::text)),0) FROM '+quote(table)+' t');count,total,largest=cur.fetchone();need(count<=10000 and total<=8388608 and largest<=131072,'RawRowsBeforeAllocationBound')
  cur.execute('SELECT to_jsonb(t)::text FROM '+quote(table)+' t ORDER BY id LIMIT 10001');value=[json.loads(r[0])for r in cur.fetchall()];need(len(value)==count,'RawRowsStableRepeatableRead');return value
def redis_clients():
 import redis
 return{n:redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=n,socket_connect_timeout=2,socket_timeout=2)for n in range(4)}
def state(database,schema):
 connection=connect(owned(database));clients=redis_clients()
 try:
  result={'public_manifest':manifest(adapter(connection),schema),**{key:rows(connection,table)for key,table in TABLES.items()},'raw_Redis':[inventory(clients[n],n)for n in range(4)],'capture_scope':'Raw PG6259 +literal selected rows/Redis0..3; no NativeChannels decode or Source app'}
  need(len(json.dumps(result).encode())<=16777216,'NativeState16MiB');return result
 finally:
  connection.close()
  for c in clients.values():c.close()
def default(database,schema):
 need(database=='dispatcharr','ExactDefaultOnly');c=connect(database)
 try:
  full=manifest(adapter(c),schema);need(len(full['tables'])==62 and len(full['sequences'])==59,'Full6259Default');named={'tables':{n:full['tables'][n]for n in NAMED11},'sequences':{n:full['sequences'][n+'_id_seq']for n in NAMED11},'scope':'Exact11-table/11-sequence subset derived from the SAME actual repeatable-read full62/59 manifest, not a second independent query algorithm'};return {'public62':full,'named11':named}
 finally:c.close()
def import_existing(database,source,schema):
 """Explicit Source existing-state fixture, NOT Native bootstrap/signals parity."""
 need(source['capture_complete']is True and source['actual_request_count']==2 and source['client_raise_request_exception']is False and source['Source_framework_APIClient_only']is True,'CapturedSource2RenderedOnly');seed=source['seed'];c=connect(owned(database),False);clients=redis_clients();receipt={'scope':'Imported Source existing-state lifecycle, not Native post-save/default graph','SQL_source_fields_imported':{},'Redis_source_existing_state_imported':[],'fresh_versions_generated':False,'warmed_groups_imported':False,'history_reset':False}
 try:
  for client in clients.values():need(client.dbsize()==0,'InitiallyEmptyOwnedRedisAll4')
  with c.cursor()as cur:
   for key in ['repos','configs','settings','users','events','system_notifications','notification_dismissals']:
    table=TABLES[key];cur.execute('SELECT count(*) FROM '+quote(table));need(cur.fetchone()==(0,),'InitiallyEmptyOwnedSelectedTables')
   # FK parents precede configs and dismissals. Exact scalar/JSON field names
   # are checked against actual SQL schema before jsonb_populate_record.
   for key in ['users','settings','repos','configs','events','system_notifications','notification_dismissals']:
    table=TABLES[key];items=seed[key];need(type(items)is list and len(items)<=1000,'FiniteImportedRows')
    cur.execute('SELECT column_name FROM information_schema.columns WHERE table_schema=\'public\' AND table_name=%s ORDER BY ordinal_position',(table,));columns=[r[0]for r in cur.fetchall()]
    for row in items:
     need(type(row)is dict and set(row)==set(columns),'ExactImportedSQLFieldSet:'+table)
     cur.execute('INSERT INTO '+quote(table)+' SELECT * FROM jsonb_populate_record(NULL::'+quote(table)+',%s::jsonb)',(json.dumps(row),))
    receipt['SQL_source_fields_imported'][table]={'rows':len(items),'source_rows_sha256':hashlib.sha256(json.dumps(items,separators=(',',':'),allow_nan=False).encode()).hexdigest()}
   sequences=seed['public_manifest']['sequences'];need(sorted(sequences)==schema['sequences']and len(sequences)==59,'ExactlyImported59ExistingSequenceStates')
   for name,pair in sequences.items():need(type(pair)is list and len(pair)==2 and type(pair[0])is int and pair[0]>=1 and type(pair[1])is bool,'TypedSequenceImport');cur.execute('SELECT setval(%s::regclass,%s,%s)',('public.'+name,pair[0],pair[1]))
  c.commit()
  allowed={(0,b':1:coresettings:group:stream_settings'),(1,b':1:coresettings:groupver:network_access'),(1,b':1:coresettings:groupver:system_settings'),(1,bytes.fromhex(source['probe_registration']['group_key_hex'])),(1,bytes.fromhex(source['probe_registration']['channel_key_hex']))};observed=set()
  for database_rows in seed['raw_Redis']:
   n=database_rows['database'];need(type(n)is int and n in range(4),'FiniteRedisDatabase')
   for row in database_rows['rows']:
    key=bytes.fromhex(row['key_hex']);observed.add((n,key));need((n,key)in allowed and row['type']in('string','zset'),'OnlyExactFiveDeclaredImportedKeys');ttl=row['pttl'];need(type(ttl)is int and(ttl==-1 or ttl>0),'ImportedTTLMeaning');dump=bytes.fromhex(row['dump_hex']);need(len(dump)<=65536,'ImportedDumpCapacity');clients[n].restore(key,0 if ttl==-1 else ttl,dump,replace=False);need(clients[n].dump(key)==dump and clients[n].type(key).decode()==row['type'],'ExactImportedDumpAndType');receipt['Redis_source_existing_state_imported'].append({'database':n,'key_hex':key.hex(),'dump_sha256':hashlib.sha256(dump).hexdigest(),'Source_remaining_TTL_ms':ttl,'Native_observed_TTL_ms':clients[n].pttl(key),'existing_state_not_new_version':True})
  need(observed==allowed and len(observed)==5,'AllFiveImportedKeysNoOthers')
  return receipt
 finally:
  c.close()
  for client in clients.values():client.close()
