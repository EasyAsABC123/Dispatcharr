"""Owned raw SQL fixture seed. No Source model/app/signal execution claimed."""
import json,os
from pathlib import Path
import psycopg,redis
from audit import owned,require
name=owned(os.environ['POSTGRES_DB']);c=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname=name,connect_timeout=3,options='-c statement_timeout=5000');client=redis.Redis(host='127.0.0.1',port=6379,password='playback-fixture-only',db=1,socket_connect_timeout=2,socket_timeout=2)
try:
 with c.cursor()as cursor:
  cursor.execute('SELECT current_database()');require(cursor.fetchone()==(name,),'ActualOwnedSeedDatabase');cursor.execute('SELECT count(*) FROM accounts_user');require(cursor.fetchone()==(0,),'FreshNoUsers')
  cursor.execute("INSERT INTO accounts_user(id,password,is_superuser,username,first_name,last_name,email,is_staff,is_active,date_joined,user_level,custom_properties,api_key,stream_limit)VALUES(800001,'!unusable',false,'owned-rest-read-user','','','',false,true,'2025-01-01T00:00:00Z',10,'{}','owned-rest-read-key-not-production',0)")
  cursor.execute("SELECT value::text FROM core_coresettings WHERE key='network_access'");require(cursor.fetchone()==('{}',),'ExactInitialNativeNetworkObject')
 c.commit();version=json.loads(Path('/native-inputs/imported-existing-version.json').read_text());row=version['row'];require(version['database']==1 and row['pttl']==-1 and row['type']=='string'and bytes.fromhex(row['key_hex'])==b':1:coresettings:groupver:network_access','ExactImportedOneVersion');require(client.dbsize()==0,'InitiallyColdNoHistoryOrGroup');key=bytes.fromhex(row['key_hex']);client.restore(key,0,bytes.fromhex(row['dump_hex']),replace=False);require(client.dump(key).hex()==row['dump_hex']and client.get(key).hex()==row['value_hex'],'ExactSourceExistingVersionBytes');require(client.exists(b':1:coresettings:group:network_access')==0,'ColdNetworkGroup')
 Path('/evidence/seed-receipt.json').write_text(json.dumps({'passed':True,'scope':'Native raw seed and one imported existing Source version; no Native post-save claim','version':version,'SQL_id':800001,'no_history_reset':True},indent=2)+'\n')
finally:c.close();client.close()
