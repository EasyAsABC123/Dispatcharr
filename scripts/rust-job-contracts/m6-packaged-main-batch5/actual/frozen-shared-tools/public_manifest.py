"""Bounded public-schema row/schema/sequence manifest; no app imports at module load.
Query algorithm derives from hash-bound Original backup_fixture.manifest.
"""
import json
from pathlib import Path

def require(ok,label):
 if not ok:raise ValueError(label)

def manifest(connection,expected):
 quote=connection.ops.quote_name;used=0
 with connection.cursor()as cursor:
  cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename LIMIT 63");names=[r[0]for r in cursor.fetchall()];require(names==expected['tables']and len(names)==62,'Exact62PublicTables');rows={}
  for name in names:
   require(name in expected['tables'],'ReviewedPublicIdentifier');table=quote(name)
   cursor.execute('SELECT count(*),COALESCE(sum(octet_length(to_jsonb(t)::text)),0),COALESCE(max(octet_length(to_jsonb(t)::text)),0) FROM '+table+' t');count,size,largest=cursor.fetchone();used+=size;require(type(count)is int and count<=10000 and size<=8388608 and largest<=131072 and used<=16777216,'Bounded62ManifestBeforeAggregation')
   cursor.execute("SELECT count(*),md5(COALESCE(jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)::text)::text,'[]')) FROM "+table+' t');observed,digest=cursor.fetchone();require(observed==count,'ManifestRowCountStableWithinReadOnlySnapshot');rows[name]={'count':count,'sha_like_md5':digest}
  cursor.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position LIMIT 502");columns=[list(r)for r in cursor.fetchall()]
  cursor.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' ORDER BY tablename,indexname LIMIT 256");indexes=[list(r)for r in cursor.fetchall()]
  cursor.execute("SELECT c.relname,k.conname,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY c.relname,k.conname LIMIT 199");constraints=[list(r)for r in cursor.fetchall()]
  require(columns==expected['columns']and indexes==expected['indexes']and constraints==expected['constraints'],'Exact501Columns255Indexes198Constraints')
  cursor.execute("SELECT sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename LIMIT 60");sequence_names=[r[0]for r in cursor.fetchall()];require(sequence_names==expected['sequences']and len(sequence_names)==59,'Exact59PublicSequences');sequences={}
  for name in sequence_names:
   cursor.execute('SELECT last_value,is_called FROM '+quote(name));sequences[name]=list(cursor.fetchone())
 value={'tables':rows,'columns':columns,'indexes':indexes,'constraints':constraints,'sequences':sequences};require(len(json.dumps(value).encode())<=2097152,'BoundedSchemaManifest2MiB')
 return value
