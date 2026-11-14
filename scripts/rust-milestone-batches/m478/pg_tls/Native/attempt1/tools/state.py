import psycopg,json,hashlib
c=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname='dispatcharr',options='-c default_transaction_read_only=on -c statement_timeout=5000')
r={}
with c.cursor() as q:
 q.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename");tables=q.fetchall()
 for (t,) in tables:
  q.execute('SELECT count(*),md5(COALESCE(string_agg(s,E\'\\n\' ORDER BY s),\'\')) FROM (SELECT to_jsonb(t)::text s FROM '+psycopg.sql.Identifier(t).as_string(c)+' t) x');r[t]=q.fetchone()
 q.execute("SELECT sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename");seq=q.fetchall()
 for (t,) in seq:
  q.execute('SELECT last_value,is_called FROM '+psycopg.sql.Identifier(t).as_string(c));r['sequence:'+t]=q.fetchone()
c.close();import redis
z=redis.Redis(host='127.0.0.1',password='playback-fixture-only',db=7);r['redis_db7_keys']=z.dbsize();z.close()
print(json.dumps({'tables':len(tables),'sequences':len(seq),'state':r},sort_keys=True))
