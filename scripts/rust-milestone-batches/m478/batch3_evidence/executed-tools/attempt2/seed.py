"""Raw owned clone setup only, no Original imports or signals."""
import json,os
from pathlib import Path
from native_state import connect,owned,quote,redis_clients,need
name=owned(os.environ['POSTGRES_DB']);seed=json.loads(Path('/native-root/inputs/transport16-source.json').read_bytes())['seed'];c=connect(name,False)
try:
 with c.cursor() as cur:
  for key,table in [('users','accounts_user'),('settings','core_coresettings')]:
   cur.execute('SELECT count(*) FROM '+quote(table));before=cur.fetchone()[0];need((key=='users'and before==0)or key=='settings','CloneNoForeignUsers');cur.execute('DELETE FROM '+quote(table))
   for row in seed[key]:cur.execute('INSERT INTO '+quote(table)+' SELECT * FROM jsonb_populate_record(NULL::'+quote(table)+',%s::jsonb)',(json.dumps(row),))
   cur.execute('SELECT setval(%s::regclass,%s,true)',('public.'+table+'_id_seq',max(row['id']for row in seed[key])))
 c.commit()
finally:c.close()
clients=redis_clients()
for r in clients.values():need(r.dbsize()==0,'OwnedRedisInitiallyEmpty');r.close()
print('Owned raw cloned SQL users/settings seeded; Original functions never imported')
