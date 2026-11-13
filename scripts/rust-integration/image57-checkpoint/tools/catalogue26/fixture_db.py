"""Pinned Original migration and full SQL fingerprint only; no business action."""
import os,sys,re,json,hashlib
from pathlib import Path
def setup(action):
 global DB,connection,connections,call_command
 DB=os.environ.get('POSTGRES_DB','')
 if os.environ.get('SOURCE_CAPTURE_FIXTURE')!='playback-fixture-only' or os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('POSTGRES_HOST')not in ['127.0.0.1','localhost','/var/run/postgresql']:raise RuntimeError('DisposableOnly')
 if action=='default-audit':
  if DB!='dispatcharr':raise RuntimeError('DefaultReadOnlyOnly')
 elif action not in ['migrate','audit']or not re.fullmatch(r'rust_capture_[0-9a-f]{16}',DB):raise RuntimeError('ExactOwnedActionDB')
 pins=json.loads(Path('/capture/pinned-source.json').read_text())
 for name,expected in pins['files'].items():
  if hashlib.sha256((Path('/app')/name).read_bytes()).hexdigest()!=expected:raise RuntimeError('PinnedSource')
 sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
 from dispatcharr.app_initialization import should_skip_initialization
 if should_skip_initialization()is not True:raise RuntimeError('ManagementSkipBefore')
 import django;django.setup()
 if should_skip_initialization()is not True:raise RuntimeError('ManagementSkipAfter')
 from django.db import connection,connections
 from django.core.management import call_command
def fingerprint():
 tables={};seqs={};total=0
 with connection.cursor()as cur:
  cur.execute('SELECT current_database()');assert cur.fetchone()[0]==DB
  cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename");names=[r[0]for r in cur.fetchall()]
  if len(names)>512:raise ValueError('SQLTableCount')
  for name in names:
   if not re.fullmatch('[a-z_][a-z_0-9]*',name):raise ValueError('SQLIdentifier')
   cur.execute('SELECT row_to_json(t) FROM public."'+name+'" t ORDER BY row_to_json(t)::text LIMIT 10001');rows=cur.fetchall()
   if len(rows)>10000:raise ValueError('SQLRowCapacity')
   raw=json.dumps(rows,sort_keys=True,default=str,ensure_ascii=False).encode();total+=len(raw)
   if len(raw)>2097152 or total>16777216:raise ValueError('SQLByteCapacity')
   tables[name]={'rows':len(rows),'sha256':hashlib.sha256(raw).hexdigest()}
  cur.execute("SELECT sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename")
  for name, in cur.fetchall():
   if not re.fullmatch('[a-z_][a-z_0-9]*',name):raise ValueError('SQLSequenceIdentifier')
   cur.execute('SELECT last_value,is_called FROM public."'+name+'"');last,called=cur.fetchone();seqs[name]={'last_value':last,'is_called':called}
 expected=json.loads(Path('/capture/schema.json').read_text())
 if sorted(tables)!=expected['tables']or sorted(seqs)!=expected['sequences']:raise ValueError('ExactSource62Table59SequenceSchema')
 return {'database':DB,'tables':tables,'sequences':seqs}
def main():
 action=sys.argv[1]if len(sys.argv)==2 else'';setup(action)
 try:
  if action=='migrate':call_command('migrate',interactive=False,verbosity=0);print(json.dumps({'migrated':True,'database':DB}))
  else:print(json.dumps(fingerprint()))
 finally:connections.close_all()
if __name__=='__main__':main()
