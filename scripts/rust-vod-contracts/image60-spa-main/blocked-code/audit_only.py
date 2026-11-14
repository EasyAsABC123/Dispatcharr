"""Read-only full public SQL fingerprints; normal owned management shell only."""
import hashlib,json,os,re,sys
from pathlib import Path
if os.environ.get('SPA_FIXTURE_AUTHORITY')!='owned-native-spa-get-only':raise RuntimeError('OwnedSPAObserverOnly')
from django.db import connection,connections
from dispatcharr.app_initialization import should_skip_initialization
if not should_skip_initialization()or'shell'not in sys.argv:raise RuntimeError('ActualManagementSkipMarker')
name=os.environ['POSTGRES_DB'];label=os.environ['SPA_AUDIT_LABEL']
if label not in ['default-before','default-after','owned-before','owned-after']:raise RuntimeError('FixedAuditOnly')
if name!='dispatcharr'and re.fullmatch('rust_spa_[a-f0-9]{16}',name)is None:raise RuntimeError('ExactOwnedOrDefaultDB')
if name=='dispatcharr'and not label.startswith('default-'):raise RuntimeError('DefaultReadOnlyLabel')
if name!='dispatcharr'and not label.startswith('owned-'):raise RuntimeError('OwnedReadOnlyLabel')
if connection.settings_dict['HOST']!='/var/run/postgresql':raise RuntimeError('LocalUnixObserverOnly')
def capture():
 tables={};sequences={};used=0;builtin={};builtin_tables={'m3u_m3uaccount','m3u_m3uaccountprofile','django_celery_beat_periodictask','django_celery_beat_intervalschedule'}
 with connection.cursor()as cursor:
  cursor.execute('SET default_transaction_read_only=on');cursor.execute('SET statement_timeout=5000');cursor.execute('SELECT current_database()')
  if cursor.fetchone()[0]!=name:raise RuntimeError('ActualDatabaseBinding')
  cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename");names=[r[0]for r in cursor.fetchall()]
  if not 1<=len(names)<=512:raise ValueError('SQLTableCapacity')
  for table in names:
   if re.fullmatch('[a-z_][a-z_0-9]*',table)is None:raise ValueError('SQLIdentifier')
   cursor.execute('SELECT row_to_json(t) FROM public."'+table+'" t ORDER BY row_to_json(t)::text LIMIT 10001');rows=cursor.fetchall();raw=json.dumps(rows,sort_keys=True,default=str,ensure_ascii=False).encode();used+=len(raw)
   if len(rows)>10000 or len(raw)>2097152 or used>16777216:raise ValueError('OwnedSQLCapacity')
   tables[table]={'rows':len(rows),'sha256':hashlib.sha256(raw).hexdigest()}
   if table in builtin_tables:builtin[table]=rows
  cursor.execute("SELECT sequencename FROM pg_sequences WHERE schemaname='public' ORDER BY sequencename");names=[r[0]for r in cursor.fetchall()]
  if len(names)>512:raise ValueError('SequenceCapacity')
  for seq in names:
   if re.fullmatch('[a-z_][a-z_0-9]*',seq)is None:raise ValueError('SequenceIdentifier')
   cursor.execute('SELECT last_value,is_called FROM public."'+seq+'"');last,called=cursor.fetchone();sequences[seq]={'last_value':last,'is_called':called}
 context={'database':name,'SQL_read_only':True,'table_rows':builtin};path=Path('/evidence')/(label+'-builtin-context.json');path.write_text(json.dumps(context,indent=2,default=str)+'\n');path.chmod(0o644)
 return {'database':name,'tables':tables,'sequences':sequences}
try:
 value=capture();path=Path('/evidence')/(label+'.json');path.write_text(json.dumps(value,indent=2)+'\n');path.chmod(0o644);print(json.dumps({'audit':label,'tables':len(value['tables']),'sequences':len(value['sequences']),'SQL_connections_closed_in_finally':True}))
finally:connections.close_all()
