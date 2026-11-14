"""One actual pinned Original settings/backend connection; no Source API/setup."""
import json,os,sys,time
from pathlib import Path
start=time.monotonic();row={'case':os.environ['TLS_CASE'],'role_argv':sys.argv,'Source_API_calls':0,'django_setup_called':False}
try:
 from django.conf import settings
 config=settings.DATABASES['default'];row['engine']=config['ENGINE'];row['host']=config.get('HOST');row['options']={k:v for k,v in config.get('OPTIONS',{}).items() if k not in ['password']}
 from django.db import connections
 c=connections['default']
 with c.cursor() as q:
  if 'sqlite3' in config['ENGINE']:
   q.execute('SELECT sqlite_version()');row['sqlite_version']=q.fetchone()[0]
  else:
   q.execute("SELECT current_database(),current_user,inet_server_addr()::text,pg_backend_pid(),s.ssl,s.version,s.cipher,s.client_dn FROM pg_stat_ssl s WHERE s.pid=pg_backend_pid()")
   row['observation']=q.fetchone()
 c.close();row['connected']=True
except BaseException as e:
 row.update(connected=False,error={'type':type(e).__name__,'message':str(e)[:8192]})
row['elapsed_seconds']=time.monotonic()-start
print('TLS_RESULT='+json.dumps(row,allow_nan=False))
