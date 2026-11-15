"""Raw SQL/HTTP observer only; never imports Original application code."""
import hashlib,json,os,time,urllib.request,urllib.error
from pathlib import Path
import psycopg
expected=os.environ['NATIVE_READY_EXPECTED']=='1';end=time.monotonic()+(8 if expected else 6.5);calls=[];attempted=0
while True:
 attempted+=1
 try:
  try:response=urllib.request.urlopen('http://127.0.0.1:9192/health/ready',timeout=.3)
  except urllib.error.HTTPError as error:response=error
  with response:
   body=response.read(4097)
   if len(body)>4096:raise ValueError('HealthResponse4KiB')
   calls.append({'status':response.status,'headers':list(response.headers.items()),'body_hex':body.hex(),'body_sha256':hashlib.sha256(body).hexdigest(),'body_bytes':len(body)})
   if response.status==200:break
 except (ConnectionError,TimeoutError,OSError) as error:
  if time.monotonic()>=end:calls.append({'error':type(error).__name__,'message':str(error)[:512]});break
 if time.monotonic()>=end:break
 time.sleep(.1)
c=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname='postgres',options='-c default_transaction_read_only=on -c statement_timeout=3000')
with c.cursor()as q:
 q.execute('SELECT a.pid,a.application_name,a.client_addr::text,s.ssl,s.version,s.cipher,s.client_dn FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.datname=%s ORDER BY a.pid',(os.environ['TARGET_DB'],));rows=[dict(zip(['pid','application_name','client_addr','ssl','version','cipher','client_dn'],row))for row in q.fetchall()]
c.close();p=Path('/native-data/dispatcharr.db')
print(json.dumps({'actual_HTTP_attempts':attempted,'HTTP_observations':calls,'native_sessions':rows,'sqlite_exists':p.exists(),'sqlite_sha256':hashlib.sha256(p.read_bytes()).hexdigest()if p.exists()else None,'Source_application_imports':0,'Source_driver_calls':0,'Source_API_calls':0},allow_nan=False))
