"""Raw SQL/HTTP observer; no Original app imports."""
import json,os,time,urllib.request,hashlib,psycopg
from pathlib import Path
expected=os.environ['NATIVE_READY_EXPECTED']=='1';end=time.monotonic()+(8 if expected else 6.5);calls=[]
while True:
 try:
  with urllib.request.urlopen('http://127.0.0.1:9192/health/ready',timeout=.3)as response:
   calls.append({'status':response.status,'body':response.read(4096).decode()});break
 except BaseException as e:
  if time.monotonic()>=end:calls.append({'error':type(e).__name__,'message':str(e)[:512]});break
  time.sleep(.1)
c=psycopg.connect(host='/var/run/postgresql',user='dispatch',dbname='postgres',options='-c default_transaction_read_only=on -c statement_timeout=3000')
with c.cursor()as q:
 q.execute('SELECT a.pid,a.application_name,a.client_addr::text,s.ssl,s.version,s.cipher,s.client_dn FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.datname=%s ORDER BY a.pid',(os.environ['TARGET_DB'],));rows=q.fetchall()
c.close();p=Path('/native-data/dispatcharr.db')
print(json.dumps({'http':calls,'native_sessions':rows,'sqlite_exists':p.exists(),'sqlite_sha256':hashlib.sha256(p.read_bytes()).hexdigest()if p.exists()else None},allow_nan=False))
