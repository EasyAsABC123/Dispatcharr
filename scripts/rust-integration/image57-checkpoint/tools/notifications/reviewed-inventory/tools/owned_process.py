"""UNEXECUTED exact owned container PID/starttime launcher and stopper."""
import json,os,re,signal,sys
from fixture_guards import stop_record
from pathlib import Path
assert os.environ.get('PENDING_MAIN_FIXTURE')=='playback-fixture-only'
role=sys.argv[2];assert re.fullmatch(r'[a-z-]{1,32}',role);path=Path('/evidence/owned-'+role+'.json');nonce=os.environ['PENDING_MAIN_NONCE']
def starttime(pid):
 raw=Path('/proc',str(pid),'stat').read_text();return raw[raw.rfind(')')+2:].split()[19]
if sys.argv[1]=='start':
 path.write_text(json.dumps({'pid':os.getpid(),'starttime':starttime(os.getpid()),'nonce':nonce,'role':role}));os.execvp(sys.argv[3],sys.argv[3:])
elif sys.argv[1]=='stop':
 row=json.loads(path.read_text());assert row['nonce']==nonce and row['role']==role
 try:
  pid=stop_record(row,role,nonce,starttime)
  os.kill(pid,signal.SIGTERM)
 except FileNotFoundError:pass
else:raise RuntimeError('Unknown process action')
