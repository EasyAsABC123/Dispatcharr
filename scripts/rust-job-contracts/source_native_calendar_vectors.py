"""Pinned Original calendar vectors/query offsets vs bounded native pure codec.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
This controller proves only calculation; DB tick/worker proofs are separate.
"""
import json,os,select,signal,subprocess,time
from pathlib import Path
import source_scheduler_calendar as reference
assert reference.report['passed'];f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'vectors':[],'scope':'Actual Original calendar11+ZoneInfo/DB/Beat references and bounded native pure crontab/TZif2026e vector/query-offset comparison; native calendar tick/main not enabled'}
process=None;buffer=b''
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def request(value):
 global buffer
 process.stdin.write((json.dumps(value)+'\n').encode());process.stdin.flush();deadline=time.monotonic()+20
 while time.monotonic()<deadline:
  while b'\n'in buffer:
   line,buffer=buffer.split(b'\n',1)
   if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):return json.loads(line.split(b'=',1)[1])
  ready,_,_=select.select([process.stdout],[],[],.1)
  if ready:
   data=os.read(process.stdout.fileno(),65536)
   if not data:raise RuntimeError('Native calendar peer exited')
   buffer+=data
   if len(buffer)>2*1024*1024:raise RuntimeError('Response bound')
 raise TimeoutError('Native calendar command')
try:
 f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed()
 with Path('/evidence/native-calendar-stderr-private.log').open('ab')as log:process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database),stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,start_new_session=True)
 result=request({'action':'setup'});check('native-calendar-peer-defaults-prepared',len(result['installed'])==6 and not result['failures'])
 for vector in reference.report['vectors']:
  spec=vector['input'];actual=request({'action':'calendar-evaluate','mode':vector['mode'],**{k:spec[k]for k in ['minute','hour','day_of_week','day_of_month','month_of_year','timezone','last_run_at','now']}});expected=vector['actual'];check('native-'+vector['mode']+'-'+spec['name']+'-exact-due-next',{'due':actual['due'],'next_seconds':actual['next_seconds']}==expected);check('native-'+vector['mode']+'-'+spec['name']+'-exact-query-offset',actual['query_hour_offset_2023']==reference.report['query_offsets_2023'][spec['timezone']]);report['vectors'].append({'mode':vector['mode'],'input':spec,'actual':actual})
 result=request({'action':'shutdown'});check('native-calendar-peer-actual-drain',result['drained']);process.stdin.close();process.wait(timeout=10);check('native-calendar-peer-exit0',process.returncode==0);process=None;report['passed']=True
finally:
 if process is not None and process.poll()is None:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 report['tzdb']=reference.report.get('tzdb');Path('/evidence/source-native-calendar-vectors-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
