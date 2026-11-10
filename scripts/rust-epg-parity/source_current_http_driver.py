"""Original actual HTTP Parsing→index→programme reference for native driver bridge.
Original Dispatcharr authors retain all application/algorithm credit.
"""
import datetime as dt,hashlib,json,os,signal,subprocess
from pathlib import Path
import requests
import source_index_worker as f
from apps.accounts.models import User
from apps.epg.models import EPGSource,EPGSourceIndex,EPGData
from rest_framework_simplejwt.tokens import RefreshToken
report=f.report;report['passed']=False;process=None
try:
 # No worker is alive after the base corpus; this isolated broker belongs only
 # to this fixture, so remove its known prior protocol traffic before bridge.
 f.broker.flushdb();f.backend.flushdb()
 with Path('/evidence/current-http-original-web.log').open('ab') as log:
  process=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','127.0.0.1:5657','--module','dispatcharr.wsgi:application','--master','--workers','1','--threads','1','--die-on-term','--need-app'],env=dict(os.environ,PYTHONPATH='/app'),stdout=log,stderr=log,start_new_session=True)
 def ready():
  try:return requests.get('http://127.0.0.1:5657/api/core/version/',timeout=2).status_code==200
  except requests.RequestException:return False
 f.wait(ready,timeout=60);f.quarantine()
 now=dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
 path=f.root/'current-http.xml'
 path.write_text('<tv><programme channel="fixture&amp;one" start="'+(now-dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'" stop="'+(now+dt.timedelta(hours=1)).strftime('%Y%m%d%H%M%S +0000')+'"><title>Owned HTTP guide</title><desc>Original driver reference</desc></programme></tv>')
 source=EPGSource.objects.create(name='rust-current-http-driver-owned',source_type='xmltv',is_active=False,file_path=str(path));epg=EPGData.objects.create(name='rust-current-http-driver-owned',tvg_id='fixture&one',epg_source=source)
 user=User.objects.create(username='rust-current-http-driver-admin',password='!',is_active=True,is_staff=True,user_level=10)
 headers={'Authorization':'Bearer '+str(RefreshToken.for_user(user).access_token),'Accept':'application/json'}
 def read():
  response=requests.post('http://127.0.0.1:5657/api/epg/current-programs/',headers=headers,json={'epg_data_ids':[epg.pk]},timeout=15)
  if len(response.content)>16384:raise RuntimeError('Bounded current programme response required')
  return {'status':response.status_code,'mime':response.headers.get('Content-Type'),'body':response.json()}
 first=read();report['current_http_driver_first_observation']=first;f.check('original-http-missing-index-parsing',first['status']==200 and first['body']==[{'epg_data_id':epg.pk,'parsing':True}])
 f.quarantine()
 queued=f.broker.lrange('celery',0,63);envelopes=[json.loads(raw)for raw in queued];matching=[e for e in envelopes if e['headers']['task']==f.task.name];f.check('original-http-published-one-index',len(matching)==1 and len(queued)==1)
 f.task.run(source.pk);second=read();f.check('original-http-built-index-programme',second['status']==200 and len(second['body'])==1 and second['body'][0].get('title')=='Owned HTTP guide')
 report['current_http_driver']={'source_id':source.pk,'epg_entry_id':epg.pk,'user_id':user.pk,'file_path':str(path),'tvg_id':'fixture&one','clock':now.isoformat(),'guide_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'first':first,'second':second,'initial_envelope':matching[0]}
 EPGSourceIndex.objects.filter(source=source).delete();f.broker.flushdb();f.backend.flushdb();report['passed']=True
finally:
 if process is not None and process.poll() is None:
  os.killpg(process.pid,signal.SIGTERM)
  try:process.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);report['forced_web_cleanup']=True
 Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
