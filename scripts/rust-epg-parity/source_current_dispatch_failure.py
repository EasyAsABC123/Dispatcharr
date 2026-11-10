"""Actual Original HTTP500 for missing-index dependency publication failure.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import base64,hashlib,json,os,signal,subprocess,time
from pathlib import Path
import source_index_worker as f
from apps.accounts.models import User
from apps.epg.models import EPGData,EPGSource,EPGSourceIndex
from rest_framework_simplejwt.tokens import RefreshToken
import requests
source=EPGSource(name='rust-current-dispatch-owned',source_type='xmltv',is_active=False,file_path=str(f.path));EPGSource.objects.bulk_create([source]);epg=EPGData.objects.create(name='rust-current-dispatch-owned',tvg_id='fixture&one',epg_source=source)
user=User.objects.create(username='rust-current-dispatch-admin',password='!',is_active=True,is_staff=True,user_level=10);token=str(RefreshToken.for_user(user).access_token)
report=f.report;report['passed']=False;report['scope']='Actual Original current-programme missing-index dependency-failure HTTP rendering; no native HTTP/application hook';process=None
try:
 with Path('/evidence/current-dispatch-web.log').open('ab')as log:
  process=subprocess.Popen(['/dispatcharrpy/bin/uwsgi','--http','127.0.0.1:5657','--module','current_dispatch_failure_wsgi:application','--master','--workers','1','--threads','1','--die-on-term','--need-app'],env=dict(os.environ,PYTHONPATH='/index-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 def ready():
  try:return requests.get('http://127.0.0.1:5657/api/core/version/',timeout=2).status_code==200
  except requests.RequestException:return False
 f.wait(ready,timeout=60);f.quarantine()
 response=requests.post('http://127.0.0.1:5657/api/epg/current-programs/',headers={'Authorization':'Bearer '+token,'Accept':'application/json'},json={'epg_data_ids':[epg.pk]},timeout=15)
 if len(response.content)>16384:raise RuntimeError('Bounded Source failure body expected')
 calls=[json.loads(line)for line in Path('/evidence/current-dispatch-attempts.jsonl').read_text().splitlines()]
 report['current_dispatch_failure']={'source_id':source.pk,'epg_entry_id':epg.pk,'user_id':user.pk,'file_path':str(f.path),'tvg_id':'fixture&one','source_type':'xmltv','status':response.status_code,'mime':response.headers.get('Content-Type'),'body_bytes':len(response.content),'body_sha256':hashlib.sha256(response.content).hexdigest(),'body_base64':base64.b64encode(response.content).decode(),'calls':calls,'index_exists':EPGSourceIndex.objects.filter(source_id=source.pk).exists(),'exception_class':'kombu.exceptions.OperationalError','fault_scope':'Controlled broker dependency failure only; unchanged Original WSGI/view/error renderer.'}
 f.check('original-dispatch-failure-http500-html',response.status_code==500 and response.headers.get('Content-Type')=='text/html; charset=utf-8')
 f.check('original-dispatch-attempt-no-parsing',len(calls)==1 and calls[0]['args']==[source.pk]and not EPGSourceIndex.objects.filter(source_id=source.pk).exists())
 malformed=[('malformed-root',[]),('malformed-channels',{'channels':None}),('negative-offset',{'channels':{'fixture&one':[-1]}}),('float-offset',{'channels':{'fixture&one':[0.5]}}),('null-offsets',{'channels':{'fixture&one':None}}),('empty-interleaved',{'channels':{'fixture&one':[]},'interleaved_channels':['fixture&one']})]
 report['malformed_index_http']=[]
 for name,index in malformed:
  EPGSourceIndex.objects.update_or_create(source=source,defaults={'data':index})
  response=requests.post('http://127.0.0.1:5657/api/epg/current-programs/',headers={'Authorization':'Bearer '+token,'Accept':'application/json'},json={'epg_data_ids':[epg.pk]},timeout=15)
  if len(response.content)>16384:raise RuntimeError('Bounded Source malformed-index error body expected')
  observed={'name':name,'index':index,'status':response.status_code,'mime':response.headers.get('Content-Type'),'body_bytes':len(response.content),'body_sha256':hashlib.sha256(response.content).hexdigest(),'body_base64':base64.b64encode(response.content).decode()}
  report['malformed_index_http'].append(observed)
  f.check('original-malformed-index-http-'+name,response.status_code==500 and observed['mime']=='text/html; charset=utf-8')
 EPGSourceIndex.objects.filter(source=source).delete()

 report['passed']=True
finally:
 if process is not None and process.poll()is None:
  os.killpg(process.pid,signal.SIGTERM)
  try:process.wait(timeout=10)
  except subprocess.TimeoutExpired:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5);report['forced_web_cleanup']=True
 Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
