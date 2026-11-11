"""Typed native periodic publisher→actual Original worker→native result reader.
Original Dispatcharr/Celery/framework authors retain credit and licenses.
Only inactive refresh and Series0; copied DB and broker8/result9 remain guarded.
"""
import json,os,signal,subprocess,time,uuid
from pathlib import Path
import source_scheduler_worker as reference
assert reference.report['passed']
f=reference.f
report={'passed':False,'checks':list(reference.report['checks']),'cases':[],'scope':'Native known-periodic publisher/authority→actual Original inactive-refresh+Series0 worker→native normal result reader; no native scheduler timer/main authority'};worker=None;sources=[]
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def peer(data):
 p=subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','periodic_headers::peer::native_periodic_publisher_peer','--nocapture'],input=json.dumps(data)+'\n',capture_output=True,text=True,timeout=15)
 with Path('/evidence/native-periodic-publisher-private.log').open('a')as log:log.write(p.stdout+p.stderr)
 p.check_returncode();return json.loads(next(line.split('NATIVE_PERIODIC_PUBLISHER_JSON=',1)[1]for line in p.stdout.splitlines()if 'NATIVE_PERIODIC_PUBLISHER_JSON='in line))
def projection(envelope):
 v=json.loads(json.dumps(envelope));v['headers']['id']=v['headers']['root_id']=v['properties']['correlation_id']='<owned-task-id>';v['headers']['origin']='<runtime-origin>';v['properties']['reply_to']='<random-reply>';v['properties']['delivery_tag']='<random-delivery>';import base64;v['body']=json.loads(base64.b64decode(v['body']));return v
try:
 f.broker.flushdb();f.backend.flushdb();source_id=reference.report['cases'][0]['prerun']['args'][0];source=f.EPGSource(id=source_id,name='rust-owned-periodic-native-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 expected=[]
 for observed in reference.report['cases']:
  name=observed['name'];ident=str(uuid.uuid4());periodic=observed['envelope']['headers']['periodic_task_name'];data={'action':'publish','family':name,'task_id':ident,'source_id':source.pk,'periodic_name':periodic,'broker_url':os.environ['CELERY_BROKER_URL'],'result_url':os.environ['CELERY_RESULT_BACKEND']};audit=peer(data);check('native-'+name+'-publisher-process-drained',audit['passed']and audit['drained']);envelope=next(v for v in f.queue()if v['headers']['id']==ident);check('native-'+name+'-exact-observed-semantic-envelope',projection(envelope)==projection(observed['envelope']));expected.append((name,ident,periodic,data,envelope))
 check('native-authority-no-extra-publication',len(f.queue())==2)
 with Path('/evidence/periodic-source-worker-private.log').open('ab')as log:worker=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_worker_observer:app','worker','--pool=prefork','--concurrency=1','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=rust-periodic-native@fixture'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 for name,ident,periodic,data,envelope in expected:
  post=reference.wait(lambda:next((v for v in reference.rows('postrun')if v['id']==ident),None));prerun=next(v for v in reference.rows('prerun')if v['id']==ident);metadata=json.loads(f.backend.get(f.app.backend.get_key_for_task(ident)));check('native-'+name+'-actual-Source-worker-SUCCESS',post['state']=='SUCCESS'and prerun['headers']=={'periodic_task_name':periodic});data['action']='read';read=peer(data);check('native-'+name+'-result-read-and-drain',read['passed']and read['drained']and read['result']==metadata['result']);check('native-'+name+'-normal-typed-return',metadata['result']==(None if name=='refresh'else{'scheduled':0,'details':[]}));report['cases'].append({'name':name,'envelope':envelope,'prerun':prerun,'metadata':metadata,'reader':read})
 check('native-no-queue-reservation-residue',f.broker.llen('celery')==0 and f.broker.hlen('unacked')==0);report['passed']=True
finally:
 if worker is not None and worker.poll()is None:
  os.killpg(worker.pid,signal.SIGTERM)
  try:worker.wait(timeout=15)
  except subprocess.TimeoutExpired:os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-native-scheduler-periodic-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
