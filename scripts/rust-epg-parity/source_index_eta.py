"""Original ETA reservation and redelivery extension. All application credit
belongs to original Dispatcharr authors; copied DB and owned broker only.
"""
import hashlib,json,time
from pathlib import Path
import source_index_worker as f
from apps.epg.models import EPGSource,EPGSourceIndex
source=EPGSource(name='rust-index-eta-owned',source_type='xmltv',is_active=False,file_path=str(f.path));EPGSource.objects.bulk_create([source]);key='task_lock_epg_source_file_'+str(source.id);marker=b'rust-index-eta-owned';f.cache.set(key,marker,ex=300)
report=f.report;report['scope']='Original-only index ETA reservation/raw redelivery/forced-aged-score restore; no native ETA consumer or real visibility-expiry claim';observations=[];report['eta_observations']=observations

def queued(ident):
 rows=f.broker.lrange('celery',0,63)
 if len(rows)>=64:raise RuntimeError('Owned queue observation cap')
 for raw in rows:
  if len(raw)>65536:raise RuntimeError('Queue envelope cap')
  item=json.loads(raw)
  if item['headers']['id']==ident:return raw,item
 return None

def reserved(ident):
 values=f.broker.hgetall('unacked')
 if len(values)>64:raise RuntimeError('Owned reservation cap')
 for tag,raw in values.items():
  if len(raw)>131072 or len(tag)>128:raise RuntimeError('Reservation byte cap')
  item=json.loads(raw)
  if item[0]['headers']['id']==ident:
   return {'delivery_tag':tag.decode(),'raw_tuple':item,'raw_sha256':hashlib.sha256(raw).hexdigest(),'raw_bytes':len(raw),'score':f.broker.zscore('unacked_index',tag),'wall_seconds':time.time(),'types':{name:f.broker.type(name).decode()for name in ['unacked','unacked_index','celery']},'ttls':{name:f.broker.ttl(name)for name in ['unacked','unacked_index']}}
 return None

def child_of(parent):
 return next((row for row in f.rows('index-child-envelopes.jsonl')if row.get('observed')and row['observed']['envelope']['headers'].get('parent_id')==parent),None)

def held_child():
 parent=f.publish(source.id);f.launch();f.wait(lambda:f.finished(parent));child=f.wait(lambda:child_of(parent))['id'];reservation=f.wait(lambda:reserved(child),timeout=3)
 f.check('eta-reserved-before-execution-'+str(len(observations)),not f.finished(child)and not any(row['id']==child for row in f.rows('index-prerun.jsonl')),reservation=reservation)
 return parent,child,reservation
try:
 parent,warm,before=held_child();observations.append({'phase':'before-warm-stop','parent':parent,'child':warm,**before});f.stop();restored=f.wait(lambda:queued(warm),timeout=3)
 observations.append({'phase':'after-warm-restore','parent':parent,'child':warm,'envelope':restored[1],'raw_sha256':hashlib.sha256(restored[0]).hexdigest(),'wall_seconds':time.time(),'reservation_after':reserved(warm)})
 f.check('graceful-restore-removes-reservation',reserved(warm)is None)
 # Exact known queued child removed only to isolate the next experiment. This
 # is fixture cleanup, not a successful worker execution/result/ACK claim.
 removed=f.broker.lrem('celery',1,restored[0]);f.check('owned-warm-child-fixture-removal',removed==1)
 parent,hard,before=held_child();observations.append({'phase':'before-hard-stop','parent':parent,'child':hard,**before});f.stop(hard=True)
 remaining=reserved(hard);f.check('hard-stop-leaves-eta-reservation',remaining is not None and not f.finished(hard),reservation=remaining)
 observations.append({'phase':'after-hard-stop','parent':parent,'child':hard,**remaining})
 tag=remaining['delivery_tag'];old_score=remaining['score'];forced_score=time.time()-3605;f.broker.zadd('unacked_index',{tag:forced_score})
 # Run the unchanged original Kombu transport restoration implementation. Only
 # this owned reservation's score is aged; neither task ETA nor wall clock nor
 # the configured3600s visibility timeout is patched. No actual1h wait claim.
 with f.app.connection_for_read()as connection:
  channel=connection.channel()
  def restore():
   channel.qos.restore_visible(start=0,num=10,interval=1)
   return queued(hard)
  raw,item=f.wait(restore,timeout=3)
 observations.append({'phase':'fixture-forced-aged-score-restoration','parent':parent,'child':hard,'original_score':old_score,'forced_score':forced_score,'actual_visibility_timeout':f.app.conf.broker_transport_options.get('visibility_timeout'),'invocation':'Original Kombu channel.qos.restore_visible invoked by owned controller; not worker polling/real elapsed timeout','envelope':item,'raw_sha256':hashlib.sha256(raw).hexdigest(),'reservation_after':reserved(hard),'wall_seconds':time.time()})
 f.check('aged-original-transport-restores-exact-child',item['headers']['id']==hard and reserved(hard)is None)
 f.check('file-lock-still-owned-before-restart',f.cache.get(key)==marker);f.cache.delete(key);f.launch();f.wait(lambda:f.finished(hard),timeout=20)
 run=next(row for row in f.rows('index-prerun.jsonl')if row['id']==hard);result=f.result(hard)
 observations.append({'phase':'original-restarted-worker-consumption','prerun':run,'result':result,'reservation_after':reserved(hard)})
 f.check('restored-child-real-worker-completes',run['parent_id']==parent and run['root_id']==parent and result['status']=='SUCCESS'and result['result']is None and EPGSourceIndex.objects.filter(source_id=source.id).exists()and not f.cache.exists(key))
 f.check('no-unrelated-eta-task-executed',not f.rows('index-unexpected-tasks.jsonl'))
 report.update(passed=True,actual_visibility_expiry_observed=False,native_eta_consumer_verified=False)
finally:
 f.stop()
 if f.cache.get(key)==marker:f.cache.delete(key)
 report['observer']={name:f.rows(name)for name in ['index-prerun.jsonl','index-postrun.jsonl','index-child-envelopes.jsonl','index-unexpected-tasks.jsonl']}
 Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
