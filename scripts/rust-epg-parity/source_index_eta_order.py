"""Original multi-message restore lists and future-ETA prefetch observation.
Original Dispatcharr/Celery/Kombu authors retain algorithm and application credit.
Only owned brokerDB8/copyDB/files are used. Future180s is a transport fixture,
not a claim that the index wrapper's observed five-second deferral changed.
"""
import datetime,json,time,uuid
from pathlib import Path
import source_index_worker as f
report=f.report;report['passed']=False;report['scope']='Original restore queue ends/order and actual future-ETA versus active prefetch; no native implementation acceptance';observations=[];report['ordering_observations']=observations
serial=0

def publish(label,eta=True):
 global serial
 serial+=1;ident=str(uuid.UUID(int=0x40008000000000000000000000000000+serial));parent=str(uuid.uuid4())
 kwargs={'task_id':ident}
 if eta:kwargs.update(countdown=180,root_id=parent,parent_id=parent)
 f.task.apply_async(args=[f.source.id],kwargs={'_defer_retry':1}if eta else{},**kwargs)
 labels[ident]=label
 return ident

def queue_rows():
 values=f.broker.lrange('celery',0,64)
 if len(values)>64:raise RuntimeError('Queue observation cap')
 rows=[]
 for raw in values:
  if len(raw)>65536:raise RuntimeError('Queue frame cap')
  message=json.loads(raw);ident=message['headers']['id'];rows.append({'label':labels.get(ident,'UNEXPECTED'),'envelope':message})
 return rows

def pop_all():
 order=[]
 for _ in range(64):
  raw=f.broker.rpop('celery')
  if raw is None:return order
  message=json.loads(raw);order.append(labels.get(message['headers']['id'],'UNEXPECTED'))
 raise RuntimeError('Pop bound')

def reserve(channel):
 raw=f.broker.rpop('celery');payload=json.loads(raw);message=channel.message_to_python(payload);channel.qos.append(message,message.delivery_tag);return message.delivery_tag

def reset():
 if f.broker.llen('celery')or f.broker.hlen('unacked')or f.broker.zcard('unacked_index'):raise RuntimeError('Ordering phase not empty')

labels={}
try:
 for mode in ['warm','visible','visible-tie']:
  reset();a=publish('A');b=publish('B')
  with f.app.connection_for_read()as connection:
   channel=connection.channel();tags=[reserve(channel),reserve(channel)]
   f.check('actual-reservation-insertion-'+mode,[labels[channel.qos._delivered[tag].headers['id']]for tag in channel.qos._delivered]==['A','B'])
   publish('C',False);publish('D',False);before=queue_rows();scores=None
   if mode=='warm':channel.qos.restore_unacked()
   else:
    base=time.time()-4000;scores={tags[0]:base,tags[1]:base if mode=='visible-tie'else base+100};f.broker.zadd('unacked_index',scores);channel.qos.restore_visible(start=0,num=10,interval=1)
   after=queue_rows();order=pop_all();observations.append({'mode':mode,'fixture_eta_seconds':180,'before_queue_left_to_right':before,'delivered_insertion_labels':['A','B'],'reservation_tags':tags,'forced_expired_scores':scores,'after_queue_left_to_right':after,'actual_rpop_delivery_order':order,'hash_remaining':f.broker.hlen('unacked'),'zset_remaining':f.broker.zcard('unacked_index')})
   expected=(['B','A','C','D']if mode=='warm'else [label for _,label in sorted(zip(tags,['A','B']))]+['C','D']if mode=='visible-tie'else['A','B','C','D'])
   f.check('original-list-order-'+mode,order==expected and f.broker.hlen('unacked')==0 and f.broker.zcard('unacked_index')==0,observed=order)
 # Actual original worker: future ETA reservations increase QoS and are not
 # bounded by its ordinary concurrency1*prefetch_multiplier4 active limit.
 reset();ids=[publish('ETA-'+str(i))for i in range(8)];f.launch();f.wait(lambda:f.broker.hlen('unacked')==8,timeout=8)
 raw=f.broker.hgetall('unacked');reservations=[]
 for tag,value in raw.items():
  if len(value)>131072:raise RuntimeError('Reservation frame cap')
  message=json.loads(value)[0];reservations.append({'tag':tag.decode(),'label':labels[message['headers']['id']],'tuple':json.loads(value),'bytes':len(value),'score':f.broker.zscore('unacked_index',tag)})
 stats=f.app.control.inspect(timeout=2).stats()or{};counts=[value.get('prefetch_count')for value in stats.values()];f.check('eta-exceeds-active-prefetch-without-execution',f.broker.hlen('unacked')==8 and not any(row['id']in ids for row in f.rows('index-prerun.jsonl')),reserved=8,configured_active_prefetch=4,actual_stats_prefetch_counts=counts)
 f.stop();after=queue_rows();order=pop_all();observations.append({'mode':'real-worker-eight-future-eta-warm-stop','fixture_eta_seconds':180,'reservations':reservations,'configured_active_prefetch':4,'actual_stats_prefetch_counts':counts,'after_queue_left_to_right':after,'actual_rpop_delivery_order':order})
 f.check('real-worker-warm-restore-retains-all-eight',len(after)==8 and set(order)=={'ETA-'+str(i)for i in range(8)}and f.broker.hlen('unacked')==0 and f.broker.zcard('unacked_index')==0)
 f.check('no-unrelated-ordering-task-executed',not f.rows('index-unexpected-tasks.jsonl'))
 report.update(passed=True,actual_visibility_expiry_observed=False,native_eta_consumer_verified=False)
finally:
 f.stop();report['observer']={name:f.rows(name)for name in ['index-prerun.jsonl','index-postrun.jsonl','index-child-envelopes.jsonl','index-unexpected-tasks.jsonl']};Path('/evidence/source-index-worker-private.json').write_text(json.dumps(report,indent=2,default=str))
