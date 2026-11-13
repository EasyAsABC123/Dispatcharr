"""Real Original Celery solo worker, owned queue; passive observers declared."""
import datetime,json,os,sys
from pathlib import Path
# Marker must exist before any application import/setup.
sys.argv.append('celery');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from backup_common import *
from celery import signals
from dispatcharr.app_initialization import should_skip_initialization
require(should_skip_initialization()is True,'RealCeleryStartupMarker');ownership();dependencies();app,config=configured_app(producer=False)
record={'pid':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'nonce':nonce(),'role':'backup-worker','configuration':config,'Original_receivers_before_passive_observers':signal_snapshot(),'passive_observers_only':True,'app_methods_patched':False,'consumed_task_ids':[]}
save(EVIDENCE/'worker-PID.json',record)
def observe_ready(sender=None,**kwargs):
 save(EVIDENCE/'worker-ready.json',{'nonce':nonce(),'receivers':signal_snapshot(),'locks':locks(),'snapshot':snapshot(),'consumer_queues':sorted(sender.app.amqp.queues.consume_from),'passive_observer':True})
def observe_prerun(task_id=None,task=None,**kwargs):
 record['consumed_task_ids'].append(task_id);require(len(record['consumed_task_ids'])<=1,'SingleConsumedTaskCapacity');save(EVIDENCE/'worker-prerun.json',{'observed_utc':datetime.datetime.now(datetime.UTC).isoformat(),'task_id':task_id,'task_name':task.name,'delivery_info':literal(task.request.delivery_info),'request':{'id':task.request.id,'root_id':task.request.root_id,'parent_id':task.request.parent_id,'retries':task.request.retries,'is_eager':task.request.is_eager},'locks':locks(),'snapshot':snapshot(),'passive_observer':True});save(EVIDENCE/'worker-PID.json',record)
def observe_postrun(task_id=None,task=None,retval=None,state=None,**kwargs):
 save(EVIDENCE/'worker-postrun.json',{'observed_utc':datetime.datetime.now(datetime.UTC).isoformat(),'task_id':task_id,'task_name':task.name,'state':state,'retval':literal(retval),'locks':locks(),'snapshot':snapshot(),'passive_observer':True})
# Source receivers are neither disconnected, replaced nor invoked by the fixture.
signals.worker_ready.connect(observe_ready,weak=False,dispatch_uid='rust-probe-backup-ready')
signals.task_prerun.connect(observe_prerun,weak=False,dispatch_uid='rust-probe-backup-prerun')
signals.task_postrun.connect(observe_postrun,weak=False,dispatch_uid='rust-probe-backup-postrun')
try:app.worker_main(['worker','--pool=solo','--concurrency=1','--prefetch-multiplier=1','--without-gossip','--without-mingle','--without-heartbeat','--queues=rust_probe_backupjob_'+nonce(),'--hostname=backup-fixture@'+nonce(),'--loglevel=INFO'])
finally:
 from django.db import connections
 last=snapshot();connections.close_all();save(EVIDENCE/'worker-returned.json',{'nonce':nonce(),'connections_close_called':True,'consumed_task_ids':record['consumed_task_ids'],'snapshot':last})
