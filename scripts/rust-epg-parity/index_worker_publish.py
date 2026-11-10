"""Short-lived original publisher; no worker/native code. Original authors credit."""
import argparse,json,os,sys
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_'):raise RuntimeError('Copied fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from apps.epg.tasks import build_programme_index_task as task
p=argparse.ArgumentParser();p.add_argument('--source',type=int);p.add_argument('--retry',type=int);p.add_argument('--inspect',action='store_true');p.add_argument('shell',nargs='?');a=p.parse_args()
if a.inspect:print(json.dumps({'name':task.name,'attrs':{key:getattr(task,key,None)for key in ['acks_late','ignore_result','track_started','max_retries','serializer','rate_limit','reject_on_worker_lost','acks_on_failure_or_timeout','time_limit','soft_time_limit']},'route':str(task.app.amqp.router.route({},task.name,args=[a.source],kwargs={}))},default=str))
else:
 result=task.delay(a.source)if a.retry is None else task.apply_async(args=[a.source],kwargs={'_defer_retry':a.retry})
 print(json.dumps({'id':result.id,'name':task.name}))
