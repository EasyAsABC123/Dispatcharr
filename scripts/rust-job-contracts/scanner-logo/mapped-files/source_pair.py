"""Actual pinned Original logo file scan; separately observed real descendants.
Owned disposable volume/DB only. No provider/network/file content processing.
Original Dispatcharr/Django/Celery/Channels/Redis authors retain credit/licenses.
"""
import base64,hashlib,inspect,json,os,signal,subprocess,sys,time,uuid
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app','/pair-tools'];sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
import redis
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import connection
from django.db.models import Q
from celery.signals import before_task_publish
from dispatcharr.celery import app
from apps.channels.models import Logo
from apps.epg.models import EPGSource,EPGSourceIndex
from core.utils import send_websocket_update
import core.tasks as source
NOW=1700000000.125
TASK='core.tasks.scan_and_process_files';PARENT_QUEUE='rust-owned-scanner-parents';ROOT=Path('/data/logos')
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);cache=redis.Redis(host='localhost',port=6379,password='playback-fixture-only',db=0)
assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
assert broker.dbsize()==backend.dbsize()==0
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:cache.set(key,'1',ex=3600)
app.loader.import_default_modules();layer=get_channel_layer();channel='specific.rust_owned_scanner!receiver';channel_key=layer.prefix+channel.split('!')[0]+'!';group_key=layer._group_key('updates');sentinel='rust:owned:scanner:sentinel';cache.set(sentinel,'preserve',ex=3600)
report={'previous_predeclared_reference_only':['ordered-first-index','same-process-no-repeat','restart-repeat','second-index-LPUSH-denied','same-process-after-caught-publication-failure','helper-server-P0001','same-process-after-caught-SQL-failure','259-files-walk','259-files-one-caught-SQL-error'],'passed':False,'native_verified':False,'checks':[],'cases':[],'scope':'Actual unchanged Original scanner logo rows/markers/events/partial errors; separately observed unconsumed Index children with real default routing. Parent queue is an explicit fixture control. No child execution or native/whole-family claim.','fixed_scanner_clock':NOW};worker=None;made=set();owned_markers=set();published={}
@before_task_publish.connect
def parent_publication(sender=None,body=None,headers=None,properties=None,exchange=None,routing_key=None,**extra):
 if sender==TASK:published[headers['id']]={'headers':headers,'body':body,'properties':properties,'exchange':exchange,'routing_key':routing_key}
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def records(kind):
 p=Path('/evidence/periodic-worker-'+kind+'.jsonl');return [json.loads(v)for v in p.read_text().splitlines()]if p.exists()else[]
def wait(fn):
 end=time.monotonic()+40
 while time.monotonic()<end:
  result=fn()
  if result:return result
  if worker and worker.poll()is not None:raise RuntimeError('Owned worker exited')
  time.sleep(.025)
 raise TimeoutError('Owned scanner task deadline')
_native_buffer=b''
def native_response():
 global _native_buffer
 import select
 end=time.monotonic()+40
 while True:
  if b'\n'in _native_buffer:
   line,_sep,_native_buffer=_native_buffer.partition(b'\n')
   if line.startswith(b'NATIVE_SCANNER_FILES_JSON='):return json.loads(line.split(b'=',1)[1])
   continue
  remaining=end-time.monotonic()
  if remaining<=0 or not select.select([worker.stdout],[],[],remaining)[0]:raise TimeoutError('Native mapped scanner output')
  chunk=os.read(worker.stdout.fileno(),8192)
  if not chunk:raise RuntimeError('Native mapped scanner EOF')
  _native_buffer+=chunk
  if len(_native_buffer)>65536:raise ValueError('Native diagnostic response bound')
def native_send(value):worker.stdin.write(json.dumps(value)+'\n');worker.stdin.flush()
def start():
 global _native_buffer
 _native_buffer=b''
 log=open('/evidence/native-scanner-private.log','a');p=subprocess.Popen(['/native-task-peer','scanner_files_peer::native_scanner_files_peer','--ignored','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,text=True,start_new_session=True);log.close();return p
def stop():
 global worker
 if worker and worker.poll()is None:
  try:native_send({'action':'shutdown'});check('actual-native-owned-drain',native_response()['drained']);worker.stdin.close();check('native-exit0',worker.wait(timeout=10)==0)
  except Exception:
   os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=5);report['forced_cleanup']=True;raise
 worker=None
def drop_trigger():
 with connection.cursor()as c:c.execute('DROP TRIGGER IF EXISTS rust_owned_logo_fail ON dispatcharr_channels_logo');c.execute('DROP FUNCTION IF EXISTS rust_owned_logo_fail()')
def rows():return list(Logo.objects.order_by('id').values('id','name','url'))
def files():
 return [{'path':str(p),'bytes':p.stat().st_size,'mtime_ns':p.stat().st_mtime_ns,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in sorted(made)if p.exists()]
def markers():
 return {key:{'value_base64':None if cache.get(key)is None else base64.b64encode(cache.get(key)).decode(),'value_ascii':None if cache.get(key)is None else cache.get(key).decode('ascii'),'ttl':cache.ttl(key)}for key in sorted(owned_markers)}
def queued():
 result=[]
 for raw in broker.lrange('celery',0,127):
  msg=json.loads(raw);result.append({'raw_base64':base64.b64encode(raw).decode(),'sha256':hashlib.sha256(raw).hexdigest(),'message':msg,'body':json.loads(base64.b64decode(msg['body']))})
 return result
def reset():
 drop_trigger()
 for p in made:
  if p.exists():p.unlink()
 made.clear()
 for key in owned_markers:cache.delete(key)
 Logo.objects.filter(url__startswith=str(ROOT)+'/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub1/rust-owned-').delete();Logo.objects.filter(url__startswith=str(ROOT)+'/sub2/rust-owned-').delete()
 cache.delete(channel_key,group_key);async_to_sync(layer.group_add)('updates',channel)
def make(name,age):
 p=ROOT/name;p.parent.mkdir(parents=True,exist_ok=True);assert not p.exists();p.write_bytes(b'owned synthetic metadata-only logo fixture\n');os.utime(p,(NOW-age,NOW-age));made.add(p);owned_markers.add('processed_file:'+str(p));return p
def execute(label,expected_counts=None,acl=None,expected_failure=False,extra=None):
 before={'rows':rows(),'files':files(),'markers':markers(),'queue':queued()};taskid=str(uuid.uuid4());result=app.tasks[TASK].apply_async(args=[],kwargs={},task_id=taskid,queue=PARENT_QUEUE)
 native_send({'now':NOW,**({'marker_acl_user':acl}if acl else{})});check(label+'-actual-native-prepared',native_response()['prepared'])
 denied=bool(extra and extra.get('deny_child_task'))
 try:
  if denied:cache.execute_command('ACL','SETUSER','default','-lpush')
  native_send({'action':'run'});completion=native_response();Path('/evidence/last-native-completion.json').write_text(json.dumps(completion,indent=2))
 finally:
  if denied:cache.execute_command('ACL','SETUSER','default','+lpush')
 check(label+'-worker-terminal-run',completion['run']==('Ok(Failed)'if expected_failure else'Ok(Succeeded)'))
 wait(lambda:result.ready());metadata=json.loads(backend.get(app.backend.get_key_for_task(taskid)));events=[layer.deserialize(v)for v in cache.zrange(channel_key,0,15)];after={'rows':rows(),'files':files(),'markers':markers(),'queue':queued()}
 before_hash={v['sha256']for v in before['queue']};new_frames=list(reversed([v for v in after['queue']if v['sha256']not in before_hash]));attempts=completion['attempts'];attempts.extend(v['message']for v in new_frames if v['message']['headers']['task']=='apps.epg.tasks.build_programme_index_task')
 children=[{'task':v['headers']['task'],'headers':v['headers'],'body':json.loads(base64.b64decode(v['body'])),'properties':{'correlation_id':v['properties']['correlation_id'],'reply_to':v['properties']['reply_to']},'exchange':v['properties']['delivery_info']['exchange'],'routing_key':v['properties']['delivery_info']['routing_key']}for v in attempts]
 case={'name':label,'parent_id':taskid,'parent_publication':published[taskid],'before':before,'after':after,'native_completion':completion,'metadata':metadata,'events':events,'children':children};report['cases'].append(case)
 check(label+'-actual-result',metadata['status']==('FAILURE'if expected_failure else'SUCCESS'))
 if not expected_failure:check(label+'-typed-null',metadata['result']is None and metadata['traceback']is None and result.get(timeout=2)is None)
 else:
  expected=next(v for v in reference['cases']if v['name']==label)['metadata']['result'];check(label+'-actual-Source-failure-fields-exact',metadata['result']==expected)
  try:result.get(timeout=2)
  except Exception as error:check(label+'-actual-Source-decoder-class-args',error.__class__.__name__==expected['exc_type']and list(error.args)==expected['exc_message'])
  else:raise AssertionError('Expected actual Source exception')
  check(label+'-honest-native-diagnostic',metadata['traceback'].startswith('Native Dispatcharr mapped scanner diagnostic')and 'Traceback (most recent call last)'not in metadata['traceback'])
 check(label+'-unchanged-owned-files',before['files']==after['files']);check(label+'-earlyACK-and-no-child-consumption',broker.llen(PARENT_QUEUE)==broker.hlen('unacked')==0 and all(backend.get(app.backend.get_key_for_task(v['headers']['id']))is None for v in children));check(label+'-result-ttl',3580<=backend.ttl(app.backend.get_key_for_task(taskid))<=3600);check(label+'-sentinel',cache.get(sentinel)==b'preserve'and cache.ttl(sentinel)>3300)
 if children:check(label+'-shared-child-reply-origin',len({v['properties']['reply_to']for v in children})==1 and len({v['headers']['origin']for v in children})==1)
 return case
from apps.m3u.models import M3UAccount,M3UAccountProfile
from core.models import CoreSettings,SYSTEM_SETTINGS_KEY
from django_celery_beat.models import PeriodicTask,PeriodicTasks,IntervalSchedule
reference=json.loads(Path('/evidence/reference.json').read_text());assert reference['passed']and len(reference['checks'])==217
report['scope']='Actual Source producer/native normal mapped-file worker/Original result consumer against frozen217 reference; explicit parent-only route preserves all descendants; no childexecution/Main/wholefamily.'
report['predeclared_cases']=['bool-false','bool-true','string-true','string-True','string-TRUE','numeric-one','null','existing-inactive-old','existing-inactive-current','existing-active-current','epg-insert-P0001','m3u-insert-P0001-stops-before-epg','epg-marker-SET-denied','m3u-marker-SET-denied-stops-before-epg','epg-signal-LPUSH-denied','m3u-signal-LPUSH-denied-stops-before-epg','case-sensitive-extensions']
M3U_ROOT=Path('/data/m3us');EPG_ROOT=Path('/data/epgs')
def drop_branch_triggers():
 with connection.cursor()as c:
  for table in ['m3u_m3uaccount','epg_epgsource']:c.execute('DROP TRIGGER IF EXISTS rust_owned_branch_insert_fail ON '+table)
  c.execute('DROP FUNCTION IF EXISTS rust_owned_branch_insert_fail()')
def reset_branch(value):
 global worker
 stop();drop_branch_triggers();reset();EPGSource.objects.all().update(is_active=False);M3UAccount.objects.all().update(is_active=False);CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':value});cache.delete(channel_key);worker=start()
def watch_file(root,name,age=7.125):
 p=root/name;assert not p.exists();p.write_text('#EXTM3U\n'if root==M3U_ROOT else'<tv></tv>\n');os.utime(p,(NOW-age,NOW-age));made.add(p);owned_markers.add('processed_file:'+str(p));return p
def own_rows(m=None,e=None):
 accounts=[]if m is None else list(M3UAccount.objects.filter(file_path=str(m)).values());sources=[]if e is None else list(EPGSource.objects.filter(file_path=str(e)).values());ids=[v['id']for v in accounts];tasks=[v['refresh_task_id']for v in accounts+sources if v['refresh_task_id']]
 profiles=list(M3UAccountProfile.objects.filter(m3u_account_id__in=ids).values());periodic=list(PeriodicTask.objects.filter(pk__in=tasks).values('id','name','task','args','kwargs','enabled','interval_id','crontab_id','one_off'));intervals=list(IntervalSchedule.objects.filter(pk__in=[v['interval_id']for v in periodic]).values('id','every','period'))
 return {'accounts':accounts,'sources':sources,'profiles':profiles,'periodic':periodic,'intervals':intervals,'sentinel':str(PeriodicTasks.last_change())}
def branch_case(label,m=None,e=None,**kwargs):
 before=own_rows(m,e);case=execute(label,expected_counts=None,**kwargs);case['branch_before']=before;case['branch_after']=own_rows(m,e);case['configured_auto_import_value']=CoreSettings.get_auto_import_mapped_files();expected=next(v for v in reference['cases']if v['name']==label)
 for table in ['accounts','sources','profiles','periodic','intervals']:
  actual_rows=case['branch_after'][table];expected_rows=expected['branch_after'][table];check(label+'-'+table+'-row-count',len(actual_rows)==len(expected_rows))
  for actual_row,expected_row in zip(actual_rows,expected_rows):
   check(label+'-'+table+'-field-presence',set(actual_row)==set(expected_row))
   check(label+'-'+table+'-all-stable-fields-including-ID',all(actual_row[key]==expected_row[key]for key in expected_row if key!='created_at'))
   if 'created_at'in actual_row:
    from datetime import datetime
    check(label+'-'+table+'-real-created-at',actual_row['created_at']is not None and datetime.fromisoformat(str(actual_row['created_at'])).tzinfo is not None)
 check(label+'-sentinel-change-relation',(case['branch_before']['sentinel']!=case['branch_after']['sentinel'])==(expected['branch_before']['sentinel']!=expected['branch_after']['sentinel']))
 check(label+'-Source-result-status',case['metadata']['status']==expected['metadata']['status']);check(label+'-Source-attempted-child-order',child_tasks(case)==child_tasks(expected))
 return case
def durable_child_ids(case):return [v[0][0]for v in case['metadata']['children']]
def child_tasks(case):return [v['task']for v in case['children']]
def actual_new_frames(case):
 before={v['sha256']for v in case['before']['queue']};return list(reversed([v for v in case['after']['queue']if v['sha256']not in before]))
def check_committed(case):
 frames=actual_new_frames(case);ids=[v['message']['headers']['id']for v in frames];check(case['name']+'-parent-metadata-only-durable-children',durable_child_ids(case)==ids);check(case['name']+'-prior-children-preserved',all(v['sha256']in[x['sha256']for x in case['after']['queue']]for v in case['before']['queue']));return frames
def combined(label,value):
 reset_branch(value);m=watch_file(M3U_ROOT,'rust-owned-branch-'+label+'.m3u');e=watch_file(EPG_ROOT,'rust-owned-branch-'+label+'.xml');case=branch_case(label,m,e);active=value in[True,'true','True'];a=case['branch_after']['accounts'];s=case['branch_after']['sources'];check(label+'-both-created-default-fields',len(a)==len(s)==1 and a[0]['name']==m.name and s[0]['name']==e.name and s[0]['source_type']=='xmltv'and a[0]['is_active']is active and s[0]['is_active']is active);profiles=case['branch_after']['profiles'];check(label+'-inactive-still-default-active-profile',len(profiles)==1 and profiles[0]['is_default']is True and profiles[0]['is_active']is True and profiles[0]['name']==m.name+' Default');check(label+'-both-markers-committed',all(cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode()for p in[m,e]));check(label+'-both-marker-ttl',all(259100<=cache.ttl('processed_file:'+str(p))<=259200 for p in[m,e]));expected_events=[{'type':'update','data':{'success':True,'type':'m3u_file','filename':m.name}}]if active else[];check(label+'-exact-m3u-event',[{k:v for k,v in event.items()if k!='__asgi_channel__'}for event in case['events']]==expected_events);expected=['apps.m3u.tasks.refresh_m3u_groups']+(['apps.m3u.tasks.refresh_single_m3u_account','apps.epg.tasks.refresh_epg_data','apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task']if active else[]);check(label+'-exact-signal-scanner-helper-child-order',child_tasks(case)==expected);check(label+'-disabled-hour1-default-schedules',len(case['branch_after']['periodic'])==2 and all(not v['enabled']for v in case['branch_after']['periodic'])and all(v['every']==1 and v['period']=='hours'for v in case['branch_after']['intervals']));frames=check_committed(case);check(label+'-all-lineage-and-real-default-route',all(v['message']['headers']['root_id']==v['message']['headers']['parent_id']==case['parent_id']and v['message']['properties']['delivery_info']=={'exchange':'','routing_key':'celery'}for v in frames));return m,e,case
try:
 for root in [M3U_ROOT,EPG_ROOT,ROOT]:root.mkdir(exist_ok=True);check('fresh-empty-owned-'+root.name,not list(root.iterdir()))
 for label,obj in [('scanner',source.scan_and_process_files.run),('first-index-helper',source._rebuild_programme_indices)]:Path('/evidence/original-'+label+'.py').write_text(inspect.getsource(obj))
 import apps.m3u.signals as ms,apps.epg.signals as es
 for label,obj in [('m3u-save',M3UAccount.save),('m3u-default-profile',__import__('apps.m3u.models',fromlist=['create_profile_for_m3u_account']).create_profile_for_m3u_account),('m3u-refresh-signal',ms.refresh_account_on_save),('m3u-schedule-signal',ms.create_or_update_refresh_task),('epg-refresh-signal',es.trigger_refresh_on_new_epg_source),('epg-schedule-signal',es.create_or_update_refresh_task)]:Path('/evidence/original-'+label+'.py').write_text(inspect.getsource(obj))
 for label,value in [('bool-false',False),('bool-true',True),('string-true','true'),('string-True','True'),('string-TRUE','TRUE'),('numeric-one',1),('null',None)]:combined(label,value)
 inactive_name='rust-owned-branch-bool-false'
 for label,age in [('existing-inactive-old',35.0),('existing-inactive-current',7.125)]:
  reset_branch(True);m=watch_file(M3U_ROOT,inactive_name+'.m3u',age);e=watch_file(EPG_ROOT,inactive_name+'.xml',age);case=branch_case(label,m,e);check(label+'-does-not-reactivate-or-rename',not case['branch_after']['accounts'][0]['is_active']and not case['branch_after']['sources'][0]['is_active']);check(label+'-no-signal-or-helper-children',case['children']==[]);check(label+'-sets-both-markers',all(cache.get('processed_file:'+str(p))==repr(p.stat().st_mtime).encode()for p in[m,e]));check_committed(case)
 reset_branch(False);m=watch_file(M3U_ROOT,'rust-owned-branch-bool-true.m3u');e=watch_file(EPG_ROOT,'rust-owned-branch-bool-true.xml');M3UAccount.objects.filter(file_path=str(m)).update(is_active=True);EPGSource.objects.filter(file_path=str(e)).update(is_active=True);case=branch_case('existing-active-current',m,e);check('existing-active-stays-active-despite-settingfalse',case['branch_after']['accounts'][0]['is_active']and case['branch_after']['sources'][0]['is_active']);check('existing-active-no-create-signal-duplicates',child_tasks(case)==['apps.m3u.tasks.refresh_single_m3u_account','apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task']);check_committed(case)
 for label,root,table,failure in [('epg-insert-P0001',EPG_ROOT,'epg_epgsource',False),('m3u-insert-P0001-stops-before-epg',M3U_ROOT,'m3u_m3uaccount',True)]:
  reset_branch(True);m=watch_file(M3U_ROOT,'rust-owned-branch-'+label+'.m3u')if failure else None;e=watch_file(EPG_ROOT,'rust-owned-branch-'+label+'.xml')
  with connection.cursor()as c:c.execute("CREATE FUNCTION rust_owned_branch_insert_fail() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'rust-owned branch insert failure' USING ERRCODE='P0001'; END$$");c.execute('CREATE TRIGGER rust_owned_branch_insert_fail BEFORE INSERT ON '+table+' FOR EACH ROW EXECUTE FUNCTION rust_owned_branch_insert_fail()')
  case=branch_case(label,m,e,expected_failure=failure);check(label+'-no-row-marker-or-child',not case['branch_after']['accounts']and not case['branch_after']['sources']and case['children']==[]and all(cache.get('processed_file:'+str(p))is None for p in[m,e]if p));check(label+'-first-state-catch-boundary',case['native_completion']['first']is(not failure));check_committed(case)
 cache.execute_command('ACL','SETUSER','rust-owned-scanner-no-set','on','>playback-fixture-only','~*','&*','+@all','-set')
 for label,is_m3u in [('epg-marker-SET-denied',False),('m3u-marker-SET-denied-stops-before-epg',True)]:
  reset_branch(True);m=watch_file(M3U_ROOT,'rust-owned-branch-'+label+'.m3u')if is_m3u else None;e=watch_file(EPG_ROOT,'rust-owned-branch-'+label+'.xml');case=branch_case(label,m,e,acl='rust-owned-scanner-no-set',expected_failure=is_m3u);check(label+'-created-row-committed-before-marker-fault',len(case['branch_after']['accounts']if is_m3u else case['branch_after']['sources'])==1);check(label+'-no-marker',cache.get('processed_file:'+str(m if is_m3u else e))is None);expected=['apps.m3u.tasks.refresh_m3u_groups']if is_m3u else['apps.epg.tasks.refresh_epg_data','apps.epg.tasks.build_programme_index_task'];check(label+'-retains-actual-signal-children',child_tasks(case)==expected);check(label+'-later-epg-not-reached-if-m3u',not is_m3u or not case['branch_after']['sources']);check_committed(case)
 for label,is_m3u in [('epg-signal-LPUSH-denied',False),('m3u-signal-LPUSH-denied-stops-before-epg',True)]:
  reset_branch(True);m=watch_file(M3U_ROOT,'rust-owned-branch-'+label+'.m3u')if is_m3u else None;e=watch_file(EPG_ROOT,'rust-owned-branch-'+label+'.xml');task='apps.m3u.tasks.refresh_m3u_groups'if is_m3u else'apps.epg.tasks.refresh_epg_data';case=branch_case(label,m,e,expected_failure=is_m3u,extra={'deny_child_task':task});check(label+'-model-transaction-rolledback',not case['branch_after']['accounts']and not case['branch_after']['sources']and not case['branch_after']['profiles']and not case['branch_after']['periodic']);check(label+'-attempt-only-no-durable-child',child_tasks(case)==[task]and case['metadata']['children']==[]and case['after']['queue']==case['before']['queue']);check_committed(case)
 reset_branch(True);m=watch_file(M3U_ROOT,'rust-owned-branch-ignore.M3U');e=watch_file(EPG_ROOT,'rust-owned-branch-ignore.XML');case=branch_case('case-sensitive-extensions',m,e);check('upper-extensions-no-rows-children-or-markers',not case['branch_after']['accounts']and not case['branch_after']['sources']and case['children']==[]and all(cache.get('processed_file:'+str(p))is None for p in[m,e]));check_committed(case)
 # Native conservative admission only: Python float1.0 Source semantics are
 # outside Source217's integer-one capture, so do not silently accept it.
 stop();reset();drop_branch_triggers();EPGSource.objects.all().update(is_active=False);CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':1.0});m=watch_file(M3U_ROOT,'rust-owned-branch-native-float-domain.m3u');worker=start();native_send({'now':NOW});check('native-float-domain-prepared',native_response()['prepared']);produced=app.tasks[TASK].apply_async(queue=PARENT_QUEUE);queued_before=broker.lrange(PARENT_QUEUE,0,2);native_send({'action':'run'});answer=native_response();check('native-float-domain-refused',answer['run']=='Ok(UnsupportedHead)'and answer['first']is False);check('native-float-domain-beforeACK',broker.lrange(PARENT_QUEUE,0,2)==queued_before);check('native-float-domain-no-row-or-result',not M3UAccount.objects.filter(file_path=str(m)).exists()and backend.get(app.backend.get_key_for_task(produced.id))is None);CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});native_send({'action':'run'});answer=native_response();check('owned-repair-same-frame',answer['run']=='Ok(Succeeded)'and produced.get(timeout=2)is None);report['native_admission_controls']={'scope':'Native conservative rejection and owned same-frame repair; NO Source float1.0 behavior/parity claim','passed':True};stop()
 if os.environ.get('SCANNER_RESOURCE_MODE')=='1':
  stop();reset();drop_branch_triggers();EPGSource.objects.all().update(is_active=False);CoreSettings._update_group(SYSTEM_SETTINGS_KEY,'System Settings',{'auto_import_mapped_files':True});report['physical_cases']=[]
  names=['a_sql_lock_after_early_ack_keeps_caller_until_rollback_close','b_actual_child_lpush_reply_is_before_model_commit_and_retains_abort','c_actual_marker_set_reply_after_commit_retains_abort','d_actual_channels_reply_retains_abort_after_sql_and_children','e_lost_committed_marker_reply_is_operational_without_guessed_terminal','f_committed_result_reply_past_timeout_keeps_caller_and_admission','g_lost_child_reply_rolls_back_model_but_preserves_real_orphan_child','h_unknown_actual_source_producer_frame_retained_before_ack']
  resource_path=Path('/data/m3us/rust-owned-branch-resource.m3u')
  for name in names:
   if resource_path.exists():resource_path.unlink()
   cache.delete('processed_file:'+str(resource_path),channel_key);async_to_sync(layer.group_add)('updates',channel);M3UAccount.objects.filter(file_path=str(resource_path)).delete()
   ident=str(uuid.uuid4());unknown=name.startswith('h_');produced=app.send_task('rust_owned.unported_scanner_boundary',args=[],kwargs={},task_id=ident,queue=PARENT_QUEUE)if unknown else app.tasks[TASK].apply_async(args=[],kwargs={},task_id=ident,queue=PARENT_QUEUE)
   value=subprocess.run(['/native-task-peer','scanner_files_resources::'+name,'--exact','--ignored','--nocapture','--test-threads=1'],env=dict(os.environ,SCANNER_RESOURCE_TASK_ID=ident),capture_output=True,text=True,timeout=45);Path('/evidence/physical-'+name+'.log').write_text(value.stdout+value.stderr);report['physical_cases'].append({'name':name,'task_id':ident,'exit':value.returncode,'scope':'Native actual physical ownership/control; Original producer only, not Original handler execution'});check(name+'-physical-exit0',value.returncode==0);check(name+'-one-actual-test','1 passed; 0 failed'in value.stdout)
  report['physical_tests']=len(names)
 report['native_verified']=True;report['passed']=True
finally:
 stop();cache.execute_command('ACL','SETUSER','default','+lpush');drop_branch_triggers();drop_trigger();Path('/evidence/source-logo-scanner-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
print(json.dumps({'passed':report['passed'],'checks':len(report['checks'])}))
