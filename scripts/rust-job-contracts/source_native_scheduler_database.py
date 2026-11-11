"""Actual Original/native SQL scheduler setup and cache reload; copied DB only.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No timers, workers or task publication are enabled by the native component.
"""
import datetime as dt,json,os,re,select,subprocess,time
from pathlib import Path
import source_scheduler_database as f
assert f.report['passed']
report={'passed':False,'checks':list(f.report['checks']),'scope':'Pinned Original→native bounded database scheduler default setup, cached reload and caught per-entry SQL-error boundary; no tick/publication/main/calendar acceptance'}
process=None;buffer=b''
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def request(action):
 global buffer
 body=(json.dumps({'action':action})+'\n').encode();process.stdin.write(body);process.stdin.flush();end=time.monotonic()+30
 while time.monotonic()<end:
  while b'\n'in buffer:
   line,buffer=buffer.split(b'\n',1)
   if line.startswith(b'NATIVE_DATABASE_SCHEDULER_JSON='):return json.loads(line.split(b'=',1)[1])
  ready,_,_=select.select([process.stdout],[],[],.2)
  if ready:
   data=os.read(process.stdout.fileno(),65536)
   if not data:raise RuntimeError('Native scheduler peer exited')
   buffer+=data
   if len(buffer)>2*1024*1024:raise RuntimeError('Peer record bound')
 raise TimeoutError('Native scheduler physical command deadline')
def semantic_rows(rows):
 keys=['name','task','enabled','args','kwargs','queue','exchange','routing_key','headers','priority','expires','expire_seconds','one_off','total_run_count','description','interval','crontab']
 return [{key:row[key]for key in keys}for row in rows]
def cache(entries):return {v['name']:{'enabled':v['enabled'],'args':v['args'],'kwargs':v['kwargs'],'count':v['count']}for v in entries}
def original_cache(value):return {k:{key:entry[key]for key in ['enabled','args','kwargs','count']}for k,entry in value.items()}
try:
 f.PeriodicTask.objects.all().delete();f.PeriodicTasks.update_changed()
 env=dict(os.environ,SCHEDULER_DATABASE_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+f.database)
 log=Path('/evidence/native-database-scheduler-stderr-private.log').open('wb')
 process=subprocess.Popen(['/evidence/native-refresh-worker','--ignored','--exact','database_schedule::peer::native_database_scheduler_peer','--nocapture'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=log,env=env,bufsize=0);log.close()
 setup=request('setup');check('native-setup-all-six-success',len(setup['installed'])==6 and not setup['failures'])
 check('native-default-rows-exact-semantic-projection',semantic_rows(f.rows())==semantic_rows(f.report['installed_rows']))
 first=request('refresh');check('native-startup-cache-matches-Original',cache(first['entries'])==original_cache(f.report['startup_cache']))
 f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();check('native-signalled-disabled-defaults-absent',not request('refresh')['entries'])
 interval=f.IntervalSchedule.objects.create(every=3600,period=f.IntervalSchedule.SECONDS)
 row=f.PeriodicTask.objects.create(name='rust-owned-startup-reload',task='apps.backups.tasks.scheduled_backup_task',interval=interval,args='[]',kwargs='{}',last_run_at=f.timezone.now());check('native-created-save-reloads',cache(request('refresh')['entries'])==original_cache(f.report['created_cache']))
 request('refresh');f.PeriodicTask.objects.filter(pk=row.pk).update(enabled=False);check('native-unsignalled-bulk-disable-retains-cached-model',cache(request('refresh')['entries'])==original_cache(f.report['unsignalled_cache']))
 f.PeriodicTasks.update_changed();check('native-explicit-marker-removes-disabled',not request('refresh')['entries'])
 row.enabled=True;row.save(update_fields=['enabled']);check('native-save-reenables-cache',len(request('refresh')['entries'])==1)
 row.delete();check('native-delete-reloads-empty',not request('refresh')['entries'])
 row=f.PeriodicTask.objects.create(name='rust-owned-nochanges',task='apps.epg.tasks.refresh_epg_data',interval=interval,args='[1]',kwargs='{}');request('refresh');request('refresh');before=f.marker();row.no_changes=True;row.enabled=False;row.save(update_fields=['enabled']);check('native-nochanges-keeps-marker-and-cache',before==f.marker()and request('refresh')['entries'][0]['enabled'])
 f.PeriodicTasks.update_changed();check('native-nochanges-explicit-marker-clears-cache',not request('refresh')['entries']);row.no_changes=False;row.delete()
 altered=f.PeriodicTask.objects.get(name='scan-files');altered.task='owned.wrong';altered.enabled=False;altered.args='[99]';altered.kwargs='{"owned":true}';altered.queue='owned';altered.headers='{"old":true}';altered.one_off=True;altered.total_run_count=7;altered.description='owned persistent description';altered.last_run_at=f.timezone.now()-dt.timedelta(days=1);altered.save()
 setup=request('setup');check('native-reconstruct-all-six-success',not setup['failures']);check('native-reconstructed-rows-match-Original',semantic_rows(f.rows())==semantic_rows(f.report['reconstructed_rows']))
 # Actual known guarded startup write error. The native service must wait its
 # rollback, continue other defaults, and preserve the existing failed row.
 with f.connection.cursor()as cursor:
  cursor.execute("CREATE FUNCTION rust_owned_scheduler_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.name='scan-files' THEN RAISE EXCEPTION 'owned scheduler P0001'; END IF; RETURN NEW; END $$")
  cursor.execute('CREATE TRIGGER rust_owned_scheduler_failure BEFORE INSERT OR UPDATE ON django_celery_beat_periodictask FOR EACH ROW EXECUTE FUNCTION rust_owned_scheduler_fail()')
 try:
  value=request('setup');check('native-caught-P0001-only-failed-entry',value['failures']==[['scan-files','P0001']]and len(value['installed'])==5);check('native-caught-startup-rows-match-Original',semantic_rows(f.rows())==semantic_rows(f.report['caught_startup_rows']))
 finally:
  with f.connection.cursor()as cursor:cursor.execute('DROP TRIGGER rust_owned_scheduler_failure ON django_celery_beat_periodictask');cursor.execute('DROP FUNCTION rust_owned_scheduler_fail()')
 check('no-broker-publication-or-results',f.broker.dbsize()==0 and f.backend.dbsize()==0)
 report['rows']=semantic_rows(f.rows());report['shutdown']=request('shutdown');check('native-service-drained',report['shutdown']['drained']);process.stdin.close();process.wait(timeout=10);check('native-peer-normal-exit',process.returncode==0)
 if os.environ.get('SCHEDULER_PHYSICAL_GATES')=='1':
  physical=subprocess.run(['/evidence/native-refresh-worker','database_schedule::physical::','--ignored','--nocapture','--test-threads=1'],env=env,capture_output=True,text=True,timeout=90)
  Path('/evidence/native-scheduler-physical.log').write_text(physical.stdout+physical.stderr);physical.check_returncode();matched=re.search(r'test result: ok\. (\d+) passed; 0 failed',physical.stdout);assert matched and int(matched.group(1))>=2;check('native-physical-resource-gates',True);report['physical_sql_tests']=2;report['physical_resource_tests']=int(matched.group(1));report['physical_retained_tick_tests']=int('SCHEDULER_RETAINED_TICK_JSON='in physical.stdout)
 report['passed']=True
finally:
 if process is not None and process.poll()is None:process.terminate();process.wait(timeout=10)
 Path('/evidence/source-native-scheduler-database-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
