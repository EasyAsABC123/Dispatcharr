"""Pinned Original known-crontab DB/Beat/DST reference with explicit clocks.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No task handler or scheduling calculation is replaced; no worker consumes tasks.
"""
import datetime as dt,hashlib,importlib.metadata,inspect,json,math,os,signal,subprocess,time,zoneinfo
from pathlib import Path
import source_scheduler_publication as f
assert f.report['passed']
from django_celery_beat.models import CrontabSchedule
from source_calendar_specs import LEGACY
CLOCK=Path('/evidence/calendar-clock-reference.json');os.environ['CALENDAR_DISPOSABLE_CLOCK']=str(CLOCK)
def setclock(now,mode='zoneinfo'):
 data=json.dumps({'now':now,'mode':mode});temp=CLOCK.with_suffix('.tmp');temp.write_text(data);temp.replace(CLOCK)
setclock('2026-11-11T04:00:00+00:00')
import calendar_clock_observer as controlled
report={'passed':False,'checks':list(f.report['checks']),'vectors':[],'cases':[],'scope':'Actual pinned Original known crontab ModelEntry +eager database filtering/tick/sync +real Beat with explicit clock dependencies; no elapsed DST/native/main/unknown task authority'}
schedulers=[];sources=[];process=None
def check(name,value):
 report['checks'].append({'name':name,'passed':bool(value)})
 if not value:raise AssertionError(name)
def capture_zones():
 result={'tzpath':list(zoneinfo.TZPATH),'tzdata_package':importlib.metadata.version('tzdata'),'versions':{},'zones':{}}
 for root in zoneinfo.TZPATH:
  version=Path(root)/'tzdata.zi'
  if version.is_file():result['versions'][str(version)]=version.read_text().splitlines()[0]
 for name in ['UTC','America/New_York','Australia/Lord_Howe','Asia/Kathmandu','Europe/Paris']:
  path=next(Path(root)/name for root in zoneinfo.TZPATH if(Path(root)/name).is_file());raw=path.read_bytes();assert len(raw)<=65536;target=Path('/evidence/calendar-tzdb')/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw);result['zones'][name]={'path':str(path),'evidence_file':str(target),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
 notice=Path('/usr/share/doc/tzdata/copyright')
 if notice.is_file():raw=notice.read_bytes();Path('/evidence/calendar-tzdb/COPYRIGHT').write_bytes(raw);result['notice_sha256']=hashlib.sha256(raw).hexdigest()
 return result

def makecron(spec):
 return CrontabSchedule.objects.create(timezone=spec.get('timezone','UTC'),minute=spec.get('minute','0'),hour=spec.get('hour','4'),day_of_week=spec.get('day_of_week','*'),day_of_month=spec.get('day_of_month','*'),month_of_year=spec.get('month_of_year','*'))
def state(row):
 row.refresh_from_db();return {'name':row.name,'enabled':row.enabled,'count':row.total_run_count,'last_run_at':row.last_run_at.isoformat()if row.last_run_at else None,'one_off':row.one_off}
def begin(label,spec,changes=None):
 f.broker.flushdb();f.backend.flushdb();setclock(spec['now']);scheduler=controlled.ClockScheduler(app=f.app,lazy=False,max_interval=5);schedulers.append(scheduler);f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();scheduler.schedule;scheduler.schedule;cron=makecron(spec);row=f.PeriodicTask.objects.create(name='rust-owned-calendar-'+label,task=f.refresh_epg_data.name,args=json.dumps([source.pk]),kwargs='{}',headers='{}',crontab=cron,last_run_at=dt.datetime.fromisoformat(spec['last_run_at']),**(changes or {}));scheduler.schedule;scheduler.schedule;return scheduler,row,cron

def sample(scheduler,row):
 marker=f.PeriodicTasks.last_change();delay=scheduler.tick();scheduler.sync();return {'delay':delay,'state':state(row),'queue_count':f.broker.llen('celery'),'marker_changed':f.PeriodicTasks.last_change()!=marker,'cached_names':list(scheduler._schedule)}
try:
 report['tzdb']=capture_zones();source=f.EPGSource(name='rust-owned-calendar-inactive',source_type='xmltv',is_active=False,custom_properties={});f.EPGSource.objects.bulk_create([source]);sources.append(source.pk)
 # Preserve every old11 expected result; ZoneInfo-clock counterparts stay distinct.
 for mode in ['legacy-fixed-offset','zoneinfo']:
  for spec in LEGACY:
   setclock(spec['now'],mode);cron=makecron(spec);row=f.PeriodicTask.objects.create(name='rust-owned-calendar-vector-'+mode+'-'+spec['name'],task=f.refresh_epg_data.name,args=json.dumps([source.pk]),kwargs='{}',headers='{}',crontab=cron,last_run_at=dt.datetime.fromisoformat(spec['last_run_at']));entry=controlled.ClockEntry(row,app=f.app);value=entry.is_due();actual={'due':value.is_due,'next_seconds':value.next};check(mode+'-'+spec['name']+'-finite-positive-delay',isinstance(value.is_due,bool)and math.isfinite(value.next)and value.next>0)
   if mode=='legacy-fixed-offset':check('legacy11-'+spec['name']+'-unchanged',actual=={'due':spec['due'],'next_seconds':spec['next_seconds']})
   report['vectors'].append({'mode':mode,'input':spec,'actual':actual});row.delete()
 base={'timezone':'UTC','minute':'0','hour':'4','now':'2026-11-11T04:00:00+00:00','last_run_at':'2026-11-10T04:00:00+00:00'}
 cases=[('utc-default-shape',base,{},2),('utc-before',{**base,'now':'2026-11-11T03:59:59+00:00'},{},1),('ny-spring',{'timezone':'America/New_York','minute':'30','hour':'2','now':'2026-03-08T07:00:00+00:00','last_run_at':'2026-03-07T07:30:00+00:00'},{},2),('ny-fall',{'timezone':'America/New_York','minute':'30','hour':'1','now':'2026-11-01T05:30:00+00:00','last_run_at':'2026-10-31T05:30:00+00:00'},{},1),('lord-howe',{'timezone':'Australia/Lord_Howe','minute':'15','hour':'2','now':'2026-10-03T15:30:00+00:00','last_run_at':'2026-10-02T15:45:00+00:00'},{},1),('kathmandu',{'timezone':'Asia/Kathmandu','minute':'0','hour':'9','now':'2026-11-10T03:15:00+00:00','last_run_at':'2026-11-09T03:15:00+00:00'},{},1),('start-future',base,{'start_time':dt.datetime.fromisoformat(base['now'])+dt.timedelta(seconds=1,microseconds=250000)},1),('start-utc-wild',{**base,'minute':'*','hour':'*'},{'start_time':dt.datetime.fromisoformat(base['now'])+dt.timedelta(seconds=1,microseconds=250000)},1),('start-ny-wild',{**base,'timezone':'America/New_York','minute':'*','hour':'*'},{'start_time':dt.datetime.fromisoformat(base['now'])+dt.timedelta(seconds=1,microseconds=250000)},1),('expired',base,{'expires':dt.datetime.fromisoformat(base['now'])-dt.timedelta(microseconds=1)},1),('oneoff',base,{'one_off':True},2),('future-expiry-header',base,{'expires':dt.datetime.fromisoformat(base['now'])+dt.timedelta(hours=1)},1)]
 for label,spec,changes,count in cases:
  scheduler,row,cron=begin(label,spec,changes);start_gate=None
  if 'start_time'in changes:
   value=controlled.ClockEntry(row,app=f.app).is_due();start_gate={'due':value.is_due,'next_seconds':value.next,'aligned_start':row.due_start_time(f.app.timezone).isoformat()}
  samples=[sample(scheduler,row)for _ in range(count)]
  if label=='ny-spring':setclock('2026-03-08T07:30:00+00:00');samples.append(sample(scheduler,row));check('real-zoneinfo-gap-delayed-publication',samples[0]['queue_count']==0 and samples[-1]['queue_count']==1)
  if label=='ny-fall':setclock('2026-11-01T06:30:00+00:00');samples.append(sample(scheduler,row));check('real-zoneinfo-fall-fold-no-second-publication',samples[-1]['queue_count']==1)
  check(label+'-bounded-actual-db-ticks',all(math.isfinite(v['delay'])and 0<=v['delay']<=5 for v in samples));report['cases'].append({'name':label,'input':spec,'changes':{k:v.isoformat()if hasattr(v,'isoformat')else v for k,v in changes.items()},'samples':samples,'start_gate':start_gate,'envelopes':f.queue(),'query':str(scheduler.enabled_models_qs().query)});scheduler.close()
 scheduler,row,cron=begin('reload-zone',base);samples=[sample(scheduler,row)];setclock('2026-11-11T04:01:00+00:00');cron.timezone='America/New_York';cron.save();samples.append(sample(scheduler,row));check('timezone-sentinel-queryfilter-removes-outside-window',row.name not in scheduler.schedule and samples[-1]['queue_count']==1);cron.hour='*';cron.save();samples.append(sample(scheduler,row));check('timezone-sentinel-wildcard-reloads',row.name in scheduler.schedule and samples[-1]['queue_count']==1);report['cases'].append({'name':'reload-zone','input':base,'samples':samples,'envelopes':f.queue()});scheduler.close()
 report['query_offsets_2023']={name:scheduler._get_timezone_offset(name)for name in report['tzdb']['zones']}
 import celery.utils.time as time_helpers
 for name in ['remaining','ffwd','maybe_make_aware','make_aware']:
  text=inspect.getsource(getattr(time_helpers,name));Path('/evidence/calendar-source-'+name+'.py').write_text(text)
 report['app_timezone']=str(f.app.timezone);report['enable_utc']=f.app.conf.enable_utc;report['beat_cron_starting_deadline']=f.app.conf.beat_cron_starting_deadline
 # Actual Beat process, controlled ZoneInfo spring-gap clock; no OS time travel.
 spec=cases[2][1];scheduler,row,cron=begin('beat-dst',spec);scheduler.close();env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app',CALENDAR_BEAT_READY='/evidence/calendar-beat-ready.json')
 with Path('/evidence/calendar-beat-private.log').open('ab')as log:process=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_timer_observer:app','beat','--scheduler','calendar_clock_observer:ClockScheduler','--max-interval','1','--pidfile=','--loglevel=WARNING'],env=env,stdout=log,stderr=log,start_new_session=True)
 deadline=time.monotonic()+20
 while not Path('/evidence/calendar-beat-ready.json').exists():
  if process.poll()is not None:raise RuntimeError('Controlled calendar Beat exited before ready')
  if time.monotonic()>deadline:raise TimeoutError('Controlled calendar Beat ready')
  time.sleep(.01)
 time.sleep(1.2);check('controlled-DST-gap-initially-no-publication',f.broker.llen('celery')==0 and state(row)['count']==0)
 setclock('2026-03-08T07:30:00+00:00');deadline=time.monotonic()+20
 while f.broker.llen('celery')<1:
  if process.poll()is not None:raise RuntimeError('Controlled calendar Beat exited')
  if time.monotonic()>deadline:raise TimeoutError('Controlled calendar Beat publication')
  time.sleep(.01)
 os.killpg(process.pid,signal.SIGTERM);process.wait(timeout=15);check('controlled-DST-real-Beat-graceful-exit',process.returncode==0);process=None;check('controlled-DST-real-Beat-one-durable-publication',f.broker.llen('celery')==1 and state(row)['count']==1);report['beat_dst']={'input':spec,'state':state(row),'envelopes':f.queue(),'clock_scope':'Actual Beat process with explicit fixture clock dependencies; not elapsed DST or changed OS time.'}
 # Real-clock Original Beat for BOTH known crontab families. Pick a minute
 # two ahead, with a last-run two hours back: immediate overdue publication,
 # followed by at least ~60s before another due time, avoiding boundary races.
 f.broker.flushdb();f.backend.flushdb();f.PeriodicTask.objects.all().update(enabled=False);f.PeriodicTasks.update_changed();actual_now=f.timezone.now();minute=(actual_now.minute+2)%60;cron=CrontabSchedule.objects.create(timezone='UTC',minute=str(minute),hour='*',day_of_week='*',day_of_month='*',month_of_year='*');real_rows=[]
 for suffix,task,args in [('z-refresh',f.refresh_epg_data,[source.pk]),('a-series',f.evaluate_series_rules,[])]:real_rows.append(f.PeriodicTask.objects.create(name='rust-owned-calendar-real-'+suffix,task=task.name,args=json.dumps(args),kwargs='{}',headers='{}',crontab=cron,last_run_at=actual_now-dt.timedelta(hours=2)))
 with Path('/evidence/calendar-real-beat-private.log').open('ab')as log:process=subprocess.Popen(['/dispatcharrpy/bin/celery','-A','periodic_timer_observer:app','beat','--scheduler','django_celery_beat.schedulers:DatabaseScheduler','--max-interval','1','--pidfile=','--loglevel=WARNING'],env=dict(os.environ,C_FORCE_ROOT='true',PYTHONPATH='/job-tools:/app'),stdout=log,stderr=log,start_new_session=True)
 deadline=time.monotonic()+20
 while f.broker.llen('celery')<2:
  if process.poll()is not None:raise RuntimeError('Real-clock calendar Beat exited')
  if time.monotonic()>deadline:raise TimeoutError('Real-clock calendar two known publications')
  time.sleep(.01)
 os.killpg(process.pid,signal.SIGTERM);process.wait(timeout=15);check('real-clock-calendar-Beat-graceful-exit',process.returncode==0);process=None;envelopes=f.queue();check('real-clock-calendar-exact-two-known-frames',[v['headers']['periodic_task_name']for v in reversed(envelopes)]==[v.name for v in real_rows]);check('real-clock-calendar-both-count1',all(state(row)['count']==1 for row in real_rows));report['real_beat']={'minute':str(minute),'hour':'*','timezone':'UTC','initial_last_run':(actual_now-dt.timedelta(hours=2)).isoformat(),'states':[state(row)for row in real_rows],'envelopes':envelopes}
 report['passed']=True
finally:
 if process is not None and process.poll()is None:os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
 for scheduler in schedulers:scheduler.close()
 f.EPGSource.objects.filter(pk__in=sources).delete();f.broker.flushdb();f.backend.flushdb();report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()];Path('/evidence/source-scheduler-calendar-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
