"""Fixture clock dependencies around unmodified Original calendar methods.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
No task, publication, sync or schedule calculation is replaced.
"""
import datetime as dt,json,os
from pathlib import Path
from zoneinfo import ZoneInfo
from django.conf import settings
import django_celery_beat.schedulers as original
from periodic_timer_observer import app
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned calendar fixture required')

def clock(dependency=None):
 path=Path(os.environ['CALENDAR_DISPOSABLE_CLOCK'])
 if path.parent!=Path('/evidence')or not path.name.startswith('calendar-clock-'):raise RuntimeError('Owned clock path required')
 data=path.read_bytes()
 if len(data)>4096:raise RuntimeError('Clock input bound')
 value=json.loads(data);now=dt.datetime.fromisoformat(value['now'])
 if now.tzinfo is None:raise RuntimeError('Aware clock required')
 if dependency == "entry-now" and os.environ.get("CALENDAR_BEAT_CLOCK_READS"):
  target=Path(os.environ["CALENDAR_BEAT_CLOCK_READS"])
  if target != Path("/evidence/calendar-fall-fold-clock-private.jsonl"):raise RuntimeError("Owned clock log required")
  row=(json.dumps({"pid":os.getpid(),"clock":now.isoformat(),"dependency":dependency})+"\n").encode()
  fd=os.open(target,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
  try:os.write(fd,row)
  finally:os.close(fd)
 return now,value.get('mode','zoneinfo')

def server_now():return clock()[0].astimezone(ZoneInfo(settings.TIME_ZONE))
original.aware_now=server_now
# CrontabSchedule.due_start_time constructs a fresh schedule, outside Entry's
# cached schedule. Control that clock dependency too; its calculation is intact.
from django_celery_beat.tzcrontab import TzAwareCrontab
def schedule_now(self):return clock()[0].astimezone(self.tz)
TzAwareCrontab.nowfunc=schedule_now

class ClockEntry(original.ModelEntry):
 def __init__(self,*args,**kwargs):
  super().__init__(*args,**kwargs)
  if hasattr(self,'schedule'):
   def now():
    value,mode=clock("entry-now")
    return value if mode=='legacy-fixed-offset'else value.astimezone(getattr(self.schedule,'tz',self.app.timezone))
   self.schedule.nowfun=now
 def _default_now(self):return clock()[0].astimezone(self.app.timezone)

class ClockScheduler(original.DatabaseScheduler):
 Entry=ClockEntry
 def __init__(self,*args,**kwargs):
  super().__init__(*args,**kwargs)
  if os.environ.get('CALENDAR_BEAT_READY') and self._schedule is not None:
   target=Path(os.environ['CALENDAR_BEAT_READY']);assert target==Path('/evidence/calendar-beat-ready.json')
   target.write_text(json.dumps({'names':list(self._schedule),'clock':clock()[0].isoformat()}))
