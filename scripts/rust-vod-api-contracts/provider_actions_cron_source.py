"""Guarded actual Original TCP nested account cron-field fixture; no worker execution.
Original Dispatcharr authors retain application and algorithm credit.
"""
import datetime,hashlib,http.client,json,os,sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from core.models import CoreSettings,SystemEvent
from django_celery_beat.models import CrontabSchedule,PeriodicTask,PeriodicTasks
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date,datetime.time)):return value.isoformat()
  if isinstance(value,datetime.tzinfo):return str(value)
  return super().default(value)
prefix='rust-probe-vod-rest-';show=Series.objects.filter(name__startswith=prefix).order_by('id').first();relation=M3USeriesRelation.objects.get(series=show);account=relation.m3u_account;assert account.is_active and account.server_url.startswith('http://127.0.0.1:')
saved=account.refresh_task_id;crontab=None;task=None;records=[]
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream,CrontabSchedule,PeriodicTask,PeriodicTasks]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}
before=fingerprint()
try:
 crontab=CrontabSchedule.objects.bulk_create([CrontabSchedule(minute='13',hour='4',day_of_month='2',month_of_year='1',day_of_week='3',timezone='UTC')])[0]
 task=PeriodicTask.objects.bulk_create([PeriodicTask(name=prefix+'never-execute-controlled-cron',task='fixture.invalid.never_execute',crontab=crontab,enabled=False,args='[]',kwargs='{}')])[0]
 M3UAccount.objects.filter(pk=account.pk).update(refresh_task_id=task.pk)
 state={'crontab':CrontabSchedule.objects.filter(pk=crontab.pk).values().get(),'task':PeriodicTask.objects.filter(pk=task.pk).values().get(),'account_id':account.pk,'old_refresh_task_id':saved}
 path=f'/api/vod/series/{show.pk}/providers/'
 for role in ['admin','basic']:
  user=User.objects.get(username=prefix+role);client=http.client.HTTPConnection('127.0.0.1',5656,timeout=10)
  try:client.request('GET',path,headers={'Host':'localhost','Accept':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(user))});response=client.getresponse();body=response.read(1024*1024+1);assert len(body)<=1024*1024;status=response.status;headers={key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']}
  finally:client.close()
  data=json.loads(body);assert status==200 and len(data)==1 and data[0]['m3u_account']['cron_expression']=='13 4 2 1 3' and 'password'not in data[0]['m3u_account']
  records.append({'role':role,'path':path,'method':'GET','observation':'actual Original TCP','expected':{'status':status,'body_text':body.decode(),'body_sha256':hashlib.sha256(body).hexdigest(),'headers':headers}})
finally:
 M3UAccount.objects.filter(pk=account.pk).update(refresh_task_id=saved)
 with connection.cursor()as cursor:
  if task is not None:cursor.execute('DELETE FROM django_celery_beat_periodictask WHERE id=%s',[task.pk])
  if crontab is not None:cursor.execute('DELETE FROM django_celery_beat_crontabschedule WHERE id=%s',[crontab.pk])
assert fingerprint()==before,'Exact-microsecond21table restore failed'
print(json.dumps({'passed':True,'scope':'actual Original TCP two nested-account nonempty-cron cases; disabled task, no scheduler/worker/provider execution','records':records,'private_fixture_state':state,'full_state_before':before,'full_state_after':fingerprint(),'owned_metadata_restored':True},cls=ExactEncoder,ensure_ascii=False))
