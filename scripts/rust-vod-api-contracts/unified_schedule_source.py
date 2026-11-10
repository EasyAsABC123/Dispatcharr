"""Guarded Original unified raw-page/projection/count schedule observations.
APIRequestFactory/actual PostgreSQL, not Source network or native acceptance.
Original Dispatcharr authors/contributors retain credit and AGPL licensing.
"""
import hashlib,json,os,sys
from datetime import date,datetime
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Explicit disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.urls import resolve
from rest_framework.test import APIRequestFactory,force_authenticate
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings
PREFIX='rust-probe-vod-rest-';user=User.objects.get(username=PREFIX+'standard')
accounts=M3UAccount.objects.filter(name__startswith=PREFIX);assert accounts.count()==3
assert not accounts.exclude(server_url__startswith='http://127.0.0.1:').exists()
movies=list(Movie.objects.filter(name__startswith=PREFIX).order_by('id')[:2]);assert len(movies)==2
account=accounts.filter(is_active=True).order_by('id').first();assert account
saved=[(m.id,m.name,m.rating)for m in movies];saved_logo=movies[0].logo_id;account_active=account.is_active
MODELS=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings]
def fingerprint():
 return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=DjangoJSONEncoder).encode()).hexdigest()for m in MODELS}
def exact_values(queryset):
 # DjangoJSONEncoder shortens datetimes to milliseconds; fixture replays must
 # preserve the exact PostgreSQL microseconds emitted by DRF's serializer.
 return [{key:value.isoformat()if isinstance(value,(date,datetime))else value for key,value in row.items()}for row in queryset.values()]
before=fingerprint();factory=APIRequestFactory();records=[];large_ids=[];large_fixture=None;zero_logo_fixture=None
with connection.cursor()as cursor:cursor.execute('SHOW statement_timeout');original_timeout=cursor.fetchone()[0]
other=connection.copy(alias='unified_owned_mutation');other.ensure_connection()
def observe(kind):
 events=[];lock_held=False
 def hook(execute,sql,params,many,context):
  nonlocal lock_held
  is_page='WITH unified_content AS ('in sql
  is_count='SELECT COUNT(*) FROM ('in sql
  if is_page:events.append('page_execute')
  if is_count:events.append('count_execute')
  result=execute(sql,params,many,context)
  if is_page:
   if kind=='fresh_count':
    with other.cursor()as c:c.execute('UPDATE m3u_m3uaccount SET is_active=false WHERE id=%s',[account.id])
    events.append('independent_account_commit_after_page')
   elif kind=='raw_page':
    with other.cursor()as c:c.execute('UPDATE vod_movie SET name=%s WHERE id=%s',[PREFIX+'mutated-after-raw-page',movies[0].id])
    events.append('independent_name_commit_after_page')
   elif kind=='nan_count_timeout':
    other.set_autocommit(False)
    with other.cursor()as c:c.execute('LOCK TABLE vod_movie IN ACCESS EXCLUSIVE MODE')
    lock_held=True;events.append('count_relation_locked_after_page')
  return result
 path='/api/vod/all/?page_size='+('250'if kind=='large_small_rows'else'2'if kind=='nan_then_bad'else'1')
 request=factory.get(path,HTTP_ACCEPT='application/json',HTTP_HOST='localhost');force_authenticate(request,user=user)
 match=resolve('/api/vod/all/');request.resolver_match=match
 try:
  with connection.execute_wrapper(hook):
   response=match.func(request,*match.args,**match.kwargs)
   record={'kind':kind,'status_before_render':response.status_code,'events':events.copy()}
   try:response.render();body=response.content;record.update(status=response.status_code,json=json.loads(body),body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
   except Exception as error:record.update(renderer_unhandled=type(error).__name__,message=str(error))
   response.close()
 finally:
  if lock_held:other.rollback();other.set_autocommit(True)
 records.append(record)
try:
 observe('raw_page')
 assert records[-1]['json']['results'][0]['name']==saved[0][1]
 Movie.objects.filter(pk=movies[0].id).update(name=saved[0][1])
 observe('fresh_count')
 assert records[-1]['events']==['page_execute','independent_account_commit_after_page','count_execute']
 with connection.cursor()as cursor:
  cursor.execute('SELECT count(*) FROM(SELECT 1 FROM vod_movie WHERE id IN(SELECT movie_id FROM vod_m3umovierelation r JOIN m3u_m3uaccount a ON r.m3u_account_id=a.id WHERE a.is_active)UNION ALL SELECT 1 FROM vod_series WHERE id IN(SELECT series_id FROM vod_m3useriesrelation r JOIN m3u_m3uaccount a ON r.m3u_account_id=a.id WHERE a.is_active))c');assert records[-1]['json']['count']==cursor.fetchone()[0]
 assert records[-1]['json']['results'][0]['id']==movies[0].id
 M3UAccount.objects.filter(pk=account.id).update(is_active=account_active)
 Movie.objects.filter(pk=movies[0].id).update(rating='NaN');Movie.objects.filter(pk=movies[1].id).update(rating='bad')
 observe('nan_then_bad');assert records[-1]['status']==500 and records[-1]['json']=={'error':"could not convert string to float: 'bad'"}and records[-1]['events']==['page_execute']
 Movie.objects.filter(pk=movies[1].id).update(rating=saved[1][2])
 with connection.cursor()as cursor:cursor.execute("SELECT set_config('statement_timeout','200ms',false)")
 observe('nan_count_timeout');assert records[-1]['status']==500 and'canceling statement due to statement timeout'in records[-1]['json']['error']
 Movie.objects.filter(pk=movies[0].id).update(rating=saved[0][2])
 with connection.cursor()as cursor:cursor.execute("SELECT set_config('statement_timeout',%s,false)",[original_timeout])
 assert not VODLogo.objects.filter(pk=0).exists()
 zero=VODLogo.objects.create(id=0,name=PREFIX+'zero-id-logo',url='https://fixture.invalid/zero-id-logo.jpg')
 zero_logo_fixture=VODLogo.objects.filter(pk=0).values().get()
 Movie.objects.filter(pk=movies[0].id).update(logo_id=0)
 observe('zero_logo');assert records[-1]['json']['results'][0]['logo']is None
 Movie.objects.filter(pk=movies[0].id).update(logo_id=saved_logo);zero.delete()
 added=Movie.objects.bulk_create([Movie(name=PREFIX+'bounded-unified-'+str(i).zfill(4),rating='0',custom_properties={'tiny':i})for i in range(200)])
 large_ids=[m.id for m in added]
 M3UMovieRelation.objects.bulk_create([M3UMovieRelation(movie=m,m3u_account=account,stream_id='bounded-unified-'+str(m.id))for m in added])
 large_fixture={'movies':exact_values(Movie.objects.filter(id__in=large_ids).order_by('id')),'relations':exact_values(M3UMovieRelation.objects.filter(movie_id__in=large_ids).order_by('id'))}
 observe('large_small_rows');assert len(records[-1]['json']['results'])==229 and records[-1]['json']['count']==229

finally:
 try:other.rollback()
 except Exception:pass
 other.close()
 if large_ids:Movie.objects.filter(id__in=large_ids).delete()
 Movie.objects.filter(pk=movies[0].id).update(logo_id=saved_logo)
 VODLogo.objects.filter(pk=0,name=PREFIX+'zero-id-logo').delete()
 for row_id,name,rating in saved:Movie.objects.filter(pk=row_id).update(name=name,rating=rating)
 M3UAccount.objects.filter(pk=account.id).update(is_active=account_active)
 with connection.cursor()as cursor:cursor.execute("SELECT set_config('statement_timeout',%s,false)",[original_timeout])
assert fingerprint()==before
print(json.dumps({'passed':True,'scope':'Actual Original APIRequestFactory/PostgreSQL scheduling, force-authenticated Standard (no auth/network acceptance)','records':records,'large_fixture':large_fixture,'zero_logo_fixture':zero_logo_fixture,'fixture_datetime_precision':'Exact Python isoformat microseconds, not DjangoJSONEncoder millisecond truncation','sequence_scope':'Existing full rows restored; PostgreSQL sequences may advance during owned bulk fixture setup (not compared as row data)','owned_rows_restored':True,'fingerprints_before':before,'fingerprints_after':fingerprint(),'provider_refresh_called':False,'provider_guard':'No provider/action endpoint; raw independent SQL updates existing owned rows only; loopback-only account URLs','native_verified':False,'source_sha256':hashlib.sha256(open('/app/apps/vod/api_views.py','rb').read()).hexdigest()},cls=DjangoJSONEncoder,ensure_ascii=False))
