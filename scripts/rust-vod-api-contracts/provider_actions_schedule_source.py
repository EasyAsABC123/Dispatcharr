"""Guarded Original Factory+real-SQL phase witness, NOT Source network.
No views/renderers/auth replaced. Controlled SQL changes occur after observed
SELECT replies; original serializers determine field caching and fresh queries.
Original Dispatcharr authors retain application/algorithm credit.
"""
import datetime,hashlib,json,os,sys,uuid
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.urls import resolve
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from core.models import CoreSettings,SystemEvent
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date,datetime.time)):return value.isoformat()
  return super().default(value)
prefix='rust-probe-vod-rest-';user=User.objects.get(username=prefix+'standard');assert user.user_level==1
accounts=list(M3UAccount.objects.filter(name__startswith=prefix).order_by('id'));assert len(accounts)==3 and all(a.server_url.startswith('http://127.0.0.1:')for a in accounts)
show=Series.objects.filter(name__startswith=prefix).order_by('id').first();episodes=list(Episode.objects.filter(series=show).order_by('id'));assert len(episodes)==3
first=episodes[0];relation=M3UEpisodeRelation.objects.filter(episode=first).order_by('id').first();account=relation.m3u_account;profile=account.profiles.filter(is_default=True).first();logo=show.logo;assert logo is not None
saved={'Series':{'name':show.name},'Episode':{'name':first.name},'VODLogo':{'name':logo.name,'url':logo.url},'M3UAccount':{'is_active':account.is_active},'M3UEpisodeRelation':{'custom_properties':relation.custom_properties},'M3UAccountProfile':{'exp_date':profile.exp_date}}
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('id').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}
before=fingerprint();events=[];queries=[];fired_series=False;fired_profiles=False;mutating=False;new_episode=None
changed={'Series_name':prefix+'SERIES_AFTER_FIRST_SNAPSHOT','Episode_name':prefix+'EPISODE_AFTER_PREFETCH_4K','Logo_name':prefix+'LOGO_AFTER_SERIES_SNAPSHOT','Logo_url':'https://fixture.invalid/changed-after-series.png','Relation_properties':{'quality':'mutated-after-prefetch'},'Profile_expiry':datetime.datetime(2027,5,6,1,2,3,456789,tzinfo=datetime.timezone.utc)}
def wrapper(execute,sql,params,many,context):
 global fired_series,fired_profiles,mutating,new_episode
 result=execute(sql,params,many,context)
 if mutating:return result
 queries.append({'sql':sql,'params_repr':repr(params)[:1000]})
 if not fired_series and sql.startswith('SELECT "vod_series".') and 'WHERE "vod_series"."id" = %s' in sql:
  fired_series=True;mutating=True
  try:
   Series.objects.filter(pk=show.pk).update(name=changed['Series_name'])
   Episode.objects.filter(pk=first.pk).update(name=changed['Episode_name'])
   VODLogo.objects.filter(pk=logo.pk).update(name=changed['Logo_name'],url=changed['Logo_url'])
   M3UAccount.objects.filter(pk=account.pk).update(is_active=False)
   M3UEpisodeRelation.objects.filter(pk=relation.pk).update(custom_properties=changed['Relation_properties'])
   new_episode=Episode.objects.bulk_create([Episode(series=show,uuid=uuid.UUID('00000000-0000-4000-8000-000000000013'),name=prefix+'NEW_AFTER_PREFETCH',season_number=9,episode_number=99,custom_properties={'controlled':True})])[0]
   events.append({'phase':'after_first_fresh_Series_FK_SELECT_reply','new_episode':Episode.objects.filter(pk=new_episode.pk).values().get()})
  finally:mutating=False
 if not fired_profiles and sql.startswith('SELECT "m3u_m3uaccountprofile".'):
  fired_profiles=True;mutating=True
  try:M3UAccountProfile.objects.filter(pk=profile.pk).update(exp_date=changed['Profile_expiry']);events.append({'phase':'after_first_declared_profiles_SELECT_reply'})
  finally:mutating=False
 return result
path=f'/api/vod/series/{show.pk}/episodes/';request=APIRequestFactory().get(path,HTTP_HOST='localhost',HTTP_ACCEPT='application/json',HTTP_AUTHORIZATION='Bearer '+str(AccessToken.for_user(user)),REMOTE_ADDR='127.0.0.1');match=resolve(path);request.resolver_match=match
try:
 with connection.execute_wrapper(wrapper):response=match.func(request,*match.args,**match.kwargs);response.render();body=response.content;status=response.status_code;mime=response.get('Content-Type');response.close()
 assert fired_series and fired_profiles and status==200
 data=json.loads(body);assert len(data)==3
 first_body=next(row for row in data if row['id']==first.pk);nested=first_body['providers'][0];assert first_body['name']==saved['Episode']['name'] and nested['episode']['name']==saved['Episode']['name']
 assert first_body['series']['name']==saved['Series']['name'] and nested['episode']['series']['name']==saved['Series']['name']
 assert first_body['series']['logo']['name']==changed['Logo_name'] and first_body['series']['episode_count']==4
 assert nested['m3u_account']['is_active']is True and nested['custom_properties']==saved['M3UEpisodeRelation']['custom_properties']
 assert nested['m3u_account']['profiles'][0]['exp_date']is None
 assert nested['m3u_account']['earliest_expiration']=='2027-05-06T01:02:03Z'and nested['m3u_account']['exp_date']=='2027-05-06T01:02:03Z'
 assert nested['quality_info']is None
 report={'passed':True,'scope':'Original APIRequestFactory +actualSQL controlled phase mutation, NOT Source network or native acceptance','path':path,'expected':{'status':status,'MIME':mime,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body_text':body.decode(),'json':data},'events':events,'query_observations':queries,'fixture_ids':{'series':show.pk,'episode':first.pk,'logo':logo.pk,'account':account.pk,'relation':relation.pk,'profile':profile.pk},'changes':changed,'snapshot_vs_fresh_checks':True}
finally:
 for model,pk,name in [(Series,show.pk,'Series'),(Episode,first.pk,'Episode'),(VODLogo,logo.pk,'VODLogo'),(M3UAccount,account.pk,'M3UAccount'),(M3UEpisodeRelation,relation.pk,'M3UEpisodeRelation'),(M3UAccountProfile,profile.pk,'M3UAccountProfile')]:model.objects.filter(pk=pk).update(**saved[name])
 if new_episode is not None:
  with connection.cursor()as cursor:cursor.execute('DELETE FROM vod_episode WHERE id=%s',[new_episode.pk])
assert fingerprint()==before,'Full-microsecond18table restoration failed'
report.update(full_state_before=before,full_state_after=fingerprint(),owned_metadata_restored=True,provider_refresh_or_remote_contact=False)
print(json.dumps(report,cls=ExactEncoder,ensure_ascii=False))
