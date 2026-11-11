"""Guarded Original provider-actions oracle; no native or remote-refresh acceptance.
Original Dispatcharr authors retain application credit. Source Factory and real
network observations remain distinct; owned metadata is restored exactly.
"""
import datetime,hashlib,http.client,json,os,sys
from pathlib import Path
from urllib.parse import quote
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
mode=os.environ.get('VOD_ACTION_MODE');assert mode in ['factory','http']
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.urls import resolve
from django.core.serializers.json import DjangoJSONEncoder
from rest_framework.test import APIRequestFactory
from rest_framework_simplejwt.tokens import AccessToken
class ExactEncoder(DjangoJSONEncoder):
 def default(self,value):
  if isinstance(value,(datetime.datetime,datetime.date,datetime.time)):return value.isoformat()
  return super().default(value)
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings,SystemEvent
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
PREFIX='rust-probe-vod-rest-'
users={role:User.objects.get(username=PREFIX+role)for role in ['admin','standard','basic','movies_off','series_off','adult_hidden']}
accounts=list(M3UAccount.objects.filter(name__startswith=PREFIX).order_by('id'));assert len(accounts)==3 and all(a.server_url.startswith('http://127.0.0.1:')for a in accounts)
movies=list(Movie.objects.filter(name__startswith=PREFIX).order_by('id'));series=list(Series.objects.filter(name__startswith=PREFIX).order_by('id'));assert len(movies)>=4 and len(series)==4
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('pk').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}
before=fingerprint();factory=APIRequestFactory();records=[]
def case(path,role='standard',method='GET',label='baseline',accept='application/json'):
 assert len(records)<260 and path.startswith('/api/vod/');path=quote(path,safe='/?=&|,');token='Bearer '+str(AccessToken.for_user(users[role]))if role else None
 # Private controlled mutation state for a future native replay; serialized
 # before the request, outside Factory SQL tracing. No expectation refresh.
 state={m._meta.db_table:list(m.objects.filter(m3u_account__in=accounts).order_by('pk').values())for m in [M3UAccountProfile,M3UFilter,ChannelGroupM3UAccount,M3UEpisodeRelation]}
 state[ChannelGroup._meta.db_table]=list(ChannelGroup.objects.filter(name__startswith=PREFIX).order_by('pk').values())
 state[Stream._meta.db_table]=list(Stream.objects.filter(name__startswith=PREFIX).order_by('pk').values())
 state['movie_relation']=list(M3UMovieRelation.objects.filter(movie=movies[0]).order_by('pk').values('id','custom_properties','category_id'))
 state['account_preferences']=list(M3UAccount.objects.filter(pk__in=[a.pk for a in accounts]).order_by('pk').values('id','custom_properties'))
 observed={};body=None;sql=[]
 if mode=='http':
  client=http.client.HTTPConnection('127.0.0.1',5656,timeout=15)
  headers={'Host':'localhost','Accept':accept}
  if token:headers['Authorization']=token
  try:
   client.request(method,path,headers=headers);response=client.getresponse();body=response.read(4*1024*1024+1);assert len(body)<=4*1024*1024
   observed={'status':response.status,'headers':{k:response.getheader(k)for k in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']}}
  finally:client.close()
 else:
  def wrapper(execute,text,params,many,context):
   sql.append({'sql':text,'params_repr':repr(params)[:2000]});return execute(text,params,many,context)
  headers={'HTTP_HOST':'localhost','HTTP_ACCEPT':accept,'REMOTE_ADDR':'127.0.0.1'}
  if token:headers['HTTP_AUTHORIZATION']=token
  request=factory.generic(method,path,**headers);match=resolve(path.split('?')[0]);request.resolver_match=match
  with connection.execute_wrapper(wrapper):
   try:
    response=match.func(request,*match.args,**match.kwargs);response.render();body=response.content;observed={'status':response.status_code,'headers':{k:response.get(k)for k in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']}};response.close()
   except Exception as error:observed={'unhandled':type(error).__name__,'message':str(error)}
 if body is not None:
  observed.update(body_sha256=hashlib.sha256(body).hexdigest(),body_bytes=len(body),body_text=body.decode('utf-8'))
  try:observed['json']=json.loads(body)
  except ValueError:pass
 records.append({'path':path,'role':role,'method':method,'accept':accept,'label':label,'observation':'actual Original TCP'if mode=='http'else'Original APIRequestFactory,notnetwork','expected':observed,'private_pre_request_native_replay_state':state,'Factory_SQL':sql if mode=='factory'else None})
# Explicitly only these pure read actions. Provider-info refresh is a separate gate.
paths=[f'/api/vod/movies/{movies[0].pk}/providers/',f'/api/vod/series/{series[0].pk}/providers/',f'/api/vod/series/{series[0].pk}/episodes/']
for path in paths:
 for role in ['admin','standard','basic','movies_off','series_off','adult_hidden',None]:case(path,role)
 for method in ['HEAD','OPTIONS','POST','PUT','PATCH','DELETE']:
  for role in ['admin','standard','basic',None]:case(path,role,method)
 for suffix in ['?search=definitely-missing','?name=definitely-missing','?m3u_account=bad','?category=unmatched','?page=999999','?page_size=1','?ordering=-name','?format=json']:
  case(path+suffix,label='get_object_filter_or_action_query')
 for accept in ['application/json; indent=4','application/xml']:case(path,accept=accept,label='representation')
for endpoint in ['movies','series']:
 case(f'/api/vod/{endpoint}/999999999/providers/',label='missing')
 case(f'/api/vod/{endpoint}/bad/providers/',label='bad_lookup')
for role in ['admin','standard','adult_hidden']:
 case(f'/api/vod/movies/{movies[3].pk}/providers/',role,label='adult_or_inactive')
# Source includes every series episode, even when no active provider relation.
show=series[0];episodes=list(Episode.objects.filter(series=show).order_by('id'));assert len(episodes)==3
relation=M3UMovieRelation.objects.filter(movie=movies[0],m3u_account__is_active=True).first();saved=relation.custom_properties
try:
 for props in [{'quality':'Custom 8K','resolution':'ignored'},{'quality':'','resolution':'800x600'},{},None,False,[],{'quality':8},{'quality':[1,2]},'truthy-nonmapping']:
  M3UMovieRelation.objects.filter(pk=relation.pk).update(custom_properties=props);case(paths[0],label='movie_quality_'+repr(props))
finally:M3UMovieRelation.objects.filter(pk=relation.pk).update(custom_properties=saved)
# Predeclared nested DTO controls, independently restored before final hashes.
active=accounts[0];inactive=accounts[2]
assert active.is_active and not inactive.is_active
profiles=[];filters=[];groups=[];group_relations=[];extra_episode_relation=[];streams=[]
account_props=active.custom_properties
category_id=relation.category_id
try:
 profiles=M3UAccountProfile.objects.bulk_create([
  M3UAccountProfile(m3u_account=active,name=PREFIX+'extra-active',max_streams=3,is_active=True,is_default=False,current_viewers=2,search_pattern='^(.+)$',replace_pattern='prefix-$1',custom_properties={'extra':True},exp_date=datetime.datetime(2027,1,2,3,4,5,123456,tzinfo=datetime.timezone.utc)),
  M3UAccountProfile(m3u_account=active,name=PREFIX+'extra-inactive',max_streams=1,is_active=False,is_default=False,search_pattern='',replace_pattern='',custom_properties=None,exp_date=datetime.datetime(2026,1,2,tzinfo=datetime.timezone.utc))])
 filters=M3UFilter.objects.bulk_create([M3UFilter(m3u_account=active,filter_type='url',regex_pattern='é.*',exclude=False,order=3,custom_properties={'extra':1}),M3UFilter(m3u_account=active,filter_type='name',regex_pattern='Z',exclude=True,order=1,custom_properties=None)])
 groups=ChannelGroup.objects.bulk_create([ChannelGroup(name=PREFIX+'extra-provider-group')])
 group_relations=ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(m3u_account=active,channel_group=groups[0],enabled=False,auto_channel_sync=False,auto_sync_channel_start=10.5,auto_sync_channel_end=99.75,custom_properties={'channel_profile_ids':[1,2]})])
 group_relations+=ChannelGroupM3UAccount.objects.bulk_create([ChannelGroupM3UAccount(m3u_account=accounts[1],channel_group=groups[0],enabled=True,custom_properties={})])
 streams=Stream.objects.bulk_create([Stream(name=PREFIX+'count-0',url='http://127.0.0.1:1/no-contact',m3u_account=active,channel_group=groups[0]),Stream(name=PREFIX+'count-1',url='http://127.0.0.1:1/no-contact',m3u_account=accounts[1],channel_group=groups[0]),Stream(name=PREFIX+'count-2',url='http://127.0.0.1:1/no-contact',m3u_account=accounts[1],channel_group=groups[0])])
 extra_episode_relation=M3UEpisodeRelation.objects.bulk_create([M3UEpisodeRelation(episode=episodes[2],m3u_account=inactive,series_relation=M3USeriesRelation.objects.filter(series=show).first(),stream_id='inactive-only-probe',container_extension='mkv',custom_properties={'quality':'inactive hidden'})])
 for path in paths:
  for role in ['admin','basic']:case(path,role,label='populated_nested_account_inactive_only_episode')
 for props in [{},None,False,{'enable_vod':False,'auto_enable_new_groups_live':False,'hash_key':'name,url'}]:
  M3UAccount.objects.filter(pk=active.pk).update(custom_properties=props);case(paths[0],label='nested_account_properties_'+repr(props))
 M3UMovieRelation.objects.filter(pk=relation.pk).update(category_id=None);case(paths[0],label='nullable_category')
finally:
 M3UMovieRelation.objects.filter(pk=relation.pk).update(category_id=category_id)
 M3UAccount.objects.filter(pk=active.pk).update(custom_properties=account_props)
 for model,rows in [(Stream,streams),(M3UEpisodeRelation,extra_episode_relation),(ChannelGroupM3UAccount,group_relations),(ChannelGroup,groups),(M3UFilter,filters),(M3UAccountProfile,profiles)]:model.objects.filter(pk__in=[row.pk for row in rows]).delete()
assert fingerprint()==before,'Read actions changed/restoration failed'
print(json.dumps({'passed':True,'mode':mode,'records':records,'fullmicro18table_before':before,'fullmicro18table_after':fingerprint(),'owned_metadata_restored':True,'fingerprint_datetime_codec':'exact isoformat,including all microseconds','provider_info_or_refresh_executed':False,'source_hashes':{p:hashlib.sha256(Path('/app/'+p).read_bytes()).hexdigest()for p in ['apps/vod/api_views.py','apps/vod/serializers.py','apps/m3u/serializers.py','apps/accounts/permissions.py']}},cls=ExactEncoder,ensure_ascii=False))
