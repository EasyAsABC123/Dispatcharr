"""Guarded Original provider-actions oracle; no native or remote-refresh acceptance.
Original Dispatcharr authors retain application credit. Source Factory and real
network observations remain distinct; owned metadata is restored exactly.
"""
import datetime,hashlib,http.client,json,os,sys,copy
from pathlib import Path
from urllib.parse import quote
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
mode=os.environ.get('VOD_ACTION_MODE');assert mode in ['factory','http']
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction
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

payload=json.load(sys.stdin);source=payload['source'];policy=payload['policy'];assert source['passed']and source['mode']=='http'and len(source['records'])==150
fresh_indices=policy['case_indices'];assert len(fresh_indices)==25 and len(set(fresh_indices))==25
accounts_ids=[a.pk for a in accounts];movie=movies[0];before=fingerprint();records=[];differences=[];fresh=[]
def capture():
 state={m._meta.db_table:list(m.objects.filter(m3u_account__in=accounts).order_by('pk').values())for m in [M3UAccountProfile,M3UFilter,ChannelGroupM3UAccount,M3UEpisodeRelation]}
 state[ChannelGroup._meta.db_table]=list(ChannelGroup.objects.filter(name__startswith=PREFIX).order_by('pk').values());state[Stream._meta.db_table]=list(Stream.objects.filter(name__startswith=PREFIX).order_by('pk').values());state['movie_relation']=list(M3UMovieRelation.objects.filter(movie=movie).order_by('pk').values('id','custom_properties','category_id'));state['account_preferences']=list(M3UAccount.objects.filter(pk__in=accounts_ids).order_by('pk').values('id','custom_properties'));return state
original=capture()
TABLES=[('m3u_m3uaccountprofile','m3u_account_id=ANY(%s)'),('m3u_m3ufilter','m3u_account_id=ANY(%s)'),('dispatcharr_channels_channelgroup',"name LIKE 'rust-probe-vod-rest-%%'"),('dispatcharr_channels_channelgroupm3uaccount','m3u_account_id=ANY(%s)'),('dispatcharr_channels_stream',"name LIKE 'rust-probe-vod-rest-%%'"),('vod_m3uepisoderelation','m3u_account_id=ANY(%s)')]
def restore(state):
 with transaction.atomic(),connection.cursor()as cursor:
  for index in [5,4,3,2,1,0]:
   table,predicate=TABLES[index];cursor.execute('DELETE FROM '+table+' WHERE '+predicate,[accounts_ids]if'%s'in predicate else None)
  for index in [2,0,1,3,4,5]:
   table,_=TABLES[index];cursor.execute(f'INSERT INTO {table} SELECT * FROM json_populate_recordset(NULL::{table},%s::json)',[json.dumps(state[table],cls=ExactEncoder)])
  for row in state['movie_relation']:cursor.execute('UPDATE vod_m3umovierelation SET custom_properties=%s::jsonb,category_id=%s WHERE id=%s',[json.dumps(row['custom_properties'])if row['custom_properties']is not None else None,row['category_id'],row['id']])
  for row in state['account_preferences']:cursor.execute('UPDATE m3u_m3uaccount SET custom_properties=%s::jsonb WHERE id=%s',[json.dumps(row['custom_properties'])if row['custom_properties']is not None else None,row['id']])
def request(host,port,case):
 headers={'Host':'localhost','Accept':case['accept']}
 if case['role']is not None:headers['Authorization']='Bearer '+str(AccessToken.for_user(users[case['role']]))
 client=http.client.HTTPConnection(host,port,timeout=15)
 try:
  client.request(case['method'],case['path'],headers=headers);response=client.getresponse();body=response.read(4*1024*1024+1);assert len(body)<=4*1024*1024
  result={'status':response.status,'headers':{key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']},'body_text':body.decode(),'body_sha256':hashlib.sha256(body).hexdigest(),'body_bytes':len(body)}
  try:result['json']=json.loads(body)
  except ValueError:pass
  return result
 finally:client.close()
def stable(rows):
 result={}
 for row in rows:
  row=copy.deepcopy(row)
  for group in row['m3u_account']['channel_groups']:group.pop('stream_count')
  result[str(row['id'])]=row
 return result
def context_counts(rows):
 first=rows[0]['m3u_account']['id']
 for row in rows:
  account=row['m3u_account']['id']
  for group in row['m3u_account']['channel_groups']:
   expected=Stream.objects.filter(m3u_account_id=account,channel_group_id=group['channel_group']).count()if account==first else 0
   assert group['stream_count']==expected
sentinel=request('127.0.0.1',5656,{'role':'standard','accept':'application/json','method':'GET','path':f'/api/vod/movies/{movie.pk}/'})
assert sentinel['status']==200 and sentinel['json']['uuid']==str(movie.uuid)and sentinel['json']['name']==movie.name
try:
 for index,case in enumerate(source['records']):
  restore(case['private_pre_request_native_replay_state']);unchanged=fingerprint();expected=case['expected']
  if index in fresh_indices:
   assert case['method']=='GET'and'/movies/'in case['path']and'/providers/'in case['path']
   expected=request('127.0.0.1',5656,case);assert expected['status']==200;assert stable(expected['json'])==stable(case['expected']['json']);context_counts(expected['json']);assert fingerprint()==unchanged
   fresh.append({'index':index,'historical':case['expected'],'fresh_actual_Source':expected,'no_intervening_state_change':True})
  actual=request('candidate',9192,case);fields=[key for key in ['status','headers','body_text','body_sha256','body_bytes']if actual[key]!=expected[key]]
  assert fingerprint()==unchanged,('request changed full18 rows',index)
  record={'index':index,'path':case['path'],'role':case['role'],'method':case['method'],'expected':expected,'actual':actual,'differences':fields};records.append(record)
  if fields:differences.append(record)
finally:restore(original)
assert fingerprint()==before
print(json.dumps({'passed':not differences,'scope':'actual canonical packaged provider-action150 TCP, fixed25 same-state freshSource; no cached-info/image/React/performance acceptance','compared':len(records),'records':records,'differences':differences,'predeclared_fresh_references':fresh,'Source_database':connection.settings_dict['NAME'],'Source_fixture_uuid':str(movie.uuid),'Source_sentinel':sentinel,'full18_before':before,'full18_after':fingerprint(),'owned_metadata_restored':True},cls=ExactEncoder,ensure_ascii=False))
