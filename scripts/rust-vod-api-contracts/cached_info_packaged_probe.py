"""Actual Original cached provider-info oracle; local deny trap, no fake refresh.
Refresh-required cases deliberately excluded pending their own side-effect gate.
All application and algorithm credit belongs to original Dispatcharr authors.
"""
import datetime,hashlib,http.client,json,os,sys,threading,concurrent.futures
from django.db import connection,transaction
from http.server import BaseHTTPRequestHandler,HTTPServer
from urllib.parse import quote
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
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
prefix='rust-probe-vod-rest-';users={role:User.objects.get(username=prefix+role)for role in ['admin','standard','basic','movies_off','series_off','adult_hidden']};accounts=list(M3UAccount.objects.filter(name__startswith=prefix).order_by('id'));assert len(accounts)==3 and all(a.server_url.startswith('http://127.0.0.1:')for a in accounts)
movie=Movie.objects.filter(name__startswith=prefix).order_by('id').first();show=Series.objects.filter(name__startswith=prefix).order_by('id').first();movie_relations=list(M3UMovieRelation.objects.filter(movie=movie).order_by('id'));series_relation=M3USeriesRelation.objects.get(series=show);primary=movie_relations[0];models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('id').values()),sort_keys=True,separators=(',',':'),cls=ExactEncoder).encode()).hexdigest()for m in models}

source=json.load(sys.stdin);assert source['passed']and len(source['records'])==90 and not source['provider_trap_attempts'];before=fingerprint();records=[];differences=[]
def current():return {'movie':Movie.objects.filter(pk=movie.pk).values().get(),'series':Series.objects.filter(pk=show.pk).values().get(),'movie_relations':list(M3UMovieRelation.objects.filter(movie=movie).order_by('id').values()),'series_relation':M3USeriesRelation.objects.filter(pk=series_relation.pk).values().get()}
original=current()
def restore(metadata):
 with transaction.atomic(),connection.cursor()as cursor:
  for table,rows in [('vod_movie',[metadata['movie']]),('vod_series',[metadata['series']]),('vod_m3umovierelation',metadata['movie_relations']),('vod_m3useriesrelation',[metadata['series_relation']])]:
   for row in rows:
    assert all(key.replace('_','').isalnum()for key in row)
    fields=','.join('"'+key+'"'for key in row)
    cursor.execute(f'UPDATE {table} SET ({fields})=(SELECT {fields} FROM json_populate_record(NULL::{table},%s::json)) WHERE id=%s',[json.dumps(row,cls=ExactEncoder),row['id']])
def request(host,port,case):
 headers={'Host':'localhost','Accept':case['accept']}
 if case['role']is not None:headers['Authorization']='Bearer '+str(AccessToken.for_user(users[case['role']]))
 client=http.client.HTTPConnection(host,port,timeout=15)
 try:
  client.request(case['method'],case['path'],headers=headers);response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024
  return {'status':response.status,'headers':{key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']},'body_text':body.decode(),'body_sha256':hashlib.sha256(body).hexdigest()}
 finally:client.close()
sentinel=request('127.0.0.1',5656,{'role':'standard','accept':'application/json','method':'GET','path':f'/api/vod/movies/{movie.pk}/'})
assert sentinel['status']==200 and json.loads(sentinel['body_text'])['uuid']==str(movie.uuid)
try:
 for index,case in enumerate(source['records']):
  restore(case['private_pre_request_metadata']);unchanged=fingerprint();actual=request('candidate',9192,case);expected={key:case['expected'][key]for key in actual};fields=[key for key in actual if actual[key]!=expected[key]];assert fingerprint()==unchanged,('read changed full18 rows',index)
  record={'index':index,'path':case['path'],'role':case['role'],'method':case['method'],'expected':expected,'actual':actual,'differences':fields};records.append(record)
  if fields:differences.append(record)
 restore(original);assert fingerprint()==before
 # Both services read the identical restored rows; no expected-body refresh or
 # response sorting/normalization. This is burst correctness, not performance.
 selected=[row for row in source['records']if row['role']=='standard'and row['method']=='GET'and row['label']=='baseline'and row['query']==''];assert len(selected)==2;bursts=[]
 for host,port in [('127.0.0.1',5656),('candidate',9192)]:
  barrier=threading.Barrier(11)
  def simultaneous(index):
   case=selected[index%2];barrier.wait(timeout=5);actual=request(host,port,case);expected={key:case['expected'][key]for key in actual};return {'index':index,'actual':actual,'matched':actual==expected}
  with concurrent.futures.ThreadPoolExecutor(max_workers=10)as executor:
   futures=[executor.submit(simultaneous,index)for index in range(10)];barrier.wait(timeout=5);rows=[future.result(timeout=20)for future in futures]
  bursts.append(rows);assert fingerprint()==before
finally:restore(original)
assert fingerprint()==before
print(json.dumps({'passed':not differences and all(row['matched']for burst in bursts for row in burst),'compared':len(records),'scope':'actual canonical packaged cached-info90 TCP and Source10/candidate10; no remote refresh/image/browser/performance acceptance','records':records,'differences':differences,'Source_unstaggered10':bursts[0],'candidate_unstaggered10':bursts[1],'Source_database':connection.settings_dict['NAME'],'Source_fixture_uuid':str(movie.uuid),'full18_exact_before':before,'full18_exact_after':fingerprint(),'owned_metadata_restored':True},cls=ExactEncoder,ensure_ascii=False))
