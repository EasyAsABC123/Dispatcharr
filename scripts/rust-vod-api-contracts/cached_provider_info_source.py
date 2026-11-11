"""Actual Original cached provider-info oracle; local deny trap, no fake refresh.
Refresh-required cases deliberately excluded pending their own side-effect gate.
All application and algorithm credit belongs to original Dispatcharr authors.
"""
import datetime,hashlib,http.client,json,os,sys,threading
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
before=fingerprint();saved_accounts={a.pk:a.server_url for a in accounts};saved_movie={key:getattr(movie,key)for key in ['custom_properties','year','rating','duration_secs','genre']};saved_series={key:getattr(show,key)for key in ['custom_properties','genre']};saved_relations={r.pk:{key:getattr(r,key)for key in ['custom_properties','last_advanced_refresh']}for r in movie_relations};saved_series_relation={key:getattr(series_relation,key)for key in ['custom_properties','last_episode_refresh']};attempts=[];records=[]
class Trap(BaseHTTPRequestHandler):
 def deny(self):
  attempts.append({'method':self.command,'path':self.path[:1024]});self.send_response(503);self.send_header('Content-Length','0');self.end_headers()
 do_GET=deny;do_POST=deny;do_HEAD=deny
 def log_message(self,*args):pass
trap=HTTPServer(('127.0.0.1',0),Trap);thread=threading.Thread(target=trap.serve_forever,daemon=True);thread.start()
def state():return {'movie':Movie.objects.filter(pk=movie.pk).values().get(),'series':Series.objects.filter(pk=show.pk).values().get(),'movie_relations':list(M3UMovieRelation.objects.filter(movie=movie).order_by('id').values()),'series_relation':M3USeriesRelation.objects.filter(pk=series_relation.pk).values().get()}
def case(endpoint,query='',role='standard',method='GET',label='baseline',accept='application/json'):
 assert len(records)<180
 obj=movie if endpoint=='movies'else show;path=f'/api/vod/{endpoint}/{obj.pk}/provider-info/'+query;path=quote(path,safe='/?=&|,');headers={'Host':'localhost','Accept':accept}
 if role:headers['Authorization']='Bearer '+str(AccessToken.for_user(users[role]))
 captured=state();client=http.client.HTTPConnection('127.0.0.1',5656,timeout=15)
 try:client.request(method,path,headers=headers);response=client.getresponse();body=response.read(2*1024*1024+1);assert len(body)<=2*1024*1024;expected={'status':response.status,'headers':{key:response.getheader(key)for key in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate']},'body_text':body.decode(),'body_sha256':hashlib.sha256(body).hexdigest()}
 finally:client.close()
 assert not attempts,'Cached-only request unexpectedly attempted provider access'
 try:expected['json']=json.loads(body)
 except ValueError:pass
 records.append({'path':path,'endpoint':endpoint,'query':query,'role':role,'method':method,'accept':accept,'label':label,'observation':'actual Original TCP, unpatched views/renderers/auth','expected':expected,'private_pre_request_metadata':captured})
try:
 M3UAccount.objects.filter(pk__in=saved_accounts).update(server_url='http://127.0.0.1:'+str(trap.server_port))
 for endpoint in ['movies','series']:
  for role in ['admin','standard','basic','movies_off','series_off','adult_hidden',None]:case(endpoint,role=role)
  for method in ['HEAD','OPTIONS','POST','PATCH','DELETE']:
   for role in ['admin','standard','basic',None]:case(endpoint,role=role,method=method)
  for query in ['?relation_id=bad','?relation_id=','?relation_id=-1','?relation_id=999999999','?m3u_account=bad','?search=definitely-missing','?force_refresh=false','?page=99999']:
   case(endpoint,query)
  for accept in ['application/json; indent=4','application/xml']:case(endpoint,accept=accept,label='representation')
 for relation in movie_relations:case('movies','?relation_id='+str(relation.pk),label='specific_provider')
 for query in ['?include_episodes=false','?include_episodes=TRUE','?include_episodes=0','?include_episodes=','?include_episodes=false&include_episodes=true','?refresh_interval=bad','?refresh_interval=1000000000000000000000000000']:
  case('series',query,label='cached_series_parameters')
 Movie.objects.filter(pk=movie.pk).update(year=None,rating=None,duration_secs=None,genre='',custom_properties={})
 rich={'name':'Provider title','o_name':None,'description':'Remote description','plot':None,'year':'2024','release_date':'2024-02-03','genre':'Drama','director':['A','B'],'actors':'Cast','country':'US','rating':4.5,'tmdb_id':8,'imdb_id':'tt123','youtube_trailer':'trailer-id','duration_secs':'123','age':18,'bitrate':'4500','video':{'width':1920},'audio':{'codec':'aac'}}
 for label,info in [('rich',rich),('loose_types',dict(rich,name=0,rating=False,year=[],duration_secs=[],bitrate=[1],video=[1],audio=False)),('null_info',None),('list_info',[1]),('string_info','bad')]:
  props={'detailed_fetched':True,'detailed_info':info,'movie_data':{'container_extension':'mkv','direct_source':None,'category_id':9,'added':1234},'movie_image':'https://fixture.invalid/cover.jpg','backdrop_path':['https://fixture.invalid/back.jpg']}
  M3UMovieRelation.objects.filter(pk=primary.pk).update(custom_properties=props,last_advanced_refresh=timezone.now());case('movies','?relation_id='+str(primary.pk),label='movie_info_'+label)
 M3UMovieRelation.objects.filter(pk=primary.pk).update(**saved_relations[primary.pk]);Movie.objects.filter(pk=movie.pk).update(**saved_movie)
 for label,props in [('false_text_flags',{'episodes_fetched':'false','detailed_fetched':'false'}),('unhashable_flag',{'episodes_fetched':{'x':1},'detailed_fetched':True})]:
  M3USeriesRelation.objects.filter(pk=series_relation.pk).update(custom_properties=props,last_episode_refresh=timezone.now());case('series',label='series_'+label)
finally:
 for key,value in saved_accounts.items():M3UAccount.objects.filter(pk=key).update(server_url=value)
 Movie.objects.filter(pk=movie.pk).update(**saved_movie);Series.objects.filter(pk=show.pk).update(**saved_series)
 for key,values in saved_relations.items():M3UMovieRelation.objects.filter(pk=key).update(**values)
 M3USeriesRelation.objects.filter(pk=series_relation.pk).update(**saved_series_relation)
 trap.shutdown();trap.server_close();thread.join(timeout=2)
assert fingerprint()==before
print(json.dumps({'passed':True,'scope':'actual Original cached-provider-info TCP only, no native policy/remote refresh acceptance','records':records,'provider_trap_attempts':attempts,'full18_exact_microseconds_before':before,'full18_exact_microseconds_after':fingerprint(),'owned_metadata_restored':True,'explicit_excluded':'force_refresh=true /stale or absent fetched flags requiring real provider mutation are separate side-effect capability'},cls=ExactEncoder,ensure_ascii=False))
