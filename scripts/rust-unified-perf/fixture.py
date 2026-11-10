"""Guarded original ORM VOD benchmark fixture; never contacts providers.
Original Dispatcharr authors retain application credit. Replies contain private JWT.
"""
import datetime,hashlib,json,os,sys,uuid
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_PERF_DISPOSABLE')!='playback-fixture-only':raise RuntimeError('Explicit disposable VOD fixture guards required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction
from django.utils import timezone
from rest_framework_simplejwt.tokens import AccessToken
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from core.models import CoreSettings
request=json.load(sys.stdin)
if set(request)!={'op'}or request['op']not in ['seed','phase']:raise RuntimeError('Only fixed seed/phase operations')
prefix='rust-probe-vod-perf-';at=datetime.datetime(2026,11,10,tzinfo=datetime.timezone.utc)
if request['op']=='seed':
 if User.objects.filter(username__startswith=prefix).exists()or Movie.objects.exists()or Series.objects.exists()or Episode.objects.exists():raise RuntimeError('Fresh VOD catalogue required')
 with transaction.atomic():
  User.objects.bulk_create([User(username=prefix+'standard',password='!',user_level=1,is_active=True,custom_properties={'vod_movies_enabled':True,'vod_series_enabled':True,'hide_adult_content':False})])
  accounts=M3UAccount.objects.bulk_create([M3UAccount(name=prefix+str(i),account_type='XC',is_active=i<2,priority=i,server_url='http://origin:8080/vod-never-refresh',custom_properties={'enable_vod':True})for i in range(3)])
  M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=a,name=prefix+'default',is_default=True,is_active=True,search_pattern='^(.*)$',replace_pattern='$1',max_streams=0)for a in accounts])
  logos=VODLogo.objects.bulk_create([VODLogo(name=prefix+'logo'+str(i),url='https://never-requested.invalid/vod-perf/logo-'+str(i)+'.png')for i in range(25)])
  categories=VODCategory.objects.bulk_create([VODCategory(name=prefix+'category'+str(i),category_type='movie'if i<10 else'series')for i in range(15)])
  M3UVODCategoryRelation.objects.bulk_create([M3UVODCategoryRelation(category=c,m3u_account=a,enabled=True,custom_properties={})for c in categories for a in accounts])
  def content(model,kind):
   return model.objects.bulk_create([model(uuid=uuid.uuid5(uuid.NAMESPACE_URL,prefix+kind+str(i)),name=prefix+kind+f'{i:05}'+(' café東京'if i%10==0 else''),year=2000+i%25,description='Owned catalogue A & B < C 東京 '+('bounded metadata '*8),genre='Drama',rating='7.50',language='en',logo=logos[i%25]if i%4==0 else None,custom_properties={'fixture':i,'nil':None,'unicode':'café東京'},**({'duration_secs':7200,'is_adult':i%7==0}if model is Movie else{}))for i in range(5000)],batch_size=500)
  movies=content(Movie,'Movie');series=content(Series,'Series')
  movie_links=[];series_links=[];episode_links=[]
  for i,movie in enumerate(movies):
   selected=[accounts[0]if i%10 else accounts[2]]+([accounts[1]]if i%10==1 else[])
   movie_links.extend(M3UMovieRelation(movie=movie,m3u_account=a,stream_id=str(i),category=categories[i%10],container_extension='mp4',custom_properties={'detailed_fetched':True},last_advanced_refresh=at,last_seen=at)for a in selected)
  for i,show in enumerate(series):
   selected=[accounts[0]if i%10 else accounts[2]]+([accounts[1]]if i%10==1 else[])
   series_links.extend(M3USeriesRelation(series=show,m3u_account=a,external_series_id=str(i),category=categories[10+i%5],custom_properties={'episodes_fetched':True},last_episode_refresh=at,last_seen=at)for a in selected)
  M3UMovieRelation.objects.bulk_create(movie_links,batch_size=500);M3USeriesRelation.objects.bulk_create(series_links,batch_size=500)
  primary={row.series_id:row for row in series_links if row.m3u_account_id!=accounts[1].id}
  for start in range(0,5000,100):
   batch=[Episode(uuid=uuid.uuid5(uuid.NAMESPACE_URL,prefix+'episode'+str(i)+'-'+str(n)),series=series[i],name=prefix+f'Episode{i:05}-{n:02}',season_number=1,episode_number=n+1,duration_secs=1800,custom_properties={'fixture':i,'n':n})for i in range(start,start+100)for n in range(10)]
   Episode.objects.bulk_create(batch,batch_size=500)
   M3UEpisodeRelation.objects.bulk_create([M3UEpisodeRelation(episode=e,m3u_account_id=primary[e.series_id].m3u_account_id,series_relation=primary[e.series_id],stream_id=str(e.id),container_extension='mp4')for e in batch],batch_size=500)
  for model in [Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation]:
   model.objects.filter(**({'name__startswith':prefix}if model in [Movie,Series,Episode]else{'m3u_account__name__startswith':prefix})).update(created_at=at,updated_at=at)
 with connection.cursor()as cursor:
  for model in [Movie,Series,Episode,VODLogo,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation]:cursor.execute('ANALYZE '+connection.ops.quote_name(model._meta.db_table))
user=User.objects.get(username=prefix+'standard');digest=hashlib.sha256()
for model,query in [(Movie,Movie.objects.filter(name__startswith=prefix)),(Series,Series.objects.filter(name__startswith=prefix)),(Episode,Episode.objects.filter(name__startswith=prefix)),(VODLogo,VODLogo.objects.filter(name__startswith=prefix)),(M3UAccount,M3UAccount.objects.filter(name__startswith=prefix)),(M3UMovieRelation,M3UMovieRelation.objects.filter(m3u_account__name__startswith=prefix)),(M3USeriesRelation,M3USeriesRelation.objects.filter(m3u_account__name__startswith=prefix)),(M3UEpisodeRelation,M3UEpisodeRelation.objects.filter(m3u_account__name__startswith=prefix)),(CoreSettings,CoreSettings.objects.all())]:
 digest.update(model._meta.db_table.encode())
 for row in query.order_by('id').values().iterator(chunk_size=512):digest.update(json.dumps(row,sort_keys=True,separators=(',',':'),default=str).encode()+b'\n')
with connection.cursor()as cursor:
 cursor.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public' ORDER BY table_name,ordinal_position");schema=hashlib.sha256(json.dumps(cursor.fetchall(),separators=(',',':'),default=str).encode()).hexdigest()
print(json.dumps({'movies':5000,'series':5000,'episodes':50000,'active_movies':Movie.objects.filter(m3u_relations__m3u_account__is_active=True).distinct().count(),'active_series':Series.objects.filter(m3u_relations__m3u_account__is_active=True).distinct().count(),'fixture_sha256':digest.hexdigest(),'schema_sha256':schema,'token':str(AccessToken.for_user(user)),'clock':timezone.now().isoformat()},separators=(',',':')))
