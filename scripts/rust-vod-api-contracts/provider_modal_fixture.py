"""UNEXECUTED reversible modal fixture. No refresh/playback or remote origin.
New synthetic rows use bulk_create to avoid post-save task producers. Actual
login audit writes are reported separately; discarded with the owned project.
Original Dispatcharr authors retain application credit.
"""
import datetime,hashlib,ipaddress,json,os,signal,subprocess,sys
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only' or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import transaction
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from apps.accounts.models import User
from apps.m3u.models import M3UAccount
from apps.vod.models import Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation
class Encoder(DjangoJSONEncoder):
 def default(self,v):
  if isinstance(v,(datetime.datetime,datetime.date,datetime.time)):return v.isoformat()
  return super().default(v)
config=json.load(sys.stdin);state=Path('/tmp/dispatcharr-modal-browser-state.json');counts=Path('/tmp/dispatcharr-modal-browser-origin.json');prefix='rust-probe-vod-rest-';tag=prefix+'00 Modal '
models=[Movie,Series,Episode,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UAccount]
def fingerprint():return {m._meta.db_table:hashlib.sha256(json.dumps(list(m.objects.order_by('id').values()),sort_keys=True,cls=Encoder).encode()).hexdigest() for m in models}
assert Movie.objects.filter(name__startswith=prefix).count() in [27,28] and Series.objects.filter(name__startswith=prefix).count() in [4,5]
if config['op']=='prepare':
 assert not state.exists() and not Movie.objects.filter(name__startswith=tag).exists()
 address=ipaddress.ip_address(config['origin_ip']);assert address.is_private and not address.is_loopback
 password=config['password'];assert len(password)>=24
 user=User.objects.get(username=prefix+'standard');assert user.user_level==1 and user.is_active and user.custom_properties=={}
 accounts=list(M3UAccount.objects.filter(name__startswith=prefix,is_active=True).order_by('priority','id'));assert len(accounts)==2
 saved={'before':fingerprint(),'users':list(User.objects.filter(pk=user.pk).values()),'created':{},'movie_logos':list(Movie.objects.filter(name__startswith=prefix).values('id','logo_id')),'series_logos':list(Series.objects.filter(name__startswith=prefix).values('id','logo_id'))}
 state.write_text(json.dumps(saved,cls=Encoder));os.chmod(state,0o600)
 try:
  with transaction.atomic():
   user.set_password(password);User.objects.filter(pk=user.pk).update(password=user.password)
   # Existing oracle logos deliberately reference failed URLs; remove only their
   # owned card links so the gate does not start with unrelated broken images.
   Movie.objects.filter(name__startswith=prefix).update(logo=None);Series.objects.filter(name__startswith=prefix).update(logo=None)
   Movie.objects.bulk_create([Movie(name=tag+'Movie',is_adult=False)]);Series.objects.bulk_create([Series(name=tag+'Series')])
   movie=Movie.objects.get(name=tag+'Movie');series=Series.objects.get(name=tag+'Series')
   saved['created']={'movie':movie.pk,'series':series.pk};state.write_text(json.dumps(saved,cls=Encoder))
   Episode.objects.bulk_create([Episode(series=series,name=tag+'Episode '+str(n),season_number=1,episode_number=n,description='Owned browser episode '+str(n))for n in [1,2,3]])
   episodes=list(Episode.objects.filter(series=series).order_by('episode_number'));now=timezone.now();origin='http://'+str(address)+':18881'
   for index,account in enumerate(accounts):
    suffix='a'if index==0 else'b';art=origin+'/poster-'+suffix+'.svg'
    M3UMovieRelation.objects.bulk_create([M3UMovieRelation(movie=movie,m3u_account=account,stream_id=tag+'movie-'+suffix,container_extension='mp4',last_advanced_refresh=now,custom_properties={'detailed_fetched':True,'movie_image':art,'detailed_info':{'name':movie.name,'plot':'Owned cached movie provider '+suffix,'description':'Owned cached movie provider '+suffix}})])
    M3USeriesRelation.objects.bulk_create([M3USeriesRelation(series=series,m3u_account=account,external_series_id=tag+'series-'+suffix,last_episode_refresh=now,custom_properties={'episodes_fetched':True,'detailed_fetched':True,'movie_image':art})])
    relation=M3USeriesRelation.objects.get(series=series,m3u_account=account)
    M3UEpisodeRelation.objects.bulk_create([M3UEpisodeRelation(episode=episode,m3u_account=account,series_relation=relation,stream_id=tag+'episode-'+suffix+'-'+str(episode.pk),container_extension='mp4',custom_properties={'movie_image':origin+'/episode-'+suffix+'.svg'})for episode in (episodes[:2]if index==0 else episodes[1:])])
  counts.write_text('[]')
  process=subprocess.Popen([sys.executable,str(Path(__file__).with_name('provider_modal_origin.py')),str(counts)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
  saved['origin_pid']=process.pid;state.write_text(json.dumps(saved,cls=Encoder))
  print(json.dumps({'prepared':True,'username':user.username,'movie':{'id':movie.pk,'name':movie.name},'series':{'id':series.pk,'name':series.name},'episodes':[{'id':ep.pk,'name':ep.name}for ep in episodes],'origin':origin,'freshness':'Both provider caches stamped now; never force_refresh','provider_ids':{'movie':list(M3UMovieRelation.objects.filter(movie=movie).values_list('id',flat=True)),'series':list(M3USeriesRelation.objects.filter(series=series).values_list('id',flat=True))}}))
 except BaseException:
  # Caller must invoke restore even when preparation has partially failed.
  raise
elif config['op']=='restore':
 saved=json.loads(state.read_text());origin_counts=json.loads(counts.read_text())if counts.exists()else[]
 if saved.get('origin_pid'):
  try:os.killpg(saved['origin_pid'],signal.SIGTERM)
  except ProcessLookupError:pass
 with transaction.atomic():
  if saved['created'].get('movie'):Movie.objects.filter(pk=saved['created']['movie'],name=tag+'Movie').delete()
  if saved['created'].get('series'):Series.objects.filter(pk=saved['created']['series'],name=tag+'Series').delete()
  for model,key in [(Movie,'movie_logos'),(Series,'series_logos')]:
   for row in saved[key]:model.objects.filter(pk=row['id'],name__startswith=prefix).update(logo_id=row['logo_id'])
  for row in saved['users']:User.objects.filter(pk=row['id'],username=row['username']).update(**{k:v for k,v in row.items()if k!='id'})
 assert fingerprint()==saved['before'];assert json.loads(json.dumps(list(User.objects.filter(pk=saved['users'][0]['id']).values()),cls=Encoder))==saved['users']
 state.unlink();counts.unlink(missing_ok=True)
 print(json.dumps({'restored':True,'seven_model_row_fingerprints_exact':True,'user_full_microsecond_row_exact':True,'origin_requests':origin_counts,'no_provider_contact':all(row['allowed']for row in origin_counts),'scope':'Login audit events and sequence advancement remain genuine fixture effects; owned project must be destroyed.'}))
else:raise RuntimeError('Unknown operation')
