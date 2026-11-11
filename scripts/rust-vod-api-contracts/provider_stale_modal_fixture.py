"""Reversible owned missing/stale provider modal fixture; real direct Source calls.
No .delay/task/helper replacement. Original Dispatcharr authors retain credit.
"""
import datetime,hashlib,inspect,ipaddress,json,os,signal,subprocess,sys
from pathlib import Path
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'or os.environ.get('VOD_API_DISPOSABLE_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection,transaction
from django.core.serializers.json import DjangoJSONEncoder
from django.utils import timezone
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile,M3UFilter
from apps.vod.models import Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,Stream
from core.models import CoreSettings,SystemEvent
class Encoder(DjangoJSONEncoder):
 def default(self,v):
  if isinstance(v,(datetime.datetime,datetime.date,datetime.time)):return v.isoformat()
  return super().default(v)
models=[User,M3UAccount,M3UAccountProfile,Movie,Series,Episode,VODLogo,VODCategory,M3UMovieRelation,M3USeriesRelation,M3UEpisodeRelation,M3UVODCategoryRelation,CoreSettings,SystemEvent,M3UFilter,ChannelGroup,ChannelGroupM3UAccount,Stream]
state=Path('/tmp/dispatcharr-stale-modal-state.json');counts=Path('/tmp/dispatcharr-stale-modal-origin.json');prefix='rust-probe-vod-rest-';tag=prefix+'00 Modal '
def rows():return {m._meta.db_table:list(m.objects.order_by('id').values())for m in models}
def fingerprints():return {n:hashlib.sha256(json.dumps(v,sort_keys=True,cls=Encoder).encode()).hexdigest()for n,v in rows().items()}
def sequences():
 result={}
 with connection.cursor()as c:
  for m in models:
   c.execute("SELECT pg_get_serial_sequence(%s,'id')",[m._meta.db_table]);name=c.fetchone()[0]
   if name:c.execute('SELECT last_value,is_called FROM '+name);r=c.fetchone();result[name]={'last_value':r[0],'is_called':r[1]}
 return result
template=Path('/tmp/dispatcharr-stale-modal-template.json')
config=json.load(sys.stdin)
if config['op']=='prepare'and not config.get('reseed_template'):
 assert not state.exists()and not template.exists();assert Movie.objects.filter(name__startswith=prefix).count()==27 and Series.objects.filter(name__startswith=prefix).count()==4
 saved={'rows':rows(),'fingerprints':fingerprints(),'sequences':sequences(),'created':{}};state.write_text(json.dumps(saved,cls=Encoder));os.chmod(state,0o600)
 user=User.objects.get(username=prefix+'standard');assert user.user_level==1 and user.custom_properties=={}
 address=ipaddress.ip_address(config['origin_ip']);assert address.is_private and not address.is_loopback;origin='http://'+str(address)+':18881';accounts=list(M3UAccount.objects.filter(name__startswith=prefix,is_active=True).order_by('priority','id'));assert len(accounts)==2
 with transaction.atomic():
  user.set_password(config['password']);User.objects.filter(pk=user.pk).update(password=user.password)
  Movie.objects.filter(name__startswith=prefix).update(logo=None);Series.objects.filter(name__startswith=prefix).update(logo=None)
  Movie.objects.bulk_create([Movie(name=tag+'Movie',is_adult=False)]);Series.objects.bulk_create([Series(name=tag+'Series')]);movie=Movie.objects.get(name=tag+'Movie');series=Series.objects.get(name=tag+'Series');saved['created']={'movie':movie.pk,'series':series.pk}
  Episode.objects.bulk_create([Episode(series=series,name=tag+'Old Episode '+str(n),season_number=1,episode_number=n)for n in [1,2,3]]);episodes=list(Episode.objects.filter(series=series).order_by('episode_number'))
  for index,account in enumerate(accounts):
   suffix='a'if index==0 else'b';M3UAccount.objects.filter(pk=account.pk).update(server_url=origin,username='owned-modal-'+suffix,password='owned-modal-password');stamp=None if index==0 else timezone.now()-datetime.timedelta(hours=25)
   props={}if index==0 else{'detailed_fetched':True,'detailed_info':{'plot':'Stale movie details'}}
   M3UMovieRelation.objects.bulk_create([M3UMovieRelation(movie=movie,m3u_account=account,stream_id=tag+'movie-'+suffix,container_extension='mp4',last_advanced_refresh=stamp,custom_properties=props)])
   props={'movie_image':origin+'/art/series-'+suffix+'.svg'}
   if index:props.update(detailed_fetched=True,episodes_fetched=True)
   M3USeriesRelation.objects.bulk_create([M3USeriesRelation(series=series,m3u_account=account,external_series_id=tag+'series-'+suffix,last_episode_refresh=stamp,custom_properties=props)]);relation=M3USeriesRelation.objects.get(series=series,m3u_account=account)
   M3UEpisodeRelation.objects.bulk_create([M3UEpisodeRelation(episode=e,m3u_account=account,series_relation=relation,stream_id='owned-old-'+suffix+'-'+str(e.pk),container_extension='mp4')for e in(episodes[:2]if index==0 else episodes[1:])])
 counts.write_text('[]');process=subprocess.Popen([sys.executable,str(Path(__file__).with_name('provider_stale_modal_origin.py')),str(counts)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True);saved['origin_pid']=process.pid;state.write_text(json.dumps(saved,cls=Encoder))
 from apps.vod.tasks import refresh_movie_advanced_data,refresh_series_episodes,batch_refresh_series_episodes
 from dispatcharr.celery import app
 fixture={'prepared':True,'username':user.username,'movie':{'id':movie.pk,'name':movie.name},'series':{'id':series.pk,'name':series.name},'episodes':[{'id':e.pk,'name':e.name}for e in episodes],'origin':origin,'freshness':'account index0 missing timestamp/flags; index1 stale25hours, flags true','initial_relations':{'movie':list(M3UMovieRelation.objects.filter(movie=movie).values('id','m3u_account_id','last_advanced_refresh','custom_properties')),'series':list(M3USeriesRelation.objects.filter(series=series).values('id','m3u_account_id','last_episode_refresh','custom_properties'))},'source_callgraph':{'movie_task_name':refresh_movie_advanced_data.name,'movie_view_call':'api_views.py229 synchronous Task.__call__, then DB reload','series_view_call':'api_views.py582 plain helper refresh_series_episodes, then DB reload','series_has_delay':hasattr(refresh_series_episodes,'delay'),'separate_series_batch_task':batch_refresh_series_episodes.name,'celery_eager':bool(app.conf.task_always_eager),'default_queue':app.conf.task_default_queue,'worker_started':False,'no_callable_replacement':True}}
 fixture['prepared_model_fingerprints']=fingerprints();fixture['prepared_sequences']=sequences()
 template.write_text(json.dumps({'baseline_fingerprints':saved['fingerprints'],'baseline_sequences':saved['sequences'],'prepared_rows':rows(),'prepared_sequences':sequences(),'fixture':fixture},cls=Encoder));os.chmod(template,0o600)
 counts.chmod(0o600)
 print(json.dumps(fixture,cls=Encoder))
elif config['op']=='prepare'and config.get('reseed_template'):
 assert not state.exists()and template.exists();t=json.loads(template.read_text());assert fingerprints()==t['baseline_fingerprints'];assert sequences()==t['baseline_sequences']
 saved={'rows':rows(),'fingerprints':fingerprints(),'sequences':sequences(),'created':{'movie':t['fixture']['movie']['id'],'series':t['fixture']['series']['id']}};state.write_text(json.dumps(saved,cls=Encoder));os.chmod(state,0o600)
 with transaction.atomic():
  for m in models:
   for row in t['prepared_rows'][m._meta.db_table]:
    values={k:v for k,v in row.items()if k!='id'}
    if not m.objects.filter(pk=row['id']).update(**values):m.objects.bulk_create([m(**row)]);m.objects.filter(pk=row['id']).update(**values)
  with connection.cursor()as c:
   for name,seq in t['prepared_sequences'].items():c.execute('SELECT setval(%s::regclass,%s,%s)',[name,seq['last_value'],seq['is_called']])
 assert fingerprints()==t['fixture']['prepared_model_fingerprints'];assert sequences()==t['prepared_sequences'];assert User.objects.get(username=t['fixture']['username']).check_password(config['password'])
 counts.write_text('[]');counts.chmod(0o600);process=subprocess.Popen([sys.executable,str(Path(__file__).with_name('provider_stale_modal_origin.py')),str(counts)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True);saved['origin_pid']=process.pid;state.write_text(json.dumps(saved,cls=Encoder))
 fixture=t['fixture'];fixture['boundary_reseed_exact']=True;print(json.dumps(fixture,cls=Encoder))

elif config['op']=='restore':
 saved=json.loads(state.read_text());observed=rows();origin_counts=json.loads(counts.read_text())if counts.exists()else[]
 if saved.get('origin_pid'):
  try:os.killpg(saved['origin_pid'],signal.SIGTERM)
  except ProcessLookupError:pass
 label=config.get('label','unknown');assert label in ['Original','native','boundary','unknown'];captured={'rows':observed,'sequences':sequences(),'prepared_rows':json.loads(template.read_text())['prepared_rows']};effectfile=Path('/evidence')/('stale-modal-'+label+'-rows-private.json');effectfile.write_text(json.dumps(captured,cls=Encoder));os.chmod(effectfile,0o600)
 refresh=[]
 for m,field in [(M3UMovieRelation,'last_advanced_refresh'),(M3USeriesRelation,'last_episode_refresh')]:
  for row in m.objects.filter(**{('movie_id'if m is M3UMovieRelation else'series_id'):saved['created'].get('movie'if m is M3UMovieRelation else'series')}).values():refresh.append({'kind':'movie'if m is M3UMovieRelation else'series','account':row['m3u_account_id'],'fresh':row[field]is not None,'last_refresh':row[field],'flags':row['custom_properties']})
 episode_rows=list(M3UEpisodeRelation.objects.filter(episode__series_id=saved['created'].get('series')).values('stream_id'));replacement={'new_relations':sum(r['stream_id'].startswith('owned-refreshed-')for r in episode_rows),'old_relations':sum(r['stream_id'].startswith('owned-old-')for r in episode_rows)}
 with transaction.atomic():
  for m in reversed(models):m.objects.exclude(pk__in=[v['id']for v in saved['rows'][m._meta.db_table]]).delete()
  for m in models:
   for row in saved['rows'][m._meta.db_table]:
    values={k:v for k,v in row.items()if k!='id'}
    if not m.objects.filter(pk=row['id']).update(**values):m.objects.bulk_create([m(**row)]);m.objects.filter(pk=row['id']).update(**values)
  with connection.cursor()as c:
   for name,seq in saved['sequences'].items():c.execute('SELECT setval(%s::regclass,%s,%s)',[name,seq['last_value'],seq['is_called']])
 assert fingerprints()==saved['fingerprints'];assert sequences()==saved['sequences'];state.unlink();counts.unlink(missing_ok=True)
 print(json.dumps({'restored':True,'tracked18_models_exact':True,'sequences_exact':True,'origin_requests':origin_counts,'owned_origin_only':all(r['allowed']for r in origin_counts),'refreshed_relations':refresh,'episode_replacement':replacement,'effect_rows_sha256':hashlib.sha256(effectfile.read_bytes()).hexdigest(),'effect_rows_file':effectfile.name,'scope':'Tracked SQL models/sequences restored; real login/Redis cache/rate-limit effects remain owned until project destruction, no globalRedis reset.'},cls=Encoder))
elif config['op']=='drop-template':
 assert not state.exists()
 if template.exists():
  t=json.loads(template.read_text());assert fingerprints()==t['baseline_fingerprints'];assert sequences()==t['baseline_sequences'];template.unlink()
 print(json.dumps({'template_removed':True,'tracked_baseline_exact':True}))
else:raise RuntimeError('Unknown fixture op')
