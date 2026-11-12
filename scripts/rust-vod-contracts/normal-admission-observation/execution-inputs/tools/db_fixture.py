"""UNEXECUTED copied-DB Main fixture preparation; genuine ORM/JWT, no app stub."""
import datetime,hashlib,json,os,sys,uuid
from pathlib import Path
if os.environ.get('PENDING_MAIN_FIXTURE')!='playback-fixture-only':raise RuntimeError('Owned fixture required')
from fixture_guards import database as owned_database
action=sys.argv[1]
database=os.environ.get('POSTGRES_DB','')
if action=='default-audit':
 if database!='dispatcharr':raise RuntimeError('ReadOnlyDefaultDatabaseBinding')
else:owned_database(database)
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django;django.setup()
from django.db import connection,connections
from apps.accounts.models import User
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.vod.models import Movie,M3UMovieRelation,Series,Episode,M3UEpisodeRelation
from core.models import CoreSettings
from rest_framework_simplejwt.tokens import AccessToken
ROOT=Path('/evidence');action=sys.argv[1]
def fingerprint():
 models=[User,M3UAccount,M3UAccountProfile,Movie,M3UMovieRelation,Series,Episode,M3UEpisodeRelation,CoreSettings];rows={};sequences={};total=0
 for model in models:
  data=list(model.objects.order_by(model._meta.pk.name).values()[:10001])
  if len(data)>10000:raise ValueError('FingerprintRowCapacity')
  raw=json.dumps(data,sort_keys=True,default=str).encode();total+=len(raw)
  if len(raw)>1048576 or total>8388608:raise ValueError('FingerprintByteCapacity')
  rows[model._meta.db_table]={'rows':len(data),'sha256':hashlib.sha256(raw).hexdigest()}
  with connection.cursor()as cursor:
   cursor.execute('SELECT pg_get_serial_sequence(%s,%s)',[model._meta.db_table,model._meta.pk.column]);sequence=cursor.fetchone()[0]
   if sequence:
    # Sequence name is returned by PostgreSQL for this fixed model table.
    if not all(c.isalnum()or c in '._'for c in sequence):raise ValueError('SequenceIdentifier')
    cursor.execute('SELECT last_value,is_called FROM '+sequence);last,called=cursor.fetchone();sequences[sequence]={'last_value':last,'is_called':called}
 with connection.cursor()as cursor:cursor.execute('SELECT current_database()');actual=cursor.fetchone()[0]
 assert actual==database
 return{'database':actual,'tables':rows,'sequences':sequences,'dataset_sha256':hashlib.sha256(json.dumps({'tables':rows,'sequences':sequences},sort_keys=True).encode()).hexdigest()}
if action=='migrate':
 from django.core.management import call_command
 call_command('migrate',interactive=False,verbosity=0);print(json.dumps({'migrated':True,'database':database}))
elif action=='seed':
 exec(compile(Path('/vod-verification/seed.py').read_bytes(),'/vod-verification/seed.py','exec'))
 seed=json.loads((ROOT/'vod-seed.json').read_text());account=M3UAccount.objects.get(pk=seed['provider_account_id']);assert account.server_url=='http://origin:8080/player_api.php'and account.username=='fixture'and account.password=='fixture'
 other,_=Movie.objects.update_or_create(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'owned-Main-pending-other'),defaults={'name':'Owned Main pending other movie'})
 M3UMovieRelation.objects.update_or_create(m3u_account=account,stream_id='fixture-idle-other',defaults={'movie':other,'container_extension':'mp4'})
 profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);account.max_streams=2;account.save(update_fields=['max_streams']);profile.max_streams=2;profile.save(update_fields=['max_streams'])
 probes={}
 for index in[1,2]:
  movie,_=Movie.objects.update_or_create(uuid=uuid.uuid5(uuid.NAMESPACE_URL,'owned-normal-admission-probe'+str(index)),defaults={'name':'Owned normal admission probe'+str(index)})
  M3UMovieRelation.objects.update_or_create(m3u_account=account,stream_id='fixture-normal-probe'+str(index),defaults={'movie':movie,'container_extension':'mp4'});probes['probe'+str(index)+'_movie_uuid']=str(movie.uuid)
 seed.update(probes)
 user,_=User.objects.update_or_create(username='rust-pending-real-admin',defaults={'password':'!','is_active':True,'user_level':10,'stream_limit':0,'is_staff':True,'is_superuser':True,'custom_properties':{}})
 token=AccessToken.for_user(user);token.set_exp(lifetime=datetime.timedelta(hours=2));seed.update(other_movie_uuid=str(other.uuid),real_user_id=user.pk,real_username=user.username,access_token=str(token),fixture_clock=datetime.datetime.now(datetime.timezone.utc).isoformat())
 (ROOT/'pending-main-private.json').write_text(json.dumps(seed));(ROOT/'pending-main-private.json').chmod(0o600)
 print(json.dumps({'prepared':True,'fingerprint':fingerprint()}))
elif action in['fingerprint','default-audit']:print(json.dumps(fingerprint()))
elif action=='caps':
 seed=json.loads((ROOT/'pending-main-private.json').read_text());account=M3UAccount.objects.get(pk=seed['provider_account_id']);profile=M3UAccountProfile.objects.get(pk=seed['provider_profile_id']);print(json.dumps({'account_max_streams':account.max_streams,'profile_max_streams':profile.max_streams,'account_id':account.pk,'profile_id':profile.pk,'fixed_at_seed':True}))
elif action=='cap':raise RuntimeError('NormalCapMutationForbiddenAfterSeed')
else:raise RuntimeError('Unknown copiedDB action')
connections.close_all()
