"""Only owned copiedDB fixture input; no task/callable replacements."""
import os,sys,json,datetime
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
sys.path[:0]=['/app'];sys.argv.append('shell')
exec(compile(Path('/verification/seed.py').read_text(),'/verification/seed.py','exec'))
from django.db import connection
from apps.accounts.models import User
from apps.epg.models import EPGSource,EPGData
from apps.channels.models import Recording
from rest_framework_simplejwt.tokens import AccessToken
from django.core.cache import cache
from dispatcharr.celery import app
import redis
seed=json.loads(Path('/evidence/seed.json').read_text());profile=account.profiles.get(is_default=True);profile.max_streams=1;profile.save(update_fields=['max_streams'])
user=User.objects.create(username='rust-m0-concurrent-owned',password='!',user_level=10,is_superuser=True,is_staff=True,is_active=True,custom_properties={});token=AccessToken.for_user(user);token.set_exp(lifetime=datetime.timedelta(hours=2))
now=datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0);root=Path('/evidence/workflow-files');root.mkdir(exist_ok=True);root.chmod(0o755);guide=root/'guide.xml';guide.write_text('<tv/>');guide.chmod(0o644)
e=EPGSource(name='rust-m0-concurrent-source',source_type='xmltv',is_active=True,file_path=str(guide),custom_properties={});EPGSource.objects.bulk_create([e]);epg=EPGData.objects.create(epg_source=e,tvg_id='workflow-owned',name='Workflow owned');Channel.objects.filter(uuid=seed['channel_uuid']).update(epg_data=epg)
CoreSettings.objects.update_or_create(key='dvr_settings',defaults={'name':'Owned no rules','value':{'series_rules':[]}})
cache.delete(CoreSettings.group_cache_key('dvr_settings'));cache.delete(CoreSettings.group_cache_ver_key('dvr_settings'))
r=redis.Redis(host='localhost',password='playback-fixture-only',db=0)
for key in ['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']:r.set(key,'1',ex=3600)
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND']);assert broker.connection_pool.connection_kwargs['db']==8 and backend.connection_pool.connection_kwargs['db']==9
# Only disposable owned broker/result DBs; never media/cache DB0.
broker.flushdb();backend.flushdb()
with connection.cursor()as c:c.execute('SELECT current_database()');database=c.fetchone()[0]
seed.update(token=str(token),user_id=user.pk,username=user.username,profile_id=profile.pk,profile_max_streams=1,epg_source=e.pk,epg_data=epg.pk,clock=now.isoformat(),database=database)
Path('/evidence/concurrent-seed-private.json').write_text(json.dumps(seed));os.chmod('/evidence/concurrent-seed-private.json',0o600);print(json.dumps({'prepared':True,'database':database,'profile_max_streams':profile.max_streams}))
