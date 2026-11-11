"""Original backup service/ORM copied-DB helper; disposable targets only.
Original Dispatcharr authors retain application credit. Never publish raw dumps.
"""
import hashlib,json,os,sys
from pathlib import Path
if (os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only'
        or os.environ.get('BACKUP_DISPOSABLE_FIXTURE')!='playback-fixture-only'):
    raise RuntimeError('Explicit disposable backup fixture required')
action=json.load(sys.stdin);database=os.environ.get('POSTGRES_DB','')
if action['action']!='baseline_manifest'and (not database.startswith('rust_probe_backup_')or len(database)>63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_'for c in database)):
    raise RuntimeError('Only exact owned copied databases are allowed')
if action['action']=='baseline_manifest'and database!='dispatcharr':raise RuntimeError('Unexpected baseline name')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from django.db import connection
from django.core.management import call_command
from django.conf import settings

def manifest():
    with connection.cursor()as cursor:
        cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public'ORDER BY tablename")
        tables=[row[0]for row in cursor.fetchall()];rows={}
        for name in tables:
            quoted=connection.ops.quote_name(name)
            cursor.execute('SELECT count(*),md5(COALESCE(jsonb_agg(to_jsonb(t)ORDER BY to_jsonb(t)::text)::text,\'[]\'))FROM '+quoted+' t')
            count,digest=cursor.fetchone();rows[name]={'count':count,'sha_like_md5':digest}
        cursor.execute("SELECT table_name,column_name,data_type,is_nullable,column_default FROM information_schema.columns WHERE table_schema='public'ORDER BY table_name,ordinal_position")
        columns=cursor.fetchall()
        cursor.execute("SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public'ORDER BY tablename,indexname")
        indexes=cursor.fetchall()
        cursor.execute("SELECT c.relname,k.conname,pg_get_constraintdef(k.oid)FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'ORDER BY c.relname,k.conname")
        constraints=cursor.fetchall()
        cursor.execute("SELECT sequencename FROM pg_sequences WHERE schemaname='public'ORDER BY sequencename")
        sequences={}
        for (name,)in cursor.fetchall():
            cursor.execute('SELECT last_value,is_called FROM '+connection.ops.quote_name(name))
            sequences[name]=cursor.fetchone()
    return {'tables':rows,'columns':columns,'indexes':indexes,'constraints':constraints,'sequences':sequences}

def seed():
    call_command('migrate',interactive=False,verbosity=0)
    from apps.accounts.models import User
    from django.contrib.auth.models import Group,Permission
    from apps.channels.models import Logo,Stream,Channel,ChannelGroup,ChannelProfile,ChannelProfileMembership,ChannelStream,ChannelOverride
    from apps.m3u.models import M3UAccount
    from core.models import UserAgent,StreamProfile,OutputProfile,CoreSettings
    prefix='rust-probe-backup-'
    admin=User.objects.create_user(username=prefix+'admin',password='backup-fixture-password',email='Fixture@example.invalid',user_level=10,is_staff=True,is_superuser=True,api_key='backup-fixture-api-key')
    viewer=User.objects.create_user(username=prefix+'viewer',password='backup-viewer-password',user_level=1,custom_properties={'nested':[1,1.0,'1',True,None],'unicode':'café'})
    group=Group.objects.create(name=prefix+'group');group.permissions.add(*Permission.objects.order_by('id')[:3]);admin.groups.add(group)
    agent=UserAgent.objects.create(name=prefix+'agent',user_agent='Rust-Probe/backup')
    profile=StreamProfile.objects.create(name=prefix+'stream-profile',command='',parameters='',user_agent=agent)
    output=OutputProfile.objects.create(name=prefix+'output-profile',command='ffmpeg',parameters='-i pipe:0 -c copy -f mpegts pipe:1')
    M3UAccount.objects.bulk_create([M3UAccount(name=prefix+'provider',account_type='STD',is_active=False,server_url='http://example.invalid/source.m3u',username='fixture-provider',password='fixture-provider-password',custom_properties={'user_info':{'active_cons':3}},user_agent=agent,stream_profile=profile)])
    provider=M3UAccount.objects.get(name=prefix+'provider')
    logo=Logo.objects.create(name=prefix+'logo',url='/data/logos/backup-fixture.svg')
    channel_group=ChannelGroup.objects.create(name=prefix+'channel-group')
    stream=Stream.objects.create(name=prefix+'stream',url='http://example.invalid/source.ts',m3u_account=provider,stream_profile=profile,custom_properties={'fixture':True,'ratio':1.5})
    channel=Channel.objects.create(name=prefix+'channel',channel_number=90000.5,logo=logo,channel_group=channel_group,stream_profile=profile)
    ChannelStream.objects.create(channel=channel,stream=stream,order=0)
    ChannelOverride.objects.create(channel=channel,name=prefix+'overridden')
    channel_profile=ChannelProfile.objects.create(name=prefix+'channel-profile')
    ChannelProfileMembership.objects.update_or_create(channel_profile=channel_profile,channel=channel,defaults={'enabled':True});viewer.channel_profiles.add(channel_profile)
    CoreSettings.objects.update_or_create(key='backup_fixture',defaults={'name':'Backup Fixture','value':{'output_profile':output.pk,'sql_null':None,'typed':[1,1.0,'1',True]}})
    return manifest()

kind=action['action']
if kind=='seed':result={'manifest':seed()}
elif kind in ['manifest','baseline_manifest']:result={'manifest':manifest()}
elif kind=='source_create':
    from apps.backups import services
    archive=services.create_backup()
    from zipfile import ZipFile
    with ZipFile(archive)as z:
        metadata=json.loads(z.read('metadata.json'));members=z.namelist()
    result={'filename':archive.name,'size':archive.stat().st_size,'members':members,'metadata':metadata}
elif kind=='source_restore':
    filename=action['filename']
    if Path(filename).name!=filename or'..'in filename:raise RuntimeError('Only owned filename allowed')
    from apps.backups.tasks import restore_backup_task
    result=restore_backup_task.run(filename)
    if result.get('status')!='completed':raise RuntimeError('Original restore task did not complete')
    result={'completed':True,'manifest':manifest()}
elif kind=='source_restore_boundary':
    from apps.backups import services
    filename=action['filename']
    if Path(filename).name!=filename or'..'in filename:raise RuntimeError('Only owned filename allowed')
    try:
        services.restore_backup(Path(settings.BACKUP_ROOT)/filename)
        result={'failed':False}
    except Exception as error:result={'failed':True,'exception':type(error).__name__}
    result['manifest']=manifest()
elif kind=='source_dump_fault':
    from apps.backups import services
    target=Path('/evidence/source-dump-fault.dump')
    # The source fixture's Unix socket is trust-authenticated; use its configured TCP password boundary.
    settings.DATABASES['default']['HOST']='127.0.0.1'
    if action['fault']=='password':settings.DATABASES['default']['PASSWORD']='deliberately-invalid-fixture-password'
    elif action['fault']=='privilege':settings.DATABASES['default'].update(USER=database+'_denied',PASSWORD='denied-fixture-password')
    else:raise RuntimeError('Unknown owned fault')
    try:
        services._dump_postgresql(target)
        result={'failed':False}
    except Exception as error:
        result={'failed':True,'exception':type(error).__name__}
    finally:target.unlink(missing_ok=True)
elif kind=='credentials':
    from apps.accounts.models import User
    admin=User.objects.get(username='rust-probe-backup-admin');viewer=User.objects.get(username='rust-probe-backup-viewer')
    result={'admin_password':admin.check_password('backup-fixture-password'),'viewer_password':viewer.check_password('backup-viewer-password'),'api_key':admin.api_key=='backup-fixture-api-key','groups':admin.groups.count(),'channel_profiles':viewer.channel_profiles.count()}
else:raise RuntimeError('Unknown guarded fixture action')
print(json.dumps(result,sort_keys=True,separators=(',',':')))
