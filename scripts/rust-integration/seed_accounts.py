"""Create ephemeral compatibility users; print credentials only to the runner pipe."""
import json
import os
import secrets
import sys
from datetime import datetime, timezone

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('This seed is restricted to the disposable playback fixture')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelOverride, ChannelGroup, ChannelGroupM3UAccount, Stream
from apps.m3u.models import M3UAccount, M3UAccountProfile, M3UFilter, ServerGroup
from apps.epg.models import EPGSource, EPGData
from django_celery_beat.models import CrontabSchedule, PeriodicTask
from core.models import CoreSettings, SystemEvent
from core.utils import RedisClient
from rest_framework_simplejwt.tokens import RefreshToken

users = {}
# Startup/environment comparisons must not contact public geolocation services.
# Disable only that optional feature in the disposable source fixture using its
# own model/signal path; both backends read the same migrated settings row.
system=CoreSettings.objects.filter(key='system_settings').first()
if system is None:
    system=CoreSettings(key='system_settings',name='System Settings',value={})
system.value={**(system.value or {}),'enable_ip_lookup':False}
system.save()
for name, level, active in [('admin', 10, True), ('standard', 1, True),
                            ('viewer', 0, True), ('inactive', 10, False)]:
    password = secrets.token_urlsafe(24)
    user, _ = User.objects.update_or_create(username='rust-probe-' + name,
        defaults={'user_level': level, 'is_active': active, 'stream_limit': 0,
                  'custom_properties': None if name == 'viewer' else {},
                  'is_staff': level == 10, 'is_superuser': level == 10})
    user.set_password(password)
    user.api_key = secrets.token_urlsafe(32)
    user.save()
    refresh = RefreshToken.for_user(user)
    users[name] = {'id': user.pk, 'username': user.username, 'password': password,
                   'api_key': user.api_key, 'access': str(refresh.access_token),
                   'refresh': str(refresh)}

# Through-model fixtures cannot be produced by public group create, which always
# claims a group as manual. These rows belong only to this guarded disposable DB.
provider = M3UAccount.objects.filter(name='rust-probe-group-provider').first()
if provider is None:
    # Bulk creation intentionally bypasses refresh/scheduler signals. This is
    # a relationship-only provider fixture, never an external import target.
    M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-group-provider',
        account_type='STD', is_active=True, locked=False, server_url=None)])
    provider = M3UAccount.objects.get(name='rust-probe-group-provider')
group_fixtures = {}
for key, claimed in [('provider_group', False), ('claimed_provider_group', True)]:
    group, _ = ChannelGroup.objects.get_or_create(name='rust-probe-' + key.replace('_', '-'))
    ChannelGroupM3UAccount.objects.filter(channel_group=group).delete()
    ChannelGroupM3UAccount.objects.create(channel_group=group, m3u_account=provider)
    if claimed:
        ChannelGroup.objects.mark_custom(group)
    group_fixtures[key] = {'id': group.pk, 'name': group.name}
inactive_provider = M3UAccount.objects.filter(name='rust-probe-inactive-provider').first()
if inactive_provider is None:
    M3UAccount.objects.bulk_create([M3UAccount(name='rust-probe-inactive-provider',
        account_type='STD', is_active=False, locked=False, server_url=None)])
    inactive_provider = M3UAccount.objects.get(name='rust-probe-inactive-provider')
users['_fixtures'] = {**group_fixtures, 'provider_id': provider.pk,
                      'inactive_provider_id': inactive_provider.pk}
# Provider/EPG metadata fixtures bypass import/refresh signals and use only
# disabled scheduling records. No URL in this block may be fetched.
def metadata_rows(model,rows,identity):
    missing=[row for row in rows if not model.objects.filter(**{
        field:getattr(row,field) for field in identity}).exists()]
    if missing:model.objects.bulk_create(missing)

servers,_=ServerGroup.objects.get_or_create(name='rust-probe-provider-servers')
cron,_=CrontabSchedule.objects.get_or_create(minute='5',hour='3',day_of_week='1',day_of_month='*',month_of_year='*')
task,_=PeriodicTask.objects.get_or_create(name='rust-probe-disabled-metadata-task',
    defaults={'task':'rust-probe.never-execute','enabled':False,'crontab':cron})
M3UAccount.objects.filter(pk=provider.pk).update(server_group=servers,user_agent=None,
    username='fixture-provider-user',password='fixture-provider-secret',refresh_task=task,
    custom_properties={'enable_vod':True,'auto_enable_new_groups_live':False,'hash_key':'name,url',
                       'probe_numbers':{'integer':1,'fraction':1.5,'integral_float':2.0,'large':1e23}})
provider.refresh_from_db()
metadata_rows(M3UAccountProfile,[
    M3UAccountProfile(m3u_account=provider,name='rust-probe-default',is_default=True,is_active=True,
        max_streams=3,current_viewers=1,exp_date=datetime(2030,1,2,3,4,5,123456,tzinfo=timezone.utc),
        custom_properties={'user_info':{'active_cons':'11','max_connections':'13','exp_date':'2000000000'}}),
    M3UAccountProfile(m3u_account=provider,name='rust-probe-inactive-expiry',is_active=False,
        search_pattern='/old/',replace_pattern='/new/',exp_date=datetime(2029,1,1,tzinfo=timezone.utc)),
    M3UAccountProfile(m3u_account=provider,name='rust-probe-later-expiry',is_active=True,
        search_pattern='token=old',replace_pattern='token=new',exp_date=datetime(2031,1,1,tzinfo=timezone.utc)),
],('m3u_account_id','name'))
default_profile=provider.profiles.get(name='rust-probe-default')
# These are intentionally different sources of usage information. The REST
# serializer reads DB current_viewers, not the legacy Redis reservation count
# nor the XC account-info getter. Expire the fixture-only counter regardless.
RedisClient.get_client().set('profile_connections:'+str(default_profile.pk),7,ex=1800)
fixture_profile_connections=int(RedisClient.get_client().get('profile_connections:'+str(default_profile.pk)))
metadata_rows(Stream,[
    Stream(name='rust-probe-read-count-a',url='http://example.invalid/count-a.ts',m3u_account=provider,
           channel_group_id=group_fixtures['provider_group']['id']),
    Stream(name='rust-probe-read-count-b',url='http://example.invalid/count-b.ts',m3u_account=provider,
           channel_group_id=group_fixtures['provider_group']['id']),
    Stream(name='rust-probe-read-count-c',url='http://example.invalid/count-c.ts',m3u_account=provider,
           channel_group_id=group_fixtures['claimed_provider_group']['id']),
],('name',))
metadata_rows(M3UFilter,[
    M3UFilter(m3u_account=provider,filter_type='group',regex_pattern='^fixture',order=9,exclude=True),
    M3UFilter(m3u_account=provider,filter_type='name',regex_pattern='allow',order=1,exclude=False,custom_properties=None),
],('m3u_account_id','filter_type','regex_pattern','order'))
ChannelGroupM3UAccount.objects.filter(m3u_account=provider,
    channel_group_id=group_fixtures['provider_group']['id']).update(
    custom_properties=json.dumps({'channel_profile_ids':['1',True,2.9,'bad',None]}))
metadata_rows(M3UAccount,[M3UAccount(name='rust-probe-read-xc',account_type='XC',
    is_active=False,server_url='http://example.invalid',username='fixture-xc-user',password='fixture-xc-secret')],('name',))
xc=M3UAccount.objects.get(name='rust-probe-read-xc')
metadata_rows(M3UAccountProfile,[M3UAccountProfile(m3u_account=xc,name='rust-probe-xc-profile',is_default=True,is_active=False)],('m3u_account_id','name'))
metadata_rows(EPGSource,[
    EPGSource(name='rust-probe-read-epg',source_type='xmltv',url='http://example.invalid/epg.xml',
        username='fixture-epg-user',password='fixture-epg-secret',refresh_task=task,custom_properties=None),
    EPGSource(name='rust-probe-read-override-epg',source_type='dummy',is_active=False),
],('name',))
epg=EPGSource.objects.get(name='rust-probe-read-epg');override_epg=EPGSource.objects.get(name='rust-probe-read-override-epg')
metadata_rows(EPGData,[
    EPGData(name='rust-probe-read-epg-row',tvg_id='fixture-tvg',icon_url='http://example.invalid/icon.svg',epg_source=epg),
    EPGData(name='rust-probe-read-override-row',tvg_id=None,icon_url=None,epg_source=override_epg),
],('name',))
epg_row=EPGData.objects.get(name='rust-probe-read-epg-row');override_row=EPGData.objects.get(name='rust-probe-read-override-row')
metadata_rows(Channel,[
    Channel(name='rust-probe-read-raw-epg-channel',channel_number=7654,user_level=1,hidden_from_output=True,epg_data=epg_row),
    Channel(name='rust-probe-read-override-channel',channel_number=7655.5,user_level=1,hidden_from_output=True,auto_created=True),
],('name',))
override_channel=Channel.objects.get(name='rust-probe-read-override-channel')
metadata_rows(ChannelOverride,[ChannelOverride(channel=override_channel,epg_data=override_row)],('channel_id',))
users['_fixtures']['provider_reads']={'server_group_id':servers.pk,'xc_id':xc.pk,
    'profile_ids':list(provider.profiles.values_list('id',flat=True)),
    'usage_profile_id':default_profile.pk,'redis_profile_connections':fixture_profile_connections,
    'filter_ids':list(provider.filters.values_list('id',flat=True)),
    'epg_source_id':epg.pk,'override_epg_source_id':override_epg.pk,
    'epg_data_id':epg_row.pk,'override_epg_data_id':override_row.pk}
# Read-only utility oracle rows use deterministic timestamps and bypass event
# retention signals; this is a disposable database, never a production seed.
SystemEvent.objects.filter(event_type='rust-probe-event-kind').delete()
for ordinal in [1,2,3]:
    event=SystemEvent(event_type='rust-probe-event-kind',channel_id='00000000-0000-0000-0000-000000000000' if ordinal==3 else None,
        channel_name=None if ordinal==1 else 'rust-probe-event',details={'ordinal':ordinal,'fraction':1.5,'integral':2.0})
    SystemEvent.objects.bulk_create([event])
    SystemEvent.objects.filter(pk=event.pk).update(timestamp=datetime(2020,1,ordinal,3,4,5,123456 if ordinal==2 else 0,tzinfo=timezone.utc))
known_event=SystemEvent(event_type='vod_stop',channel_name='rust-probe-display-event',details={'owned_fixture':True})
SystemEvent.objects.bulk_create([known_event])
SystemEvent.objects.filter(pk=known_event.pk).update(timestamp=datetime(2020,1,4,3,4,5,tzinfo=timezone.utc))
print(json.dumps(users))
