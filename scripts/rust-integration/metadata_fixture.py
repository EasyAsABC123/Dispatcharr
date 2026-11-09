"""Restricted relational fixture preparation; no account-save/import signals.

Original Dispatcharr authors retain all application credit. This helper is only
for the disposable root integration project; never invoke on production data.
"""
import json
import os
import sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
data=json.load(sys.stdin)
# Notification setup is a management operation. Use the source's documented
# management-command initialization branch so re-running this helper does not
# synchronize away owned developer fixtures before the HTTP read under test.
if str(data.get('action','')).startswith(('notification_','environment_ip_')):sys.argv.append('shell')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.m3u.models import M3UAccount,ServerGroup

def sanitized_failure(kind,value,tb):
    while tb.tb_next is not None:tb=tb.tb_next
    print(json.dumps({'fixture_error_type':kind.__name__,'fixture_error_line':tb.tb_lineno}),file=sys.stderr)
sys.excepthook=sanitized_failure
if str(data.get('action','')).startswith('environment_ip_'):
    import time
    from core.models import CoreSettings
    from django.core.cache import cache
    settings=CoreSettings.objects.get(key='system_settings')
    value=settings.value if isinstance(settings.value,dict) else {}
    restore=data['action']=='environment_ip_restore'
    CoreSettings.objects.filter(pk=settings.pk).update(value={**value,'enable_ip_lookup':not restore})
    cache.delete(CoreSettings.group_cache_key('system_settings'))
    cache.delete('dispatcharr:ip_lookup_result');cache.delete('dispatcharr:ip_lookup_lock')
    if not restore:
        # The package proof intentionally cannot start a real public-provider
        # lookup: even cached cases retain the original shared lock.
        cache.set('dispatcharr:ip_lookup_lock',True,30)
        mode=data.get('mode')
        if mode!='locked-cold':
            payload={'public_ip':'198.51.100.10' if mode=='fresh' else None,'local_ip':'127.0.0.1','country_code':'TS' if mode=='fresh' else None,'country_name':'Testland' if mode=='fresh' else None,'city':'Fixture City' if mode=='fresh' else None,'verified_at':time.time()}
            cache.set('dispatcharr:ip_lookup_result',payload,3600)
    print(json.dumps({'prepared':not restore,'restored_disabled':restore}))
    raise SystemExit(0)
if str(data.get('action','')).startswith('notification_'):
    from notification_fixture import run
    print(json.dumps(run(data)))
    raise SystemExit(0)
if data.get('action') in ('link_stream_profile','verify_stream_profile_removed'):
    from core.models import StreamProfile
    from apps.channels.models import Channel,ChannelOverride,Stream
    import hashlib
    account=M3UAccount.objects.get(name='rust-probe-inactive-provider',is_active=False)
    stream=Stream.objects.get(name='rust-probe-read-count-a')
    channel=Channel.objects.get(name='rust-probe-read-raw-epg-channel')
    override=ChannelOverride.objects.get(channel__name='rust-probe-read-override-channel')
    rows=[account,stream,channel,override]
    if data['action']=='link_stream_profile':
        profile=StreamProfile.objects.get(pk=data['profile_id'])
        if profile.name!=data['profile_name'] or not profile.name.startswith('rust-probe-stream-profile-') or profile.locked or any(row.stream_profile_id is not None for row in rows):raise RuntimeError('Unsafe fixture profile relation')
        for row in rows:type(row).objects.filter(pk=row.pk).update(stream_profile_id=profile.pk)
    else:
        if any(row.stream_profile_id is not None for row in rows):raise RuntimeError('Collector did not clear every relation')
    fingerprint=hashlib.sha256(json.dumps([row.updated_at.isoformat() if row.updated_at is not None else None for row in rows]).encode()).hexdigest()
    print(json.dumps({'relations':len(rows),'timestamp_sha256':fingerprint}))
    raise SystemExit(0)
if data.get('action')!='link_server_group':raise RuntimeError('Unsupported fixture action')
account=M3UAccount.objects.get(pk=data['account_id'])
group=ServerGroup.objects.get(pk=data['group_id'])
if (account.name!='rust-probe-inactive-provider' or account.is_active or account.server_group_id is not None
    or group.name!=data['group_name'] or not group.name.startswith('rust-probe-server-crud-')):
    raise RuntimeError('Unsafe fixture relation')
before=account.updated_at
M3UAccount.objects.filter(pk=account.pk).update(server_group_id=group.pk)
account.refresh_from_db()
print(json.dumps({'linked':account.server_group_id==group.pk,'updated_at_preserved':before==account.updated_at}))
