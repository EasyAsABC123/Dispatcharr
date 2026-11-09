"""Owned notification rows/cache setup in the isolated source integration DB.

Never invoke on production data. Original Dispatcharr authors retain credit.
"""
import hashlib
import json
import os
from datetime import datetime,timezone
PREFIX='rust-probe-notification-'
def run(data):
    if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
    from apps.accounts.models import User
    from core.models import SystemNotification,NotificationDismissal,CoreSettings
    from django.core.cache import cache
    action=data['action']
    if action=='notification_prepare':
        SystemNotification.objects.filter(notification_key__startswith=PREFIX).delete()
        definitions=[('public',{}),('admin',{'admin_only':True}),('inactive',{'is_active':False}),('expired',{'expires_at':datetime(2000,1,1,tzinfo=timezone.utc)}),('future',{'expires_at':datetime(2030,1,1,tzinfo=timezone.utc),'priority':'low'}),('developer-empty',{'source':'developer'}),('developer-known',{'source':'developer','action_data':{'condition':['m3u_epg_network_insecure']}}),('developer-unknown',{'source':'developer','action_data':{'condition':'unknown-future-condition'}})]
        for ordinal,(name,fields) in enumerate(definitions):
            row=SystemNotification(notification_key=PREFIX+name,title=PREFIX+name,message='Disposable contract fixture',**fields)
            SystemNotification.objects.bulk_create([row]);SystemNotification.objects.filter(pk=row.pk).update(created_at=datetime(2020,1,ordinal+1,3,4,5,123456 if ordinal%2 else 0,tzinfo=timezone.utc))
        return {'ids':{row.notification_key.removeprefix(PREFIX):row.pk for row in SystemNotification.objects.filter(notification_key__startswith=PREFIX)}}
    if action=='notification_cleanup':
        SystemNotification.objects.filter(notification_key__startswith=PREFIX).delete();cache.delete(CoreSettings.group_cache_key('network_access'));return {'owned_notifications_removed':True}
    user=User.objects.get(pk=data['user_id'])
    if user.username!=data['username'] or not user.username.startswith('rust-probe-'):raise RuntimeError('Owned fixture user required')
    rows=list(SystemNotification.objects.filter(notification_key__startswith=PREFIX));known=next(row for row in rows if row.notification_key==PREFIX+'developer-known')
    if action=='notification_reset':
        NotificationDismissal.objects.filter(user=user).delete()
        for row in rows:cache.delete(f'dev_notif_condition_{row.pk}_{user.pk}')
        settings=CoreSettings.objects.filter(key='network_access').first();settings=settings.value if settings is not None and isinstance(settings.value,dict) else {}
        settings={**settings,'M3U_EPG':'::/0' if data.get('network_allow') is True else ('127.0.0.1/32' if data.get('network_allow') != 'malformed' else ['::/0'])}
        cache.set(CoreSettings.group_cache_key('network_access'),settings,300)
        if data.get('cached_condition') is not None:cache.set(f'dev_notif_condition_{known.pk}_{user.pk}',data['cached_condition'],300)
        return {'reset':True}
    if action=='notification_state':
        dismissals=list(NotificationDismissal.objects.filter(user=user,notification__notification_key__startswith=PREFIX).order_by('notification_id').values('notification_id','action_taken','dismissed_at'))
        values=[{'notification_id':row['notification_id'],'action_taken':row['action_taken']} for row in dismissals]
        timestamps=[row['dismissed_at'].isoformat() for row in dismissals]
        cached=cache.get(f'dev_notif_condition_{known.pk}_{user.pk}')
        return {'dismissal_count':len(dismissals),'actions_sha256':hashlib.sha256(json.dumps(values,sort_keys=True).encode()).hexdigest(),'timestamps_sha256':hashlib.sha256(json.dumps(timestamps).encode()).hexdigest(),'condition_cache_bool':cached if type(cached) is bool else None}
    raise RuntimeError('Unsupported notification fixture action')
