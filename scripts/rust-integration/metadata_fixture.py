"""Restricted relational fixture preparation; no account-save/import signals.

Original Dispatcharr authors retain all application credit. This helper is only
for the disposable root integration project; never invoke on production data.
"""
import json
import os
import sys
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.m3u.models import M3UAccount,ServerGroup

data=json.load(sys.stdin)
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
