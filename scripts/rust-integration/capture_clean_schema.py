"""Inventory fresh original default rows, rejecting any provider/user data.

Original Dispatcharr contributors retain application credit. Only the pinned
original migrations and normal URL/app initialization run before capture.
"""
import os,sys,json
if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':raise RuntimeError('Disposable schema required')
sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
import dispatcharr.urls
from django.db import connection
from apps.accounts.models import User
from apps.channels.models import Channel,Stream
from apps.m3u.models import M3UAccount, CUSTOM_M3U_ACCOUNT_NAME
from apps.epg.models import EPGSource
if User.objects.exists() or Channel.objects.exists() or Stream.objects.exists() or EPGSource.objects.exists():raise RuntimeError('Fresh defaults must contain no users, channels, streams or EPG sources')
for account in M3UAccount.objects.all():
    if account.name!=CUSTOM_M3U_ACCOUNT_NAME or not account.locked or account.username or account.password or account.server_url:raise RuntimeError('Non-default provider data forbidden')
counts={};refresh=[]
with connection.cursor() as cursor:
    cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename")
    tables=[row[0]for row in cursor.fetchall()]
    for table in tables:
        cursor.execute('SELECT count(*) FROM '+connection.ops.quote_name(table));counts[table]=cursor.fetchone()[0]
    cursor.execute("SELECT table_name,column_name FROM information_schema.columns WHERE table_schema='public' AND data_type IN('timestamp with time zone','timestamp without time zone') AND column_name IN('created_at','updated_at','date_changed','last_update','applied') ORDER BY table_name,column_name")
    refresh=[{'table':table,'column':column}for table,column in cursor.fetchall() if counts.get(table,0)>0]
    cursor.execute('SELECT app,name FROM django_migrations ORDER BY app,name');migrations=[list(row)for row in cursor.fetchall()]
print(json.dumps({'scope':'Fresh original migrations and normal app/URL initialization, no benchmark or user seed','tables':counts,'refresh_creation_timestamps':refresh,'migrations':migrations,'users':0,'channels':0,'streams':0,'epg_sources':0},sort_keys=True))
