"""Imported only inside the existing Source management guest; emits no row data."""
import hashlib,json
from psycopg import sql
from django.db import connection
from apps.accounts.models import User
from apps.channels.models import Channel,ChannelStream,Stream
from apps.m3u.models import M3UAccount,M3UAccountProfile
from core.models import CoreSettings

def snapshot():
    counts={};critical={};sequences={}
    with connection.cursor()as cursor:
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'AND table_type='BASE TABLE'ORDER BY table_name")
        tables=[row[0]for row in cursor.fetchall()]
        if len(tables)>512:raise RuntimeError('BackupSchemaTableBudget')
        for table in tables:
            cursor.execute(sql.SQL('SELECT count(*)FROM public.{}').format(sql.Identifier(table)));counts[table]=cursor.fetchone()[0]
        for model in [User,Channel,ChannelStream,Stream,M3UAccount,M3UAccountProfile,CoreSettings]:
            table=model._meta.db_table;cursor.execute(sql.SQL('SELECT row_to_json(t)::text FROM public.{} t ORDER BY id').format(sql.Identifier(table)));digest=hashlib.sha256();rows=0
            while True:
                page=cursor.fetchmany(256)
                if not page:break
                for raw, in page:
                    digest.update(json.dumps(json.loads(raw),sort_keys=True,separators=(',',':')).encode());digest.update(b'\n');rows+=1
                if rows>100000:raise RuntimeError('BackupCriticalRowBudget')
            critical[table]={'rows':rows,'sha256':digest.hexdigest()}
        cursor.execute("SELECT sequencename,last_value FROM pg_sequences WHERE schemaname='public'ORDER BY sequencename")
        sequences=dict(cursor.fetchall())
    return {'table_counts':counts,'critical_rows':critical,'sequence_last_values':sequences}
