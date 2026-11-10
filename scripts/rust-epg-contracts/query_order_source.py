"""Explain original/native EPG queries on one disposable PostgreSQL snapshot.
Original Dispatcharr authors retain all application credit.
Injected QUERY_ORDER_INPUT contains SQL, never credentials.
"""
import json
import os
import sys
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection
results = {}
with connection.cursor() as cursor:
    cursor.execute('SHOW server_version')
    version = cursor.fetchone()[0]
    queries = dict(QUERY_ORDER_INPUT)
    for prefix in ['native','candidate']:
        prepared = queries.pop(prefix+'_prepared', None)
        if prepared is None:
            continue
        cursor.execute('PREPARE dispatcharr_order_'+prefix+'(bigint,bigint) AS '+prepared)
        for mode in ['force_custom_plan','force_generic_plan','auto']:
            queries[prefix+'_'+mode] = ('EXECUTE dispatcharr_order_'+prefix+'(NULL,NULL)', mode)
    for name, query in queries.items():
        if isinstance(query, tuple):
            query, mode = query
            cursor.execute('SET plan_cache_mode = '+mode)
        cursor.execute('EXPLAIN (ANALYZE, VERBOSE, BUFFERS, FORMAT JSON) '+query)
        plan = cursor.fetchone()[0]
        cursor.execute(query)
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        ids = []
        for row in rows:
            value = row[names.index('id')] if 'id' in names else row[names.index('raw')]
            if isinstance(value, str):
                value = json.loads(value)
            ids.append(value['id'] if isinstance(value, dict) else value)
        results[name] = {'sql':query, 'ids':ids, 'plan':plan}
print(json.dumps({'server_version':version, 'queries':results}, default=str))
