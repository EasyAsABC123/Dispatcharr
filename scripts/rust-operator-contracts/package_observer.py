"""Original operator CLI oracle. Original Dispatcharr authors retain full credit.
Only safe network-reset commands run, exclusively against an owned copied DB.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

if (os.environ.get('OPERATOR_DISPOSABLE_FIXTURE') != 'playback-fixture-only'
        or not os.environ.get('POSTGRES_DB', '').startswith('rust_probe_backup_operator_')):
    raise RuntimeError('Explicit owned copied operator fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from core.models import CoreSettings, NETWORK_ACCESS_KEY
from django.db.models import Value, JSONField
from django.db import connection
from core.models import SystemNotification
from django.core.cache import cache
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import asyncio
from operator_compare import typed_equal,ttl_class_matches
loop = asyncio.new_event_loop()
layer = get_channel_layer()
channel = loop.run_until_complete(layer.new_channel('operator.owned.'))
loop.run_until_complete(layer.group_add('updates', channel))
async def receive_events():
    result=[]
    while len(result)<8:
        try: result.append(await asyncio.wait_for(layer.receive(channel), timeout=.15))
        except asyncio.TimeoutError: break
    return result
condition='dev_notif_condition_owned_operator'
unrelated='operator_owned_unrelated'



cases=json.loads(Path('/evidence/source-cases.json').read_text())['cases']
def state():
    with connection.cursor() as cursor:
        cursor.execute('SELECT last_value,is_called FROM core_systemnotification_id_seq')
        notification_sequence=list(cursor.fetchone())
    return {'notification_sequence':notification_sequence, 'network_identity':list(CoreSettings.objects.filter(key=NETWORK_ACCESS_KEY).values('id','key')), 'network': list(CoreSettings.objects.filter(key=NETWORK_ACCESS_KEY).values('key','name','value')),
            'users': list(User.objects.order_by('id').values('username','custom_properties')),
            'notifications':list(SystemNotification.objects.order_by('id').values('id','notification_key','notification_type','priority','source','title','message','action_data','is_active','admin_only','expires_at','created_at','updated_at')),
            'user_timestamps':list(User.objects.order_by('id').values('id','date_joined','last_login')),
            'cache':{key:cache.get(key) for key in [CoreSettings.group_cache_key(NETWORK_ACCESS_KEY),CoreSettings.group_cache_ver_key(NETWORK_ACCESS_KEY),condition,unrelated]},
            'cache_ttls':{key:cache.client.get_client().ttl(cache.make_key(key)) for key in [CoreSettings.group_cache_key(NETWORK_ACCESS_KEY),CoreSettings.group_cache_ver_key(NETWORK_ACCESS_KEY),condition,unrelated]}}


from datetime import datetime, timezone
from django.utils.dateparse import parse_datetime
source_cases=list(cases)
native_cases=[]

def restore(before):
    # All state belongs to the guarded copied fixture. QuerySet/raw SQL bypasses
    # signal hooks while arranging the independently captured before state.
    for row in before['users']:
        User.objects.filter(username=row['username']).update(custom_properties=row['custom_properties'])
    with connection.cursor() as cursor:
        cursor.execute('DELETE FROM core_coresettings WHERE key=%s',[NETWORK_ACCESS_KEY])
        for row in before['network']:
            identity=next(item['id'] for item in before['network_identity'] if item['key']==row['key'])
            cursor.execute('INSERT INTO core_coresettings(id,key,name,value) VALUES(%s,%s,%s,%s::jsonb)',[identity,row['key'],row['name'],json.dumps(row['value'])])
        cursor.execute('DELETE FROM core_notificationdismissal')
        cursor.execute('DELETE FROM core_systemnotification')
        columns=['id','notification_key','notification_type','priority','source','title','message','action_data','is_active','admin_only','expires_at','created_at','updated_at']
        for row in before['notifications']:
            values=[json.dumps(row[k]) if k=='action_data' else row[k] for k in columns]
            cursor.execute('INSERT INTO core_systemnotification('+','.join(columns)+') VALUES('+','.join(['%s']*len(columns))+')',values)
        cursor.execute("SELECT setval('core_systemnotification_id_seq',%s,%s)",before['notification_sequence'])
    for key,value in before['cache'].items():
        if value is None:cache.delete(key)
        else:cache.set(key,value,timeout=None if ':groupver:' in key else 300)
    loop.run_until_complete(receive_events())

def compare_notifications(original,actual,before,start,end):
    checks={}
    expected={x['notification_key']:x for x in original}
    actual={x['notification_key']:x for x in actual}
    previous={x['notification_key']:x for x in before}
    checks['notification_keys_exact']=expected.keys()==actual.keys()
    for key,row in expected.items():
        if key not in actual:continue
        got=actual[key]
        for field,value in row.items():
            if field in ['created_at','updated_at']:continue
            checks['notification_'+key+'_'+field]=typed_equal(got[field],value)
        for field in ['created_at','updated_at']:
            changed=key not in previous or row[field]!=previous[key][field]
            if changed:
                instant=parse_datetime(str(got[field]))
                checks['notification_'+key+'_'+field+'_actual_execution_window']=start<=instant<=end
            else:checks['notification_'+key+'_'+field+'_preserved']=got[field]==row[field]
    return checks

# Fresh official fixture database: seed only observed users/PK/dates, bypass hooks.
first=cases[0]['before']
for user, dates in zip(first['users'],first['user_timestamps']):
 assert user['username']
 User.objects.bulk_create([User(id=dates['id'],username=user['username'],password='!',custom_properties=user['custom_properties'],date_joined=parse_datetime(dates['date_joined']),last_login=parse_datetime(dates['last_login']) if dates['last_login'] else None)])
print(json.dumps({'ready':True,'users':len(first['users'])}),flush=True)
for line in sys.stdin:
 request=json.loads(line)
 if request['op']=='close':break
 case=cases[request['index']]
 if request['op']=='restore':
  restore(case['before']);answer={'restored':True}
 elif request['op']=='collect':
  after=json.loads(json.dumps(state(),default=str));events=loop.run_until_complete(receive_events())
  start=datetime.fromisoformat(request['start']);end=datetime.fromisoformat(request['end'])
  checks={'network_rows':typed_equal(after['network'],case['after']['network']),'network_PK_identity':typed_equal(after['network_identity'],case['after']['network_identity']),'user_properties':typed_equal(after['users'],case['after']['users']),'user_dates_preserved':typed_equal(after['user_timestamps'],case['after']['user_timestamps']),'notification_sequence_exact':typed_equal(after['notification_sequence'],case['after']['notification_sequence']),'actual_Channels_envelopes_order_count':typed_equal(events,case['events'])}
  for key,value in case['after']['cache'].items():
   if ':groupver:' in key and value!=case['before']['cache'][key]:
    checks['cache_group_version_integer_bumped']=type(after['cache'][key]) is int and after['cache'][key]!=case['before']['cache'][key]
    checks['cache_group_version_persistent']=after['cache_ttls'][key]==case['after']['cache_ttls'][key]==-1
   else:checks['cache_'+key]=typed_equal(after['cache'][key],value)
   checks['cache_ttl_class_'+key]=ttl_class_matches(case['after']['cache_ttls'][key],after['cache_ttls'][key])
  checks.update(compare_notifications(case['after']['notifications'],after['notifications'],case['before']['notifications'],start,end))
  answer={'after':after,'events':events,'checks':checks}
 else:raise ValueError('Unsupported owned observer operation')
 print(json.dumps({'response':request['op'],**answer},ensure_ascii=False,default=str),flush=True)
