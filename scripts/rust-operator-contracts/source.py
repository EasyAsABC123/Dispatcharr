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


out = Path('/evidence/operator-cli-raw')
out.mkdir(exist_ok=False)
prefix = 'rust-operator-owned-'
assert not User.objects.filter(username__startswith=prefix).exists()
users = {name: User.objects.create(username=prefix+name, password='!', custom_properties=value)
         for name, value in [('a', {'allowed_networks':['192.0.2.0/24'], 'keep':{'typed':[1,True,None]}}),
                             ('b', {'keep':'café'}), ('null', None), ('empty', {})]}
cases = []

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


def run(name, arguments, code=0):
    # QuerySet writes bypass the reset signal, so every command starts with
    # independently observable stale cache and notification rows.
    if arguments[0]=='reset_network_access' and name not in ['reset_network_access-help','network-missing']:
        cache.set(CoreSettings.group_cache_key(NETWORK_ACCESS_KEY), {'owned_stale':True}, timeout=300)
        cache.set(condition, {'owned':True}, timeout=300)
        cache.set(unrelated, {'preserve':True}, timeout=300)
        SystemNotification.objects.filter(notification_key='network_security_m3u_epg').update(title='Owned stale title',is_active=False)
        SystemNotification.objects.get_or_create(notification_key='owned_operator_obsolete', defaults={'source':'developer','title':'Owned obsolete','message':'remove only copied row'})
        SystemNotification.objects.get_or_create(notification_key='owned_operator_system', defaults={'source':'system','title':'Owned system','message':'preserve copied row'})
    loop.run_until_complete(receive_events())
    before = state()
    result = subprocess.run([sys.executable, '/app/manage.py', *arguments], capture_output=True,
                            timeout=20, env=dict(os.environ, PYTHONUNBUFFERED='1'))
    after = state()
    events = loop.run_until_complete(receive_events())
    for stream in ('stdout','stderr'):
        (out/(name+'.'+stream)).write_bytes(getattr(result,stream))
    row = {'id':name, 'arguments':arguments, 'exit_code':result.returncode,
           'stdout':result.stdout.decode('utf-8'), 'stderr':result.stderr.decode('utf-8'),
           'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),
           'stderr_sha256':hashlib.sha256(result.stderr).hexdigest(),
           'before':before,'after':after,'events':events}
    cases.append(row)
    Path('/evidence/operator-cli-partial.json').write_text(json.dumps({'cases':cases},indent=2,ensure_ascii=False,default=str)+'\n')
    assert result.returncode == code, (name, result.returncode, result.stderr[-2000:])
    return row

for command in ('reset_network_access','reset_user_network'):
    row=run(command+'-help',[command,'--help'])
    assert all(row['before'][key]==row['after'][key] for key in ['network','users'])
row=run('unknown-flag',['reset_user_network','--owned-unknown'],2)
assert all(row['before'][key]==row['after'][key] for key in ['network','users'])
for suffix,value in [('object',{'allowed':['192.0.2.0/24'],'keep':True}), ('array',[1,'value']), ('scalar','text'), ('null',None)]:
    stored_value = Value(None, output_field=JSONField()) if value is None else value
    CoreSettings.objects.update_or_create(key=NETWORK_ACCESS_KEY, defaults={'name':'Owned network setting','value':stored_value})
    row=run('network-'+suffix,['reset_network_access'])
    assert row['after']['network']==[{'key':NETWORK_ACCESS_KEY,'name':'Owned network setting','value':{}}]
    assert row['before']['users']==row['after']['users']
CoreSettings.objects.filter(key=NETWORK_ACCESS_KEY).delete()
row=run('network-missing',['reset_network_access'],1)
assert all(row['before'][key]==row['after'][key] for key in ['network','users']) and 'DoesNotExist' in row['stderr']
row=run('user-target',['reset_user_network','--user',prefix+'a'])
assert row['after']['users'] != row['before']['users']
users['a'].refresh_from_db()
assert users['a'].custom_properties=={'keep':{'typed':[1,True,None]}}
assert 'Reset allowed_networks for 1 user(s).' in row['stdout']
for suffix,name in [('already-clear','a'),('null','null'),('empty','empty'),('missing','missing')]:
    row=run('user-'+suffix,['reset_user_network','--user',prefix+name])
    assert all(row['before'][key]==row['after'][key] for key in ['network','users']) and 'Reset allowed_networks for 0 user(s).' in row['stdout']
for name in ('a','b'):
    User.objects.filter(pk=users[name].pk).update(custom_properties={'allowed_networks':[], 'keep':name})
row=run('users-all',['reset_user_network'])
assert 'Reset allowed_networks for 2 user(s).' in row['stdout']
for name in ('a','b'):
    users[name].refresh_from_db()
    assert users[name].custom_properties=={'keep':name}
User.objects.filter(pk=users['a'].pk).update(custom_properties={'allowed_networks':'192.0.2.1', 'keep':'a'})
row=run('users-empty-filter',['reset_user_network','--user',''])
assert 'Reset allowed_networks for 1 user(s).' in row['stdout']
# JSONField accepts non-object values. Membership may be false (no update), or
# true followed by an actual TypeError; capture these original edge semantics.
User.objects.filter(pk=users['a'].pk).update(custom_properties=['other'])
row=run('user-array-no-match',['reset_user_network','--user',prefix+'a'])
assert all(row['before'][key]==row['after'][key] for key in ['network','users'])
User.objects.filter(pk=users['a'].pk).update(custom_properties=['allowed_networks'])
row=run('user-array-match',['reset_user_network','--user',prefix+'a'],1)
assert all(row['before'][key]==row['after'][key] for key in ['network','users']) and 'TypeError' in row['stderr']
report={'passed':True,'scope':'Original actual manage.py subprocesses on owned copied PostgreSQL',
        'native_verified':False,'performance_verified':False,'cases':cases,
        'commands':['reset_network_access','reset_user_network'],
        'destructive_commands_executed':False}
Path('/evidence/operator-cli-source.json').write_text(json.dumps(report,indent=2,ensure_ascii=False,default=str)+'\n')
print(json.dumps({'passed':True,'cases':len(cases)}))
