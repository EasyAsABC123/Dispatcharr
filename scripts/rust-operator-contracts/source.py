"""Original operator CLI oracle. Original Dispatcharr authors retain full credit.
Network resets and the pinned help-only roster use an owned copied DB.
"""
import hashlib
import json
import os
import selectors
from pathlib import Path
import subprocess
import sys
import time

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


case_set = os.environ.get('OPERATOR_CASE_SET', 'legacy')
if case_set not in ('legacy', 'repeated-user', 'arguments-help'):
    raise ValueError('Unsupported operator case set')

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


def help_arguments(path):
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != "4f86ffebdee37c448083e4bfd96de0c2d362e5c874a677c258bbc9a5abbc5bed":
        raise ValueError("Exact Original help roster required")
    plan = json.loads(raw)
    commands = ("", "createsuperuser", "reset_network_access", "reset_user_network",
                "proxy", "dropdb", "kill_processes")
    if plan["Source_commands"] != len(commands) or len(plan["commands"]) != len(commands):
        raise ValueError("Expected all seven Original help invocations")
    for index, (entry, command) in enumerate(zip(plan["commands"], commands), 1):
        arguments = ([command] if command else []) + ["--help"]
        if (entry["index"] != index or entry["stdin"] != "DEVNULL"
                or entry["destructive_handle_must_not_run"] is not True
                or entry["argv"] != ["/dispatcharrpy/bin/python", "manage.py", *arguments]):
            raise ValueError("Only the exact read-only help invocations are allowed")
        yield (command or "global") + "-help", arguments


def capture_help(argv, environment):
    child = subprocess.Popen(argv, env=environment, stdin=subprocess.DEVNULL,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output = {"stdout": bytearray(), "stderr": bytearray()}
    deadline = time.monotonic() + 20
    try:
        process = Path("/proc") / str(child.pid)
        generation = int((process / "stat").read_text().rpartition(")")[2].split()[19])
        namespace = os.readlink(process / "ns/pid")
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ, "stdout")
            selector.register(child.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                ready = selector.select(max(0, deadline - time.monotonic()))
                if not ready:
                    raise subprocess.TimeoutExpired(argv, 20)
                for key, _ in ready:
                    chunk = os.read(key.fd, 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    buffer = output[key.data]
                    if len(buffer) + len(chunk) > 1024 * 1024:
                        raise ValueError("help capture capacity")
                    buffer.extend(chunk)
                if time.monotonic() >= deadline:
                    raise subprocess.TimeoutExpired(argv, 20)
        exit_code = child.wait(timeout=max(0, deadline - time.monotonic()))
        stdout, stderr = bytes(output["stdout"]), bytes(output["stderr"])
        result = subprocess.CompletedProcess(argv, exit_code, stdout, stderr)
        return result, {"pid": child.pid, "start_ticks": generation, "pid_namespace": namespace,
                        "stdout_bytes": len(stdout), "stderr_bytes": len(stderr),
                        "pipes_closed": child.stdout.closed and child.stderr.closed}
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=5)
        for stream in (child.stdout, child.stderr):
            stream.close()


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
    argv = [sys.executable, '/app/manage.py', *arguments]
    environment = dict(os.environ, PYTHONUNBUFFERED='1')
    if case_set == 'arguments-help':
        result, process = capture_help(argv, environment)
        used = sum(row["process"]["stdout_bytes"] + row["process"]["stderr_bytes"] for row in cases)
        if used + process["stdout_bytes"] + process["stderr_bytes"] > 8 * 1024 * 1024:
            raise ValueError("aggregate help capture capacity")
    else:
        result = subprocess.run(argv, capture_output=True, timeout=20, env=environment)

    after = state()
    events = loop.run_until_complete(receive_events())
    for stream in ('stdout','stderr'):
        (out/(name+'.'+stream)).write_bytes(getattr(result,stream))
    row = {'id':name, 'arguments':arguments, 'exit_code':result.returncode,
           'stdout':result.stdout.decode('utf-8'), 'stderr':result.stderr.decode('utf-8'),
           'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),
           'stderr_sha256':hashlib.sha256(result.stderr).hexdigest(),
           'before':before,'after':after,'events':events}
    if case_set == 'arguments-help':
        row.update(actual_argv=argv, process=process)
    cases.append(row)
    Path('/evidence/operator-cli-partial.json').write_text(json.dumps({'cases':cases},indent=2,ensure_ascii=False,default=str)+'\n')
    assert result.returncode == code, (name, result.returncode, result.stderr[-2000:])
    return row

def finish():
    report = {
        'passed': True,
        'scope': 'Original actual manage.py subprocesses on owned copied PostgreSQL',
        'native_verified': False,
        'performance_verified': False,
        'cases': cases,
        'commands': ['reset_network_access', 'reset_user_network'],
        'destructive_commands_executed': False,
    }
    if case_set != 'legacy':
        report['case_set'] = case_set
    if case_set == 'arguments-help':
        report['commands'] = [case['arguments'] for case in cases]
    Path('/evidence/operator-cli-source.json').write_text(
        json.dumps(report, indent=2, ensure_ascii=False, default=str)+'\n')
    print(json.dumps({'passed': True, 'cases': len(cases)}))

def check_repeated_user_startup_notification(before, after):
    key = 'network_security_m3u_epg'
    previous = next((row for row in before['notifications'] if row['notification_key'] == key), None)
    current, = (row for row in after['notifications'] if row['notification_key'] == key)
    assert current['source'] == 'developer'
    if previous is None:
        # The first CLI performs app-ready synchronization in the new copied DB.
        # PostgreSQL nextval returns last_value unchanged when is_called is false.
        last, called = before['notification_sequence']
        inserted = last + int(called)
        assert current['id'] == inserted
        assert after['notification_sequence'] == [inserted, True]
    else:
        assert current['id'] == previous['id']
        assert after['notification_sequence'] == before['notification_sequence']


def run_repeated_user_cases():
    def seed():
        for name in ('a', 'b'):
            User.objects.filter(pk=users[name].pk).update(custom_properties={
                'allowed_networks': ['192.0.2.0/24' if name == 'a' else '198.51.100.0/24'],
                'keep': {'name': name, 'typed': [1, True, None]},
            })
        cache.set(CoreSettings.group_cache_key(NETWORK_ACCESS_KEY), {'stale': 'owned'}, timeout=300)
        cache.set(CoreSettings.group_cache_ver_key(NETWORK_ACCESS_KEY), 501, timeout=None)
        cache.set(condition, {'condition': 'owned'}, timeout=300)
        cache.set(unrelated, {'preserve': True}, timeout=300)

    def check(name, options, targets, exit_code=0):
        seed()
        row = run(name, ['reset_user_network', *options], exit_code)
        before, after = row['before'], row['after']
        expected = [dict(item) for item in before['users']]
        for item in expected:
            if item['username'] in targets:
                item['custom_properties'] = {key: value for key, value in
                                             item['custom_properties'].items()
                                             if key != 'allowed_networks'}
        assert json.dumps(after['users'], sort_keys=True) == json.dumps(expected, sort_keys=True), name
        for field in ('network', 'network_identity', 'user_timestamps', 'cache'):
            assert after[field] == before[field], (name, field)
        check_repeated_user_startup_notification(before, after)
        original_ttls = before['cache_ttls']
        actual_ttls = after['cache_ttls']
        assert actual_ttls.keys() == original_ttls.keys()
        for key, ttl in original_ttls.items():
            assert 0 < actual_ttls[key] <= ttl if ttl > 0 else actual_ttls[key] == ttl
        assert row['events'] == [{'type': 'update', 'data': {'type': 'notifications_cleared'}}]
        stdout = f'Reset allowed_networks for {len(targets)} user(s).\n' if exit_code == 0 else ''
        assert row['stdout'] == stdout, name

    check('repeat-user-target-last', ['--user='+prefix+'a', '--user', prefix+'b'], {prefix+'b'})
    check('repeat-user-empty-last', ['--user', prefix+'a', '--user='], {prefix+'a', prefix+'b'})
    check('repeat-user-malformed-last', ['--user='+prefix+'a', '--user'], set(), 2)


if case_set == 'arguments-help':
    for name, arguments in help_arguments(Path(__file__).with_name("operator-help-cases.json")):
        row = run(name, arguments)
        assert row["stdout"], name
        assert all(row["before"][field] == row["after"][field]
                   for field in ("network", "users", "user_timestamps")), name
    finish()
    raise SystemExit(0)

if case_set == 'repeated-user':
    run_repeated_user_cases()
    finish()
    raise SystemExit(0)

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
finish()
