"""Owned Original createsuperuser oracle. All original authors retain credit.
Synthetic passwords are generated in memory and never written to evidence.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time
import unicodedata
import operator_rpc

if (os.environ.get('OPERATOR_DISPOSABLE_FIXTURE') != 'playback-fixture-only'
        or not os.environ.get('POSTGRES_DB', '').startswith('rust_probe_backup_operator_')):
    raise RuntimeError('Explicit owned copied operator fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
from django.db import connection
from core.models import SystemNotification
from channels.layers import get_channel_layer

loop = asyncio.new_event_loop()
layer = get_channel_layer()
channel = loop.run_until_complete(layer.new_channel('operator.create.'))
loop.run_until_complete(layer.group_add('updates', channel))
prefix = 'rust-create-owned-'
assert not User.objects.filter(username__startswith=prefix).exists()
output = Path('/evidence/operator-cli-raw')
output.mkdir(exist_ok=False)
cases = []
checks = []
password = secrets.token_urlsafe(32)

async def events():
    result = []
    while len(result) < 16:
        try:
            result.append(await asyncio.wait_for(layer.receive(channel), .15))
        except asyncio.TimeoutError:
            return result
    raise AssertionError('Unexpected event volume')

def state():
    # No stored password bytes or live configuration is exported.
    result = []
    for user in User.objects.filter(username__startswith=prefix).order_by('id'):
        row = {field: getattr(user, field) for field in (
            'id', 'username', 'email', 'first_name', 'last_name', 'is_staff',
            'is_superuser', 'is_active', 'user_level', 'avatar_config',
            'custom_properties', 'api_key', 'stream_limit', 'date_joined', 'last_login')}
        row.update(has_usable_password=user.has_usable_password(),
                   generated_password_valid=user.check_password(password),
                   empty_password_valid=user.check_password(''),
                   groups=list(user.groups.values_list('id', flat=True)),
                   permissions=list(user.user_permissions.values_list('id', flat=True)))
        result.append(row)
    with connection.cursor() as cursor:
        cursor.execute('SELECT last_value,is_called FROM accounts_user_id_seq')
        sequence = list(cursor.fetchone())
    return {'users': result, 'user_sequence': sequence,
            'notifications': list(SystemNotification.objects.order_by('id').values())}

def check(name, value):
    checks.append({'name': name, 'passed': bool(value)})
    assert value, name

def run(name, arguments, *, env=None, code=0, new_username=None, usable=None, empty_password=False):
    loop.run_until_complete(events())
    before = state()
    environment = {k: v for k, v in os.environ.items()
                   if not k.startswith('DJANGO_SUPERUSER_')}
    environment.update(env or {})
    start = time.time()
    environment.update(DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/' + os.environ['POSTGRES_DB'], REDIS_URL='redis://:playback-fixture-only@127.0.0.1:6379/0')
    child = operator_rpc.run(['/evidence/native-operator', 'createsuperuser', *arguments],
                           env=environment, stdin=subprocess.DEVNULL,
                           capture_output=True, timeout=30)
    end = time.time()
    after = state()
    observed_events = loop.run_until_complete(events())
    for stream in ('stdout', 'stderr'):
        raw = getattr(child, stream)
        assert password.encode() not in raw
        (output / (name + '.' + stream)).write_bytes(raw)
    case = {'id': name, 'arguments': arguments,
            'environment_keys': sorted((env or {}).keys()),
            'synthetic_password_value_omitted': True,
            'exit_code': child.returncode, 'stdout': child.stdout.decode(),
            'stderr': child.stderr.decode(), 'start_epoch': start, 'end_epoch': end,
            'stdout_sha256': hashlib.sha256(child.stdout).hexdigest(),
            'stderr_sha256': hashlib.sha256(child.stderr).hexdigest(),
            'before': before, 'after': after, 'events': observed_events}
    cases.append(case)
    Path('/evidence/operator-cli-partial.json').write_text(json.dumps(
        {'cases': cases, 'checks': checks}, default=str, ensure_ascii=False, indent=2) + '\n')
    check(name + ':exit', child.returncode == code)
    if new_username is None:
        check(name + ':users-unchanged', before['users'] == after['users'])
    else:
        created = [r for r in after['users'] if r['username'] == new_username]
        check(name + ':one-new-row', len(after['users']) == len(before['users']) + 1 and len(created) == 1)
        row = created[0]
        check(name + ':admin-flags', row['is_staff'] is True and row['is_superuser'] is True
              and row['is_active'] is True and row['user_level'] == 10)
        check(name + ':defaults', row['first_name'] == '' and row['last_name'] == ''
              and row['stream_limit'] == 0 and row['api_key'] is None
              and row['avatar_config'] == {} and row['custom_properties'] == {}
              and row['last_login'] is None and row['groups'] == [] and row['permissions'] == [])
        check(name + ':joined-in-command-window', start <= row['date_joined'].timestamp() <= end)
        check(name + ':password', row['has_usable_password'] is usable
              and row['generated_password_valid'] is (usable and not empty_password)
              and row['empty_password_valid'] is empty_password)
        check(name + ':existing-users-preserved', all(r in after['users'] for r in before['users']))
    return case

run('help', ['--help'])
run('unknown-flag', ['--owned-unknown'], code=2)
run('no-input-missing-username', ['--noinput'], code=1)
run('invalid-username', ['--noinput', '--username', prefix + 'bad space', '--email', 'owned@example.invalid'], code=1)
run('invalid-email', ['--noinput', '--username', prefix + 'bad-email', '--email', 'bad-email'], code=1)
normal = prefix + 'normal'
run('normal', ['--noinput', '--username', normal, '--email', 'Owned@EXAMPLE.INVALID'],
    env={'DJANGO_SUPERUSER_PASSWORD': password}, new_username=normal, usable=True)
check('normal:email-domain-normalization', User.objects.get(username=normal).email == 'Owned@example.invalid')
run('duplicate', ['--noinput', '--username', normal, '--email', 'other@example.invalid'], code=1)
env_user = prefix + 'environment'
run('environment', ['--noinput'], env={'DJANGO_SUPERUSER_USERNAME': env_user,
    'DJANGO_SUPERUSER_EMAIL': 'env@example.invalid', 'DJANGO_SUPERUSER_PASSWORD': password},
    new_username=env_user, usable=True)
unusable = prefix + 'unusable'
run('missing-password', ['--noinput', '--username', unusable, '--email', 'owned@example.invalid'],
    new_username=unusable, usable=False)
empty = prefix + 'empty-password'
run('empty-password', ['--noinput', '--username', empty, '--email', 'owned@example.invalid'],
    env={'DJANGO_SUPERUSER_PASSWORD': ''}, new_username=empty, usable=True, empty_password=True)
wide = prefix + '\uff21\uff22\uff23'
run('unicode-nfkc', ['--noinput', '--username', wide, '--email', 'wide@example.invalid'],
    env={'DJANGO_SUPERUSER_PASSWORD': password}, new_username=unicodedata.normalize('NFKC', wide), usable=True)
quiet = prefix + 'quiet'
run('quiet', ['--noinput', '--username=' + quiet, '--email=quiet@example.invalid', '--verbosity', '0'],
    env={'DJANGO_SUPERUSER_PASSWORD': password}, new_username=quiet, usable=True)

for suffix, address, status in [('single-tld','a@b.c',1), ('numeric-tld','a@b.12',1), ('mixed-tld','a@b.a1',1), ('localhost','a@localhost',0), ('hyphen-tld','a@b.c-d',0), ('punycode-tld','a@b.xn--a',0)]:
    target = prefix + suffix
    run(suffix, ['--noinput','--username',target,'--email',address], code=status,
        new_username=target if status == 0 else None, usable=False if status == 0 else None)
run('required-email', ['--noinput','--username',prefix+'required-email'], code=1)
run('empty-email', ['--noinput','--username',prefix+'empty-email','--email='],
    new_username=prefix+'empty-email', usable=False)
run('option-like-value', ['--noinput','--username','--email','owned@example.invalid'], code=2)
run('normalized-duplicate', ['--noinput','--username',wide,'--email','duplicate@example.invalid'], code=1)

loop.run_until_complete(layer.group_discard('updates', channel))
Path('/evidence/operator-cli-source.json').write_text(json.dumps({
    'passed': all(c['passed'] for c in checks), 'scope': 'Bounded native createsuperuser effects/exit-code cases observed with Original Django; not stdout/stderr byte parity',
    'native_verified': True, 'performance_verified': False, 'cases': cases, 'checks': checks,
    'credit': 'All original application and Django behavior credit remains with their authors.'},
    default=str, ensure_ascii=False, indent=2) + '\n')
