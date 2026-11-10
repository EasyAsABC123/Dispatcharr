"""Owned original plugin/control-plane oracle; no native executor acceptance.
Original Dispatcharr authors and contributors retain all application credit.
Run only in the isolated copied-database fixture created by run.py.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

if (os.environ.get('PLUGIN_DISPOSABLE_FIXTURE') != 'playback-fixture-only'
        or not os.environ.get('POSTGRES_DB', '').startswith('rust_probe_backup_plugins_')):
    raise RuntimeError('Explicit copied plugin fixture required')
root = Path('/evidence/plugin-fixture')
root.mkdir(exist_ok=False)
plugins = root/'plugins'
plugins.mkdir()
os.environ['DISPATCHARR_PLUGINS_DIR'] = str(plugins)
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.urls import resolve
from rest_framework.test import APIRequestFactory, force_authenticate
from apps.accounts.models import User
from apps.plugins import loader
from apps.plugins.loader import PluginManager
from apps.plugins.models import PluginConfig

KEY = 'owned_contract'
PREFIX = 'rust-plugin-contract-'
if PluginConfig.objects.filter(key=KEY).exists() or User.objects.filter(username__startswith=PREFIX).exists():
    raise RuntimeError('Fresh owned plugin namespace required')
directory = plugins/KEY
directory.mkdir()
marker = root/'calls.jsonl'
fields = [{'id': 'prefix', 'label': 'Prefix', 'type': 'string', 'default': 'default-prefix'},
          {'id': 'counter', 'label': 'Counter', 'type': 'number', 'default': 3}]
actions = [{'id': name, 'label': name.title()} for name in ['echo', 'scalar', 'fail', 'orm']]
manifest = dict(name='Manifest fixture plugin', version='1.0', description='Owned original probe',
                author='Manifest fixture author', fields=fields, actions=actions)
(directory/'plugin.json').write_text(json.dumps(manifest))
plugin_code = '''import json
from pathlib import Path
MARKER = Path(__MARKER__)
def record(value):
    with MARKER.open('a') as stream: stream.write(json.dumps(value)+'\\n')
record({'event':'import','version':'live-1'})
class Plugin:
    name = 'Live fixture plugin'
    version = 'live-1'
    description = 'Owned original probe'
    author = 'Live fixture author'
    fields = __FIELDS__
    actions = __ACTIONS__
    def run(self, action, params, context):
        record({'event':'run','action':action,'params':params})
        if action == 'fail': raise ValueError('owned plugin failure')
        if action == 'scalar': return ['owned', 7]
        if action == 'orm':
            from apps.plugins.models import PluginConfig
            row = PluginConfig.objects.get(key='owned_contract')
            settings = dict(row.settings)
            settings['owned_effect'] = settings.get('owned_effect', 0)+1
            row.settings = settings
            row.save(update_fields=['settings','updated_at'])
            return {'status':'ok','owned_effect':settings['owned_effect']}
        return {'status':'ok','action':action,'params':params,
                'settings':context['settings'],'action_ids':list(context['actions']),
                'logger_name':context['logger'].name}
    def stop(self, context):
        record({'event':'stop','reason':context.get('reason'),'settings':context['settings']})
'''.replace('__MARKER__', repr(str(marker))).replace('__FIELDS__', repr(fields)).replace('__ACTIONS__', repr(actions))
(directory/'plugin.py').write_text(plugin_code)
(directory/'logo.png').write_bytes(base64.b64decode(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+aX1sAAAAASUVORK5CYII='))
manager = PluginManager.get()
manager.discover_plugins(sync_db=True)
assert not marker.exists(), 'Never-enabled manifest discovery unexpectedly imported code'
users = {name: User.objects.create(username=PREFIX+name, password='!', user_level=level, is_active=True)
         for name, level in [('admin', 10), ('standard', 1)]}
factory = APIRequestFactory()
cases = []


def observe(name, method, suffix='', data=None, role='admin', headers=None):
    path = '/api/plugins/plugins/'+suffix
    request = factory.generic(method, path, data=json.dumps(data).encode() if data is not None else b'',
                              content_type='application/json', HTTP_HOST='plugin-fixture.invalid:8088',
                              **(headers or {}))
    if role in users:
        force_authenticate(request, user=users[role])
    match = resolve(path)
    response = match.func(request, *match.args, **match.kwargs)
    if getattr(response, 'streaming', False):
        body = b''.join(response.streaming_content)
    else:
        if hasattr(response, 'render'):
            response.render()
        body = response.content
    mime = response.get('Content-Type', '').split(';')[0]
    result = {'name': name, 'method': method, 'path': path, 'role': role,
              'status': response.status_code, 'headers': dict(response.items()),
              'body_bytes': len(body), 'body_sha256': hashlib.sha256(body).hexdigest(),
              'factory_rendered_not_network_body': True}
    if mime == 'application/json':
        result['json'] = json.loads(body)
    if PluginConfig.objects.filter(key=KEY).exists():
        row = PluginConfig.objects.get(key=KEY)
        result['owned_state'] = {'enabled': row.enabled, 'ever_enabled': row.ever_enabled,
                                 'settings': row.settings, 'owned_tasks': row.owned_tasks,
                                 'suspended_schedules': row.suspended_schedules}
    result['import_count'] = sum(json.loads(line).get('event') == 'import'
                                 for line in marker.read_text().splitlines()) if marker.exists() else 0
    cases.append(result)
    return result


for role in ['anonymous', 'standard', 'admin']:
    observe('list-never-enabled-'+role, 'GET', role=role)
observe('head-factory-list', 'HEAD')
observe('options-standard', 'OPTIONS', role='standard')
observe('options-admin', 'OPTIONS')
observe('run-disabled', 'POST', KEY+'/run/', {'action': 'echo'})
observe('run-missing-action', 'POST', KEY+'/run/', {})
observe('run-unknown-plugin', 'POST', 'missing/run/', {'action': 'echo'})
observe('settings-standard', 'POST', KEY+'/settings/', {'settings': {'prefix': 'not-written'}}, role='standard')
observe('settings-owned', 'POST', KEY+'/settings/', {'settings': {'prefix': 'saved', 'unknown': 'preserved'}})
for value in [None, 'invalid', [], {}, 2]:
    observe('enabled-invalid-'+type(value).__name__, 'POST', KEY+'/enabled/', {'enabled': value})
enabled = observe('enable', 'POST', KEY+'/enabled/', {'enabled': 'yes'})
assert enabled['status'] == 200 and enabled['owned_state']['ever_enabled'] and enabled['import_count'] > 0
real_close = loader.close_old_connections
cleanup_calls = []


def cleanup():
    cleanup_calls.append('actual-close-old-connections')
    return real_close()


with patch.object(loader, 'close_old_connections', side_effect=cleanup):
    for action in ['echo', 'scalar', 'orm', 'fail', 'undeclared']:
        before = len(cleanup_calls)
        record = observe('run-enabled-'+action, 'POST', KEY+'/run/', {'action': action, 'params': {'value': '日本語'}})
        record['actual_connection_cleanup_calls'] = len(cleanup_calls)-before
        assert record['actual_connection_cleanup_calls'] >= 1
observe('logo-admin', 'GET', KEY+'/logo/')
observe('logo-anonymous', 'GET', KEY+'/logo/', role='anonymous')
observe('reload', 'POST', 'reload/', {})
disabled = observe('disable', 'POST', KEY+'/enabled/', {'enabled': 'off'})
assert not disabled['owned_state']['enabled'] and disabled['owned_state']['ever_enabled']
observe('list-ever-enabled-disabled', 'GET')
observe('run-disabled-again', 'POST', KEY+'/run/', {'action': 'echo'})
observe('re-enable', 'POST', KEY+'/enabled/', {'enabled': True})
observe('delete', 'DELETE', KEY+'/delete/')
assert not directory.exists() and not PluginConfig.objects.filter(key=KEY).exists()
observe('list-after-delete', 'GET')
events = [json.loads(line) for line in marker.read_text().splitlines()]
assert {'reload', 'disable', 'delete'} <= {event.get('reason') for event in events if event['event'] == 'stop'}
report = {'passed': True, 'scope': 'Original-only owned plugin manifest/trust/settings/actions/stop/ORM and Factory control surface',
          'cases': cases, 'events': events, 'actual_close_connection_calls': len(cleanup_calls),
          'plugin_code_sha256': hashlib.sha256(plugin_code.encode()).hexdigest(),
          'all_plugin_files_and_database_rows_removed': True,
          'native_verified': False, 'performance_verified': False,
          'limitations': ['APIRequestFactory forced-role responses, not network HEAD/error middleware or cached-auth acceptance.',
                          'No remote repo, managed ZIP/signature install, periodic-task ownership, concurrency or native interpreter/executor acceptance.'],
          'credit': 'Original Dispatcharr authors and contributors retain all application and algorithm credit and AGPL licensing.'}
Path('/evidence/plugin-source.json').write_text(json.dumps(report, indent=2)+'\n')
print(json.dumps({'passed': True, 'cases': len(cases)}))
