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
        if action == 'hold_orm':
            import time
            release=MARKER.parent/'owned-action-release'
            record({'event':'hold-start'})
            deadline=time.monotonic()+6
            while not release.exists():
                if time.monotonic()>deadline:raise TimeoutError('Owned action release absent')
                time.sleep(.01)
            from apps.plugins.models import PluginConfig
            row=PluginConfig.objects.get(key='owned_contract')
            settings=dict(row.settings)
            settings['owned_hold_effect']=settings.get('owned_hold_effect',0)+1
            row.settings=settings
            row.save(update_fields=['settings','updated_at'])
            record({'event':'hold-finished','owned_hold_effect':settings['owned_hold_effect']})
            return {'status':'ok','owned_hold_effect':settings['owned_hold_effect']}
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

import http.client,subprocess,time,signal,traceback
from rest_framework_simplejwt.tokens import AccessToken
port=5657
ini=Path('/evidence/plugin-http-uwsgi.ini')
ini.write_text(Path('/verification/uwsgi.ini').read_text().replace('0.0.0.0:5656','127.0.0.1:'+str(port)))
log=Path('/evidence/plugin-http-server.log').open('w')
worker=None
cases=[]
def request(method,path,data=None,role='admin',headers=None):
    client=http.client.HTTPConnection('127.0.0.1',port,timeout=20)
    supplied={'Host':'plugin-fixture.invalid:8088','Accept':'application/json'}
    if role in users:supplied['Authorization']='Bearer '+str(AccessToken.for_user(users[role]))
    supplied.update(headers or {})
    body=json.dumps(data,ensure_ascii=False).encode()if data is not None else None
    if body is not None:supplied['Content-Type']='application/json'
    try:
        client.request(method,path,body=body,headers=supplied);reply=client.getresponse();payload=reply.read(2*1024*1024+1);assert len(payload)<=2*1024*1024
        return reply.status,dict(reply.getheaders()),payload
    finally:client.close()
def observe(name,method,suffix='',data=None,role='admin',headers=None):
    path='/api/plugins/plugins/'+suffix
    status,head,body=request(method,path,data,role,headers)
    item={'name':name,'method':method,'path':path,'role':role,'status':status,'headers':{k:head.get(k)for k in ['Content-Type','Content-Length','Allow','Vary','WWW-Authenticate','Cache-Control','X-Worker-ID']},'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest(),'body_base64':base64.b64encode(body).decode(),'actual_original_TCP':True}
    if body and head.get('Content-Type','').startswith('application/json'):item['json']=json.loads(body)
    row=PluginConfig.objects.filter(key=KEY).first()
    if row:item['owned_state']={k:getattr(row,k)for k in ['enabled','ever_enabled','settings','owned_tasks','suspended_schedules']}
    item['import_count']=sum(json.loads(line).get('event')=='import'for line in marker.read_text().splitlines())if marker.exists() else 0
    cases.append(item);return item
report={'passed':False,'scope':'Actual pinned Original uWSGI/TCP plugin control +owned Python/ORM actions; no native/compat executor acceptance','cases':cases,'native_verified':False,'performance_verified':False,'credit':'Original Dispatcharr authors retain all application/algorithm credit and AGPL licensing.'}
try:
    worker=subprocess.Popen(['uwsgi','--ini',str(ini)],env=dict(os.environ),stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    deadline=time.monotonic()+60
    while True:
        try:
            if request('GET','/api/core/version/')[0]==200:break
        except(OSError,http.client.HTTPException):pass
        if worker.poll()is not None:raise RuntimeError('Owned Original uWSGI exited')
        if time.monotonic()>deadline:raise TimeoutError('Owned Original uWSGI readiness')
        time.sleep(.1)
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
    for action in ['echo','scalar','orm','fail','undeclared']:
        observe('run-enabled-'+action,'POST',KEY+'/run/',{'action':action,'params':{'value':'日本語'}})
    release=root/'owned-action-release'
    assert not release.exists()
    wire=http.client.HTTPConnection('127.0.0.1',port,timeout=10)
    try:
        payload=json.dumps({'action':'hold_orm','params':{}}).encode()
        wire.request('POST','/api/plugins/plugins/'+KEY+'/run/',body=payload,headers={'Host':'plugin-fixture.invalid:8088','Accept':'application/json','Content-Type':'application/json','Authorization':'Bearer '+str(AccessToken.for_user(users['admin']))})
        deadline=time.monotonic()+4
        while True:
            events=[json.loads(line)for line in marker.read_text().splitlines()]if marker.exists()else[]
            if any(e.get('event')=='hold-start'for e in events):break
            if time.monotonic()>deadline:raise TimeoutError('Owned plugin action not admitted')
            time.sleep(.01)
        import socket
        if wire.sock:wire.sock.shutdown(socket.SHUT_RDWR)
        wire.close()
        assert PluginConfig.objects.get(key=KEY).settings.get('owned_hold_effect',0)==0
        release.touch()
        deadline=time.monotonic()+4
        while True:
            events=[json.loads(line)for line in marker.read_text().splitlines()]
            if any(e.get('event')=='hold-finished'for e in events):break
            if time.monotonic()>deadline:raise TimeoutError('Owned plugin action did not complete after caller abort')
            time.sleep(.01)
        assert PluginConfig.objects.get(key=KEY).settings['owned_hold_effect']==1
        report['caller_abort_control']={'actual_HTTP_action_admitted_before_abort':True,'no_ORM_effect_before_release':True,'actual_ORM_effect_after_caller_abort':True,'owned_release_and_completion_witness':True,'not_counted_as_successful_HTTP_response':True}
    finally:
        release.touch();wire.close()
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
    for accept in ['application/json; indent=2','text/plain','application/xml','text/html']:
        observe('list-negotiation-'+accept,'GET',headers={'Accept':accept})

    events=[json.loads(line)for line in marker.read_text().splitlines()]
    assert {'reload','disable','delete'}<={e.get('reason')for e in events if e['event']=='stop'}
    assert next(x for x in cases if x['name']=='head-factory-list')['body_bytes']==0
    assert next(x for x in cases if x['name']=='run-enabled-orm')['json']['result']['owned_effect']==1
    report.update(passed=True,events=events,plugin_code_sha256=hashlib.sha256(plugin_code.encode()).hexdigest(),all_plugin_files_and_config_rows_removed=not directory.exists()and not PluginConfig.objects.filter(key=KEY).exists(),limitations=['Actual original TCP on its owned copied-database uWSGI child; no native Rust/compatibility worker pass.','Only the controlled owned plugin: no managed ZIP/signature/repos/schedule/concurrency/general monkey-patch compatibility.','Connection-cleanup monkey patch from prior Factory oracle is not represented as a network observation.'])
except BaseException as error:
    report['error_type']=type(error).__name__;Path('/evidence/plugin-http-helper-failure.log').write_text(traceback.format_exc())
finally:
    if worker is not None:
        try:os.killpg(worker.pid,signal.SIGTERM)
        except ProcessLookupError:pass
        try:worker.wait(timeout=8)
        except subprocess.TimeoutExpired:
            os.killpg(worker.pid,signal.SIGKILL);worker.wait(timeout=3)
    log.close()
    if directory.exists():
        import shutil
        shutil.rmtree(directory)
    PluginConfig.objects.filter(key=KEY).delete()
    User.objects.filter(username__startswith=PREFIX).delete()
    report['owned_uWSGI_stopped']=worker is None or worker.poll()is not None
    report['owned_fixture_rows_removed']=not PluginConfig.objects.filter(key=KEY).exists()and not User.objects.filter(username__startswith=PREFIX).exists()
    report['passed']=report['passed']and report['owned_uWSGI_stopped']and report['owned_fixture_rows_removed']
    Path('/evidence/plugin-http-source.json').write_text(json.dumps(report,indent=2,ensure_ascii=False)+'\n')
print(json.dumps({'passed':report['passed'],'cases':len(cases)}))
if not report['passed']:raise SystemExit(1)
