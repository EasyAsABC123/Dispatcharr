"""Actual pinned Original import HTTP/publication on an isolated copied database.
Original Dispatcharr authors retain all application credit and AGPL licensing.
No task worker is launched: queued envelopes are observed, never consumed.
"""
import base64, hashlib, inspect, json, os, signal, subprocess, sys, time
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE') != 'playback-fixture-only':
    raise RuntimeError('Owned fixture required')
database = os.environ.get('POSTGRES_DB', '')
if not database.startswith('rust_probe_backup_jobs_') or len(database) > 63 or not all(c in 'abcdefghijklmnopqrstuvwxyz0123456789_' for c in database):
    raise RuntimeError('Copied database required')
sys.path[:0] = ['/app', '/job-tools']
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
import redis, requests
from apps.accounts.models import User
from apps.epg.models import EPGSource
from apps.epg.api_views import EPGImportAPIView
from rest_framework_simplejwt.tokens import RefreshToken
broker = redis.Redis.from_url(os.environ['CELERY_BROKER_URL'])
backend = redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
cache = redis.Redis(host='127.0.0.1', password='playback-fixture-only', db=0)
if broker.connection_pool.connection_kwargs['db'] != 8 or backend.connection_pool.connection_kwargs['db'] != 9:
    raise RuntimeError('Owned broker/result namespaces required')
for key in ['dvr:recover_dispatch_lock', 'core:version_check_dispatch_lock']:
    cache.set(key, '1', ex=3600)
report = {'passed': False, 'checks': [], 'cases': [], 'scope': 'Original HTTP and durable broker publication only; no worker/native execution', 'copied_database': database}
processes = []
users = []
sources = []
def check(name, condition):
    report['checks'].append({'name': name, 'passed': bool(condition)})
    if not condition:
        raise AssertionError(name)
def start(port, denied=False):
    env = dict(os.environ, PYTHONPATH='/app')
    if denied:
        env['CELERY_BROKER_URL'] = 'redis://rust_import_denied:playback-fixture-only@localhost:6379/8'
    log = Path('/evidence/import-web-' + str(port) + '-private.log').open('ab')
    process = subprocess.Popen(['/dispatcharrpy/bin/uwsgi', '--http', '127.0.0.1:' + str(port), '--module', 'dispatcharr.wsgi:application', '--master', '--workers', '1', '--threads', '1', '--die-on-term', '--need-app'], env=env, stdout=log, stderr=log, start_new_session=True)
    log.close()
    processes.append(process)
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        try:
            if requests.get('http://127.0.0.1:' + str(port) + '/api/core/version/', timeout=2).status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(.1)
    raise TimeoutError('Copied-database web readiness')
def request(label, role='admin', method='POST', data=None, raw=None, expected=None, expected_payload=None, port=5657):
    broker.flushdb()
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
    if role == 'invalid':
        headers['Authorization'] = 'Bearer invalid'
    elif role in tokens:
        headers['Authorization'] = 'Bearer ' + tokens[role]
    body = raw if raw is not None else json.dumps(data)
    response = requests.request(method, 'http://127.0.0.1:' + str(port) + '/api/epg/import/', headers=headers, data=body, timeout=25)
    if len(response.content) > 65536:
        raise RuntimeError('HTTP response bound')
    queued = broker.lrange('celery', 0, 64)
    if len(queued) > 64 or any(len(v) > 65536 for v in queued):
        raise RuntimeError('Envelope count/byte bound')
    envelopes = [json.loads(v) for v in queued]
    for value in envelopes:
        check(label + '-only-owned-refresh-family', value['headers']['task'] == 'apps.epg.tasks.refresh_epg_data')
    try:
        parsed = response.json()
    except ValueError:
        parsed = None
    report['cases'].append({'name': label, 'method': method, 'role': role, 'request': data if raw is None else {'raw': raw}, 'status': response.status_code, 'mime': response.headers.get('Content-Type'), 'body': parsed, 'body_sha256': hashlib.sha256(response.content).hexdigest(), 'body_bytes': len(response.content), 'envelopes': envelopes})
    if expected is not None:
        check(label + '-status', response.status_code == expected)
    if response.status_code == 202:
        check(label + '-exact-ack-body', parsed == {'success': True, 'message': 'EPG data refresh initiated.'})
        check(label + '-one-durable-publication', len(envelopes) == 1)
        payload = json.loads(base64.b64decode(envelopes[0]['body'], validate=True))
        if expected_payload is not None:
            check(label + '-exact-published-args', payload[:2] == expected_payload)
        check(label + '-result-not-written', backend.dbsize() == 0)
    else:
        check(label + '-no-publication', not envelopes)
from apps.epg.models import EPGData, ProgramData
from apps.channels.models import Channel
from dispatcharr.celery import app
import datetime as dt
try:
    broker.flushdb(); backend.flushdb()
    root = Path('/evidence/import-application-files'); root.mkdir()
    start_time = dt.datetime.now(dt.timezone.utc).replace(microsecond=0) + dt.timedelta(hours=1)
    end = start_time + dt.timedelta(hours=1)
    guide = root / 'guide.xml'
    guide.write_text('<tv><channel id="app-owned"><display-name>Owned HTTP channel</display-name></channel><programme channel="app-owned" start="' + start_time.strftime('%Y%m%d%H%M%S +0000') + '" stop="' + end.strftime('%Y%m%d%H%M%S +0000') + '"><title>Owned application programme</title></programme></tv>')
    for label, active, kind in [('inactive', False, 'xmltv'), ('dummy', True, 'dummy'), ('imported', True, 'xmltv')]:
        source = EPGSource(name='rust-native-refresh-app-' + label, is_active=active, source_type=kind, file_path=str(guide), custom_properties={})
        EPGSource.objects.bulk_create([source]); sources.append(source.pk)
    inactive, dummy, imported = sources
    missing = max(sources) + 999
    epg = EPGData.objects.create(epg_source_id=imported, tvg_id='app-owned', name='Owned old guide')
    channel = Channel(name='rust-native-refresh-app', channel_number=1987654, epg_data=epg)
    Channel.objects.bulk_create([channel])
    tokens = {}
    for label, level, active in [('basic', 0, True), ('standard', 1, True), ('admin', 10, True), ('inactive', 10, False)]:
        user = User.objects.create(username='rust-import-app-' + label, password='!', user_level=level, is_active=active, custom_properties={})
        users.append(user.pk); tokens[label] = str(RefreshToken.for_user(user).access_token)
    start(5657)
    cases = []
    for role, status in [('anonymous',401),('invalid',401),('basic',403),('standard',403),('inactive',401)]:
        cases.append(('role-' + role,role,'POST',json.dumps({'id':inactive}),status))
    cases += [
        ('dummy','admin','POST',json.dumps({'id':dummy}),400),
        ('malformed','admin','POST','{',400),
        ('null-document','admin','POST','null',500),
        ('list-document','admin','POST','[]',500),
        ('admin-get','admin','GET','{}',405),
        ('standard-get','standard','GET','{}',405),
        ('standard-head','standard','HEAD','{}',405),
        ('admin-head','admin','HEAD','{}',405),
        ('admin-options','admin','OPTIONS','{}',200),
        ('standard-options','standard','OPTIONS','{}',403),
        ('admin-put','admin','PUT','{}',405),
        ('inactive-source','admin','POST',json.dumps({'id':inactive,'force':'false'}),202),
        ('missing-source','admin','POST',json.dumps({'id':missing}),202),
        ('imported','admin','POST',json.dumps({'id':imported,'force':True}),202),
    ]
    native_requests = []
    for label, role, method, raw, status in cases:
        request(label,role=role,method=method,raw=raw,expected=status)
        token = 'invalid' if role == 'invalid' else tokens.get(role)
        native_requests.append({'name':label,'method':method,'raw':raw,'token':token})
    cache.execute_command('ACL','SETUSER','rust_import_denied','reset','on','>playback-fixture-only','~*','&*','+@all','-lpush')
    start(5658,denied=True)
    request('broker-lpush-denied',data={'id':inactive},port=5658,expected=500)
    broker.flushdb(); backend.flushdb()
    native_input = {'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/' + database, 'broker_url':os.environ['CELERY_BROKER_URL'], 'result_url':os.environ['CELERY_RESULT_BACKEND'], 'requests':native_requests, 'expected_results':3, 'control_token':tokens['admin'], 'control_source_id':inactive, 'denied_broker_url':'redis://rust_import_denied:playback-fixture-only@localhost:6379/8'}
    peer = subprocess.run(['/evidence/native-refresh-worker','--ignored','--exact','refresh_application::peer::native_refresh_application_http_peer','--nocapture'],input=json.dumps(native_input) + '\n',capture_output=True,text=True,timeout=60)
    Path('/evidence/native-application-http-private.log').write_text(peer.stdout + peer.stderr)
    peer.check_returncode()
    marker = 'NATIVE_REFRESH_APPLICATION_JSON='
    actual = json.loads(next(line.split(marker,1)[1] for line in peer.stdout.splitlines() if marker in line))
    check('native-actual-http-response-count',len(actual['records']) == len(report['cases']))
    for source, native in zip(report['cases'],actual['records']):
        check('native-actual-http-status-' + source['name'],source['status'] == native['status'])
        # HEAD has an empty wire body for both servers.
        check('native-actual-http-json-' + source['name'],source['body'] == native['body'])
        check('native-actual-http-bytes-and-mime-' + source['name'],source['body_sha256']==native['body_sha256'] and source['body_bytes']==native['bytes'] and source['mime']==native['mime'])
    keys = backend.keys('celery-tasks:celery-task-meta-*')
    check('three-actual-native-terminal-records',len(keys) == 3)
    results=[]
    for key in sorted(keys):
        raw = backend.get(key); check('bounded-native-result',raw is not None and len(raw) <= 262144)
        metadata = json.loads(raw); ident = metadata['task_id']; result = app.AsyncResult(ident)
        value = result.get(timeout=2,propagate=False)
        check('actual-Original-AsyncResult-decoding',result.ready() and result.successful() and metadata['status']=='SUCCESS' and metadata['traceback'] is None)
        check('actual-terminal-expiry',3580 <= backend.ttl(key) <= 3600)
        results.append(metadata)
    check('exact-native-null-and-cleanup-returns',sum(row['result'] is None for row in results)==2 and any(row['result']==f'EPG source {missing} does not exist, task cleaned up' for row in results))
    state = list(EPGSource.objects.filter(pk=imported).values('status','last_message','custom_properties'))
    check('imported-programme-committed',ProgramData.objects.filter(epg=epg,title='Owned application programme').exists())
    check('imported-source-terminal-success',state[0]['status']==EPGSource.STATUS_SUCCESS)
    queued = [json.loads(value) for value in reversed(broker.lrange('celery',0,63))]
    check('nonrefresh-child-families-preserved', [value['headers']['task'] for value in queued] == ['apps.epg.tasks.build_programme_index_task','apps.channels.tasks.evaluate_series_rules','apps.epg.tasks.refresh_epg_data'])
    check('actual-service-drained',actual['drained'])
    check('held-actual-SQL-aborted-client-and-retained-shutdown',actual['held_sql_caller_abort'] and actual['active_shutdown_reported_incomplete'] and not actual['aborted_http_response_observed'])
    pending_id = queued[-1]['headers']['id']
    check('aborted-client-actual-publication-not-invented-result',app.AsyncResult(pending_id).state=='PENDING' and backend.get(app.backend.get_key_for_task(pending_id)) is None)
    report['native'] = actual
    report['results'] = results
    report['source_state'] = state
    report['queued_children'] = queued
    report['scope'] = 'Actual Original/native authenticated JSON HTTP positive-ID publication and local import/normal result/status plus drain; no packaged app/main, raw-ID shapes, provider/ETA/general-family execution or bus-wire equivalence'
    report['passed'] = True
finally:
    for process in processes:
        if process.poll() is None:
            os.killpg(process.pid,signal.SIGTERM)
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: os.killpg(process.pid,signal.SIGKILL);process.wait(timeout=5)
    cache.execute_command('ACL','DELUSER','rust_import_denied')
    User.objects.filter(pk__in=users).delete()
    Channel.objects.filter(name='rust-native-refresh-app').delete()
    EPGSource.objects.filter(pk__in=sources).delete()
    broker.flushdb();backend.flushdb()
    report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()]
    Path('/evidence/source-native-refresh-http-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
