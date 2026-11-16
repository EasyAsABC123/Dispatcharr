"""Source-only actual recording events in the existing operations Fixture.
Original authors retain all application/algorithm credit. No starts on import.
"""
import base64
import datetime
import hashlib
import json
import traceback
from pathlib import Path

CASE = 'recording-plugin-characterization'
KEY = 'owned_recording_events'


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=str) + '\n')


def prepare_context(f):
    if f.backend != 'Original' or f.case != CASE:
        raise ValueError('IndependentOriginalRecordingEventContextRequired')
    source = Path(__file__).with_name('recording_event_plugin.py').read_bytes()
    guest = '/shared/' + f.case + '/plugins/' + KEY
    # This is ordinary fixture installation before web/worker initialization.
    # No constructor, action, helper or backend task is invoked by the controller.
    f.python("import pathlib,os,base64,hashlib,json;p=pathlib.Path(" + repr(guest) +
             ");p.mkdir();s=p/'plugin.py';s.write_bytes(base64.b64decode(" +
             repr(base64.b64encode(source).decode()) +
             "));[(os.chown(x,65532,65532),x.chmod(0o755 if x.is_dir()else 0o444))for x in(p,s)];"
             "print(json.dumps({'sha256':hashlib.sha256(s.read_bytes()).hexdigest()}))",
             secret=True)
    # Real WebhookHandler POST to an existing owned Origin endpoint. The valid
    # recover control targets an unused path, never the media capture stream.
    url = 'http://origin:8080/control'
    template = '{"path":"/live/65499.ts","action":"recover"}'
    seeded = f.sql("from apps.plugins.models import PluginConfig;"
                   "from apps.connect.models import Integration,EventSubscription;"
                   "from core.models import CoreSettings;"
                   "from django_celery_beat.models import PeriodicTask,PeriodicTasks\n"
                   "with connection.cursor()as c:\n"
                   " c.execute('CREATE TABLE owned_recording_event_probe(ordinal bigserial PRIMARY KEY,payload jsonb NOT NULL,backend_pid integer NOT NULL)')\n"
                   " c.execute('CREATE TABLE owned_recording_event_startup(ordinal bigserial PRIMARY KEY,payload jsonb NOT NULL)')\n"
                   "PluginConfig.objects.create(key=" + repr(KEY) +
                   ",name='Owned recording events',enabled=True,ever_enabled=True);"
                   "i=Integration.objects.create(name='Owned recording event webhook',type='webhook',config={'url':" + repr(url) +
                   "},enabled=True);rows=[]\n"
                   "for event in('recording_start','recording_end'):\n"
                   " s=EventSubscription.objects.create(event=event,integration=i,enabled=True,payload_template=" + repr(template) +
                   ");rows.append({'id':s.pk,'event':event})\n"
                   "d=dict(CoreSettings.get_dvr_settings());d['comskip_enabled']=False;"
                   "s=CoreSettings.objects.get(key='dvr_settings');s.value=d;s.save();"
                   "print(json.dumps({'subscriptions':rows,'plugin_enabled_before_workers':True}))",
                   secret=True)
    f._recording_event_seed = seeded
    save(Path(f.output) / 'declared-recording-event-inputs.json', {
        'backend': 'Original', 'context': CASE, 'plugin_sha256': hashlib.sha256(source).hexdigest(),
        'plugin_enabled_before_workers': True, 'events': ['recording_start', 'recording_end'],
        'webhook': {'endpoint': 'existing owned Origin/control', 'unused_path': '/live/65499.ts'},
        'helper_or_Task_body_patched': False, 'Native_parity_claimed': False,
    })


def decoded_jsonb_object(value):
 # Pinned Django raw PostgreSQL cursor returns JSONB text; ORM JSONField
 # values may already be decoded. Decode one string layer only.
 if isinstance(value,str):value=json.loads(value)
 if not isinstance(value,dict):raise TypeError('ActualJSONBObjectRequired')
 return value

def observations(f):
    state = f.sql("from apps.connect.models import DeliveryLog;from core.models import SystemEvent\n"
                 "with connection.cursor()as c:\n"
                 " c.execute('SELECT ordinal,payload,backend_pid FROM owned_recording_event_probe ORDER BY ordinal');rows=c.fetchall()\n"
                 " c.execute('SELECT ordinal,payload FROM owned_recording_event_startup ORDER BY ordinal');startup=c.fetchall()\n"
                 "print(json.dumps({'probe':[{'ordinal':r[0],'payload':r[1],'backend_pid':r[2]}for r in rows],"
                 "'startup':[{'ordinal':r[0],'payload':r[1]}for r in startup],"
                 "'audit':list(SystemEvent.objects.filter(event_type__in=['recording_start','recording_end']).order_by('id').values('id','event_type','details')),"
                 "'deliveries':list(DeliveryLog.objects.filter(subscription__integration__name='Owned recording event webhook').order_by('id').values('id','subscription__event','status','request_payload','response_payload'))},default=str))",
                 secret=True)
    for row in state['probe'] + state['startup']:
        row['payload'] = decoded_jsonb_object(row['payload'])
    return state


def run(f):
    if f.backend != 'Original' or f.case != CASE:
        raise ValueError('IndependentOriginalRecordingEventContextRequired')
    from workflow import Workflow, utc
    flow = Workflow(f, Path(f.output) / 'recording-event-capture')
    private = flow.out / 'private'
    private.mkdir(mode=0o700)
    report = {'status': 'running', 'cases': [], 'Source_event_characterization': True,
              'Native_parity_claimed': False, 'monkeypatch_characterized': False,
              'full_milestone_passed': False, 'performance_verified': False}
    ids = []
    try:
        before = observations(f)
        assert not before['probe'] and not before['deliveries']
        origin_before = json.loads(f.get(f.origin + '/stats')[1])
        assert origin_before['active'] == 0
        # Confirm ordinary loader discovery actually happened in process startup
        # through plugin config metadata, without controller loading the plugin.
        config = f.sql("from apps.plugins.models import PluginConfig;p=PluginConfig.objects.get(key=" + repr(KEY) +
                       ");print(json.dumps({'enabled':p.enabled,'ever_enabled':p.ever_enabled,'name':p.name,'owned_tasks':p.owned_tasks}))",
                       secret=True)
        assert config['enabled'] and config['ever_enabled']
        save(private / 'startup-plugin-config.json', {'config': config, 'ordinary_discovery_observations': before['startup']})
        assert before['startup'], 'NoActualOrdinaryPluginConstructionObservedBeforeRecording'
        for label in ('stopped', 'completed'):
            start = utc() + datetime.timedelta(seconds=3)
            ident = flow.book(start, start + datetime.timedelta(seconds=18 if label == 'completed' else 90),
                              'Owned recording event ' + label)
            ids.append(ident)
            active = flow.wait(lambda: flow.active([ident]), 30, 'event-recording-active-' + label)
            # Workflow.active returns the actual Recording serializer row.
            # For future Source bookings its task_id is the Beat schedule name,
            # not the request UUID. Obtain execution identity only from the
            # actual event action's current_task for this owned recording.
            scheduled_marker = active[0]['task_id']
            def observed_start():
                rows = [r for r in observations(f)['probe']
                        if r['payload']['event'] == 'recording_start'
                        and r['payload']['recording_id'] == ident]
                assert len(rows) <= 1
                return rows[0] if rows else False
            start_action = f.wait(observed_start, 20, 'actual-recording-start-task-' + label)
            task_id = start_action['payload']['task_id']
            assert isinstance(task_id, str) and 0 < len(task_id) <= 255
            f.wait_for_hls_segments([ident], 2, 30)
            active_snapshot = flow.snapshot('recording-event-active-' + label)
            assert active_snapshot['actual_recording_ffmpeg_count'] == 1
            if label == 'stopped':
                code, value = flow.request('POST', '/api/channels/recordings/' + str(ident) + '/stop/', {})
                assert code == 200
            terminal = flow.wait(lambda: flow.terminal(ident, label), 90, 'event-recording-terminal-' + label)
            flow.decode(ident)
            def completed_events():
                state = observations(f)
                actual = [r for r in state['probe'] if r['payload']['task_id'] == task_id]
                return state if len(actual) == 2 else False
            state = f.wait(completed_events, 20, 'actual-recording-event-actions-' + label)
            actual = [r for r in state['probe'] if r['payload']['task_id'] == task_id]
            assert [r['payload']['event'] for r in actual] == ['recording_start', 'recording_end']
            for row in actual:
                payload = row['payload']
                assert payload['task_name'] == 'apps.channels.tasks.run_recording'
                assert payload['audit'] and payload['deliveries']
                assert payload['audit'][0]['details']['recording_id'] == ident
                assert payload['deliveries'][0]['status'] == 'success'
                assert row['backend_pid'] > 0 and payload['pid'] > 0 and payload['thread_id'] > 0
                assert isinstance(payload['task_children'], list)
            assert actual[1]['payload']['recording_id'] == ident
            # Wait for the actual Source backend, not only recording status.
            def backend_terminal():
                raw = f.redis(2, "return redis.call('GET',ARGV[1])", 'celery-tasks:celery-task-meta-' + task_id)
                return json.loads(raw) if raw and json.loads(raw).get('status') in ('SUCCESS', 'FAILURE', 'REVOKED') else False
            meta = f.wait(backend_terminal, 30, 'actual-recording-event-backend-' + label)
            assert meta['status'] == 'SUCCESS'
            save(private / (label + '-observations.json'), {'recording_id': ident, 'task_id': task_id,
                 'scheduled_recording_task_marker': scheduled_marker,
                 'actual_start_action': start_action, 'recording_terminal': terminal, 'SQL': state, 'backend': meta})
            report['cases'].append({'name': label, 'status': 'passed', 'recording_id': ident,
                                    'audit_and_delivery_visible_inside_action': True,
                                    'actual_current_task': task_id, 'decoded_complete_file': True})
        after = observations(f)
        assert len(after['probe']) == 4 and len(after['deliveries']) == 4
        assert len({r['payload']['audit'][0]['id'] for r in after['probe']}) == 4
        assert len({r['payload']['deliveries'][0]['id'] for r in after['probe']}) == 4
        assert all(row['status'] == 'success' and row['response_payload']['status_code'] == 200
                   for row in after['deliveries'])
        origin_after = json.loads(f.get(f.origin + '/stats')[1])
        assert origin_after['fault_requests'] - origin_before['fault_requests'] == 4
        observations_seen = [0]
        def physically_retired():
            observations_seen[0] += 1
            final = flow.snapshot('recording-event-final-observation-' + str(observations_seen[0]))
            # Source schedules ordinary zero-client proxy retirement on later
            # monitor iterations; recording task completion is not upstream EOF.
            if not (final['all_owned_recording_children_retired']
                    and final['owned_hls_directories_absent']
                    and final['actual_recording_ffmpeg_count'] == 0
                    and final['actual_origin']['active'] == 0):
                return False
            return final
        final = flow.wait(physically_retired, 20, 'ordinary-recording-upstream-retirement')
        origin_after = final['actual_origin']
        save(private / 'final.json', {'SQL': after, 'origin_before': origin_before, 'origin_after': origin_after,
                                    'physical_media_cleanup': final})
        report['status'] = 'passed'
    except BaseException as error:
        report.update(status='failed', error_type=type(error).__name__)
        save(private / 'failure.json', {'traceback': traceback.format_exc()[-8192:]})
        raise
    finally:
        save(flow.out / 'source-event-report.json', report)
    return report
