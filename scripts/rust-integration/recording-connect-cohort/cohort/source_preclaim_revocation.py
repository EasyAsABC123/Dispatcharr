"""Control ordering inside the existing Source restart fixture; no new fixture.
Original Dispatcharr and Celery/Kombu authors retain full application credit.
"""
import json
from pathlib import Path

TARGET = 'normal-dvr@owned'


def apply(fixture, key, control_queue):
    if fixture.backend != 'Original' or not control_queue.startswith('normal-control-only-'):
        raise ValueError('OwnedSourceControlQueueRequired')
    root = Path(fixture.output)
    evidence = {'scope': 'new controlled Source preclaim revocation ordering only',
                'task_key': key, 'control_queue': control_queue, 'target': TARGET,
                'initial_Source_producer_graph_unchanged': True,
                'historical_SUCCESS_outcome_rewritten': False, 'actions': []}

    def persist():
        (root / 'restart-Source-preclaim-control.json').write_text(json.dumps(evidence, indent=2) + '\n')

    def call(name, expression):
        row = {'action': name, 'status': 'pending'}
        evidence['actions'].append(row)
        persist()
        script = ("import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';"
                  "from dispatcharr.celery import app;target=" + repr(TARGET) + ';' + expression)
        try:
            value = json.loads(fixture.python(script, seconds=fixture.remaining(8)))
            row.update(status='captured', observation=value)
            return value
        except Exception as error:
            row.update(status='failed', error_type=type(error).__name__)
            raise
        finally:
            persist()

    def queues():
        value = call('actual-targeted-active-queues',
                     "r=app.control.inspect(destination=[target],timeout=1).active_queues();"
                     "print(json.dumps(None if not r or target not in r else [q['name']for q in r[target]]))")
        if value is not None and (not isinstance(value, list) or len(value) > 4):
            raise AssertionError('BoundedActualQueueNamesRequired')
        return value

    names = fixture.wait(lambda: queues(), 20, 'actual-Source-control-only-queue')
    if names != [control_queue]:
        raise AssertionError(('SourceDvrIntakeMustRemainGated', names))
    expected_raw = fixture._restart['deleted_frames'][fixture._restart['revoked']]['raw']

    def unclaimed():
        import hashlib
        rows = fixture.redis(1, "return redis.call('LRANGE','dvr',0,-1)")
        result = fixture.redis(2, "local k='celery-tasks:celery-task-meta-'..ARGV[1];if redis.call('STRLEN',k)>262144 then return redis.error_reply('ActualBackendValueBudget')end;return redis.call('GET',k)or false", key)
        absent = result is None or result is False
        value = {'exact_target_raw_still_queued': expected_raw in rows,
                 'all_prior_raw_frames_still_queued': all(raw in rows for raw in fixture._restart['queue_before']),
                 'target_backend_absent': absent,
                 'target_backend_value_type': type(result).__name__,
                 'target_backend_raw': result, 'target_backend_metadata': None}
        # Persist the literal bounded value before parsing or asserting. Backend
        # metadata existence alone is not claim/execution authority: Source
        # _revoke proactively writes REVOKED while the raw frame stays queued.
        evidence['latest_queue_backend_observation'] = value
        persist()
        if not absent:
            if not isinstance(result, str) or len(result.encode()) > 262144:
                raise AssertionError('ActualBackendValueBudgetOrType')
            value['target_backend_sha256'] = hashlib.sha256(result.encode()).hexdigest()
            value['target_backend_metadata'] = json.loads(result)
            persist()
        value['target_backend_REVOKED'] = (isinstance(value['target_backend_metadata'], dict)
            and value['target_backend_metadata'].get('status') == 'REVOKED'
            and value['target_backend_metadata'].get('task_id') == key)
        return value

    evidence['before_revoke'] = unclaimed()
    persist()
    if not all(evidence['before_revoke'][k] for k in ('exact_target_raw_still_queued','all_prior_raw_frames_still_queued','target_backend_absent')):
        raise AssertionError('ActualSourceTargetAlreadyClaimedBeforeControlledRevoke')
    # Publish exactly once. Inspection may be polled; no revoke resend/fallback.
    reply = call('one-targeted-revoke-publication',
                 "r=app.control.revoke(" + repr(key) + ",destination=[target],terminate=False,reply=True,timeout=1);print(json.dumps(r))")
    evidence['actual_revoke_reply'] = reply
    persist()

    def applied():
        value = call('actual-targeted-inspect-revoked',
                     "r=app.control.inspect(destination=[target],timeout=1).revoked();"
                     "v=None if not r else r.get(target);"
                     "assert v is None or(isinstance(v,list)and len(v)<=64);print(json.dumps(v))")
        return value if isinstance(value, list) and key in value else False

    evidence['actual_applied_revoked_keys_before_intake'] = fixture.wait(applied, 20, 'actual-Source-revoke-applied-before-intake')
    evidence['after_applied_before_add_consumer'] = unclaimed()
    persist()
    if not all(evidence['after_applied_before_add_consumer'][k] for k in ('exact_target_raw_still_queued','all_prior_raw_frames_still_queued','target_backend_REVOKED')):
        raise AssertionError('ActualSourcePreAddRawAndREVOKEDRequired')
    evidence['add_consumer_actual_reply'] = call('one-targeted-add-dvr-consumer',
        "r=app.control.add_consumer('dvr',destination=[target],reply=True,timeout=1);print(json.dumps(r))")
    evidence['actual_queue_names_after_add'] = fixture.wait(
        lambda: (lambda current: current if current and 'dvr' in current else False)(queues()),
        20, 'actual-Source-dvr-consumer-added')
    def consumed_revoked():
        value = unclaimed()
        evidence['post_add_consumption_observation'] = value
        persist()
        return value if not value['exact_target_raw_still_queued'] and value['target_backend_REVOKED'] else False

    evidence['actual_raw_removed_and_REVOKED_after_add'] = fixture.wait(
        consumed_revoked, 20, 'actual-Source-revoked-raw-consumed-after-add')
    evidence['applied_before_dvr_intake'] = True
    evidence['later_REVOKED_backend_required'] = True
    persist()
    return evidence
