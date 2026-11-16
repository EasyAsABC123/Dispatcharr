"""One additional case INSIDE the existing owned M8 action context.
Actual Source/Celery producer and normal consumer; no task-body invocation.
Original Dispatcharr/Celery/Kombu authors retain algorithm credit.
"""
import base64
import hashlib
import json
import uuid

NAME = 'apps.plugins.tasks.refresh_plugin_repos'


def run(f, request, save):
    report = {'passed': False, 'full_M8_passed': False}
    save(f, 'builtin-refresh-report', report)
    # This context owns its cloned DB. Disable every repository, retaining all
    # rows and their prior state in evidence. No external manifest contact is
    # necessary to prove real producer/consumer/result and parent continuation.
    before = f.sql("from apps.plugins.models import PluginRepo\nrows=list(PluginRepo.objects.order_by('id').values('id','enabled','last_fetched','last_fetch_status','cached_manifest'))\nPluginRepo.objects.all().update(enabled=False)\nfrom dispatcharr.celery import app\ncalibration='00000000-0000-0000-0000-000000000000'\nactual_key=app.backend.get_key_for_task(calibration).decode()\nassert actual_key.endswith(calibration)\nprefix=actual_key[:-len(calibration)]\nprint(json.dumps({'rows':rows,'prefix':prefix},default=str))")
    report['owned_repositories_before_disable'] = before['rows']
    ledger = f.case_data / 'm8-builtin-publications.jsonl'
    ready = ledger.with_suffix('.ready')
    if ledger.exists() or ready.exists():
        raise RuntimeError('BuiltinObserverRequiresFreshOwnedPaths')
    role = 'm8-builtin-monitor'
    f.role(role, ['/dispatcharrpy/bin/python', '/normal-dvr-tools/broker_monitor.py'],
           extra={'NORMAL_DVR_LEDGER': '/shared/' + f.case + '/' + ledger.name})
    try:
        f.wait(ready.exists, 10, 'actual-builtin-MONITOR-subscribed')
        label = 'builtin-' + uuid.uuid4().hex[:8]
        actual = request(f, label, 'builtin_refresh')
        report['actual_HTTP_action'] = actual
        save(f, 'builtin-refresh-report', report)
        assert actual['status'] == 200 and actual['error'] is None, actual
        result = actual['body']['result']
        ident = result['id']
        assert result['result'] is None and result['parent'] == result['after'], result

        def observed():
            if not ledger.exists():
                return None
            data = ledger.read_bytes()
            if len(data) > 1048576:
                raise RuntimeError('ActualBuiltinLedgerBudget')
            # A live final write is allowed to be incomplete until its newline.
            rows = [json.loads(line) for line in data.splitlines(keepends=True)
                    if line.endswith(b'\n')]
            matches = [row for row in rows
                       if row['headers'].get('id') == ident]
            return matches if matches else None

        rows = f.wait(observed, 10, 'actual-exact-builtin-publication-frame')
        assert len(rows) == 1, rows
        raw = rows[0]['raw_frame']
        frame = json.loads(raw)
        headers = frame['headers']
        assert rows[0]['queue'] == 'celery' and headers['task'] == NAME, rows
        assert headers['id'] == ident and headers.get('parent_id') is None, headers
        assert headers.get('eta') is None and headers.get('expires') is None, headers
        body = json.loads(base64.b64decode(frame['body'], validate=True))
        assert body[0] == [] and body[1] == {}, body
        assert frame['properties']['delivery_info']['routing_key'] == 'celery', frame
        # Persist complete actual non-secret owned task frame, not an invented
        # template; bind both byte identity and decoded framing to the result.
        report['actual_full_frame'] = frame
        report['actual_raw_frame_sha256'] = hashlib.sha256(raw.encode()).hexdigest()
        value = f.redis(2, "return redis.call('GET',ARGV[1])or false", before['prefix'] + ident)
        backend = json.loads(value) if value else None
        assert backend and backend['task_id'] == ident, backend
        assert backend['status'] == 'SUCCESS' and backend['result'] is None, backend
        report['actual_backend'] = backend
        queued = f.redis(1, "return redis.call('LRANGE','celery',0,-1)")
        assert not any(json.loads(item).get('headers', {}).get('id') == ident for item in queued), queued
        after = f.sql("from apps.plugins.models import PluginRepo\nprint(json.dumps(list(PluginRepo.objects.order_by('id').values('id','enabled','last_fetched','last_fetch_status','cached_manifest')),default=str))")
        expected = [{**row, 'enabled': False} for row in before['rows']]
        assert after == expected, (after, expected)
        report['actual_disabled_rows_unchanged_by_consumer'] = after
        report['actual_same_parent_backend_generation_before_after'] = result['parent']
        if f.backend == 'Native':
            audit = result['native_execution_audit']
            assert audit and audit['entrypoint_guard_installed'] and not audit['actual_blocked_calls'], audit
            report['actual_native_execution_audit'] = audit
        report['business_passed'] = True
        report['required_unmeasured'] = [
            'distinct builtin child physical PG backend and causal writer loan during held upstream',
            'builtin producer ETA/restore/revoke and forced physical cancellation',
            'parented, expiring, direct/eager and other builtin producer variants',
        ]
    finally:
        report['actual_monitor_stop'] = f.role(role, [], stop=True)
        save(f, 'builtin-refresh-report', report)
    # Existing cases.run performs normal Main/Source retirement and checks all
    # actual recorded action parent PG pid/backend_start identities afterward.
    report['passed'] = report.get('business_passed') is True
    save(f, 'builtin-refresh-report', report)
    return report
