"""Serial existing-Redirect-context hooks, BEFORE its last retained Proxy cell.
No import/startup/fixture work and no Source private counter mutation.
"""
import json
from pathlib import Path


def run(f, callbacks):
    """callbacks: finite sequence of (label, existing callable(fixture))."""
    out = f.output / 'redirect-extension-composition.json'
    report = {'status': 'running', 'cells': [], 'placement': 'before final active_proxy cell and before redirect-origin stop',
              'reader_close_is_NOT_retirement': True, 'full_M3_passed': False, 'full_M5_passed': False}
    def save():
        Path(out).write_text(json.dumps(report, indent=2) + '\n')
    def snapshot():
        rows = f.redis(0, "local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('GET',p..':owner')or'',redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('EXISTS',p..':stopping'),redis.call('HGET',p..':metadata','state')or''}", f.channel_uuid, str(f._redirect_seed['profile']))
        origin = json.loads(f.get(f.origin + '/stats')[1])
        return {'base_clients': rows[0], 'base_owner_present': bool(rows[1]), 'provider_count': int(rows[2]), 'base_stopping': bool(rows[3]), 'base_state': rows[4], 'actual_Origin_active': origin['active']}
    for label, callback in callbacks:
        row = {'name': label, 'status': 'running'}
        report['cells'].append(row)
        save()
        try:
            if 'redirect-origin' not in f.roles:
                raise RuntimeError('ExistingRedirectOriginAlreadyStopped')
            row['before'] = snapshot()
            save()
            if row['before']['base_owner_present'] or row['before']['base_clients']:
                # Actual known base worker only. Never clear a private grant or
                # global provider counter merely because a prior reader closed.
                row['explicit_prior_owned_worker_stop'] = f.api('POST', '/proxy/ts/stop/' + f.channel_uuid, {}, 'admin')
                save()
                if row['explicit_prior_owned_worker_stop']['status'] not in (200, 404):
                    raise RuntimeError('ActualOwnedWorkerStopFailed')
            def ready():
                row['reuse_observation'] = snapshot()
                save()
                value = row['reuse_observation']
                return not value['base_clients'] and not value['base_owner_present'] and not value['base_stopping'] and value['base_state'] != 'stopping' and value['provider_count'] == 0 and value['actual_Origin_active'] == 0
            f.wait(ready, 40, 'actual-prior-Redirect-context-worker-provider-upstream-retirement')
            row['observation'] = callback(f)
            if row['observation'].get('status') != 'passed':
                raise AssertionError('ExistingExtensionCallbackFailed')
            # Explicit retirement obligations belong to each callback; this
            # physical barrier also prevents reusing an unfinished raw socket.
            f.wait(ready, 40, 'actual-extension-worker-provider-upstream-retirement-before-reuse')
            row['status'] = 'passed'
        except Exception as error:
            row.update(status='failed', error_type=type(error).__name__, error_label=str(error)[:160])
        save()
    report['status'] = 'passed' if report['cells'] and all(row['status'] == 'passed' for row in report['cells']) else 'failed'
    report['characterization_capture_is_not_behavior_qualification'] = True
    save()
    return report
