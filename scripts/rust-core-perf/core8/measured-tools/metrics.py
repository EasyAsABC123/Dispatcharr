"""Reuse accepted guide observer, fail closed without a fresh quiet cue.
No sampler starts on import or during functional controller actions.
"""
import datetime
import hashlib
import importlib
import json
from pathlib import Path
import sys
import time
import subprocess
from .policy import digest, number


def authorize(path, scope, plan, binary_sha256, seal=None):
    raw = Path(path).read_bytes()
    if len(raw) > 16384:
        raise ValueError('Quiet cue exceeds16KiB')
    value = json.loads(raw)
    expiry = datetime.datetime.fromisoformat(value['expires_utc'])
    if (value.get('scope') != scope or value.get('authorized') is not True
            or value.get('policy_sha256') != digest(plan)
            or value.get('native_binary_sha256') != binary_sha256
            or value.get('boot_id') != Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            or expiry.tzinfo is None or expiry <= datetime.datetime.now(datetime.timezone.utc)
            or (seal is not None and value.get('plan_sha256') != seal)):
        raise ValueError('Fresh matching parent quiet cue required')
    return hashlib.sha256(raw).hexdigest()


def observer(selected, support):
    if set(selected) != {'infrastructure', 'core'}:
        raise ValueError('Whole active-core plus common PGRedis infrastructure required')
    if len({row['id'] for row in selected.values()}) != 2:
        raise ValueError('Core and infrastructure must have independent cgroups')
    sys.path.insert(0, str(Path(support) / 'scripts/rust-guide-perf'))
    module = importlib.import_module('observer')
    class CoreObserver(module.Observer):
        def sample(self):
            value = super().sample()
            value['memory_metric'] = 'Docker working-memory sum: active core + identical PGRedis infrastructure; stopped web/origin excluded'
            return value
    return CoreObserver(selected, support)


def assert_no_oom(counters):
    services = counters.get('services', {})
    if not services or not set(services) <= {'core', 'infrastructure'}:
        raise ValueError('Lifecycle service counters missing')
    for row in services.values():
        events = row.get('memory_events', {})
        if any(number(events.get(key)) != 0 for key in ['oom', 'oom_kill']):
            raise ValueError('OOM occurred during setup/prior phase/drain, even if wrapper lives')


def lifecycle(selected, support, output):
    """Retain absolute events and Docker state before checking, outside timing."""
    sys.path.insert(0, str(Path(support) / 'scripts/rust-guide-perf'))
    cgroup = importlib.import_module('cgroup')
    value = {'services': {}, 'docker_state': {}, 'zero_oom_verified': False}
    try:
        for label, item in selected.items():
            inspect = json.loads(subprocess.check_output(['docker', 'inspect', item['id']], text=True, timeout=10))[0]
            state = inspect['State']
            value['docker_state'][label] = {key: state.get(key) for key in ['Running', 'OOMKilled', 'ExitCode', 'Error', 'StartedAt', 'FinishedAt']}
            if state.get('OOMKilled') is not False or state.get('Running') is not True:
                raise ValueError('Lifecycle container OOM/exited; counters cannot prove safety')
            value['services'][label] = cgroup.read(item['id'], item['limits'])
        assert_no_oom(value)
        value['zero_oom_verified'] = True
        return value
    except (Exception, KeyboardInterrupt) as error:
        value['error_type'] = type(error).__name__
        raise
    finally:
        Path(output).write_text(json.dumps(value, indent=2) + '\n')


def window(observed, state, plan, output):
    """Called only by separately authorized qualification/measurement actions."""
    start = state['start_ms'] / 1000 + plan['warmup_seconds']
    end = start + plan['measure_seconds']
    raw = []
    result = {'window_start_epoch': start, 'window_end_epoch': end, 'samples': raw,
              'performance_verified': False}
    try:
        if time.time() >= start:
            raise ValueError('Sampler missed complete measurement-window start')
        time.sleep(max(0, start - time.time()))
        before = observed.counters()
        result['counter_before'] = before
        assert_no_oom(before)
        while time.time() < end:
            began = time.time()
            value = observed.sample()
            completed = time.time()
            value.update(sample_started_epoch=began, sample_completed_epoch=completed,
                         included_in_window=(began >= start and completed <= end),
                         memory_metric='Docker working-memory sum: active core + identical PGRedis infrastructure; stopped web/origin excluded')
            raw.append(value)
            time.sleep(max(0, min(1, end - time.time())))
        result['counters'] = observed.finish(before)
        assert_no_oom(result['counters']['after'])
        return result
    except (Exception, KeyboardInterrupt) as error:
        result['error_type'] = type(error).__name__
        try:
            result['failure_diagnostic_counters'] = observed.counters()
        except Exception:
            result['failure_diagnostic_unavailable'] = True
        raise
    finally:
        Path(output).write_text(json.dumps(result, indent=2) + '\n')
        if state.get('drain_release'):
            # Also release on counter/observer failure so owned peers can drain
            # and clean up. No failed window becomes accepted by doing so.
            Path(state['drain_release']).touch(exist_ok=True)


def summarize(raw, plan):
    samples = [v for v in raw['samples'] if v['included_in_window']]
    if len(samples) < plan['minimum_samples']:
        raise ValueError('Insufficient complete-window samples')
    services = raw['counters']['services']
    if set(services) != {'core', 'infrastructure'}:
        raise ValueError('Complete service counter boundary missing')
    cpu = sum(number(row['cpu_percent_mean']) for row in services.values())
    if any(number(row['cpu_usage_usec']) <= 0 or number(row['cpu_percent_mean']) <= 0 for row in services.values()):
        raise ValueError('Nonzero-work service CPU counter missing/zero')
    peaks = {}
    for label in services:
        identifier = raw['counters']['selected_services'][label]['id']
        values = []
        for sample in samples:
            matching = [v for v in sample['containers'] if v['id'] == identifier]
            if len(matching) != 1:
                raise ValueError('Service memory sample missing/duplicated')
            value = number(matching[0]['memory_bytes'])
            if value <= 0:
                raise ValueError('Working memory absent/zero')
            values.append(value)
        peaks[label] = max(values)
    total_peak = max(number(row['memory_bytes']) for row in samples)
    if any(number(row['memory_bytes']) != sum(number(v['memory_bytes']) for v in row['containers'])
           or len(row['containers']) != 2 for row in samples):
        raise ValueError('Aggregate working-memory/service identity mismatch')
    return {'samples': len(samples), 'cpu_percent_mean': cpu, 'peak_working_memory_bytes': total_peak,
            'core_cpu_percent': services['core']['cpu_percent_mean'],
            'core_memory_fraction': peaks['core'] / plan['backend_limits']['memory_bytes'],
            'core_throttled_period_fraction': services['core']['throttled_period_fraction'],
            'whole_stack': ['core', 'infrastructure']}
