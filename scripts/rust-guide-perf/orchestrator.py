"""Executable qualification/matrix engine inside the existing smoke/API/M4 fixture.
Invocation is in smoke.py; fresh parent quiet cues are internal coordination only.
Original Dispatcharr authors retain full application credit and AGPL licensing.
"""
import datetime
import hashlib
import json
import time
from pathlib import Path
import acceptance
import load
import observer
import plan
import qualification


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def cache_ready(state, expected):
    return (state['ready'] == '1' and state['status'] == 'ready' and not state['locked']
            and state['chunks'] > 0 and state['bytes'] == expected['bytes']
            and state['ready_ttl_seconds'] > 0)


def normalized(measurement, counter_result, workload, period, duration, checks):
    if workload['cache_state'] == 'cold':
        consumers = [v for wave in measurement['waves'] for v in wave['consumers']]
        completed = measurement['completed']
        first = [v['ttfb_ms'] for v in consumers if v['passed']]
        elapsed = [v['completion_ms'] for v in consumers if v['passed']]
        summary = dict(completed=completed, errors=measurement['errors'],
                       late_completed=measurement['late_completed'],
                       ttfb_ms_p95=load.api.percentile(first, .95),
                       roundtrip_ms_p95=load.api.percentile(elapsed, .95))
        requested = measurement['expected_waves'] * workload['users']
    else:
        summary = measurement['summary']
        requested = duration * workload['users'] / period
    samples = measurement['resource_samples']
    return dict(cpu_percent_mean=counter_result['cpu_percent_mean'],
                peak_working_memory_bytes=max((r['memory_bytes'] for r in samples), default=0),
                ttfb_p95_ms=summary['ttfb_ms_p95'] or 0,
                completion_p95_ms=summary['roundtrip_ms_p95'] or 0,
                throughput_per_second=summary['completed'] / duration,
                pacing_fraction=min(1, summary['completed'] / requested),
                samples=len(samples), errors=summary['errors'],
                sample_errors=len(measurement['sample_errors']),
                late_completed=summary['late_completed'], oom_kills=0, counter_resets=0,
                correct_responses=summary['errors'] == 0 and summary['completed'] > 0,
                cache_state_verified=checks['cache_state_verified'],
                fixture_unchanged=checks['fixture_unchanged'], limits_verified=True)


def run(*, action, output, manifest, tools, seed, reference, current, xml,
        helper, cache, start, stop, select_services, quiet_path=None, plan_path=None,
        qualification_path=None):
    """Callbacks reuse the same immutable-tool Compose fixture as functional smoke."""
    output = Path(output)
    report = {'passed': False, 'scope': plan.POLICY['scope'], 'action': action,
              'performance_verified': False, 'trials': []}
    save(output / 'guide-run.json', report)
    now = lambda: datetime.datetime.now(datetime.timezone.utc)
    born = datetime.datetime.fromisoformat(seed['clock'])
    expected_snapshot = None

    def valid_fixture():
        nonlocal expected_snapshot
        phase = helper('phase')  # Fresh actual Source JWT, never persist token here.
        snapshot = {k: phase[k] for k in ['channels', 'stored_programmes', 'first', 'last', 'current_until', 'fixture_sha256', 'schema_sha256']}
        if snapshot['channels'] != 500 or snapshot['stored_programmes'] != 2250:
            raise ValueError('Guide fixture row counts changed')
        if expected_snapshot is None:
            expected_snapshot = snapshot
        if snapshot != expected_snapshot or (now() - born).total_seconds() >= 3600:
            raise ValueError('Guide fixture changed or exceeded sealed one-hour age')
        if now() + datetime.timedelta(minutes=1) >= datetime.datetime.fromisoformat(phase['current_until']):
            raise ValueError('Current programme will cross boundary')
        return phase['token'], snapshot

    def observe_backend(backend):
        try:
            base = start(backend)
            return base, observer.Observer(select_services(backend), tools)
        except BaseException:
            stop()
            raise

    def measured(backend, work, period, warm, duration, name):
        base, seen = observe_backend(backend)
        try:
            token, before_fixture = valid_fixture()
            spec = current if work['kind'] == 'json' else xml
            expected = reference['current' if work['kind'] == 'json' else 'xml']
            if work['kind'] == 'xml':
                cache.clear()
                load.request(base, spec, timeout=10, expected=expected)
                state = cache.state()
                if not cache_ready(state, expected):
                    raise ValueError('XML prime did not produce complete ready cache')
            else:
                state = None
            cache_valid = True
            def wave():
                nonlocal cache_valid
                prior = cache.state()
                if not cache_ready(prior, expected):
                    cache_valid = False
                    raise ValueError('Prior cold wave has not reached ready state')
                cache.clear()
                empty = cache.state()
                if empty['chunks'] != 0 or empty['ready'] or empty['status'] or empty['locked'] or empty['generation'] != state['generation']:
                    cache_valid = False
                    raise ValueError('Cold wave cache reset was incomplete')
            def request_phase(seconds, sample=None):
                if work['cache_state'] == 'cold':
                    return load.cold_waves(base, spec, expected, users=work['users'],
                                           period=period, duration=seconds, before_wave=wave, sample=sample)
                return load.paced(base, spec, token, expected, users=work['users'],
                                  period=period, duration=seconds, sample=sample)
            warmed = request_phase(warm)
            errors = warmed.get('errors', warmed.get('summary', {}).get('errors'))
            late = warmed.get('late_completed', warmed.get('summary', {}).get('late_completed'))
            if errors:
                raise ValueError('Guide warmup semantic/delivery failure')
            # Numeric cache witnesses are explicitly outside the counter window.
            before_cache = cache.state() if state is not None else None
            if state is not None and (not cache_ready(before_cache, expected)
                                      or before_cache['generation'] != state['generation']):
                raise ValueError('Warmed cache witness differs before measurement')
            before = seen.counters()
            result = request_phase(duration, seen.sample)
            save(output / (name + '-raw.json'),
                 {'backend': backend, 'workload': work['name'],
                  'measurement': result, 'before_counters': before,
                  'cache_witness_before': before_cache, 'fixture_witness': before_fixture})
            counters = seen.finish(before)
            _, after_fixture = valid_fixture()
            after = None
            if state:
                after = cache.state()
                cache_valid = cache_valid and cache_ready(after, expected) and after['generation'] == state['generation']
            normal = normalized(result, counters, work, period, duration,
                                {'cache_state_verified': cache_valid, 'fixture_unchanged': before_fixture == after_fixture})
            data = {'backend': backend, 'workload': work['name'], 'period_seconds': period,
                    'warmup_seconds': warm, 'duration_seconds': duration,
                    'normalized': normal, 'measurement': result, 'counters': counters,
                    'cache_witness': {'scope': 'Before/after numeric cache states outside measured window',
                                      'requested_state': work['cache_state'],
                                      'before': before_cache, 'after': after},
                    'fixture_witness': before_fixture}
            path = output / (name + '.json')
            save(path, data)
            report['trials'].append({'file': path.name, 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
            save(output / 'guide-run.json', report)
            return normal, counters
        except BaseException as error:
            report.setdefault('failures', []).append({'phase': name, 'error_type': type(error).__name__})
            save(output / 'guide-run.json', report)
            raise
        finally:
            stop()

    try:
        if action in ['observer-smoke', 'instrumentation-smoke']:
            rows = []
            for backend in ['source', 'candidate']:
                base, seen = observe_backend(backend)
                try:
                    token, _ = valid_fixture()
                    load.request(base, current, token, expected=reference['current'])
                    load.request(base, xml, expected=reference['xml'])
                    ready = cache.state()
                    if not cache_ready(ready, reference['xml']):
                        raise ValueError('Observer smoke cache state is not ready')
                    cache.clear()
                    empty = cache.state()
                    if empty['chunks'] or empty['ready'] or empty['status'] or empty['locked']:
                        raise ValueError('Observer smoke bounded cache reset failed')
                    load.request(base, xml, expected=reference['xml'])
                    rebuilt = cache.state()
                    if not cache_ready(rebuilt, reference['xml']) or rebuilt['generation'] != ready['generation']:
                        raise ValueError('Observer smoke cache rebuild/generation differs')
                    if action == 'observer-smoke':
                        detail = seen.smoke()
                    else:
                        # Functional diagnostics only: no resource sampler, ratios,
                        # qualification or performance acceptance is invoked here.
                        paced = load.paced(base, current, token, reference['current'],
                                           users=1, period=1, duration=2)
                        before_cold = cache.state()
                        def cold_reset():
                            cache.clear()
                        cold = load.cold_waves(base, xml, reference['xml'], users=1,
                                               period=10, duration=1, before_wave=cold_reset)
                        after_cold = cache.state()
                        if not cache_ready(after_cold, reference['xml']):
                            raise ValueError('Functional traced cold response not fully ready')
                        for result in [paced, cold]:
                            traces = result['request_traces']
                            if (not traces or any(row['error_type'] is not None for row in traces)
                                    or any(row['ttfb_ms'] is None or row['completion_ms'] is None
                                           for row in traces)):
                                raise ValueError('Functional timing trace missing/failed')
                        private_path = output / ('trace-functional-' + backend + '.json')
                        save(private_path, {'scope': 'Functional trace/cache witnesses; no performance comparison',
                                            'performance_verified': False, 'paced': paced, 'cold': cold,
                                            'cache_before': before_cold, 'cache_after': after_cold})
                        detail = {'passed': True, 'performance_verified': False,
                                  'trace_report': private_path.name,
                                  'trace_report_sha256': hashlib.sha256(private_path.read_bytes()).hexdigest(),
                                  'paced_traces': len(paced['request_traces']),
                                  'cold_traces': len(cold['request_traces'])}
                    rows.append({'backend': backend, 'cache_reset_rebuild_verified': True,
                                 'cache_witness_before': ready, 'cache_witness_after': rebuilt,
                                 **detail})
                finally:
                    stop()
            report.update(passed=True, observer_smoke=rows)
        elif action == 'qualify':
            report['quiet_authorization_sha256'] = qualification.authorize(quiet_path, manifest)
            rows = {}
            for work in plan.POLICY['workloads']:
                rows[work['name']] = []
                for index, period in enumerate(work['period_candidates_seconds']):
                    qualification.authorize(quiet_path, manifest)
                    normal, counters = measured('source', work, period, 5, 15, work['name'] + '-qualification-' + str(index))
                    backend = counters['services']['source']
                    rows[work['name']].append(qualification.row(work, period, normal,
                        {'backend': 'source', 'cpu_percent_mean': backend['cpu_percent_mean'],
                         'memory_fraction': max(container['memory_bytes'] for sample in json.loads((output/report['trials'][-1]['file']).read_text())['measurement']['resource_samples'] for container in sample['containers'] if counters['selected_services']['source']['id'].startswith(container['id'])) / (512 * 1024**2),
                         'throttled_period_fraction': backend['throttled_period_fraction']}))
                    save(output/'source-qualification.json', rows)
            record = plan.freeze(output/'frozen-plan.json', rows, manifest,
                                 {'snapshot': expected_snapshot, 'fixture_clock': seed['clock'],
                                  'reference_sha256': plan.digest(reference),
                                  'channel_uuids_sha256': plan.digest(seed['channel_uuids'])})
            report.update(passed=True, plan_sha256=record['plan_sha256'], selected_periods_seconds=record['selected_periods_seconds'])
        elif action == 'cold-preflight':
            report['quiet_authorization_sha256'] = qualification.authorize_preflight(quiet_path, manifest)
            work = next(row for row in plan.POLICY['workloads'] if row['name'] == 'xmltv-cold-10')
            # Fixed before either backend runs; no candidate rate selection.
            period = 10
            pair = {'workload': work['name'], 'trial': 1, 'order': ['source', 'candidate']}
            for backend in pair['order']:
                qualification.authorize_preflight(quiet_path, manifest)
                pair[backend], _ = measured(backend, work, period,
                                           plan.POLICY['warmup_seconds'], plan.POLICY['measurement_seconds'],
                                           work['name'] + '-preflight-' + backend)
            save(output/'pairs.json', [pair])
            report['budget_comparison'] = acceptance.compare(pair['source'], pair['candidate'])
            report['passed'] = report['budget_comparison']['passed']
            report['diagnostic_only'] = True
            report['performance_verified'] = False
            report['full_matrix_accepted'] = False
            report['source_qualification_performed'] = False
        elif action == 'matrix':
            q = json.loads(Path(qualification_path).read_text())
            frozen = json.loads(Path(plan_path).read_text())
            sealed = plan.validate_frozen(frozen, q, manifest)
            _, snapshot = valid_fixture()
            binding = {'snapshot': snapshot, 'fixture_clock': seed['clock'],
                       'reference_sha256': plan.digest(reference),
                       'channel_uuids_sha256': plan.digest(seed['channel_uuids'])}
            if frozen.get('fixture_binding') != binding:
                raise ValueError('Qualified fixture/reference changed before matrix')
            report['quiet_authorization_sha256'] = plan.authorize_quiet(quiet_path, sealed, manifest['image_id'])
            report['plan_sha256'] = sealed
            rows = []
            for work in plan.POLICY['workloads']:
                for trial, order in enumerate(plan.POLICY['orders'], 1):
                    pair = {'workload': work['name'], 'trial': trial, 'order': order}
                    for backend in order:
                        plan.authorize_quiet(quiet_path, sealed, manifest['image_id'])
                        pair[backend], _ = measured(backend, work, frozen['selected_periods_seconds'][work['name']], 15, 60,
                                                   work['name'] + '-trial-' + str(trial) + '-' + backend)
                    rows.append(pair)
                    save(output/'pairs.json', rows)
                    comparison = acceptance.compare(pair['source'], pair['candidate'])
                    if not comparison['passed']:
                        # Retain every completed phase/pair before ending. An
                        # incomplete matrix never becomes overall acceptance.
                        report['acceptance'] = {
                            'passed': False, 'incomplete': True,
                            'scope': plan.POLICY['scope'],
                            'pairs': [{'workload': row['workload'], 'trial': row['trial'],
                                       **acceptance.compare(row['source'], row['candidate'])}
                                      for row in rows],
                        }
                        report['stopped_on_failed_pair'] = {
                            'workload': work['name'], 'trial': trial,
                            'failures': comparison['failures'],
                        }
                        return report
            report['acceptance'] = acceptance.matrix(rows)
            report['passed'] = report['acceptance']['passed']
            report['performance_verified'] = report['passed']
        else:
            raise ValueError('Unknown guide action')
        return report
    finally:
        save(output/'guide-run.json', report)
