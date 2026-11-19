#!/usr/bin/env python3
"""Reconcile bounded M4 phase receipts and the measured result without Docker."""
import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path
from statistics import median

REQUESTS = {'complete': (200, 3165955), 'closed_seek': (206, 196608)}
SERVICES = {'backend', 'infrastructure', 'origin'}
RATIOS = ('whole_cpu_ratio', 'whole_peak_memory_ratio', 'throughput_ratio')


def load(path):
    return json.loads(small_file(path))


def sha(path):
    return hashlib.sha256(small_file(path)).hexdigest()


def small_file(path):
    with path.open('rb') as stream:
        data = stream.read(4 * 1024**2 + 1)
    if len(data) > 4 * 1024**2:
        raise ValueError('An input exceeds the 4 MiB summary file limit')
    return data


def p95(values):
    return sorted(values)[math.ceil(0.95 * len(values)) - 1]


def phase(result, trial, raw):
    backend, viewers, number = (trial[k] for k in ('backend', 'viewers', 'trial'))
    plan = result['plan']
    entries = raw['response_checks']
    manifest = result['media']
    assert all((manifest[name]['status'], manifest[name]['bytes']) == pair for name, pair in REQUESTS.items())
    expected = {(w, v, k) for w in range(20) for v in range(viewers) for k in REQUESTS}
    got = {(e['wave'], e['viewer'], e['request']) for e in entries}
    assert len(entries) == len(expected) and got == expected
    assert raw['passed'] and not raw['sample_errors'] and trial['offline_and_sql_unchanged'] and trial['passed']
    assert all(e['inside_wave'] and e['error_type'] is None and 0 < e['ttfb_ms'] <= e['total_ms']
               and all(e[field] == manifest[e['request']][field] for field in ('status', 'bytes', 'sha256')) for e in entries)
    delivered = sum(e['bytes'] for e in entries)
    assert raw['bytes'] == trial['bytes'] == delivered and delivered == 20 * viewers * sum(x[1] for x in REQUESTS.values())
    assert raw['bytes_per_second'] == trial['bytes_per_second'] == delivered / 60
    assert all(trial[k] == raw[k] == len(entries) for k in ('requests_expected', 'requests_attempted', 'exact_successes'))
    latency = {name: {field: p95([e[key] for e in entries if e['request'] == name])
                      for field, key in (('p95_total_ms', 'total_ms'), ('p95_first_byte_ms', 'ttfb_ms'))} for name in REQUESTS}
    assert latency == raw['per_request'] == trial['per_request']

    impact = raw['counters']
    assert impact['services'].keys() == impact['selected_services'].keys() == SERVICES
    limits = {name: item['id'] for name, item in impact['selected_services'].items()}
    assert len(set(limits.values())) == 3
    samples = raw['samples']
    assert plan['minimum_samples'] <= len(samples) == trial['sample_count']
    for sample in samples:
        groups = {item['id']: item for item in sample['containers']}
        assert len(sample['containers']) == len(groups) == 3 and set(groups) == set(limits.values())
        assert sample['memory_bytes'] == sum(item['memory_bytes'] for item in groups.values())
        for name, identity in limits.items():
            item = groups[identity]
            assert item['memory_limit_bytes'] == plan['limits'][name]['memory_bytes']
            usage, inactive = item['memory_usage_bytes'], item['inactive_file_bytes']
            assert item['memory_bytes'] == (usage - inactive if inactive < usage else usage)
    cpu, cpu_deltas, peaks = {}, {}, {}
    for name, identity in limits.items():
        start, end = (impact[k]['services'][name] for k in ('before', 'after'))
        for sample in (start, end):
            assert not any(sample['memory_events'][k] for k in ('oom', 'oom_kill'))
            assert sample['memory_max'] == plan['limits'][name]['memory_bytes']
            assert sample['cpu_quota'] * 10**9 // sample['cpu_period'] == plan['limits'][name]['nano_cpus']
        delta = end['cpu']['usage_usec'] - start['cpu']['usage_usec']
        elapsed = end['sampled_monotonic'] - start['sampled_monotonic']
        assert delta >= 0 and elapsed >= 59.9
        cpu[name] = delta / (elapsed * 10000)
        cpu_deltas[name] = {'usage_usec': delta, 'elapsed_seconds': elapsed}
        peaks[name] = max(group['memory_bytes'] for sample in samples for group in sample['containers'] if group['id'] == identity)
        expect = trial['metrics']['services'][name]
        assert cpu[name] == expect['cpu_percent_mean'] and peaks[name] == expect['peak_working_bytes']
    whole_cpu = sum(cpu.values())
    memory = max(sample['memory_bytes'] for sample in samples)
    assert whole_cpu == impact['cpu_percent_mean'] == trial['metrics']['cpu_percent']
    assert memory == trial['metrics']['peak_working_bytes']
    return {'backend': backend, 'viewers': viewers, 'trial': number, 'phase_sha256': trial['raw_sha256'],
            'requests': len(entries), 'bytes': delivered, 'sample_count': len(samples), 'latency_ms': latency,
            'cpu_percent': whole_cpu, 'peak_working_bytes': memory, 'service_cpu_percent': cpu,
            'service_cpu_counter_deltas': cpu_deltas, 'service_peak_working_bytes': peaks}


def warmup_failure(run_dir, report, pending):
    if pending is None:
        return None
    viewers, trial, backend = pending
    label = f'{backend}-{viewers}-{trial}'
    path = run_dir / f'{label}-warmup-failure.json'
    if not path.is_file():
        return None
    assert not report['passed'] and report['stage'] == 'prepare-' + label and report['error_type'] == 'ValueError'
    assert not (run_dir / f'{label}.json').exists(), 'A warmup refusal must precede any phase capture'
    raw = load(path)
    plan = report['plan']
    waves = plan['warmup_seconds'] // plan['wave_seconds']
    expected = {(w, v, k) for w in range(waves) for v in range(viewers) for k in REQUESTS}
    entries = raw['response_checks']
    actual = {(e['wave'], e['viewer'], e['request']) for e in entries}
    successes = [e for e in entries if e['error_type'] is None and e['inside_wave']]
    assert raw['passed'] is False and not raw['samples'] and raw['counters'] is None and not raw['per_request']
    assert raw['requests_expected'] == len(expected) and raw['requests_attempted'] == len(actual) == len(entries)
    assert actual <= expected and raw['exact_successes'] == len(successes)
    assert all(e['request'] != 'closed_seek' or (e['wave'], e['viewer'], 'complete') in actual for e in entries)
    assert all(all(e[key] == report['media'][e['request']][key] for key in ('status', 'bytes', 'sha256')) for e in successes)
    assert raw['bytes'] == sum(e['bytes'] for e in successes)
    assert raw['bytes_per_second'] == raw['bytes'] / plan['warmup_seconds']
    last_wave = max((e['wave'] for e in entries), default=None)
    if last_wave is not None:
        succeeded = {(e['wave'], e['viewer'], e['request']) for e in successes}
        assert all(key in succeeded for key in expected if key[0] < last_wave)
        assert not all(key in succeeded for key in expected if key[0] == last_wave) or raw['sample_errors']
    counts = Counter((e['request'], e['status'], e['error_type'], e['inside_wave']) for e in entries)
    return {'backend': backend, 'viewers': viewers, 'trial': trial, 'sha256': sha(path), 'sampled': False,
            'expected_requests': len(expected), 'attempted_requests': len(entries), 'exact_successes': len(successes),
            'last_attempted_wave_index': last_wave, 'sample_errors': raw['sample_errors'],
            'request_outcomes': [{'request': name, 'http_status': status, 'error_type': error, 'inside_wave': within, 'count': count}
                                 for (name, status, error, within), count in sorted(counts.items(), key=lambda row: repr(row[0]))]}


def receipt(run_dir, binding_dir, actual_exit):
    final = run_dir / 'performance.json'
    report = load(final)
    plan = report['plan']
    bound = load(binding_dir / 'quiet-binding.json')
    assert bound == report['binding'] and sha(binding_dir / 'freeze.json') == bound['inputs_sha256']
    assert sha(binding_dir / 'active-quiet-authorization.json') == report['quiet_sha256']
    foundation = load(binding_dir / 'functional-qualification.json')
    assert foundation['passed'] and foundation['cleaned'] and foundation['actual_exit'] == 0
    assert foundation['functional_receipt_sha256'] == bound['functional_receipt_sha256']
    assert (foundation['candidate_revision'], foundation['image_id'], foundation['candidate_build_sha256']) == (
        bound['candidate_revision'], bound['candidate_image'], bound['build_sha256'])
    assert hashlib.sha256(json.dumps(plan, sort_keys=True, separators=(',', ':')).encode()).hexdigest() == bound['plan_sha256']
    request_digest = report.get('request_manifest_sha256')
    if request_digest is not None:
        request_path = binding_dir / 'request-manifest.json'
        request = load(request_path)
        assert sha(request_path) == request_digest and request['plan_sha256'] == bound['plan_sha256']
        assert request['media'] == report['media'] and request['operations_per_viewer_per_wave'] == list(REQUESTS)
    assert not report['full_m4_parity_verified']
    assert (plan['warmup_seconds'], plan['measure_seconds'], plan['wave_seconds']) == (15, 60, 3) and plan['viewers'] == [1, 10]
    assert list(plan['requests_per_viewer_per_wave']) == list(REQUESTS)
    schedule = [(v, n, b) for v in plan['viewers'] for n, backends in enumerate(plan['orders'], 1) for b in backends]
    reported = [(t['viewers'], t['trial'], t['backend']) for t in report['trials']]
    assert reported == schedule[:len(reported)]
    pair_schedule = [(v, n) for v in plan['viewers'] for n in range(1, 4)]
    assert [(p['viewers'], p['trial']) for p in report['pairs']] == pair_schedule[:len(report['pairs'])]
    phases, failed = [], []
    for trial in report['trials']:
        backend, viewers, number = (trial[k] for k in ('backend', 'viewers', 'trial'))
        evidence = f'{backend}-{viewers}-{number}.json'
        path = run_dir / evidence
        assert evidence == trial['evidence'] and sha(path) == trial['raw_sha256']
        if trial['passed']:
            phases.append(phase(report, trial, load(path)))
        else:
            failed.append({'backend': backend, 'viewers': viewers, 'trial': number, 'phase_sha256': trial['raw_sha256']})
    pairs = []
    thresholds = plan['comparison']
    for pair in report['pairs']:
        original, native = (next((p for p in phases if (p['viewers'], p['trial'], p['backend']) ==
                                 (pair['viewers'], pair['trial'], backend)), None) for backend in ('source', 'candidate'))
        if original is None or native is None:
            assert not pair['passed']
            continue
        ratios = dict(zip(RATIOS, (native['cpu_percent'] / original['cpu_percent'],
                                   native['peak_working_bytes'] / original['peak_working_bytes'],
                                   native['bytes'] / original['bytes']), strict=True))
        for k, v in ratios.items():
            assert math.isclose(pair[k], v, rel_tol=1e-12)
        floor = thresholds['latency_floor_ms']
        latency = {name: {k: max(floor, native['latency_ms'][name][k]) / max(floor, v) for k, v in metrics.items()}
                   for name, metrics in original['latency_ms'].items()}
        assert latency == pair['per_request']
        gates = {'equal_work': (native['bytes'], native['requests']) == (original['bytes'], original['requests']),
                 'cpu': ratios[RATIOS[0]] <= thresholds['whole_cpu_max_ratio'],
                 'working_set': ratios[RATIOS[1]] <= thresholds['whole_peak_memory_max_ratio'],
                 'throughput': ratios[RATIOS[2]] >= thresholds['min_throughput_ratio'],
                 'latency': all(item <= thresholds['max_p95_ratio'] for v in latency.values() for item in v.values())}
        assert gates['equal_work'] == pair['equal_work'] and all(gates.values()) == pair['passed']
        pairs.append({'viewers': pair['viewers'], 'trial': pair['trial'], 'ratios': ratios, 'latency_ratios': latency, 'gates': gates})
    medians = {str(n): {key: median(p['ratios'][key] for p in pairs if p['viewers'] == n) for key in RATIOS}
               for n in plan['viewers'] if sum(p['viewers'] == n for p in pairs) == 3}
    if report.get('complete_equal_work'):
        assert all(math.isclose(value, report['medians'][n][key], rel_tol=1e-12)
                   for n, metrics in medians.items() for key, value in metrics.items())
    cleanup = report['cleanup']
    settled = (not cleanup.get('error_type') and not any(cleanup.get('project_resources_after', {}).values())
               and 'project_resources_after' in cleanup and all(g.get('host_generation_absent') for g in cleanup['owned_generations']))
    assert settled == cleanup['passed']
    complete = len(phases) == 12 and len(pairs) == 6
    assert report['passed'] == (complete and settled and all(all(p['gates'].values()) for p in pairs))
    pending = schedule[len(reported)] if len(reported) < len(schedule) else None
    stopped_in_warmup = warmup_failure(run_dir, report, pending)
    medians_by_backend = {str(n): {b: {key: median(p[key] for p in phases if p['viewers'] == n and p['backend'] == b)
                                      for key in ('cpu_percent', 'peak_working_bytes', 'bytes')}
                                  for b in ('source', 'candidate')}
                          for n in plan['viewers'] if sum(p['viewers'] == n for p in phases) == 6}
    assert actual_exit is None or actual_exit == (0 if report['passed'] else 1)
    result = {'scope': report['scope'], 'candidate_image': bound['candidate_image'], 'candidate_revision': bound['candidate_revision'],
            'candidate_build_sha256': bound['build_sha256'], 'functional_receipt_sha256': bound['functional_receipt_sha256'],
            'plan_sha256': bound['plan_sha256'], 'freeze_sha256': bound['inputs_sha256'],
            'terminal_report_sha256': sha(final), 'terminal_stage': report['stage'], 'reported_passed': report['passed'],
            'externally_observed_controller_exit': actual_exit, 'error_type': report.get('error_type'),
            'all_measurements_completed_and_equal': complete and all(p['gates']['equal_work'] for p in pairs),
            'owned_cleanup_verified': settled, 'container_ids_before_teardown': len(cleanup.get('container_ids_before', [])),
            'observed_generations_absent': sum(g.get('host_generation_absent', False) for g in cleanup['owned_generations']),
            'viewer_levels_with_three_passing_pairs': [n for n in plan['viewers'] if str(n) in medians
                                                        and all(all(p['gates'].values()) for p in pairs if p['viewers'] == n)],
            'plan': {field: plan[field] for field in ('viewers', 'orders', 'warmup_seconds', 'measure_seconds', 'wave_seconds',
                                                       'sample_seconds', 'minimum_samples', 'timeout_seconds', 'limits', 'comparison')},
            'pair_medians': medians, 'backend_medians': medians_by_backend, 'phases': phases, 'pairs': pairs,
            'failed_phases': failed, 'failed_warmup': stopped_in_warmup}
    if request_digest is not None:
        result['request_manifest_sha256'] = request_digest
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir', type=Path, required=True, help='Private directory containing performance.json and phase files')
    parser.add_argument('--binding-dir', type=Path, required=True, help='Private directory containing the freeze, binding and quiet authorization')
    parser.add_argument('--actual-exit', type=int, choices=(0, 1), help='Exit code from the external launcher observation')
    args = parser.parse_args()
    if not __debug__:
        parser.error('Use a standard Python interpreter without optimized assertion removal')
    print(json.dumps(receipt(args.run_dir, args.binding_dir, args.actual_exit), indent=2, sort_keys=True))
