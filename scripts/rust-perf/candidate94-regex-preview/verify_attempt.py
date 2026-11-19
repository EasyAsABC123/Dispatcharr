"""Independently reconcile the exact bounded paired C94 regex attempt."""
import argparse
import hashlib
import json
import math
from pathlib import Path
from statistics import median


C94 = 'fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f'
C94_IMAGE = 'sha256:b309daadc687ed0bb8f627373e4c34c202f995bb91ab782ae65ee2d32b712c74'
REQUEST_SHA = 'd344bb6e450ffcda06ef9644d5cddf711d41e05892f83acebf7d3e9190fdbe73'
NORMALIZED_SEAL = 'aebf852e38b428c4b7b767c8e9fe154d83b927b967a6ea3fa54665e765786a1b'
STARTUP_TASK = 'apps.plugins.tasks.refresh_plugin_repos'
ROLES = {'backend', 'infrastructure', 'origin'}


def require(value, message):
    if not value:
        raise ValueError(message)


def load(path):
    return json.loads(path.read_bytes())


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def approx(a, b):
    return all(math.isfinite(x) for x in (a, b)) and math.isclose(a, b, rel_tol=1e-10, abs_tol=1e-10)


def percentile(values):
    return sorted(values)[math.ceil(.95 * len(values)) - 1]


def sample_metrics(counters, ledger, plan):
    selected = counters['selected_services']
    require(set(selected) == ROLES and set(counters['services']) == ROLES, 'simultaneous stack service roles')
    ids = {value['id']: name for name, value in selected.items()}
    require(len(ids) == 3, 'unique phase service identities')
    service_peaks = dict.fromkeys(ROLES, 0)
    highest = 0
    prior = -1
    for sample in ledger:
        require(prior < sample['started_monotonic'] <= sample['completed_monotonic'], 'ordered stack samples')
        prior = sample['started_monotonic']
        counts = {}
        for row in sample['containers']:
            matches = [role for identifier, role in ids.items() if identifier.startswith(row['id'])]
            require(len(matches) == 1 and matches[0] not in counts, 'exact simultaneous Docker group sample')
            role = matches[0]
            memory = max(0, row['memory_usage_bytes'] - row['inactive_file_bytes'])
            require(memory == row['memory_bytes'] > 0 and row['memory_limit_bytes'] == plan['limits'][role]['memory_bytes'],
                    'declared working-set estimate and memory cap')
            counts[role] = memory
            service_peaks[role] = max(service_peaks[role], memory)
        require(set(counts) == ROLES and sum(counts.values()) == sample['memory_bytes'], 'same-sample memory sum')
        highest = max(highest, sample['memory_bytes'])
    require(len(ledger) >= plan['minimum_samples'] and highest > 0, 'sufficient sampling')
    roles = {}
    for role in ROLES:
        first = counters['before']['services'][role]
        last = counters['after']['services'][role]
        caps = plan['limits'][role]
        require(selected[role]['limits'] == caps, 'selected limits match frozen plan')
        for row in (first, last):
            require(row['memory_max'] == caps['memory_bytes']
                    and row['cpu_quota'] * 10**9 // row['cpu_period'] == caps['nano_cpus']
                    and all(row['memory_events'][key] == 0 for key in ('oom', 'oom_kill')), 'exact cgroup CPU/memory/OOM limits')
        elapsed = last['sampled_monotonic'] - first['sampled_monotonic']
        usage = last['cpu']['usage_usec'] - first['cpu']['usage_usec']
        require(elapsed >= plan['measure_seconds'] and usage >= 0, 'complete monotonic CPU counter window')
        cpu_percent = usage / (elapsed * 10_000)
        original = counters['services'][role]
        require(approx(cpu_percent, original['cpu_percent_mean']) and original['cpu_usage_usec'] == usage,
                'raw cumulative CPU delta reconciliation')
        roles[role] = {'cpu_percent': cpu_percent, 'peak_working_bytes': service_peaks[role]}
    return {'cpu_percent': sum(row['cpu_percent'] for row in roles.values()), 'peak_working_bytes': highest,
            'services': roles, 'samples': len(ledger)}


def phase(raw, plan, operations):
    waves = plan['measure_seconds'] // plan['wave_seconds']
    names = plan['requests_per_operator_per_wave']
    requests = raw['response_checks']
    required = [(wave, name) for wave in range(waves) for name in names]
    expected_used = len(required)
    require(raw['requests_expected'] == expected_used and raw['requests_attempted'] == len(requests), 'actual phase cardinality')
    require([(row['wave'], row['request']) for row in requests] == required and all(row['viewer'] == 0 for row in requests),
            'exact sequenced independent operator requests')
    lat = {name: {'p95_total_ms': [], 'p95_first_byte_ms': []} for name in names}
    size, scans = 0, 0
    previous = -1
    for row in requests:
        expected = operations[row['request']]
        require(row['error_type'] is None and row['inside_wave'] is True and row['status'] == expected['status']
                and row['sha256'] == expected['sha256'] and row['bytes'] == expected['bytes']
                and row['qualified_scan_rows'] == expected['scanned'], 'exact qualified response and completed wave')
        completion = row['completed_monotonic']
        require(previous < completion and 0 <= row['ttfb_ms'] <= row['total_ms'] <= plan['wave_seconds'] * 1_000,
                'ordered finite request latency')
        previous = completion
        size += row['bytes']
        scans += row['qualified_scan_rows']
        lat[row['request']]['p95_total_ms'].append(row['total_ms'])
        lat[row['request']]['p95_first_byte_ms'].append(row['ttfb_ms'])
    require(raw['passed'] is True and not raw['sample_errors'] and raw['exact_successes'] == expected_used
            and raw['bytes'] == size and approx(raw['bytes_per_second'], size / plan['measure_seconds']),
            'raw controller work claims match literal observations')
    quantiles = {name: {metric: percentile(rows) for metric, rows in values.items()} for name, values in lat.items()}
    require(quantiles == raw['per_request'], 'independently calculated nearest-rank p95')
    return {**sample_metrics(raw['counters'], raw['samples'], plan), 'operations': quantiles,
            'exact_requests': expected_used, 'response_bytes': size, 'qualified_scan_rows': scans,
            'bytes_per_second': size / plan['measure_seconds']}


def pair(source, native, plan):
    budgets = plan['comparison']
    floor = budgets['latency_floor_ms']
    ratios = {'whole_cpu': native['cpu_percent'] / source['cpu_percent'],
              'whole_peak_working_set': native['peak_working_bytes'] / source['peak_working_bytes'],
              'throughput': native['bytes_per_second'] / source['bytes_per_second']}
    latency = {name: {metric: max(floor, native['operations'][name][metric]) / max(floor, value)
                      for metric, value in observed.items()} for name, observed in source['operations'].items()}
    equivalent = all(source[field] == native[field] for field in ('exact_requests', 'response_bytes', 'qualified_scan_rows'))
    gates = {'equal_work': equivalent, 'CPU': ratios['whole_cpu'] <= budgets['whole_cpu_max_ratio'],
             'peak_working_set': ratios['whole_peak_working_set'] <= budgets['whole_peak_memory_max_ratio'],
             'throughput': ratios['throughput'] >= budgets['min_throughput_ratio'],
             'latency': all(v <= budgets['max_p95_ratio'] for row in latency.values() for v in row.values())}
    return {'passed': all(gates.values()), 'gates': gates, 'ratios': ratios, 'floored_operation_latency_ratios': latency}


def cleanup(report):
    raw = report['cleanup']
    remain = {role: len(ids) for role, ids in raw.get('project_resources_after', {}).items()}
    retired = sum(row.get('host_generation_absent') is True for row in raw['owned_generations'])
    expected = len(raw['owned_generations'])
    valid = (set(remain) == {'container', 'network', 'volume'} and not any(remain.values()) and retired == expected)
    require(raw['passed'] == valid, 'actual exact cleanup discrepancy')
    return {'passed': valid, 'tracked_generations': expected, 'retired_generations': retired,
            'retained_project_resource_counts': remain, 'containers_before_down': len(raw.get('container_ids_before', []))}


def startup_preparations(report, order):
    rows = report.get('phase_preparations', [])
    require([(row['trial'], row['backend']) for row in rows] == order[:len(rows)], 'fixed owned preparation order')
    after_reference = None
    result = []
    for row in rows:
        queue = row.get('owned_startup_queue', {})
        ready = row['fixture_fingerprints_at_readiness']
        after = row.get('fixture_fingerprints_before_warmup', {})
        delta = row['fixture_state_changes_at_readiness']
        known = queue.get('startup_task_classes', {})
        count = queue.get('startup_queue_size')
        valid = (queue.get('passed') is True and set(known) <= {STARTUP_TASK} and sum(known.values()) == count
                 and queue['remaining_broker_queue'] == queue['remaining_unacked_state'] == 0
                 and row['origin_idle'] is True and row.get('fixture_state_changes_before_warmup') == []
                 and set(ready) == set(after) and set(delta) <= {'queued_jobs'}
                 and [key for key in sorted(ready) if ready[key] != after[key]] == delta
                 and after['queued_jobs'] == hashlib.sha256(b'[]').hexdigest()
                 and (after_reference is None or after == after_reference))
        result.append({'trial': row['trial'], 'backend': row['backend'], 'qualified': valid,
                       'classified_pre_warmup_startup_task_count': count,
                       'setup_difference_categories': delta})
        if valid:
            after_reference = after
    return result


def actual_launches(driver, order):
    log = driver / 'actual-run.log'
    if not log.is_file():
        return None
    labels = []
    for line in log.read_text().splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict) and 'phase' in value:
            labels.append(value['phase'])
    expected = [f'measure-{backend}-1-{trial}' for trial, backend in order]
    require(labels == expected[:len(labels)], 'actual phase line order differs from fixed workload')
    return {'actual_started_measurement_phases': len(labels), 'sha256': sha(log)}


def reconcile(driver, attempt, actual_exit):
    report_path = attempt / 'performance.json'
    report = load(report_path)
    inputs = load(driver / 'preflight.json')
    require(report['plan'] == inputs['plan'] and report['binding'] == inputs['quiet_binding'], 'identical preflight/run binding')
    require(inputs['quiet_binding']['candidate_revision'] == C94 and inputs['quiet_binding']['candidate_image'] == C94_IMAGE,
            'exact qualified C94 immutable image')
    manifest = driver / 'request-manifest.json'
    require(inputs['request_manifest_sha256'] == report['request_manifest_sha256'] == sha(manifest) == REQUEST_SHA,
            'identical frozen request manifest')
    require(inputs['seal_sha256'] == sha(driver / 'freeze.json') == report['binding']['inputs_sha256'], 'exact runner seal')
    operations = load(manifest)
    require(report['operations'] == operations['requests'], 'retained operation definitions')
    cleaned = cleanup(report)
    result = {'scope': report['scope'], 'released_exact_image': C94_IMAGE, 'candidate_revision': C94,
              'actual_launcher_exit': actual_exit, 'report_sha256': sha(report_path), 'request_manifest_sha256': REQUEST_SHA,
              'runner_seal_sha256': inputs['seal_sha256'], 'started_measurement': report['measurements_started'],
              'cleanup': cleaned, 'reported_stage': report.get('stage'), 'phase_reviews': [], 'pairs': [],
              'performance_accepted': False}
    plan = inputs['plan']
    order = [(n, backend) for n, pair in enumerate(plan['orders'], 1) for backend in pair]
    launches = actual_launches(driver, order)
    if launches is not None:
        result['actual_launch_log'] = launches
        require(report['measurements_started'] == (launches['actual_started_measurement_phases'] > 0),
                'terminal started flag differs from actual launcher log')
    if inputs['seal_sha256'] == NORMALIZED_SEAL:
        result['startup_preparations'] = startup_preparations(report, order)
    trials = report['trials']
    require([(row['trial'], row['backend']) for row in trials] == order[:len(trials)], 'actual fixed paired phase order')
    complete = {}
    for row in trials:
        name = f"{row['backend']}-1-{row['trial']}.json"
        raw_path = attempt / name
        require(row['operators'] == 1 and row['evidence'] == name and row['raw_sha256'] == sha(raw_path), 'phase file custody')
        proof = {'trial': row['trial'], 'backend': row['backend'], 'raw_sha256': row['raw_sha256'], 'qualified': False}
        try:
            physical = phase(load(raw_path), plan, operations['requests'])
            require(row['offline_and_sql_unchanged'], 'owned idle origin and SQL/Redis unchanged')
            require(row['passed'] and row['sample_count'] == physical['samples']
                    and approx(physical['cpu_percent'], row['metrics']['cpu_percent'])
                    and physical['peak_working_bytes'] == row['metrics']['peak_working_bytes'], 'phase reducer claim')
            counts = operations['per_phase']['measure']
            require((physical['exact_requests'], physical['response_bytes'], physical['qualified_scan_rows']) ==
                    (counts['requests'], counts['response_bytes'], counts['qualified_scan_rows']), 'exact frozen useful work')
        except (KeyError, ValueError) as error:
            proof['assessment'] = str(error)
        else:
            proof.update(physical, qualified=True)
            complete[row['trial'], row['backend']] = physical
        result['phase_reviews'].append(proof)
    for count, saved in enumerate(report['pairs'], 1):
        require(saved['trial'] == count, 'actual paired result order')
        actuals = [complete.get((count, backend)) for backend in ('source', 'candidate')]
        calculated = pair(*actuals, plan) if all(actuals) else {'passed': False, 'gates': {'equal_work': False}}
        require(calculated['passed'] == saved['passed'], 'controller and independent pair disposition differ')
        if 'ratios' in calculated:
            for raw, independent in (('whole_cpu_ratio', 'whole_cpu'),
                                     ('whole_peak_memory_ratio', 'whole_peak_working_set'), ('throughput_ratio', 'throughput')):
                require(approx(saved[raw], calculated['ratios'][independent]), 'pair aggregate ratio discrepancy')
            require(saved['per_request'] == calculated['floored_operation_latency_ratios'], 'paired latency ratio discrepancy')
        result['pairs'].append({'trial': count, **calculated})
    result['performance_accepted'] = (actual_exit == 0 and len(complete) == len(order) == 6 and len(result['pairs']) == 3
                                      and all(r['passed'] for r in result['pairs']) and cleaned['passed'])
    if result['performance_accepted'] and 'startup_preparations' in result:
        require(len(result['startup_preparations']) == 6 and all(row['qualified'] for row in result['startup_preparations']),
                'every active phase requires exact pre-warmup fixture normalization')
    if result['performance_accepted'] and launches is not None:
        require(launches['actual_started_measurement_phases'] == len(order), 'all planned measurement starts physically observed')
    require(report['passed'] == result['performance_accepted'], 'actual launcher/report and recomputed result differ')
    if len(complete) == 6 and len(result['pairs']) == 3 and all('ratios' in row for row in result['pairs']):
        result['median_paired_ratios'] = {key: median(row['ratios'][key] for row in result['pairs'])
                                          for key in result['pairs'][0]['ratios']}
        result['backend_medians'] = {backend: {key: median(complete[(n, backend)][key] for n in range(1, 4))
            for key in ('cpu_percent', 'peak_working_bytes')} for backend in ('source', 'candidate')}
    if actual_exit:
        result['retained_failure'] = {key: report[key] for key in ('error_type', 'guard_failure', 'command_actual_exit') if key in report}
        if 'preparation' in report:
            result['retained_failure']['failed_preparation'] = {key: report['preparation'][key]
                for key in ('backend', 'trial', 'fixture_state_changes', 'origin_idle')}
        normalization = report.get('bootstrap', {}).get('read_only_state_normalization')
        if normalization is not None:
            result['retained_failure']['fixture_bootstrap_normalization'] = {key: normalization[key]
                for key in ('removed_owned_bootstrap_queued_jobs', 'remaining_owned_bootstrap_queued_jobs', 'sql_preserved')}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('driver', type=Path)
    parser.add_argument('attempt', type=Path)
    parser.add_argument('--actual-exit', type=Path, required=True)
    args = parser.parse_args()
    actual_exit = int(args.actual_exit.read_text().strip())
    print(json.dumps(reconcile(args.driver, args.attempt, actual_exit), indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
