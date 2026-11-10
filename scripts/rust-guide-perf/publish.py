"""Publish whitelisted numerical phase evidence; never copy bodies or credentials.
Original Dispatcharr authors retain application credit. This projection preserves
raw-file hashes, replaces container IDs with service names and rebases monotonic
clock values to each phase's first counter. It does not invent missing traces.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path


def numeric(row, names):
    result = {}
    for name in names:
        if name not in row:
            continue
        value = row[name]
        if value is not None and (type(value) not in [int, float, bool] or
                                   isinstance(value, float) and not math.isfinite(value)):
            raise ValueError('Non-numerical phase field: ' + name)
        result[name] = value
    return result


def project(value):
    if value['backend'] not in ['source', 'candidate']:
        raise ValueError('Unexpected phase backend')
    if value['workload'] not in ['current-500-shared50', 'xmltv-cold-1', 'xmltv-cold-10', 'xmltv-hot-1', 'xmltv-hot-10']:
        raise ValueError('Unexpected phase workload')
    counters = value['counters']
    selected = counters['selected_services']
    if set(selected) != {'baseline', 'origin', value['backend']}:
        raise ValueError('Incomplete service group')
    before = counters['before']['services']
    epoch = min(row['sampled_monotonic'] for row in before.values())
    output = {'backend': value['backend'], 'workload': value['workload'],
              **numeric(value, ['period_seconds', 'warmup_seconds', 'duration_seconds']),
              'normalized': {**numeric(value['normalized'], [
                  'cpu_percent_mean', 'peak_working_memory_bytes', 'ttfb_p95_ms',
                  'completion_p95_ms', 'throughput_per_second', 'pacing_fraction',
                  'samples', 'errors', 'sample_errors', 'late_completed', 'oom_kills',
                  'counter_resets', 'correct_responses', 'cache_state_verified',
                  'fixture_unchanged', 'limits_verified'])},
              'counter_snapshots': {}, 'resource_samples': []}
    for when in ['before', 'after']:
        rows = {}
        for name, row in counters[when]['services'].items():
            if name not in selected:
                raise ValueError('Unknown counter service')
            rows[name] = {**numeric(row, ['cpu_quota', 'cpu_period', 'memory_max',
                                         'memory_current', 'observer_seconds']),
                          'sampled_relative_seconds': row['sampled_monotonic']-epoch,
                          'cpu': numeric(row['cpu'], ['usage_usec','user_usec','system_usec','nr_periods','nr_throttled','throttled_usec','nr_bursts','burst_usec']),
                          'memory_events': numeric(row['memory_events'], ['low','high','max','oom','oom_kill','oom_group_kill','sock_throttled'])}
        output['counter_snapshots'][when] = {'observer_seconds': counters[when]['observer_seconds'],
                                             'services': rows}
    output['counter_deltas'] = {name: numeric(row, ['cpu_percent_mean','cpu_usage_usec','elapsed_seconds','throttled_period_fraction','throttled_usec']) for name, row in counters['services'].items() if name in selected}
    for sample in value['measurement']['resource_samples']:
        row = {**numeric(sample, ['cpu_percent', 'memory_bytes', 'observer_seconds', 'elapsed_seconds']),
               'services': {}}
        for container in sample['containers']:
            names = [name for name,item in selected.items() if item['id'].startswith(container['id'])]
            if len(names) != 1 or names[0] in row['services']:
                raise ValueError('Ambiguous sampled service')
            part = numeric(container, ['memory_bytes', 'memory_usage_bytes', 'inactive_file_bytes',
                                       'memory_limit_bytes', 'cpu_usage_ns', 'cpu_percent'])
            if 'sampled_monotonic' in container:
                part['sampled_relative_seconds'] = container['sampled_monotonic']-epoch
            row['services'][names[0]] = part
        if set(row['services']) != set(selected):
            raise ValueError('Missing sampled service')
        output['resource_samples'].append(row)
    measurement = value['measurement']
    if 'summary' in measurement:
        output['request_summary'] = numeric(measurement['summary'], [
            'completed', 'errors', 'users_without_success', 'late_completed', 'requests_per_second',
            'ttfb_ms_p95', 'roundtrip_ms_p95', 'cpu_percent_mean', 'memory_bytes_peak'])
    else:
        output['request_summary'] = numeric(measurement, ['completed', 'errors', 'late_completed', 'expected_waves'])
        output['cold_waves'] = [{'epoch': row['epoch'], 'consumers': [
            numeric(consumer, ['passed', 'late', 'ttfb_ms', 'completion_ms']) for consumer in row['consumers']]}
            for row in measurement['waves']]
    output['individual_current_hot_request_timings_available'] = 'request_traces' in measurement
    output['cache_numeric_witness_available'] = 'cache_witness' in value
    return output


def publish(base, output):
    base, output = Path(base), Path(output)
    report = {'scope': 'Sanitized numerical projection of stopped experiment phase evidence',
              'performance_verified': False,
              'projection': 'Explicit numeric whitelist; container IDs become service names, monotonic clocks relative to first before-counter. Bodies, headers, paths, credentials and exception text excluded.',
              'phases': [], 'credit': 'Original Dispatcharr authors and contributors retain full application credit and AGPL licensing.'}
    for stage in ['qualification', 'matrix']:
        state = json.loads((base/stage/'guide-run.json').read_text())
        for item in state['trials']:
            path = base/stage/item['file']; raw = path.read_bytes()
            digest = hashlib.sha256(raw).hexdigest()
            if digest != item['sha256']:
                raise ValueError('Retained raw phase hash changed')
            report['phases'].append({'stage': stage, 'raw_file': item['file'],
                                     'raw_sha256': digest, **project(json.loads(raw))})
    output.write_text(json.dumps(report, separators=(',', ':'), allow_nan=False)+'\n')
    return len(report['phases'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    print('Published whitelisted numerical phases:', publish(args.input, args.output))
