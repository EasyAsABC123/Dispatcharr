"""Publish whitelisted numerical phase evidence; never copy bodies or credentials.
Original Dispatcharr authors retain application credit. This projection preserves
raw-file hashes, replaces container IDs with service names and rebases monotonic
clock values to each phase's first counter. It does not invent missing traces.
"""
import argparse
import hashlib
import json
import math
import re
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
    if value['workload'] not in ['current-500-shared50', 'xmltv-cold-1', 'xmltv-cold-10', 'xmltv-hot-1', 'xmltv-hot-10', 'grid-2250-shared50-1', 'grid-2250-shared50-10']:
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
    if 'request_traces' in measurement:
        traces = measurement['request_traces']
        if len(traces) > 10000:
            raise ValueError('Trace publication ceiling')
        output['request_traces'] = []
        for trace in traces:
            row = numeric(trace, ['user_index','attempt_index','path_index','wave_index',
                'started_seconds','ttfb_ms','completion_ms','verified_complete_seconds',
                'in_window','passed','late'])
            disposition = trace['disposition']
            if disposition not in ['success','late','error']:
                raise ValueError('Unknown trace disposition')
            error = trace.get('error_type')
            if error is not None and (not isinstance(error,str) or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]{0,63}',error)):
                raise ValueError('Unsafe error class')
            row.update(disposition=disposition,error_type=error)
            output['request_traces'].append(row)
    if 'cache_witness' in value:
        witness = value['cache_witness']
        output['cache_witness'] = {}
        for when in ['before','after']:
            state = witness[when]
            if state is None:
                output['cache_witness'][when] = None
                continue
            row = numeric(state,['chunks','bytes','locked','ready_ttl_seconds',
                                 'smallest_chunk_bytes','largest_chunk_bytes'])
            generation = state['generation']
            if not isinstance(generation,str) or not generation.isdigit() or len(generation)>32:
                raise ValueError('Unknown cache generation')
            if state['ready'] not in ['','1'] or state['status'] not in ['','ready','building','error']:
                raise ValueError('Unknown cache state')
            row.update(generation=generation,ready=state['ready'],status=state['status'])
            types = state['key_types'];ttls=state['key_ttl_seconds']
            if set(types)!=set(['chunks','ready','status','lock']) or set(ttls)!=set(types):
                raise ValueError('Incomplete public cache keys')
            if any(t not in ['none','list','string'] for t in types.values()):
                raise ValueError('Unknown public cache key type')
            row['key_types']=dict(types)
            row['key_ttl_seconds']=numeric(ttls,['chunks','ready','status','lock'])
            output['cache_witness'][when]=row
    output['individual_current_hot_request_timings_available'] = 'request_traces' in measurement
    output['cache_numeric_witness_available'] = 'cache_witness' in value
    return output


def publish(base, output):
    base, output = Path(base), Path(output)
    report = {'scope': 'Sanitized numerical projection of retained experiment phase evidence',
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
