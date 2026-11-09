"""Continuous byte-checked media workload. URLs and auth stay only in memory.
Original Dispatcharr authors retain full application credit.
"""
import concurrent.futures
import hashlib
import http.client
import math
import statistics
import threading
import time
from urllib.parse import urlsplit


def continuous_viewer(endpoint, shared, ready, prefix_bytes, timeout):
    url, headers, expected_hash = endpoint
    target = urlsplit(url)
    if target.scheme != 'http' or target.username or target.password:
        raise ValueError('Only isolated plain-HTTP fixture endpoints without URL credentials are supported')
    result = {'status': None, 'errors': [], 'warmup_bytes': 0, 'measured_bytes': 0,
              'first_byte_ms': None, 'prefix_bytes': 0, 'prefix_sha256': None,
              'requests': 1, 'unexpected_eof': False}
    prefix = hashlib.sha256()
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=timeout)
    began = time.monotonic()
    try:
        connection.request('GET', target.path + ('?' + target.query if target.query else ''), headers=headers)
        response = connection.getresponse()
        result['status'] = response.status
        if response.status not in (200, 206):
            raise ValueError('Unexpected playback status')
        first = True
        while not shared['stop'].is_set():
            data = response.read1(65536)
            now = time.monotonic()
            if not data:
                result['unexpected_eof'] = True
                break
            if first:
                result['first_byte_ms'] = (now - began) * 1000
                first = False
                ready.set()
            missing = prefix_bytes - result['prefix_bytes']
            if missing > 0:
                prefix.update(data[:missing])
                result['prefix_bytes'] += min(missing, len(data))
            start = shared.get('measurement_start')
            end = shared.get('measurement_end')
            if start is not None and start <= now < end:
                result['measured_bytes'] += len(data)
            elif end is None or now < end:
                result['warmup_bytes'] += len(data)
            if end is not None and now >= end:
                break
        result['prefix_sha256'] = prefix.hexdigest()
        result['prefix_matches'] = result['prefix_bytes'] == prefix_bytes and result['prefix_sha256'] == expected_hash
    except Exception as error:
        # Exception messages can include credential-bearing endpoint data.
        result['errors'].append(type(error).__name__)
    finally:
        ready.set()
        connection.close()
    return result


def run_continuous(endpoints, warmup, duration, sample, prefix_bytes, timeout=10, sample_period=2):
    """One request per viewer across both phases, never a hidden retry/reset."""
    shared = {'stop': threading.Event()}
    ready = [threading.Event() for _ in endpoints]
    samples = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(endpoints)) as executor:
        pending = [executor.submit(continuous_viewer, endpoint, shared, event, prefix_bytes, timeout)
                   for endpoint, event in zip(endpoints, ready)]
        deadline = time.monotonic() + timeout + 5
        try:
            for event in ready:
                if not event.wait(max(0, deadline - time.monotonic())):
                    raise TimeoutError('Viewer readiness deadline')
            if any(task.done() for task in pending):
                raise RuntimeError('A viewer ended before warmup')
            shared['measurement_start'] = time.monotonic() + warmup
            shared['measurement_end'] = shared['measurement_start'] + duration
            while time.monotonic() < shared['measurement_start']:
                if any(task.done() for task in pending):
                    raise RuntimeError('A viewer ended during warmup')
                time.sleep(min(.1, max(0, shared['measurement_start'] - time.monotonic())))
            next_sample = shared['measurement_start']
            while time.monotonic() < shared['measurement_end']:
                if any(task.done() for task in pending):
                    raise RuntimeError('A viewer ended during measurement')
                now = time.monotonic()
                if now >= next_sample:
                    value = sample()
                    value['sample_offset_seconds'] = now - shared['measurement_start']
                    samples.append(value)
                    next_sample += sample_period
                else:
                    time.sleep(min(.1, next_sample - now))
        finally:
            shared['stop'].set()
        results = [task.result(timeout=timeout + 1) for task in pending]
    return {'viewers': results, 'samples': samples, 'measurement_seconds': duration,
            'warmup_seconds': warmup, 'continuous_connections': True,
            'requests': sum(row['requests'] for row in results),
            'total_measured_bytes': sum(row['measured_bytes'] for row in results),
            'median_first_byte_ms': statistics.median(row['first_byte_ms'] for row in results)}


def validate_phase(phase, viewers, expected_rate, budgets, selected_ids):
    failures = []
    rows = phase['viewers']
    if len(rows) != viewers or phase.get('requests') != viewers or not phase.get('continuous_connections'):
        failures.append('connection_count_or_reconnect')
    duration = phase['measurement_seconds']
    for index, row in enumerate(rows):
        rate = row['measured_bytes'] / duration
        if (row['errors'] or row['unexpected_eof'] or row['status'] not in (200, 206) or
                not row.get('prefix_matches') or row['first_byte_ms'] is None):
            failures.append(f'viewer_{index}_delivery')
        if not expected_rate * budgets['min_viewer_rate_fraction'] <= rate <= expected_rate * budgets['max_viewer_rate_fraction']:
            failures.append(f'viewer_{index}_rate')
    if len(phase['samples']) < budgets['min_samples']:
        failures.append('insufficient_samples')
    for sample in phase['samples']:
        if len(sample['containers']) != len(selected_ids) or {row['id'] for row in sample['containers']} != set(selected_ids):
            failures.append('incomplete_service_footprint')
        if any(not isinstance(sample.get(key), (int, float)) or not math.isfinite(sample[key]) or sample[key] < 0 for key in ('cpu_percent', 'memory_bytes')):
            failures.append('invalid_resource_sample')
    return failures


def compare_pair(source, candidate, budgets):
    for phase in (source, candidate):
        if not phase['samples'] or not phase.get('correctness_passed') or any(not math.isfinite(row[key]) or row[key] < 0 for row in phase['samples'] for key in ('cpu_percent', 'memory_bytes')):
            return {'passed': False, 'reason': 'Invalid correctness or missing samples'}
    cpu = lambda phase: statistics.mean(row['cpu_percent'] for row in phase['samples'])
    memory = lambda phase: max(row['memory_bytes'] for row in phase['samples'])
    throughput = lambda phase: phase['total_measured_bytes'] / phase['measurement_seconds']
    ratios = {'cpu_ratio': cpu(candidate) / max(cpu(source), .000001),
              'memory_ratio': memory(candidate) / max(memory(source), 1),
              'throughput_ratio': throughput(candidate) / max(throughput(source), 1),
              'first_byte_ratio': max(candidate['median_first_byte_ms'], budgets['first_byte_floor_ms']) /
                                  max(source['median_first_byte_ms'], budgets['first_byte_floor_ms'])}
    passed = (ratios['cpu_ratio'] <= budgets['max_cpu_ratio'] and
              ratios['memory_ratio'] <= budgets['max_memory_ratio'] and
              ratios['throughput_ratio'] >= budgets['min_throughput_ratio'] and
              ratios['first_byte_ratio'] <= budgets['max_first_byte_ratio'])
    return {'passed': passed, 'ratios': ratios, 'cpu_metric': 'mean whole selected group CPU%',
            'memory_metric': 'maximum sampled whole selected group Docker working memory'}
