"""Paced, bounded JSON API load; no browser or full-application acceptance.

Original application credit remains with Dispatcharr authors and contributors.
"""
import concurrent.futures
import json
import math
import statistics
import threading
import time
import urllib.request

MAX_RESPONSE_BYTES = 4 * 1024 * 1024
UNSET = object()


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values)*fraction)-1)] if values else None


def request(base, path, token, timeout, expected=UNSET):
    began = time.perf_counter()
    req = urllib.request.Request(base+path, headers={'Authorization': 'Bearer '+token})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        first = response.read(1)
        first_byte_ms = (time.perf_counter()-began)*1000
        payload = first+response.read(MAX_RESPONSE_BYTES)
        elapsed_ms = (time.perf_counter()-began)*1000
        if response.status != 200 or len(payload) > MAX_RESPONSE_BYTES:
            raise ValueError('API status or response capacity')
    value = json.loads(payload)
    canonical = lambda v: json.dumps(v, sort_keys=True, separators=(',', ':'), allow_nan=False)
    if expected is not UNSET and canonical(value) != canonical(expected):
        raise ValueError('API response differs from original fixture')
    return value, first_byte_ms, elapsed_ms


def phase(base, paths, token, expected, *, users, duration, period, timeout,
          sample=None, sample_interval=1):
    if (not 1 <= users <= 100 or not 0 < duration <= 300
            or not .05 <= period <= 10 or not 0 < timeout <= 10
            or not 0 < sample_interval <= 10 or not 1 <= len(paths) <= 32
            or any(not math.isfinite(x) for x in [duration, period, timeout, sample_interval])):
        raise ValueError('Invalid bounded API workload')
    stop = threading.Event()
    samples, sample_errors = [], []
    deadline = time.monotonic()+duration

    def observe():
        while not stop.is_set():
            try:
                row = sample()
                row['elapsed_seconds'] = duration-(deadline-time.monotonic())
                if time.monotonic() <= deadline:
                    samples.append(row)
            except Exception as error:
                sample_errors.append(type(error).__name__)
            stop.wait(sample_interval)

    def user(index):
        result = {'completed': 0, 'errors': {}, 'ttfb': [], 'roundtrip': [],
                  'per_path': {path: 0 for path in paths}}
        sequence = index
        while time.monotonic() < deadline:
            path = paths[sequence % len(paths)]
            sequence += 1
            began = time.monotonic()
            try:
                _, first, elapsed = request(base, path, token, timeout, expected[path])
                result['completed'] += 1
                result['per_path'][path] += 1
                result['ttfb'].append(first)
                result['roundtrip'].append(elapsed)
            except Exception as error:
                name = type(error).__name__
                result['errors'][name] = result['errors'].get(name, 0)+1
            # Closed-loop fixed user pacing; do not burst to catch up after a
            # slow request. Throughput gates detect a backend that falls behind.
            stop.wait(max(0, min(deadline, began+period)-time.monotonic()))
        return result

    observer = threading.Thread(target=observe, daemon=True) if sample else None
    if observer:
        observer.start()
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=users) as pool:
            rows = list(pool.map(user, range(users)))
    finally:
        stop.set()
        if observer:
            observer.join(timeout=35)
            if observer.is_alive():
                raise RuntimeError('API observer failed to stop')
    completed = sum(row['completed'] for row in rows)
    errors = sum(sum(row['errors'].values()) for row in rows)
    ttfb = [value for row in rows for value in row['ttfb']]
    roundtrip = [value for row in rows for value in row['roundtrip']]
    return {'summary': {'completed': completed, 'errors': errors,
                'users_without_success': sum(not row['completed'] for row in rows),
                'requests_per_second': completed/duration,
                'ttfb_ms_p95': percentile(ttfb, .95),
                'roundtrip_ms_p95': percentile(roundtrip, .95),
                'cpu_percent_mean': statistics.mean(r['cpu_percent'] for r in samples) if samples else None,
                'memory_bytes_peak': max((r['memory_bytes'] for r in samples), default=None),
                'per_path': {path: sum(row['per_path'][path] for row in rows) for path in paths}},
            'worker_errors': [row['errors'] for row in rows],
            'resource_samples': samples, 'sample_errors': sample_errors}


def compare(old, new):
    failures = []
    ratios = {}
    if old['workload'] != new['workload']:
        failures.append('Workloads differ')
    if old['environment'] != new['environment']:
        failures.append('Host environments differ')
    for name, data in [('original', old), ('native', new)]:
        measurement = data['measurement']
        summary = measurement['summary']
        if (summary['errors'] or summary['users_without_success']
                or measurement['sample_errors'] or len(measurement['resource_samples']) < 3):
            failures.append(name+' has errors or insufficient samples')
        if (set(summary['per_path']) != set(data['workload']['paths'])
                or any(not isinstance(n, int) or isinstance(n, bool) or n <= 0
                       for n in summary['per_path'].values())
                or sum(summary['per_path'].values()) != summary['completed']):
            failures.append(name+' has missing endpoint coverage or inconsistent request counts')
        target = data['workload']['users']/data['workload']['period']
        if summary['requests_per_second'] < .95*target:
            failures.append(name+' achieved less than95percent of declared request pacing')
    for metric, maximum in [('cpu_percent_mean', 1), ('memory_bytes_peak', 1),
                            ('ttfb_ms_p95', 1.1), ('roundtrip_ms_p95', 1.1)]:
        before, after = (data['measurement']['summary'][metric] for data in [old, new])
        if (before is None or after is None or before <= 0 or after < 0
                or not math.isfinite(before+after)):
            failures.append('Invalid metric: '+metric)
            continue
        ratios[metric] = after/before
        if ratios[metric] > maximum:
            failures.append(metric+' exceeds declared ratio '+str(maximum))
    before, after = (data['measurement']['summary']['requests_per_second'] for data in [old, new])
    ratios['requests_per_second'] = after/before if before > 0 else None
    if before <= 0 or after < .95*before:
        failures.append('Native throughput below95percent of original')
    return {'passed': not failures, 'failures': failures, 'native_over_original': ratios}
