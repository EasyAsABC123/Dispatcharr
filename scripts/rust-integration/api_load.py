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
          sample=None, sample_interval=1, read_request=None, stagger=False,
          require_complete_before_deadline=False, retain_traces=False):
    if (not 1 <= users <= 100 or not 0 < duration <= 300
            or not .05 <= period <= 10 or not 0 < timeout <= 10
            or not 0 < sample_interval <= 10 or not 1 <= len(paths) <= 32
            or any(not math.isfinite(x) for x in [duration, period, timeout, sample_interval])):
        raise ValueError('Invalid bounded API workload')
    if read_request is not None and not callable(read_request):
        raise ValueError('API request adapter must be callable')
    if type(retain_traces) is not bool:
        raise ValueError('Trace selection must be boolean')
    trace_per_user_limit = math.ceil(duration / period) + 1
    if retain_traces and users * trace_per_user_limit > 10000:
        raise ValueError('Request trace ceiling is10000 rows; reduce declared load or disable traces')
    read = request if read_request is None else read_request
    began_phase = time.monotonic()
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
        result = {'completed': 0, 'late_completed': 0, 'errors': {}, 'ttfb': [], 'roundtrip': [],
                  'per_path': {path: 0 for path in paths}, 'traces': []}
        if stagger:
            stop.wait(max(0, began_phase+index*period/users-time.monotonic()))
        sequence = index
        attempt = 0
        while time.monotonic() < deadline:
            path_index = sequence % len(paths)
            path = paths[path_index]
            sequence += 1
            began = time.monotonic()
            first, elapsed, error_name = None, None, None
            try:
                _, first, elapsed = read(base, path, token, timeout, expected[path])
                if retain_traces and (any(type(v) not in [int, float] or not math.isfinite(v)
                                         or v < 0 for v in [first, elapsed]) or elapsed < first):
                    raise ValueError('Invalid request timing from adapter')
                ended = time.monotonic()
                if require_complete_before_deadline and ended > deadline:
                    result['late_completed'] += 1
                else:
                    result['completed'] += 1
                    result['per_path'][path] += 1
                    result['ttfb'].append(first)
                    result['roundtrip'].append(elapsed)
            except Exception as error:
                ended = time.monotonic()
                error_name = type(error).__name__[:64]
                result['errors'][error_name] = result['errors'].get(error_name, 0)+1
                first, elapsed = None, None
            if retain_traces:
                if len(result['traces']) >= trace_per_user_limit:
                    result['errors']['TraceCapacity'] = result['errors'].get('TraceCapacity', 0)+1
                    break
                result['traces'].append({'user_index': index, 'attempt_index': attempt,
                    'path_index': path_index, 'started_seconds': began-began_phase,
                    'ttfb_ms': first, 'completion_ms': elapsed,
                    'verified_complete_seconds': ended-began_phase,
                    'in_window': ended <= deadline, 'error_type': error_name,
                    'disposition': 'error' if error_name else ('late' if ended > deadline else 'success')})
                attempt += 1
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
    result = {'summary': {'completed': completed, 'errors': errors,
                'users_without_success': sum(not row['completed'] for row in rows),
                'late_completed': sum(row['late_completed'] for row in rows),
                'requests_per_second': completed/duration,
                'ttfb_ms_p95': percentile(ttfb, .95),
                'roundtrip_ms_p95': percentile(roundtrip, .95),
                'cpu_percent_mean': statistics.mean(r['cpu_percent'] for r in samples) if samples else None,
                'memory_bytes_peak': max((r['memory_bytes'] for r in samples), default=None),
                'per_path': {path: sum(row['per_path'][path] for row in rows) for path in paths}},
            'worker_errors': [row['errors'] for row in rows],
            'resource_samples': samples, 'sample_errors': sample_errors}
    if retain_traces:
        traces = sorted((trace for row in rows for trace in row['traces']),
                        key=lambda trace: (trace['started_seconds'], trace['user_index']))
        result['request_traces'] = traces
        result['trace_scope'] = ('Bounded timing/indices only; no URL, token, headers, '
                                 'request/response body or exception text. Late work excluded '
                                 'when require_complete_before_deadline is true.')
        result['trace_limit'] = 10000
    return result


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
