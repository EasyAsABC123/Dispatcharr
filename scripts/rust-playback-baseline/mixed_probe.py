#!/usr/bin/env python3
"""Real HTTP slow/fast reader independence probe, not full-app performance proof."""
import argparse
import concurrent.futures
import importlib.util
import json
import math
from pathlib import Path
import socket
import statistics
import subprocess
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('mixed_perf', ROOT / 'scripts/rust-perf/perf.py')
perf = importlib.util.module_from_spec(spec)
spec.loader.exec_module(perf)


def run_probe(url, fast_viewers, slow_viewers, warmup, duration, ramp, slow_delay,
              minimum_fast_rate, ids=None, timeout=15):
    began = time.monotonic()
    measured_start, deadline = began + warmup, began + warmup + duration
    samples, sampling_errors = [], []
    stop = threading.Event()

    def read(index):
        slow = index >= fast_viewers
        result = {'role': 'slow' if slow else 'fast', 'bytes': 0, 'errors': 0,
                  'first_byte_ms': None, 'small_receive_buffer': False}
        if index and ramp:
            time.sleep(ramp)
        started = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                if slow:
                    # Backpressure must reach the sender, not only a large OS receive buffer.
                    response.fp.raw._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                    result['small_receive_buffer'] = True
                first = True
                while time.monotonic() < deadline:
                    data = response.read1(1024 if slow else 65536)
                    now = time.monotonic()
                    if not data or now >= deadline:
                        break
                    if first:
                        result['first_byte_ms'] = (now - started) * 1000
                        first = False
                    if now >= measured_start:
                        result['bytes'] += len(data)
                    if slow:
                        time.sleep(min(slow_delay, max(0, deadline - time.monotonic())))
        except Exception as error:
            result['errors'] += 1
            result['error_type'] = type(error).__name__
        result['bytes_per_second'] = result['bytes'] / duration
        return result

    def sample():
        while time.monotonic() < measured_start and not stop.is_set():
            stop.wait(min(.1, max(0, measured_start - time.monotonic())))
        while time.monotonic() < deadline and not stop.is_set():
            try:
                row = perf.resources(ids)
                if time.monotonic() < deadline:
                    row['seconds_after_measurement_start'] = time.monotonic() - measured_start
                    samples.append(row)
            except Exception as error:
                sampling_errors.append(type(error).__name__)
            stop.wait(1)

    sampler = threading.Thread(target=sample) if ids else None
    if sampler:
        sampler.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=fast_viewers + slow_viewers) as pool:
        results = list(pool.map(read, range(fast_viewers + slow_viewers)))
    stop.set()
    if sampler:
        sampler.join()
    fast = [row for row in results if row['role'] == 'fast']
    slow = [row for row in results if row['role'] == 'slow']
    failures = []
    if any(row['errors'] for row in results):
        failures.append('HTTP reader errors')
    if any(row['bytes_per_second'] < minimum_fast_rate for row in fast):
        failures.append('Fast viewer throughput below the declared floor')
    if any(not row['small_receive_buffer'] or not row['bytes'] for row in slow):
        failures.append('Slow viewer did not exercise bounded receive-buffer backpressure')
    fast_floor = min((row['bytes_per_second'] for row in fast), default=0)
    if any(row['bytes_per_second'] >= fast_floor * .5 for row in slow):
        failures.append('Slow viewer was not substantially slower than fast viewers')
    if ids and (len(samples) < 3 or sampling_errors):
        failures.append('Insufficient valid Docker resource samples')
    return {'scope': 'mixed-slow-fast-native-ts-http-probe', 'passed': not failures,
            'application_performance_verified': False, 'memory_bound_proven': False,
            'failures': failures, 'workload': {'fast_viewers': fast_viewers, 'slow_viewers': slow_viewers,
                'warmup_seconds': warmup, 'duration_seconds': duration, 'ramp_seconds': ramp,
                'slow_delay_seconds': slow_delay, 'slow_read_bytes': 1024, 'slow_receive_buffer_bytes': 4096,
                'minimum_fast_bytes_per_second': minimum_fast_rate},
            'readers': results, 'resource_samples': samples, 'sampling_errors': sampling_errors,
            'summary': {'fast_bytes_per_second_min': fast_floor,
                'slow_bytes_per_second_max': max((row['bytes_per_second'] for row in slow), default=0),
                'cpu_percent_mean': statistics.mean(row['cpu_percent'] for row in samples) if samples else None,
                'memory_bytes_sampled_peak': max((row['memory_bytes'] for row in samples), default=None)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url-file', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--fast-viewers', type=int, default=9)
    parser.add_argument('--slow-viewers', type=int, default=1)
    parser.add_argument('--warmup', type=float, default=15)
    parser.add_argument('--duration', type=float, default=30)
    parser.add_argument('--ramp', type=float, default=4)
    parser.add_argument('--slow-delay', type=float, default=.25)
    parser.add_argument('--minimum-fast-rate', type=float, default=225000)
    parser.add_argument('--compose-file', action='append', default=[])
    parser.add_argument('--project')
    parser.add_argument('--services', nargs='+')
    args = parser.parse_args()
    values = (args.warmup, args.duration, args.ramp, args.slow_delay, args.minimum_fast_rate)
    if (not all(math.isfinite(value) and value >= 0 for value in values)
            or args.fast_viewers < 1 or args.slow_viewers < 1 or args.fast_viewers + args.slow_viewers > 100
            or args.duration < 10 or args.slow_delay <= 0 or args.minimum_fast_rate <= 0
            or args.ramp >= args.warmup):
        parser.error('Invalid probe limits')
    urls = [line.strip() for line in args.url_file.read_text().splitlines() if line.strip()]
    if len(urls) != 1 or not urls[0].startswith(('http://', 'https://')):
        parser.error('URL file needs exactly one HTTP(S) playback URL')
    ids = []
    if args.compose_file:
        if not args.project or not args.services:
            parser.error('Compose sampling requires project and services')
        compose = ['docker', 'compose', '-p', args.project]
        for path in args.compose_file:
            compose += ['-f', path]
        for service in args.services:
            containers = subprocess.check_output(compose + ['ps', '--status', 'running', '-q', service], text=True, timeout=30).splitlines()
            if not containers:
                parser.error('Selected service is not running')
            ids.extend(containers)
    result = run_probe(urls[0], args.fast_viewers, args.slow_viewers, args.warmup, args.duration,
                       args.ramp, args.slow_delay, args.minimum_fast_rate, ids)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': result['passed'], 'failures': result['failures'], 'summary': result['summary']}))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
