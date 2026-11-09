#!/usr/bin/env python3
"""Bounded HTTP load and whole-stack Docker resource comparisons (stdlib only)."""
import argparse
import concurrent.futures
import datetime
import json
import math
import os
import platform
import statistics
import subprocess
import threading
import time
import urllib.request
from pathlib import Path


def command(args):
    return subprocess.check_output(args, text=True, timeout=30).strip()


def memory_bytes(value):
    value = value.split('/')[0].strip()
    units = {'B': 1, 'kB': 1000, 'KB': 1000, 'MB': 1000**2, 'GB': 1000**3,
             'TB': 1000**4, 'KiB': 1024, 'MiB': 1024**2, 'GiB': 1024**3,
             'TiB': 1024**4}
    for unit in sorted(units, key=len, reverse=True):
        if value.endswith(unit):
            return float(value[:-len(unit)]) * units[unit]
    raise ValueError('Unsupported Docker memory unit')


def resources(ids):
    rows = [json.loads(line) for line in command(
        ['docker', 'stats', '--no-stream', '--format', '{{json .}}', *ids]).splitlines()]
    if len(rows) != len(ids):
        raise RuntimeError('Docker did not return every selected container')
    return {'cpu_percent': sum(float(r['CPUPerc'].strip('%')) for r in rows),
            'memory_bytes': sum(memory_bytes(r['MemUsage']) for r in rows),
            'containers': [{'id': r['ID'], 'cpu_percent': float(r['CPUPerc'].strip('%')),
                            'memory_bytes': memory_bytes(r['MemUsage'])} for r in rows]}


def viewer(url, deadline, timeout, slow_delay):
    result = {'bytes': 0, 'requests': 0, 'errors': 0, 'empty_responses': 0,
              'first_byte_ms': []}
    while time.monotonic() < deadline:
        began = time.monotonic()
        try:
            # URL credentials never enter the output or exception messages.
            with urllib.request.urlopen(url, timeout=min(timeout, max(.001, deadline - began))) as response:
                result['requests'] += 1
                first = True
                while time.monotonic() < deadline:
                    # read1 returns available data rather than waiting for 64KiB.
                    data = response.read1(65536)
                    if time.monotonic() >= deadline:
                        break
                    if not data:
                        break
                    if first:
                        result['first_byte_ms'].append((time.monotonic() - began) * 1000)
                        first = False
                    result['bytes'] += len(data)
                    if slow_delay:
                        time.sleep(min(slow_delay, max(0, deadline - time.monotonic())))
                if first and time.monotonic() < deadline:
                    result['empty_responses'] += 1
        except Exception:
            if time.monotonic() < deadline:
                result['errors'] += 1
            time.sleep(min(0.1, max(0, deadline - time.monotonic())))
    return result


def phase(urls, viewers, duration, timeout, slow_delay, ids=None, interval=1, warmup_ramp=0):
    deadline = time.monotonic() + duration
    samples = []
    sample_errors = []
    stop = threading.Event()

    def sample():
        while not stop.is_set() and time.monotonic() < deadline:
            try:
                row = resources(ids)
                row['at_seconds_remaining'] = max(0, deadline - time.monotonic())
                if time.monotonic() < deadline:
                    samples.append(row)
            except Exception as exc:
                sample_errors.append(type(exc).__name__)
            stop.wait(interval)

    sampler = threading.Thread(target=sample) if ids else None
    if sampler:
        sampler.start()
    began = time.monotonic()
    def start_viewer(index):
        if index and warmup_ramp:
            # Keep the first warmup viewer connected while the channel initializes.
            # All warmup viewers share the same deadline; measurement has no ramp.
            time.sleep(min(warmup_ramp, max(0, deadline - time.monotonic())))
        return viewer(urls[index % len(urls)], deadline, timeout, slow_delay)

    with concurrent.futures.ThreadPoolExecutor(max_workers=viewers) as pool:
        futures = [pool.submit(start_viewer, i) for i in range(viewers)]
        outcomes = [future.result() for future in futures]
    elapsed = time.monotonic() - began
    stop.set()
    if sampler:
        sampler.join()
    latencies = [latency for row in outcomes for latency in row['first_byte_ms']]
    summary = {key: sum(row[key] for row in outcomes)
               for key in ('bytes', 'requests', 'errors', 'empty_responses')}
    summary.update(elapsed_seconds=elapsed, bytes_per_second=summary['bytes'] / duration,
                   first_byte_ms_median=statistics.median(latencies) if latencies else None,
                   first_byte_ms_p95=sorted(latencies)[math.ceil(.95 * len(latencies)) - 1]
                   if latencies else None,
                   first_byte_samples=len(latencies),
                   viewers_without_data=sum(row['bytes'] == 0 for row in outcomes))
    if ids:
        summary.update(cpu_percent_mean=statistics.mean(row['cpu_percent'] for row in samples)
                       if samples else None,
                       memory_bytes_peak=max((row['memory_bytes'] for row in samples), default=None))
    return {'summary': summary, 'resource_samples': samples, 'sample_errors': sample_errors,
            'viewers': outcomes}


def compare(baseline, candidate, gates):
    failures = []
    if any(not math.isfinite(value) or value <= 0 for value in gates.values()):
        failures.append('Gates must be finite positive ratios')
    for key in ('viewers', 'url_count', 'scenario', 'warmup', 'duration', 'slow_delay', 'timeout',
                'fixture_id', 'sample_interval'):
        if baseline['workload'][key] != candidate['workload'][key]:
            failures.append('Workload mismatch: ' + key)
    if baseline['workload'].get('warmup_ramp', 0) != candidate['workload'].get('warmup_ramp', 0):
        failures.append('Workload mismatch: warmup_ramp')
    if baseline['environment'] != candidate['environment']:
        failures.append('Host environment mismatch')
    ratios = {}
    for name, data in [('baseline', baseline), ('candidate', candidate)]:
        summary = data['measurement']['summary']
        if (summary['errors'] or summary['empty_responses'] or summary['viewers_without_data']
                or data['measurement']['sample_errors']
                or len(data['measurement']['resource_samples']) < 3):
            failures.append(name + ' has load errors, missing data, or insufficient resource samples')
    for metric, maximum in gates.items():
        old = baseline['measurement']['summary'].get(metric)
        new = candidate['measurement']['summary'].get(metric)
        if old is None or new is None or old <= 0 or new < 0 or not math.isfinite(old + new):
            failures.append('Invalid comparison metric: ' + metric)
            continue
        ratios[metric] = new / old
        if new / old > maximum:
            failures.append(f'{metric} ratio {new / old:.4f} exceeds {maximum:.4f}')
    old_rate = baseline['measurement']['summary']['bytes_per_second']
    new_rate = candidate['measurement']['summary']['bytes_per_second']
    if (not math.isfinite(old_rate + new_rate) or old_rate <= 0
            or new_rate < old_rate * 0.95):
        failures.append('Candidate throughput is below 95% of baseline')
    return {'passed': not failures, 'failures': failures, 'candidate_over_baseline': ratios}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('run')
    run.add_argument('--compose-file', action='append', required=True)
    run.add_argument('--project', required=True)
    run.add_argument('--services', nargs='+', required=True)
    run.add_argument('--urls-file', type=Path, required=True)
    run.add_argument('--scenario', choices=['same-channel', 'many-channel'], required=True)
    run.add_argument('--viewers', type=int, default=10)
    run.add_argument('--warmup', type=float, default=15)
    run.add_argument('--warmup-ramp', type=float, default=0,
                     help='Warmup-only delay before viewers2..N, keeping viewer1 connected first')
    run.add_argument('--duration', type=float, default=60)
    run.add_argument('--timeout', type=float, default=3)
    run.add_argument('--slow-delay', type=float, default=0)
    run.add_argument('--sample-interval', type=float, default=1)
    run.add_argument('--output', type=Path, required=True)
    run.add_argument('--revision', required=True, help='Exact revision/image digest under test')
    run.add_argument('--fixture-id', required=True, help='Fixture content hash and profile identifier')
    check = commands.add_parser('compare')
    check.add_argument('baseline', type=Path)
    check.add_argument('candidate', type=Path)
    check.add_argument('--max-cpu-ratio', type=float, default=1)
    check.add_argument('--max-memory-ratio', type=float, default=1)
    check.add_argument('--max-latency-ratio', type=float, default=1.10)
    args = parser.parse_args()
    if args.command == 'compare':
        result = compare(json.loads(args.baseline.read_text()), json.loads(args.candidate.read_text()),
                         {'cpu_percent_mean': args.max_cpu_ratio,
                          'memory_bytes_peak': args.max_memory_ratio,
                          'first_byte_ms_median': args.max_latency_ratio})
        print(json.dumps(result, indent=2))
        return 0 if result['passed'] else 1
    if (not all(math.isfinite(value) for value in
                (args.warmup, args.warmup_ramp, args.duration, args.timeout, args.slow_delay, args.sample_interval))
            or args.viewers < 1 or args.warmup < 0 or args.warmup_ramp < 0
            or (args.warmup_ramp and args.warmup_ramp >= args.warmup) or args.duration < 10 or args.timeout <= 0
            or args.timeout > args.duration or args.slow_delay < 0 or args.sample_interval <= 0):
        parser.error('Invalid workload limits (duration must be >=10s, timeout <= duration)')
    urls = [line.strip() for line in args.urls_file.read_text().splitlines()
            if line.strip() and not line.startswith('#')]
    if not urls or any(not url.startswith(('http://', 'https://')) for url in urls):
        parser.error('URLs file must contain HTTP(S) URLs')
    if args.scenario == 'same-channel' and len(urls) != 1:
        parser.error('same-channel requires exactly one URL')
    if args.scenario == 'many-channel' and (len(urls) != args.viewers or len(set(urls)) != args.viewers):
        parser.error('many-channel requires exactly one distinct URL per viewer')
    compose = ['docker', 'compose', '-p', args.project]
    for file in args.compose_file:
        compose.extend(['-f', file])
    ids = []
    for service in args.services:
        running = command(compose + ['ps', '--status', 'running', '-q', service]).splitlines()
        if not running:
            parser.error('Selected service is not running: ' + service)
        ids.extend(running)
    ids = list(dict.fromkeys(ids))
    if args.warmup:
        phase(urls, args.viewers, args.warmup, args.timeout, args.slow_delay,
              warmup_ramp=args.warmup_ramp)
    measured = phase(urls, args.viewers, args.duration, args.timeout, args.slow_delay,
                     ids, args.sample_interval)
    output = {'format_version': 1, 'revision': args.revision,
              'timestamp_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'environment': {'host': platform.node(), 'system': platform.platform(),
                              'logical_cpus': os.cpu_count(), 'docker': command(['docker', 'version', '--format', '{{.Server.Version}}'])},
              'workload': {key: getattr(args, key) for key in
                           ('scenario', 'viewers', 'warmup', 'warmup_ramp', 'duration', 'slow_delay', 'timeout',
                            'fixture_id', 'sample_interval')},
              'services': args.services, 'measurement': measured}
    output['workload']['url_count'] = len(urls)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps(measured['summary'], indent=2))
    return 1 if (measured['summary']['errors'] or measured['summary']['viewers_without_data']
                 or measured['summary']['empty_responses'] or measured['sample_errors']
                 or len(measured['resource_samples']) < 3) else 0


if __name__ == '__main__':
    raise SystemExit(main())
