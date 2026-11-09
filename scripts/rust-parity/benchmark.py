#!/usr/bin/env python3
"""Paired component benchmark of the actual Python TS helper and Rust port.

This measures helper work only, never application/playback savings.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import sys
import tempfile
import time

from ts_discontinuity import ROOT, load_legacy


def python_worker(fixture, iterations):
    legacy = load_legacy()
    payload = fixture.read_bytes()
    stamped = 0
    began = time.perf_counter_ns()
    cpu = time.process_time_ns()
    for _ in range(iterations):
        output, pids = legacy.stamp_first_packet_per_pid(payload, set())
        stamped += len(pids)
    cpu_ns = time.process_time_ns() - cpu
    wall_ns = time.perf_counter_ns() - began
    return {'implementation': 'python', 'wall_ns': wall_ns, 'cpu_ns': cpu_ns,
            'checksum': sum(output), 'stamped': stamped,
            'peak_rss_kib': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--iterations', type=int, default=2000)
    parser.add_argument('--samples', type=int, default=3)
    parser.add_argument('--worker-fixture', type=Path, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.iterations < 1 or args.samples < 3:
        parser.error('positive iterations and at least three samples required')
    if args.worker_fixture:
        print(json.dumps(python_worker(args.worker_fixture, args.iterations)))
        return
    if not args.output:
        parser.error('--output is required')
    payload = bytearray()
    for index in range(1024):
        pid = index % 64
        packet = bytearray([index % 251] * 188)
        packet[:6] = bytes([0x47, pid >> 8, pid & 255, 0x30 | (index % 16), 1, 0])
        payload.extend(packet)
    cargo = ['cargo', 'build', '--manifest-path', str(ROOT / 'rust/Cargo.toml'),
             '--locked', '--release', '-p', 'dispatcharr-stream-core', '--example', 'ts_bench']
    subprocess.run(cargo, check=True)
    target = Path(os.environ.get('CARGO_TARGET_DIR', ROOT / 'rust/target'))
    rust = target / 'release/examples/ts_bench'
    rows = []
    with tempfile.TemporaryDirectory(prefix='dispatcharr-ts-bench-') as temp:
        fixture = Path(temp) / 'fixture.ts'
        fixture.write_bytes(payload)
        commands = {'python': [sys.executable, str(Path(__file__).resolve()),
                               '--worker-fixture', str(fixture)],
                    'rust': [str(rust), str(fixture)]}

        def run(implementation, count):
            command = commands[implementation] + ([str(count)] if implementation == 'rust'
                                                   else ['--iterations', str(count)])
            return json.loads(subprocess.check_output(command, text=True, timeout=120))

        for implementation in commands:
            run(implementation, min(100, args.iterations))
        for sample in range(args.samples):
            pair = {}
            for implementation in (['python', 'rust'] if sample % 2 == 0 else ['rust', 'python']):
                pair[implementation] = run(implementation, args.iterations)
            assert pair['python']['checksum'] == pair['rust']['checksum'], 'output mismatch'
            assert pair['python']['stamped'] == pair['rust']['stamped'], 'PID count mismatch'
            rows.append(pair)
    ratio = statistics.median(row['rust']['wall_ns'] for row in rows) / statistics.median(
        row['python']['wall_ns'] for row in rows)
    output = {'scope': 'ts-discontinuity-helper-only', 'application_performance_verified': False,
              'fixture_sha256': hashlib.sha256(payload).hexdigest(),
              'python_source_sha256': hashlib.sha256((ROOT / 'apps/proxy/live_proxy/input/ts_discontinuity.py').read_bytes()).hexdigest(),
              'rustc': subprocess.check_output(['rustc', '--version'], text=True).strip(),
              'python': sys.version, 'iterations': args.iterations, 'samples': rows,
              'median_wall_time_ratio_rust_over_python': ratio,
              'notes': 'Linux peak RSS includes interpreter/runtime and is component-only; no memory gain gate. Fixture uses existing adaptation fields. No network, Redis, transcoding, or HTTP work.'}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + '\n')
    print(json.dumps({'component_wall_time_ratio': ratio,
                      'application_performance_verified': False}))


if __name__ == '__main__':
    main()
