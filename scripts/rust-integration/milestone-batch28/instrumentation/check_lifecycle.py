#!/usr/bin/env python3
"""Two instrumentation-only real-FD/PID lifecycle cases; no apps/network."""
import argparse
import hashlib
import importlib.util
import json
import os
import time
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--sampler', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(mode=0o755, parents=True, exist_ok=False)
    args.output.chmod(0o755)
    spec = importlib.util.spec_from_file_location('owned_lifecycle_sampler', args.sampler)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    scope = module.cgroup(os.getpid())
    if not scope.name.startswith('dispatcharr-resource-') or not scope.name.endswith('.scope'):
        raise RuntimeError('ActualDedicatedInstrumentationScopeRequired')
    groups = {'observer': {'path': str(scope), 'PID': os.getpid(), 'category': 'observer'}}
    report = {'passed': False, 'scope': 'counter instrumentation lifecycle only; no60s workload/app/Source benchmark',
              'sampler_sha256': hashlib.sha256(args.sampler.read_bytes()).hexdigest(), 'actual_observer_cgroup': str(scope),
              'cases': [], 'gain_accepted': False}
    def save():
        (args.output / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
    save()
    for name in ('cancel-before-clock-start', 'one-actual-tick-then-cancel'):
        out = args.output / name
        out.mkdir(mode=0o755)
        out.chmod(0o755)
        row = {'name': name, 'passed': False}
        report['cases'].append(row)
        worker = None
        try:
            worker = module.CounterWorker(groups, out)
            row['actual_ready'] = worker.ready
            if name == 'one-actual-tick-then-cancel':
                began = time.monotonic()
                worker.start(began)
                sample = worker.sample(0)
                row['actual_sample'] = sample
                row['unchanged_50ms_clock_check'] = abs(sample['monotonic_start'] - began) <= .05 and sample['monotonic_end'] - sample['monotonic_start'] <= .05
                if sample['index'] != 0 or not row['unchanged_50ms_clock_check'] or len(sample['counter_file_reads']) < 5:
                    raise AssertionError('ActualCounterReadAndUnchangedClockCriteria')
                values = sample['groups']['observer']
                if values['cpu_stat']['usage_usec'] < 0 or values['charged_memory_current_bytes'] <= 0 or values['inactive_file_bytes'] < 0:
                    raise AssertionError('ActualMandatoryCgroupCounters')
            row['actual_settlement'] = worker.close()
            if not row['actual_settlement']['cleanup_passed'] or row['actual_settlement']['worker_final']['measurement_completed']:
                raise AssertionError('ActualCancellationNormalExitFDAndGenerationCleanup')
            row['passed'] = True
        except BaseException as error:
            row['failure'] = {'type': type(error).__name__, 'label': str(error)[:180]}
        finally:
            if worker is not None:
                row['actual_settlement'] = worker.close()
            elif (out / 'counter-sampler-settlement.json').exists():
                row['actual_settlement'] = json.loads((out / 'counter-sampler-settlement.json').read_text())
            save()
    report['passed'] = len(report['cases']) == 2 and all(row['passed'] and row['actual_settlement']['cleanup_passed'] for row in report['cases'])
    save()
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
