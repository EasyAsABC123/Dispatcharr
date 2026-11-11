"""Full-stack read-only observer, reusing the existing Docker working-set sampler.
No process or timer starts on import. Original Dispatcharr authors retain credit.
"""
import importlib.util
import json
import time
from pathlib import Path
import cgroup
import docker_stats


def load_perf(tools):
    path = Path(tools) / 'scripts/rust-perf/perf.py'
    spec = importlib.util.spec_from_file_location('guide_existing_docker_observer', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class Observer:
    def __init__(self, selected, tools):
        # selected is keyed by inspected Compose service, not arbitrary HTTP URLs.
        if not 2 <= len(selected) <= 3:
            raise ValueError('Whole guide stack requires infrastructure, origin and backend')
        self.selected = selected
        self.perf = load_perf(tools)
        self.direct = docker_stats.Stats(selected)
        self.direct.prime()
        if len({value['id'] for value in selected.values()}) != len(selected):
            raise ValueError('Duplicate selected service identity')

    def counters(self):
        began = time.monotonic()
        values = {name: cgroup.read(value['id'], value['limits'])
                  for name, value in self.selected.items()}
        return {'services': values, 'observer_seconds': time.monotonic() - began}

    def sample(self):
        began = time.monotonic()
        value = self.direct.sample()
        observed = [row['id'] for row in value['containers']]
        if (len(set(observed)) != len(self.selected)
                or any(sum(identifier.startswith(short) for identifier in
                           [v['id'] for v in self.selected.values()]) != 1 for short in observed)):
            raise ValueError('Docker sampler identities differ from selected stack')
        value['observer_seconds'] = time.monotonic() - began
        value['memory_metric'] = 'Docker working-memory sum, infrastructure+origin+one backend'
        return value

    def finish(self, before):
        after = self.counters()
        deltas = {name: cgroup.delta(before['services'][name], after['services'][name])
                  for name in self.selected}
        return {'before': before, 'after': after, 'services': deltas, 'selected_services': self.selected,
                'cpu_percent_mean': sum(v['cpu_percent_mean'] for v in deltas.values()),
                'cpu_metric': 'Sum of complete-window cgroup usage_usec/time per selected service'}

    def smoke(self):
        before = self.counters()
        cli = self.perf.resources([v['id'] for v in self.selected.values()])
        direct = self.sample()
        # CLI rounds its MiB display. Read sequentially outside timing; require
        # tight absolute/relative agreement, retaining actual differences.
        comparisons = []
        for row in direct['containers']:
            matching = [v for v in cli['containers'] if row['id'].startswith(v['id'])]
            if len(matching) != 1:
                raise ValueError('CLI/direct statistics identities differ')
            difference = abs(row['memory_bytes'] - matching[0]['memory_bytes'])
            tolerance = max(2*1024**2, .02*row['memory_bytes'])
            comparisons.append({'service': next(name for name,v in self.selected.items() if v['id']==row['id']),
                                'difference_bytes': difference, 'tolerance_bytes': tolerance,
                                'passed': difference <= tolerance})
        if not all(row['passed'] for row in comparisons):
            raise ValueError('Docker CLI and one-shot working memory differ')
        samples = []
        began = time.monotonic()
        deadline = began + 10
        # Same completion-plus1s scheduling used by api_load.observe; no catch-up
        # bursts can make a slow sampler appear to meet the cadence contract.
        while time.monotonic() < deadline:
            value = self.sample()
            if time.monotonic() <= deadline:
                samples.append(value)
            time.sleep(max(0, min(1, deadline-time.monotonic())))
        observed_window = time.monotonic() - began
        if len(samples) < 9:
            raise ValueError('One-shot observer missed9samples/10-second functional cadence')
        result = self.finish(before)
        if any(row['memory_bytes'] <= 0 or len(row['containers']) != len(self.selected)
               for row in samples):
            raise ValueError('Full-stack observer omitted memory/service samples')
        # Functional contract only; deliberately omit observed resource magnitudes.
        return {'passed': True, 'samples': len(samples), 'services': sorted(self.selected),
                'functional_cadence_seconds': observed_window,
                'maximum_sample_observer_seconds': max(v['observer_seconds'] for v in samples),
                'cli_memory_comparisons': comparisons,
                'limits_verified': True, 'monotonic_counters_verified': True,
                'memory_metric': samples[0]['memory_metric'],
                'cpu_metric': result['cpu_metric'], 'performance_verified': False}
