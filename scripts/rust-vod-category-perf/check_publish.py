#!/usr/bin/env python3
"""Static corruption checks for a captured category export; launches no workloads.
Original Dispatcharr authors retain application credit and AGPL attribution.
"""
import argparse
import contextlib
import copy
import importlib.util
import json
from pathlib import Path
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', required=True, type=Path)
    parser.add_argument('--build-manifest', required=True, type=Path)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location('category_exporter', Path(__file__).with_name('publish.py'))
    publisher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(publisher)
    source = args.evidence.resolve()
    target_phase = sorted((source / 'experiment').glob('matrix-*/phase.json'))[0]
    original = json.loads(target_phase.read_text())
    mutations = {}
    value = copy.deepcopy(original)
    value['resources']['after']['services'][value['backend']]['cpu']['usage_usec'] += 1
    mutations['physical_cpu_counter_changed'] = value
    value = copy.deepcopy(original)
    for row in value['load']['request_traces']:
        row['completion_ms'] += 100
    mutations['request_timing_changed'] = value
    value = copy.deepcopy(original)
    value['load']['resource_samples'][0]['containers'][0]['memory_bytes'] += 1
    mutations['component_memory_sum_changed'] = value
    value = copy.deepcopy(original)
    value['normalized']['late_completed'] = 1
    mutations['late_measurement'] = value
    value = copy.deepcopy(original)
    value['warmup']['request_traces'][0]['token'] = 'must-never-be-published'
    mutations['secret_key_in_trace'] = value
    rows = []
    for name, value in mutations.items():
        with tempfile.TemporaryDirectory(prefix='category-static-export-') as temporary:
            root = Path(temporary)
            evidence = root / 'input'
            evidence.mkdir()
            for item in source.iterdir():
                if item.name != 'experiment':
                    (evidence / item.name).symlink_to(item, target_is_directory=item.is_dir())
            experiment = evidence / 'experiment'
            experiment.mkdir()
            for item in (source / 'experiment').iterdir():
                if item.name != target_phase.parent.name:
                    (experiment / item.name).symlink_to(item, target_is_directory=item.is_dir())
            changed = experiment / target_phase.parent.name
            changed.mkdir()
            (changed / 'phase.json').write_text(json.dumps(value))
            rejected = False
            with contextlib.redirect_stdout(None):
                try:
                    publisher.publish(evidence, args.build_manifest, root / 'output')
                except (AssertionError, ValueError):
                    rejected = True
            if not rejected:
                raise AssertionError(f'Corrupted evidence accepted: {name}')
            rows.append({'mutation': name, 'rejected': True})
    print(json.dumps({'passed': True, 'static_only': True, 'checks': rows}))


if __name__ == '__main__':
    main()
