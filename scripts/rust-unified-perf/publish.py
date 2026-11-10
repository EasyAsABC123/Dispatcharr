#!/usr/bin/env python3
"""Verify a complete captured experiment and publish numerical evidence.

Original Dispatcharr authors retain all application and algorithm credit.
Credentials, response values and container/host identities are not published.
"""
import argparse
import hashlib
import importlib.util
import json
import math
from pathlib import Path
import statistics
import sys


def digest_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read(path):
    return json.loads(Path(path).read_text())


def save(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + '\n')


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def numeric(value):
    if isinstance(value, dict):
        for item in value.values():
            numeric(item)
    elif isinstance(value, list):
        for item in value:
            numeric(item)
    elif isinstance(value, float) and not math.isfinite(value):
        raise ValueError('Nonfinite evidence value')


def publish(evidence, manifest_path, destination):
    evidence = evidence.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    tools = evidence / 'tools'
    protocol = load('protocol', tools / 'scripts/rust-unified-perf/protocol.py')
    acceptance = load('vod_captured_acceptance', tools / 'scripts/rust-unified-perf/acceptance.py')
    manifest = read(manifest_path)
    protocol.identity(manifest)
    report = read(evidence / 'integration.json')
    assert report['passed'] and report['performance_verified']
    assert report['cleanup'] == 'owned project removed' and not report['resources_retained']
    assert report['build_manifest_sha256'] == digest_file(manifest_path)
    assert report['candidate_binding']['image_id'] == manifest['image_id']
    for name, digest in report['tool_source_sha256'].items():
        assert digest_file(tools / name) == digest, name
    experiment = evidence / 'experiment'
    plan = read(experiment / 'frozen-plan.json')
    qualification = read(experiment / 'source-qualification.json')
    protocol.validate_plan(plan, qualification, manifest, plan['fixture'])
    matrix = read(experiment / 'matrix.json')
    assert matrix['passed'] and len(matrix['pairs']) == 6
    required = {(work['name'], trial) for work in protocol.POLICY['workloads'] for trial in [1, 2, 3]}
    seen = set()
    for pair in matrix['pairs']:
        identity = (pair['workload'], pair['trial'])
        assert identity in required and identity not in seen
        seen.add(identity)
        assert pair['order'] == protocol.POLICY['orders'][pair['trial'] - 1]
        assert pair['passed']
        assert acceptance.compare(pair['phases']['source'], pair['phases']['candidate']) == pair['acceptance']
        assert all(value['passed'] for value in pair['per_endpoint'].values())
    assert seen == required

    phases = []
    expected_measurements = {}
    for pair in matrix['pairs']:
        for backend in ['source', 'candidate']:
            name = f"matrix-{pair['workload']}-{pair['trial']}-{backend}"
            expected_measurements[name] = pair['phases'][backend]
        endpoint_phases = {
            backend: read(experiment / f"matrix-{pair['workload']}-{pair['trial']}-{backend}" / 'phase.json')['per_endpoint']
            for backend in ['source', 'candidate']
        }
        for index in map(str, range(len(protocol.PATHS))):
            old = endpoint_phases['source'][index]
            new = endpoint_phases['candidate'][index]
            retained = pair['per_endpoint'][index]
            assert retained['source'] == old and retained['candidate'] == new
            assert old['completed'] > 0 and new['completed'] > 0
            budget = protocol.POLICY['budgets']
            assert new['completion_p95_ms'] <= old['completion_p95_ms'] * budget['max_completion_p95_ratio']
            assert new['ttfb_p95_ms'] <= max(old['ttfb_p95_ms'], budget['ttfb_floor_ms']) * budget['max_ttfb_p95_ratio']
            assert new['throughput_per_second'] >= old['throughput_per_second'] * budget['min_throughput_ratio']
    for path in sorted(experiment.glob('*/phase.json')):
        name = path.parent.name
        stage = 'measurement' if name in expected_measurements else 'qualification'
        assert stage == 'measurement' or name.startswith('qualification-')
        phase = read(path)
        normalized = phase['normalized']
        loaded = phase['load']
        assert phase['fixture_binding'] == plan['fixture']
        if stage == 'measurement':
            assert normalized == expected_measurements[name]
            assert not acceptance.phase(normalized)
        else:
            assert phase['backend'] == 'source' and normalized['samples'] >= 15
        selected = phase['resources']['selected_services']
        assert math.isclose(sum(item['cpu_percent_mean'] for item in phase['resources']['services'].values()),
                            normalized['cpu_percent_mean'], rel_tol=1e-12)
        samples = []
        for sample in loaded['resource_samples']:
            containers = []
            for item in sample['containers']:
                matching = [service for service, data in selected.items() if data['id'].startswith(item['id'])]
                assert len(matching) == 1
                containers.append({'service': matching[0], 'working_memory_bytes': item['memory_bytes'],
                                   'cgroup_memory_usage_bytes': item['memory_usage_bytes'],
                                   'inactive_file_bytes': item['inactive_file_bytes'],
                                   'memory_limit_bytes': item['memory_limit_bytes'],
                                   'docker_cpu_percent_diagnostic': item['cpu_percent']})
            assert len(containers) == 3
            assert sum(item['working_memory_bytes'] for item in containers) == sample['memory_bytes']
            samples.append({'elapsed_seconds': sample['elapsed_seconds'],
                            'working_memory_bytes': sample['memory_bytes'],
                            'docker_cpu_percent_diagnostic': sample['cpu_percent'], 'services': containers})
        assert len(samples) == normalized['samples']
        assert max(item['working_memory_bytes'] for item in samples) == normalized['peak_working_memory_bytes']
        traces = loaded['request_traces']
        if stage == 'measurement':
            assert all(row['disposition'] == 'success' and row['in_window'] for row in traces)
        phases.append({'id': name, 'stage': stage, 'backend': phase['backend'],
                       'readers': phase['workload']['users'], 'period_seconds': phase['period_seconds'],
                       'raw_phase_sha256': digest_file(path), 'normalized': normalized,
                       'per_endpoint': phase['per_endpoint'],
                       'cgroup_components': phase['resources']['services'],
                       'resource_samples': samples, 'request_traces': traces})
    assert len(phases) == 20 and sum(row['stage'] == 'measurement' for row in phases) == 12

    medians = {}
    for work in protocol.POLICY['workloads']:
        pairs = [pair for pair in matrix['pairs'] if pair['workload'] == work['name']]
        ratios = {key: statistics.median(pair['acceptance']['ratios'][key] for pair in pairs)
                  for key in ['cpu_percent_mean', 'peak_working_memory_bytes', 'completion_p95_ms', 'throughput_per_second']}
        absolute = {backend: {key: statistics.median(pair['phases'][backend][key] for pair in pairs)
                              for key in ['cpu_percent_mean', 'peak_working_memory_bytes', 'completion_p95_ms']}
                    for backend in ['source', 'candidate']}
        medians[work['name']] = {'readers': work['users'], 'paired_ratios': ratios,
                                'decrease_percent': {key: (1 - value) * 100 for key, value in ratios.items()
                                                     if key != 'throughput_per_second'}, 'backend_medians': absolute}

    numerical = {'scope': protocol.POLICY['scope'], 'policy_sha256': protocol.digest(protocol.POLICY),
                 'plan_sha256': plan['plan_sha256'], 'phases': phases, 'pairs': matrix['pairs'],
                 'medians': medians, 'cpu_metric': 'Whole selected service-group complete-window cgroup CPU;100%=one core',
                 'memory_metric': 'Peak sampled Docker working-memory sum; not continuous allocator/cgroup peak',
                 'docker_cpu_samples': 'Diagnostic only; headline CPU uses complete-window cgroup counters',
                 'credit': 'Original Dispatcharr authors and contributors retain all application and algorithm credit and AGPL licensing.'}
    numeric(numerical)
    save(destination / 'numerical-evidence.json', numerical)
    summary = {'passed': True, 'scope': protocol.POLICY['scope'], 'candidate_image': manifest['image_id'],
               'candidate_source_tree': manifest['source_tree_sha256'], 'candidate_source_archive': manifest['source_archive_sha256'],
               'git_revision': manifest['git_revision'], 'build_manifest_sha256': digest_file(manifest_path),
               'evidence': str(evidence), 'integration_report_sha256': digest_file(evidence / 'integration.json'),
               'tool_manifest_sha256': protocol.digest(report['tool_source_sha256']),
               'policy_sha256': protocol.digest(protocol.POLICY), 'plan_sha256': plan['plan_sha256'],
               'pairs': 6, 'measurement_phases': 12, 'qualification_phases': 8,
               'resource_samples': sum(len(row['resource_samples']) for row in phases),
               'measurement_samples': sum(len(row['resource_samples']) for row in phases if row['stage'] == 'measurement'),
               'request_traces': sum(len(row['request_traces']) for row in phases),
               'verified_measurement_requests': sum(len(row['request_traces']) for row in phases if row['stage'] == 'measurement'),
               'selected_periods_seconds': plan['selected_periods'], 'medians': medians,
               'numerical_evidence_sha256': digest_file(destination / 'numerical-evidence.json'),
               'publisher_source_sha256': digest_file(__file__), 'cleanup': report['cleanup'],
               'performance_verified': True, 'whole_application_performance_verified': False,
               'full_milestone_complete': False, 'credit': numerical['credit']}
    save(destination / 'summary.json', summary)
    return summary


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, required=True)
    parser.add_argument('--build-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    result = publish(args.evidence, args.build_manifest, args.output)
    print(json.dumps({key: result[key] for key in ['passed', 'pairs', 'resource_samples', 'request_traces']}))
