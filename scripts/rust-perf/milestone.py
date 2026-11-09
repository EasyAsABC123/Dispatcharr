#!/usr/bin/env python3
"""Strict repeated-pair Docker HTTP resource gate, not full-milestone acceptance."""
import argparse
import hashlib
import json
import math
from pathlib import Path

import perf

TOP = {'format_version', 'scope', 'frozen', 'aggregation', 'baseline_revision',
       'candidate_revision', 'hardware_id', 'docker_version', 'minimum_repetitions',
       'baseline_services', 'candidate_services', 'scenarios'}
FIELDS = {'id', 'scenario', 'viewers', 'distinct_channels', 'fixture_id', 'warmup_seconds',
          'measurement_seconds', 'timeout_seconds', 'slow_delay_seconds', 'sample_interval_seconds',
          'maximum_errors', 'minimum_bytes_per_second', 'maximum_first_byte_p95_ms',
          'minimum_first_byte_samples', 'maximum_candidate_to_baseline_cpu_ratio',
          'maximum_candidate_to_baseline_memory_ratio',
          'minimum_candidate_to_baseline_throughput_ratio'}
WORKLOAD = {'scenario': 'scenario', 'viewers': 'viewers', 'distinct_channels': 'url_count',
            'fixture_id': 'fixture_id', 'warmup_seconds': 'warmup',
            'measurement_seconds': 'duration', 'timeout_seconds': 'timeout',
            'slow_delay_seconds': 'slow_delay', 'sample_interval_seconds': 'sample_interval'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def keys(value, expected, name):
    require(isinstance(value, dict) and set(value) == expected,
            name + ' has missing or unsupported fields')
    require(all(item is not None for item in value.values()), name + ' contains null fields')


def number(value, minimum, name, integer=False):
    require(isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value) and value >= minimum
            and (not integer or isinstance(value, int)), 'Invalid number: ' + name)


def validate(budget):
    keys(budget, TOP, 'Budget')
    require(budget['format_version'] == 1 and budget['frozen'] is True,
            'Budget must be format_version1 and frozen:true')
    require(budget['scope'] == 'docker-http-resource-harness'
            and budget['aggregation'] == 'every-pair', 'Unsupported scope or aggregation')
    number(budget['minimum_repetitions'], 3, 'minimum_repetitions', integer=True)
    for field in ('baseline_revision', 'candidate_revision', 'hardware_id', 'docker_version'):
        require(isinstance(budget[field], str) and bool(budget[field].strip()), 'Missing ' + field)
    for field in ('baseline_services', 'candidate_services'):
        values = budget[field]
        require(isinstance(values, list) and bool(values)
                and all(isinstance(value, str) and value for value in values)
                and len(set(values)) == len(values), 'Invalid ' + field)
    require(isinstance(budget['scenarios'], list) and bool(budget['scenarios']), 'No scenarios')
    seen = set()
    for scenario in budget['scenarios']:
        keys(scenario, FIELDS, 'Scenario')
        for field in ('id', 'fixture_id'):
            require(isinstance(scenario[field], str) and bool(scenario[field]), 'Missing ' + field)
        require(scenario['id'] not in seen, 'Duplicate scenario')
        seen.add(scenario['id'])
        require(scenario['scenario'] in ('same-channel', 'many-channel'), 'Invalid scenario type')
        for field in ('viewers', 'distinct_channels', 'minimum_first_byte_samples'):
            number(scenario[field], 1, field, integer=True)
        require(scenario['distinct_channels'] == (1 if scenario['scenario'] == 'same-channel'
                                                 else scenario['viewers']), 'Invalid channel count')
        for field in ('warmup_seconds', 'slow_delay_seconds'):
            number(scenario[field], 0, field)
        number(scenario['measurement_seconds'], 10, 'measurement_seconds')
        for field in ('timeout_seconds', 'sample_interval_seconds', 'minimum_bytes_per_second',
                      'maximum_first_byte_p95_ms', 'maximum_candidate_to_baseline_cpu_ratio',
                      'maximum_candidate_to_baseline_memory_ratio',
                      'minimum_candidate_to_baseline_throughput_ratio'):
            number(scenario[field], 1e-12, field)
        number(scenario['maximum_errors'], 0, 'maximum_errors', integer=True)
        require(scenario['maximum_errors'] == 0, 'Only zero-error gates are supported')
        require(scenario['minimum_candidate_to_baseline_throughput_ratio'] >= .95,
                'Throughput ratio must be at least .95')
        require(scenario['timeout_seconds'] <= scenario['measurement_seconds'], 'Timeout exceeds duration')


def digest(report):
    return hashlib.sha256(json.dumps(report, sort_keys=True, allow_nan=False).encode()).hexdigest()


def evaluate(budget, evidence):
    result = {'scope': 'docker-http-resource-harness', 'passed': False,
              'application_performance_verified': False, 'failures': [], 'pairs': []}
    try:
        validate(budget)
        keys(evidence, {'scenarios'}, 'Evidence')
        require(isinstance(evidence['scenarios'], list), 'Evidence scenarios must be a list')
        by_id = {}
        for scenario in evidence['scenarios']:
            keys(scenario, {'id', 'pairs'}, 'Scenario evidence')
            require(scenario['id'] not in by_id, 'Duplicate scenario evidence')
            by_id[scenario['id']] = scenario['pairs']
        require(set(by_id) == {row['id'] for row in budget['scenarios']},
                'Evidence does not match the complete frozen scenario matrix')
        seen = set()
        for scenario in budget['scenarios']:
            pairs = by_id[scenario['id']]
            require(isinstance(pairs, list) and len(pairs) >= budget['minimum_repetitions'],
                    'Insufficient paired trials: ' + scenario['id'])
            for index, pair in enumerate(pairs):
                keys(pair, {'baseline', 'candidate'}, 'Pair')
                for role, report in pair.items():
                    require(report['format_version'] == 1, 'Unsupported report format')
                    fingerprint = digest(report)
                    require(fingerprint not in seen, 'Repeated evidence is not an independent trial')
                    seen.add(fingerprint)
                    require(report['revision'] == budget[role + '_revision'], 'Revision mismatch')
                    require(set(report['services']) == set(budget[role + '_services']), 'Service footprint mismatch')
                    require(report['environment']['host'] == budget['hardware_id']
                            and report['environment']['docker'] == budget['docker_version'],
                            'Frozen environment mismatch')
                    for key, destination in WORKLOAD.items():
                        require(report['workload'][destination] == scenario[key],
                                'Frozen workload mismatch: ' + key)
                    summary = report['measurement']['summary']
                    rate = summary['bytes_per_second']
                    number(rate, scenario['minimum_bytes_per_second'], 'bytes_per_second')
                    samples = summary['first_byte_samples']
                    number(samples, scenario['minimum_first_byte_samples'], 'first_byte_samples', integer=True)
                    latency = summary['first_byte_ms_p95']
                    number(latency, 0, 'first_byte_ms_p95')
                    require(latency <= scenario['maximum_first_byte_p95_ms'], 'First-byte p95 exceeds budget')
                comparison = perf.compare(pair['baseline'], pair['candidate'], {
                    'cpu_percent_mean': scenario['maximum_candidate_to_baseline_cpu_ratio'],
                    'memory_bytes_peak': scenario['maximum_candidate_to_baseline_memory_ratio']})
                require(comparison['passed'], '; '.join(comparison['failures']))
                old = pair['baseline']['measurement']['summary']['bytes_per_second']
                new = pair['candidate']['measurement']['summary']['bytes_per_second']
                require(new / old >= scenario['minimum_candidate_to_baseline_throughput_ratio'],
                        'Throughput ratio below budget')
                result['pairs'].append({'scenario': scenario['id'], 'pair': index + 1,
                                        'baseline_sha256': digest(pair['baseline']),
                                        'candidate_sha256': digest(pair['candidate']),
                                        **comparison})
        result['passed'] = True
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        result['failures'].append(str(exc))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--budget', type=Path, required=True)
    parser.add_argument('--evidence', type=Path, required=True,
                        help='JSON scenarios:[{id,pairs:[{baseline:path,candidate:path}]}]')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        budget = json.loads(args.budget.read_text())
        evidence = json.loads(args.evidence.read_text())
        # Resolve artifact paths relative to the evidence manifest, not the CWD.
        for scenario in evidence['scenarios']:
            for pair in scenario['pairs']:
                for role in ('baseline', 'candidate'):
                    pair[role] = json.loads((args.evidence.parent / pair[role]).read_text())
        result = evaluate(budget, evidence)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        result = {'passed': False, 'application_performance_verified': False, 'failures': [str(exc)]}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    return 0 if result['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
