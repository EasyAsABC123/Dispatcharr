#!/usr/bin/env python3
"""Publish exact scoped default-task Source/native stats component evidence.
Original Dispatcharr/Celery authors retain application/algorithm credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(root, private_name, checks):
    integration = json.loads((root / 'integration.json').read_text())
    private = json.loads((root / private_name).read_text())
    assert integration['passed'] and private['passed']
    assert integration['cleanup'] == 'project removed' and integration['default_database_unchanged']
    assert len(private['checks']) == checks and all(v['passed'] for v in private['checks'])
    return integration, private


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['source', 'native', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    source, reference = load(args.source, 'source-default-effects-worker-private.json', 93)
    native, actual = load(args.native, 'source-native-default-stats-worker-private.json', 102)
    assert actual['physical_tests'] == {'native-stats-worker': 3, 'native-stats-clients': 3}
    assert len(reference['cases']) == 5 and len(actual['cases']) == 6
    result = {
        'passed': True,
        'scope': 'Six Original default-family baseline effects and fixture-only native fetch-stats publication/worker, shared scanner/ChannelsRedis bus, Original AsyncResult and six physical controls',
        'credit': 'Original Dispatcharr authors retain application/algorithm credit and AGPL licensing; Celery/Kombu/ChannelsRedis contributors retain framework credit and licenses.',
        'native_fetch_stats_fixture_verified': True,
        'native_default_families_verified': False,
        'main_or_default_consumer_enabled': False,
        'production_constructor_verified': False,
        'performance_verified': False,
        'default_database_unchanged': True,
        'cleanup': 'both projects removed',
        'source_checks': reference['checks'], 'native_checks': actual['checks'],
        'source_baseline_outcomes': [{'name': row['name'], 'task': row['task'],
            'status': row['metadata']['status'], 'result': row['metadata']['result'],
            'events': row['events']} for row in reference['cases']],
        'native_cases': [{'name': row['name'], 'status': row['metadata']['status'],
            'result': row['metadata']['result'], 'events': row['events'],
            'metadata_fields': list(row['metadata']), 'caller_released': row['audit']['owner_released']}
            for row in actual['cases']],
        'physical_tests': actual['physical_tests'],
        'bindings': {},
        'limitations': [
            'Fixture authority only: no production constructor, default consumer, scheduler/API/Main enablement or full-M6 claim.',
            'Other five families have narrowly characterized Original baseline branches only; native task execution remains unported.',
            'Version task uses explicit HTTP dependency redirection to an owned loopback503; no actual external upstream request/update availability claim.',
            'Stats uses six known observed metadata/event cases. Dynamic clock/numeric spellings, other malformed metadata and additional faults remain separate gates.',
            'Native build/admission/representation failures remain operational, not fabricated SUCCESS. Group-send Unavailable is handled only after its actual retained owner settles.',
            'The private helper process is a fixture bridge to existing native implementations, not production app architecture or a second scanner implementation.',
            'The old stats periodic task remains disabled as pinned Source config requires. Viewing uses direct producers; it does not enqueue this legacy task.',
        ],
        'preserved_attempts': [
            'Original stats1 weaker64-check capture used singular client metadata key, actually testing a second ghost. Source2 fixes the exact plural key and requires a live-client witness.',
            'Initial private Clippy invocation named nonexistent package dispatcharr and failed before compilation; corrected package is dispatcharr-rs. Both logs retained.',
        ],
    }
    for label, root, integration, name in [
        ('source', args.source, source, 'source-default-effects-worker-private.json'),
        ('native', args.native, native, 'source-native-default-stats-worker-private.json'),
    ]:
        result['bindings'][label] = {'path': str(root),
            'integration_sha256': sha(root / 'integration.json'),
            'private_report_sha256': sha(root / name),
            'original_revision': integration['original_revision'],
            'original_archive_sha256': integration['original_archive_sha256'],
            'tool_source_sha256': integration['tool_source_sha256']}
    result['bindings']['native'].update(rust_source_archive_sha256=native['rust_source_archive_sha256'],
        rust_source_sha256=native['rust_source_sha256'],
        binary_sha256=native['binary_sha256'], native_dependency_sha256=native['native_dependency_sha256'],
        client_binary_sha256=native['client_binary_sha256'],
        physical_log_sha256={name: sha(args.native / name) for name in ['native-stats-worker-physical.log', 'native-stats-clients-physical.log']})
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'source_checks': 93, 'native_checks': 102, 'physical_tests': 6}))


if __name__ == '__main__':
    main()
