#!/usr/bin/env python3
"""Publish bounded known-interval scheduler proofs without private task identifiers.
Original Dispatcharr, Celery and django-celery-beat authors retain credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load(root, report_name, checks):
    integration = json.loads((root / 'integration.json').read_text())
    report = json.loads((root / report_name).read_text())
    assert integration['passed'] and report['passed']
    assert integration['default_database_unchanged']
    assert integration['cleanup'] == 'project removed'
    assert len(report['checks']) == checks
    assert all(item['passed'] for item in report['checks'])
    return integration, report


def binding(root, integration, report_name):
    return {
        'path': str(root),
        'integration_sha256': sha(root / 'integration.json'),
        'private_report_sha256': sha(root / report_name),
        'runtime_archive_sha256': integration['rust_source_archive_sha256'],
        'binary_sha256': integration['binary_sha256'],
        'original_revision': integration['original_revision'],
        'original_archive_sha256': integration['original_archive_sha256'],
        'tool_source_sha256': integration['tool_source_sha256'],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--physical', type=Path, required=True)
    parser.add_argument('--tick', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    physical_name = 'source-native-scheduler-database-private.json'
    tick_name = 'source-native-scheduler-tick-private.json'
    physical, resources = load(args.physical, physical_name, 47)
    tick, observed = load(args.tick, tick_name, 108)
    assert physical['rust_source_sha256'] == tick['rust_source_sha256']
    assert resources['physical_resource_tests'] == 3
    assert resources['physical_retained_tick_tests'] == 1
    result = {
        'passed': True,
        'scope': observed['scope'],
        'physical_checks': 47,
        'physical_tests': 3,
        'tick_checks': 108,
        'tick_source_checks': 49,
        'tick_native_checks': 59,
        'bindings': {
            'physical': binding(args.physical, physical, physical_name),
            'tick': binding(args.tick, tick, tick_name),
        },
        'physical_checks_detail': resources['checks'],
        'tick_checks_detail': observed['checks'],
        'physical_log_sha256': sha(args.physical / 'native-scheduler-physical.log'),
        'cases': [item['name'] for item in observed['cases']],
        'actual_original_worker_results': len(observed['results']),
        'default_database_unchanged': True,
        'cleanup': 'both projects removed',
        'native_timer_or_main_enabled': False,
        'general_calendar_or_heap_parity': False,
        'performance_verified': False,
        'limitations': [
            'Only zero/one enabled known interval entry: refresh or no-rules Series, default routing/options; unsupported families/options fail explicitly.',
            'No production scheduler registration, automatic timer/main, general heap/tie ordering, multiple dirty entries, calendar/DST or process-crash acceptance.',
            'Actual Original workers consumed native periodic frames; this does not establish native consumer compatibility with every schedule option.',
            'SQL response aggregate bounds are not total RSS bounds; SQLx, JSON and retained command overhead remain separate.',
            'Earlier proxy-teardown failures and earlier tick compiler failures remain preserved; historical proofs retain their original source identities.',
        ],
        'credit': 'Original Dispatcharr authors retain application/algorithm credit and AGPL licensing; Celery/django-celery-beat authors retain framework credit and licenses.',
    }
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'physical_tests': 3, 'tick_checks': 108}))


if __name__ == '__main__':
    main()
