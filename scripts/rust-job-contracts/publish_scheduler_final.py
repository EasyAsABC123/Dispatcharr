#!/usr/bin/env python3
"""Publish exact known-scheduler/native-consumer and physical proof bindings.
Original Dispatcharr/Celery/django-celery-beat authors retain credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read(root, name, count):
    integration = json.loads((root / 'integration.json').read_text())
    observed = json.loads((root / name).read_text())
    assert integration['passed'] and observed['passed']
    assert integration['default_database_unchanged']
    assert integration['cleanup'] == 'project removed'
    assert len(observed['checks']) == count
    assert all(item['passed'] for item in observed['checks'])
    return integration, observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--functional', required=True, type=Path)
    parser.add_argument('--physical', required=True, type=Path)
    parser.add_argument('--crash', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    fn = 'source-native-scheduler-consumers-private.json'
    pn = 'source-native-scheduler-database-private.json'
    functional, flow = read(args.functional, fn, 74)
    physical, resources = read(args.physical, pn, 47)
    cn = 'source-native-scheduler-crash-private.json'
    crash, windows = read(args.crash, cn, 61)
    assert crash['rust_source_sha256'] == functional['rust_source_sha256']
    lib = args.functional / 'frozen/rust/crates/job-transport/src/lib.rs'
    assert 'pub mod database_schedule;' in lib.read_text()
    assert functional['rust_source_sha256'] == physical['rust_source_sha256']
    assert functional['rust_source_archive_sha256'] == physical['rust_source_archive_sha256']
    assert resources['physical_resource_tests'] == 4
    assert resources['physical_retained_tick_tests'] == 1
    assert len(flow['cases']) == 2
    assert all(item['count_after_close'] == 3 and len(item['results']) == 3 for item in flow['cases'])
    result = {
        'passed': True,
        'scope': flow['scope'],
        'functional_checks': 74,
        'functional_original_checks': 50,
        'functional_native_checks': 24,
        'physical_group_checks': 47,
        'physical_tests': 4,
        'crash_checks': 61,
        'rust_source_archive_sha256': functional['rust_source_archive_sha256'],
        'same_exact_runtime_and_tests_between_final_proofs': True,
        'source_backend_task_key_pattern': flow['source_backend_task_key_pattern'],
        'bindings': {},
        'functional_checks_detail': flow['checks'],
        'physical_checks_detail': resources['checks'],
        'cases': [{'name': v['name'], 'actual_elapsed_seconds': v['elapsed_seconds'],
                   'persisted_count_after_close': v['count_after_close'],
                   'actual_results': len(v['results'])} for v in flow['cases']],
        'public_known_constructor_registered': True,
        'main_or_default_scheduler_enabled': False,
        'general_calendar_heap_verified': False,
        'performance_verified': False,
        'default_database_unchanged': True,
        'cleanup': 'both projects removed',
        'limitations': [
            'Zero/one enabled known interval only: inactive refresh or no-rules Series, default routing/options. Unknown tasks/options and multi-entry heaps fail explicitly.',
            'No scheduler main/startup flag, arbitrary default-family authority, configured Series recording rules, calendar/timezone/DST acceptance or whole-M6 claim.',
            'The four failed native-consumer observer attempts scanned an unprefixed result namespace; these are retained fixture failures, not evidence of native execution failure. Corrected observer derives its key pattern from the actual Original backend API.',
            'Temporary production execution diagnostic was removed before this exact final replay. The unexecuted async-console experiment is excluded.',
            'SQL reply count/byte limits do not claim a total RSS bound. Repeated check groups are not summed as unique cases.',
        ],
        'credit': 'Original Dispatcharr authors retain application/algorithm credit and AGPL licensing; Celery/django-celery-beat authors retain framework credit/licenses.',
    }
    for label, root, report, name in [('functional', args.functional, functional, fn), ('physical', args.physical, physical, pn), ('crash', args.crash, crash, cn)]:
        result['bindings'][label] = {
            'path': str(root), 'integration_sha256': sha(root / 'integration.json'),
            'private_report_sha256': sha(root / name), 'binary_sha256': report['binary_sha256'],
            'original_revision': report['original_revision'],
            'original_archive_sha256': report['original_archive_sha256'],
            'tool_source_sha256': report['tool_source_sha256'],
        }
    result['physical_log_sha256'] = sha(args.physical / 'native-scheduler-physical.log')
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'functional_checks': 74, 'physical_tests': 4}))


if __name__ == '__main__':
    main()
