#!/usr/bin/env python3
"""Publish bounded multi-known-interval scheduler evidence and exact bindings.
Original Dispatcharr/Celery/django-celery-beat/Kombu authors retain credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['functional', 'workers', 'single', 'physical', 'output']:
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    definitions = [
        ('functional', args.functional, 'source-native-scheduler-multi-private.json', 184),
        ('workers', args.workers, 'source-native-scheduler-multi-worker-private.json', 58),
        ('single', args.single, 'source-native-scheduler-tick-private.json', 108),
        ('physical', args.physical, 'source-native-scheduler-database-private.json', 47),
    ]
    result = {'passed': True,
              'scope': 'Bounded known multi-interval native scheduler/owned timer, SQL persistence and native inactive Refresh/no-rules Series consumers; unknown/default task families/calendar/main remain unsupported/off',
              'bindings': {}, 'checks': {}, 'same_exact_source_and_tests_between_proofs': True,
              'physical_tests': 5, 'performance_verified': False,
              'main_or_default_scheduler_enabled': False,
              'default_database_unchanged': True, 'cleanup': 'all four projects removed',
              'credit': 'Original Dispatcharr authors retain application/algorithm credit and AGPL licensing; Celery/django-celery-beat/Kombu authors retain framework credit and licenses.',
              'limitations': [
                  'Known interval tasks only, default routing/options; calendar, unported default families and configured Series recording rules remain unsupported.',
                  'Ordinary real-clock frame/delivery order is compared exactly; true equal time/priority ties use each runtime own object-identity witness, as predeclared before native comparison. No stable cross-process label order is claimed for arbitrary identity ties.',
                  'Multi-dirty SQL-server-error partial-commit ordering remains open; successful multi-dirty persistence and single-entry legacy/server boundaries are separately scoped.',
                  'Source unordered parent queryset order is preserved by correlated related-field lookups. Query-plan fixtures do not establish a portable ordering guarantee across arbitrary PostgreSQL plans/data.',
                  'No scheduler main/startup flag or packaged application scheduler acceptance, and no full-M6 claim.',
              ],
              'preserved_failures': [
                  'multi1-3 exact raw-load-order failure; both plans prove native hash-right joins reversed parent rows. Related PK lookups corrected without sorting outputs or changing acceptance.',
                  'single-regression1 rejected already-expired known rows before Source disable transition. Publication preflight corrected; identical108 reference then passed.',
              ]}
    source_map = None
    observed = {}
    for label, root, name, count in definitions:
        integration = json.loads((root / 'integration.json').read_text())
        report = json.loads((root / name).read_text())
        assert integration['passed'] and report['passed']
        assert integration['default_database_unchanged'] and integration['cleanup'] == 'project removed'
        assert len(report['checks']) == count and all(v['passed'] for v in report['checks'])
        if source_map is None:
            source_map = integration['rust_source_sha256']
            result['rust_source_archive_sha256'] = integration['rust_source_archive_sha256']
        assert source_map == integration['rust_source_sha256']
        assert result['rust_source_archive_sha256'] == integration['rust_source_archive_sha256']
        result['bindings'][label] = {
            'path': str(root), 'integration_sha256': sha(root / 'integration.json'),
            'private_report_sha256': sha(root / name), 'binary_sha256': integration['binary_sha256'],
            'original_revision': integration['original_revision'],
            'original_archive_sha256': integration['original_archive_sha256'],
            'tool_source_sha256': integration['tool_source_sha256'],
        }
        result['checks'][label] = {'count': count, 'detail': report['checks']}
        observed[label] = report
    assert observed['physical']['physical_resource_tests'] == 5
    assert observed['workers']['counts'] == [2, 2] and len(observed['workers']['results']) == 4
    result['functional_case_names'] = [v['name'] for v in observed['functional']['cases']]
    result['native_worker_results'] = 4
    result['native_worker_persisted_counts'] = [2, 2]
    result['framework_licenses'] = observed['functional']['framework_licenses']
    result['physical_log_sha256'] = sha(args.physical / 'native-scheduler-physical.log')
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'functional': 184, 'workers': 58, 'single': 108, 'physical_tests': 5}))


if __name__ == '__main__':
    main()
