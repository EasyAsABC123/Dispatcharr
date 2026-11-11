#!/usr/bin/env python3
"""Publish exact bounded calendar proof bindings without private task IDs/secrets.
Original Dispatcharr/Celery/django-celery-beat and tzdata authors retain credit.
"""
import argparse
import hashlib
import json
from pathlib import Path


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ['vectors', 'database', 'workers', 'single', 'multi', 'physical', 'output']:
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    definitions = [
        ('vectors', args.vectors, 'source-native-calendar-vectors-private.json', 123),
        ('database', args.database, 'source-native-scheduler-calendar-private.json', 157),
        ('workers', args.workers, 'source-native-scheduler-calendar-worker-private.json', 54),
        ('single', args.single, 'source-native-scheduler-tick-private.json', 108),
        ('multi', args.multi, 'source-native-scheduler-multi-private.json', 184),
        ('physical', args.physical, 'source-native-scheduler-database-private.json', 47),
    ]
    result = {
        'passed': True,
        'scope': 'Bounded known numeric crontab scheduler on the multi-entry driver; SQL load/tick/sync/reload, controlled DST clocks, real timer publication and inactive Refresh/no-rules Series consumer interoperability',
        'credit': 'Original Dispatcharr authors retain AGPL application/algorithm credit; Celery/django-celery-beat/Kombu and tzdata contributors retain framework/data credit and licenses.',
        'same_exact_source_and_tests_between_proofs': True,
        'bindings': {}, 'checks': {}, 'physical_tests': 6,
        'main_or_default_scheduler_enabled': False,
        'performance_verified': False,
        'default_database_unchanged': True,
        'cleanup': 'all six projects removed',
        'limitations': [
            'Known Refresh/Series tasks, numeric crontab grammar, UTC server timezone and five exact Source system tzdb2026e zones only; calendar input years1970–2037. No host tzdb or POSIX-tail fallback.',
            'Other zones, aliases/unsupported expressions, unknown/default task families, configured Series rules and unproved publication options remain explicit unsupported paths.',
            'Future expiry protocol headers remain unsupported; expired-before-publication disable is proved. No REVOKED/worker-expiry parity is claimed.',
            'Legacy eleven fixed-offset clock vectors remain immutable and separately labeled from real ZoneInfo clocks. Fold recalculation86400 versus90000 is preserved.',
            'DST process references inject only explicit clock dependencies; this is not elapsed-time DST or an OS-clock-change claim.',
            'Multi-dirty SQL-server-error partial-commit order and delete/recreate-name identity remain open. Ordinary multi-dirty persistence is independently replayed.',
            'No scheduler main/default intake or packaged application acceptance, and no whole-M6 claim.',
        ],
        'preserved_failures': [
            'Original calendar1 Beat app-registry import ordering failed; CLI app module corrected without changing the scheduler.',
            'Original calendar2 incorrectly waited for a gap task before its observed1800-second delay; raw timeout retained.',
            'Original calendar3 readiness hook observed the preliminary lazy scheduler before schedule initialization; guarded only the readiness observer.',
            'Native calendar DB1 applied interval-style start rounding. Actual crontab due_start_time aligns the start to its next cron occurrence; exact Source-aligned raw next-delay gates then passed.',
        ],
    }
    source_map = None
    observed = {}
    for label, root, filename, count in definitions:
        integration = json.loads((root / 'integration.json').read_text())
        private = json.loads((root / filename).read_text())
        assert integration['passed'] and private['passed']
        assert integration['default_database_unchanged'] and integration['cleanup'] == 'project removed'
        assert len(private['checks']) == count and all(v['passed'] for v in private['checks'])
        if source_map is None:
            source_map = integration['rust_source_sha256']
            result['rust_source_archive_sha256'] = integration['rust_source_archive_sha256']
        assert source_map == integration['rust_source_sha256']
        assert result['rust_source_archive_sha256'] == integration['rust_source_archive_sha256']
        result['bindings'][label] = {
            'path': str(root), 'integration_sha256': sha(root / 'integration.json'),
            'private_report_sha256': sha(root / filename),
            'binary_sha256': integration['binary_sha256'],
            'original_revision': integration['original_revision'],
            'original_archive_sha256': integration['original_archive_sha256'],
            'tool_source_sha256': integration['tool_source_sha256'],
        }
        result['checks'][label] = {'count': count, 'detail': private['checks']}
        observed[label] = private
    assert observed['physical']['physical_resource_tests'] == 6
    assert observed['workers']['counts'] == [1, 1]
    results = observed['workers']['results']
    assert len(results) == 2 and all(v['status'] == 'SUCCESS' for v in results)
    assert sum(v['result'] is None for v in results) == 1
    assert sum(v['result'] == {'scheduled': 0, 'details': []} for v in results) == 1
    result['native_worker_persisted_counts'] = [1, 1]
    result['native_worker_results'] = [
        {'status': v['status'], 'result': v['result'], 'children': v['children'],
         'traceback_is_null': v['traceback'] is None} for v in results
    ]
    result['calendar_case_names'] = [v['name'] for v in observed['database']['cases']]
    result['tzdb'] = observed['vectors']['tzdb']
    result['vectors'] = observed['vectors']['vectors']
    result['physical_log_sha256'] = sha(args.physical / 'native-scheduler-physical.log')
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'counts': {k: v['count'] for k, v in result['checks'].items()}, 'physical_tests': 6}))


if __name__ == '__main__':
    main()
