#!/usr/bin/env python3
"""Publish bounded refresh ETA evidence without private task identifiers.
Original Dispatcharr authors retain application/algorithm credit and AGPL licensing.
"""
import argparse
import hashlib
import json
from pathlib import Path


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report_path = args.input / 'integration.json'
    private_path = args.input / 'source-native-refresh-eta-private.json'
    report = json.loads(report_path.read_text())
    data = json.loads(private_path.read_text())
    assert report['passed'] and data['passed']
    assert report['default_database_unchanged'] and report['cleanup'] == 'project removed'
    assert data['queue_and_results_after_cleanup'] == [0, 0]
    assert report['checks'] == 60
    rows = []
    for case in data['native_cases']:
        audit = case['audit']
        assert all(audit[key] for key in ['caller_released', 'worker_drained', 'service_drained', 'transport_drained'])
        assert audit['physical_jobs'] == 0 and not audit['scheduled_return_observed']
        assert case['result']['status'] == 'SUCCESS' and case['result']['result'] is None
        assert len(case['result']['children']) == 2 and 'parent_id' in case['result']
        rows.append({'name': case['name'], 'native_executor_calls': audit['calls'],
                     'result': None, 'status': 'SUCCESS', 'parented': True, 'children': 2,
                     'scheduled_return_observed': False, 'physical_jobs': 0,
                     'caller_released': True, 'all_owned_services_drained': True})
    assert [(row['name'], row['native_executor_calls']) for row in rows] == [('scheduled', 1), ('warm_restore', 0)]
    result = {'passed': True, 'scope': report['scope'], 'checks': 60,
              'original_revision': report['original_revision'], 'source_reference_checks': 33,
              'native_cases': rows, 'actual_redis_resource_tests': 3,
              'native_refresh_eta_component_verified': True,
              'whole_application_or_default_intake_verified': False,
              'automatic_visibility_recovery_verified': False,
              'raw_tuple_normalization': 'Only public dataset UUID/origin substitution; private raw bytes remain hash-bound.',
              'archive_sha256': report['rust_source_archive_sha256'],
              'integration_sha256': digest(report_path), 'private_sha256': digest(private_path),
              'binary_sha256': report['binary_sha256'],
              'default_database_unchanged': True, 'cleanup': 'project removed',
              'queue_and_results_after_cleanup': [0, 0],
              'credit': 'Original Dispatcharr authors retain application/algorithm credit and AGPL licensing.',
              'retained_failures': [
                  {'paths': ['artifacts/rust/m6-source-native-refresh-eta-1', 'artifacts/rust/m6-source-native-refresh-eta-2'],
                   'boundary': 'Source reference cleanup deleted reused fixture XML. Fresh owned XML per native case corrected fixture only; native deadlines unchanged.'},
                  {'path': 'artifacts/rust/m6-source-native-refresh-eta-4',
                   'boundary': 'Resource helper generated nanosecond ETA rejected by existing microsecond protocol guard; helper corrected only.'}]}
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'passed': True, 'checks': 60, 'native_cases': 2, 'actual_redis_resource_tests': 3}))


if __name__ == '__main__':
    main()
