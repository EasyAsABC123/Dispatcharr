#!/usr/bin/env python3
"""Publish bounded Source orphan observations; no credentials or raw package code.
Original Dispatcharr authors retain all application/algorithm credit and AGPL.
Django and django-celery-beat authors retain their framework credit/licenses.
"""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def load(path):
    if path.stat().st_size > 8 * 1024 * 1024:
        raise ValueError('Bounded fixture report required')
    return json.loads(path.read_text())

def project_case(case, worker=False):
    result = {key: case[key] for key in ('name', 'source_id', 'before', 'after', 'marker_attempts')}
    if worker:
        meta = case['metadata']
        if meta['status'] != 'SUCCESS' or meta['result'] != f"EPG source {case['source_id']} does not exist, task cleaned up":
            raise ValueError('Unexpected actual worker result')
        result.update(metadata=meta, result_ttl=case['result_ttl'], raw_metadata_sha256=case['raw_metadata_sha256'],
                      postrun_sql_count=len(case['postrun']['sql']))
    else:
        result.update(helper_return=case['helper_return'], sql=[{'verb': row['sql'].split()[0],
                      'sha256': hashlib.sha256(row['sql'].encode()).hexdigest()} for row in case['sql']])
    return result

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', required=True, type=Path)
    parser.add_argument('--collector', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    args.source = args.source.resolve()
    args.collector = args.collector.resolve()
    args.output = args.output.resolve()
    raw_path = args.source / 'source-epg-orphan-marker-private.json'
    raw = load(raw_path)
    integration = load(args.source / 'integration.json')
    collector = load(args.collector / 'proof.json')
    if not (raw['passed'] and integration['passed'] and integration['default_database_unchanged'] and
            integration['cleanup'] == 'project removed' and collector['passed']):
        raise ValueError('Successful cleaned Source evidence required')
    if len(raw['checks']) != 79 or len(raw['cases']) != 11 or len(raw['worker_cases']) != 9:
        raise ValueError('Unexpected Source scope')
    dataset = {'format_version': 1, 'scope': raw['scope'], 'versions': raw['versions'],
               'source_only': True, 'checks': raw['checks'], 'functions_sha256': raw['source_hashes'],
               'direct_cases': [project_case(c) for c in raw['cases']],
               'worker_cases': [project_case(c, True) for c in raw['worker_cases']]}
    args.output.mkdir(parents=True, exist_ok=True)
    data_path = args.output / 'source-epg-orphan-cleanup-1.json'
    data_path.write_text(json.dumps(dataset, indent=2) + '\n')
    tool_names = ['source_epg_orphan_marker.py', 'run_epg_orphan_marker.py', 'epg_orphan_worker_observer.py']
    tools = {}
    for name in tool_names:
        relative = 'scripts/rust-job-contracts/' + name
        path = args.source / 'tool-source' / relative
        if sha(path) != integration['tool_source_sha256'][relative]:
            raise ValueError('Captured successful tool hash mismatch')
        tools[relative] = {'path': str(path.relative_to(ROOT)), 'sha256': sha(path)}
    summary = {'passed': True, 'scope': 'Pinned Original helper SQL and real missing-source worker; no native helper acceptance',
               'original_revision': integration['original_revision'], 'original_image': collector['image'],
               'checks': 79, 'direct_cases': 11, 'worker_cases': 9, 'versions': raw['versions'],
               'dataset': str(data_path.relative_to(ROOT)), 'dataset_sha256': sha(data_path),
               'raw_report': str(raw_path.relative_to(ROOT)), 'raw_report_sha256': sha(raw_path),
               'integration_sha256': sha(args.source / 'integration.json'),
               'collector_proof': str((args.collector / 'proof.json').relative_to(ROOT)),
               'collector_proof_sha256': sha(args.collector / 'proof.json'),
               'collector_implementation_sha256': collector['implementation_sha256'], 'selected_tools': tools,
               'default_database_unchanged': True, 'cleanup': integration['cleanup'],
               'worker_sql_trace_observed': False, 'native_orphan_helper_verified': False, 'performance_verified': False,
               'limitations': ['Worker query observer is empty after Source resets its DB connection; direct helper is the SQL authority.',
                               'No concurrent insert/delete or native cancellation/transaction ambiguity proof.',
                               'Worker success string does not assert that helper cleanup succeeded.'],
               'credit': 'Original Dispatcharr authors retain full application and algorithm credit/AGPL; Django and django-celery-beat authors retain framework credit/licenses.'}
    path = args.output / 'verified-original-epg-orphan-cleanup-1.json'
    path.write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({'passed': True, 'summary': str(path), 'sha256': sha(path)}))

if __name__ == '__main__':
    main()
