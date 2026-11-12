"""Recompute retained 95-check component evidence only; no app or compiler."""
import hashlib
import json
from pathlib import Path
import re
HERE = Path(__file__).resolve().parent
AUTHORITY_SHA256 = '58a7e19ceb7486f428311213fec5109b0e9f00e203865a9965e36b4e22862618'
BATCHES = [('http','',15),('retained_reserve','retained_',6),('retained_reserve','ordinary_two_second_',1),('lease_physical','',13),('idle_entry','pending_follower_',4),('idle_entry','idle_entry_',5),('generation_scope','local_scope_',20),('generation_scope_physical','local_physical_',9),('pending_follower_http','pending_http_',9),('http_recovery','actual_http_',13)]


def validate(root=HERE):
    root = Path(root)
    raw = (root / 'authority.json').read_bytes()
    if hashlib.sha256(raw).hexdigest() != AUTHORITY_SHA256: raise ValueError('RetainedAuthorityChanged')
    authority = json.loads(raw)
    for name, digest in authority['files'].items():
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest: raise ValueError('RetainedInputChanged:' + name)
    report = json.loads((root / 'regressions.json').read_text())
    review = json.loads((root / 'review.json').read_text())
    outside = json.loads((root / 'outside-peers.json').read_text())
    if report.get('passed') is not True or report.get('owned_containers_removed') is not True or len(report.get('tests', [])) != 10: raise ValueError('AllTenBatchesRequired')
    if review.get('controller_observed_exit_code') != 0 or review.get('default_workers_activated') is not False or review.get('future_Main_package_regate_pending') is not True: raise ValueError('ComponentScope')
    total = 0
    for row, expected in zip(report['tests'], BATCHES):
        target, selected, count = expected
        if (row['target'], row['filter'], row['passed_count'], row['returncode']) != (target, selected, count, 0): raise ValueError('ExactExecutedBatch')
        path = root / 'logs' / (target + '-' + selected + 'tests.log')
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != row['log_sha256'] or not re.search(rb'test result: ok\. ' + str(count).encode() + rb' passed; 0 failed;', raw): raise ValueError('ActualBatchLog')
        total += count
    wanted = {item[0] for item in BATCHES}
    if set(report['binary_sha256']) != wanted or outside['source_map_sha256'] != report['source_map_sha256'] or outside.get('passed') is not True or outside.get('copied_only_after_actual_compiler_exit0') is not True: raise ValueError('ExactPeerProvenance')
    for name, digest in report['binary_sha256'].items():
        if outside['binaries'][name]['sha256'] != digest: raise ValueError('HistoricalPeerDigest')
    cleanup = json.loads((root / 'independent-cleanup.json').read_text())
    if cleanup.get('passed') is not True or len(cleanup.get('containers', [])) != 2 or {r['name'] for r in cleanup['containers']} != set(report['owned_container_names']) or any(row.get('absent') is not True or row.get('stdout') != '' for row in cleanup['containers']): raise ValueError('BothActualOwnedContainersAbsent')
    if total != 95: raise ValueError('Actual95Required')
    return {'passed':True,'actual_batches':10,'actual_passed':total,'Source_Main_or_performance_acceptance':False}


if __name__ == '__main__': print(json.dumps(validate(), indent=2))
