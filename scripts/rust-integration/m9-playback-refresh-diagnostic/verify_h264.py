"""Reconcile the exact Candidate94 H.264/AAC and two-programme Compose trial."""
import argparse
from fractions import Fraction
import hashlib
import json
from pathlib import Path
import re

REVISION = 'fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f'
SOURCE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'


def load(path):
    return json.loads(path.read_bytes())


def run(directory, manifest):
    paths = {name: directory / leaf for name, leaf in (
        ('execution','execution.json'), ('cleanup','independent-cleanup.json'), ('receipt','receipt.json'),
        ('integration','evidence/integration.json'), ('readers','evidence/soak120-h264/readers.json'),
        ('decoders','evidence/soak120-h264/decoders.json'))}
    execution, cleanup, receipt, integration, readers, decoders = (load(path) for path in paths.values())
    manifest_hash = hashlib.sha256(manifest.read_bytes()).hexdigest()
    built = load(manifest)
    assert receipt['candidate_revision'] == built['git_revision'] == REVISION
    assert receipt['source_revision'] == SOURCE
    assert built['image_id'] == receipt['image_id'] == integration['candidate_image_binding']['image_id']
    assert integration['candidate_image_binding']['manifest_verified']
    assert built['passed'] and built['exit_code']==0 and integration['build_manifest_sha256']==manifest_hash
    assert receipt['passed'] and receipt['tracked_runner_actual_exit']==execution['actual_exit']==0
    assert execution['stdout_EOF'] and execution['stderr_EOF'] and execution['process_group_settled']
    assert not execution['remaining_group_members'] and execution['failure'] is None
    assert integration['passed'] and integration['cleanup']=='project removed' and cleanup['passed']
    assert cleanup['project'] == integration['project'] == receipt['project'] and not any(cleanup['inventories'].values())
    groups = cleanup['owned_container_generations']
    assert len(groups)==4 and all(g['container_absent'] and g['host_generation_absent'] for g in groups)
    checks = {row['name']: row for row in integration['checks']}
    assert len(checks)==24 and all(row['passed'] for row in checks.values())
    operator = checks['soak120-h264:shared-live-and-two-programmes']['details']
    assert operator == {'seconds':120,'simultaneous_viewers':10,'exact_new_upstream_connections':1,
                        'encoder_generation_retired':True,'observer_samples':24,'decoded_clips':20,
                        'closed_viewer_transports':10}
    rows = readers['samples']
    assert [row['offset_seconds'] for row in rows]==list(range(5,121,5))
    assert all(row['live_readers']==10 and row['one_origin_generation'] for row in rows)
    assert all(left['origin_packets']<right['origin_packets'] for left,right in zip(rows,rows[1:]))
    terminals = readers['reader_terminals']
    assert len(terminals)==10 and all(
        row['status']==200 and row['read_owner_joined'] and row['body_and_transport_closed']
        and row['error_type'] is None and not row['unexpected_eof'] for row in terminals)
    jobs = []
    for ordinal in (1,2):
        details = checks[f'soak120:xmltv-refresh-{ordinal}']['details']
        assert details['status']==200 and details['ordinal']==ordinal-1
        assert re.fullmatch('[0-9a-f]{64}',details['task_sha256'])
        observed_hashes = set(details['result_task_sha256'])
        prior_hashes = {row['task_sha256'] for row in jobs}
        assert observed_hashes == prior_hashes | {details['task_sha256']}
        assert details['cache_generation_delta']==2 and details['result_ttl_seconds']>0
        assert all(details[key] for key in (
            'within_schedule_and_terminal_budget','fenced_single_typed_publication',
            'cache_increment_precedes_publication','exact_readback_and_result','normal_react_response'))
        jobs.append({'task_sha256':details['task_sha256'], 'http_status':200, 'actual_result_verified':True})
    assert jobs[0]['task_sha256']!=jobs[1]['task_sha256']
    assert checks['soak120:distinct-xmltv-refresh-terminals']['details']['same_active_playback_window']
    assert len(decoders)==20 and all(row['passed'] and row['actual_exit']==0 and row['stderr_bytes']==0 for row in decoders)
    minimum_seconds = {}
    for medium in ('video','audio'):
        samples=[row['interior'][medium] for row in decoders]
        exact=min(Fraction(entry['decoded_ticks'],entry['timebase_denominator']) for entry in samples)
        assert exact>=5
        minimum_seconds[medium]=float(exact)
    return {'scope':'owned 120-second H264/AAC playback with two distinct programme refreshes',
            'candidate_revision':REVISION,'original_revision':SOURCE,'candidate_image':built['image_id'],
            'package_manifest_sha256':manifest_hash,'actual_exit':0,'owned_containers_and_generations_removed':4,
            'complete_checks':24,'playback':operator,'media_minimum_complete_seconds_by_clip':minimum_seconds,
            'distinct_programme_results':jobs,'scheduled_offsets_seconds':[5,60],
            'full_M9_accepted':False,'two_hour_soak_accepted':False,'performance_measured':False,
            'raw_fingerprints':{name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in paths.items()}}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-dir',type=Path,required=True)
    parser.add_argument('--manifest',type=Path,required=True)
    args=parser.parse_args()
    if not __debug__:
        parser.error('Run with contract assertions enabled')
    print(json.dumps(run(args.run_dir,args.manifest),sort_keys=True,indent=2))


if __name__=='__main__':
    main()
