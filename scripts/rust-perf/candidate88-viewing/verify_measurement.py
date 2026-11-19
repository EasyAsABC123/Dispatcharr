"""Verify Candidate 88's finalized ten-viewer report without runtime work."""
import argparse
import hashlib
import json
import math
from pathlib import Path

REPORT = '471ca0b3259d1ef584df5d4bfc866680f7c036a05400c804c268e29778d6f369'
FROZEN = 'a40e0c826eacfbd1e6badedb093b99c021c713c0edbf54c2fac5c53d6fd6dc3d'
QUEUE = 'bb00d442d54c6a4682166b5af9c135d86cccaf4da8caf8d016c56d2ed7129d2e'
LAUNCHER_LOG = '9a6d6613c5e707443aca76bc07620d0ed2ff7aa72da29d8302909058b9c7a316'
IMAGE = 'sha256:3ee795ee96c2cb4c50b9851cd30e4057a7d6cddccb81fe137d0a115b4a58c034'
BUILD = 'e29a05ce22070067db03ed0e90a80f0ff64e14560d1eec394dd14dd77bf6903a'
ORDER = [(1, 'baseline'), (1, 'candidate'), (2, 'candidate'),
         (2, 'baseline'), (3, 'baseline'), (3, 'candidate')]
RAW = ('shared-ts.json', 'resource-samples.json', 'useful-media-complete.json')


def need(condition, label):
    if not condition:
        raise ValueError(label)


def sha(path):
    with path.open('rb') as data:
        return hashlib.file_digest(data, 'sha256').hexdigest()


def pinned(path, expected):
    need(sha(path) == expected, f'Byte custody failed for {path.name}')
    return path.read_bytes()


def phase(root, row):
    tag = f"{row['phase']}-10-{row['trial']}-{row['backend']}"
    directory = root/tag
    need(set(row['raw_sha256']) == set(RAW), f'{tag}: phase receipts')
    case_report, counters, media = (
        json.loads(pinned(directory/name, row['raw_sha256'][name])) for name in RAW
    )
    process = row['controller_process']
    need(row['actual_exit'] == process['actual_exit'] == 0 and process['failure'] is None
         and row['valid'] is True and process['remaining_group_members'] == []
         and all(process[key] is True for key in ('stdout_EOF', 'stderr_EOF', 'process_group_settled')),
         f'{tag}: controller terminal')
    expected_cleanup = {'containers', 'networks', 'volumes'} | {
        name+'-exact-CID' for name, group in counters['groups'].items() if 'CID' in group
    }
    checks = row['cleanup_checks']
    need(len(checks) == len(expected_cleanup) and {check['kind'] for check in checks} == expected_cleanup
         and all(check['actual_exit'] == 0 and check['stdout'].strip() == '' and check['passed'] is True
                 for check in checks), f'{tag}: cleanup custody')
    need(case_report['project'] == row['project'] and case_report['passed'] is True
         and case_report['candidate_build_manifest_sha256'] == BUILD, f'{tag}: application authority')
    [case] = case_report['cases']
    need(case['viewer_count'] == len(case['readers']) == len(case['decoders']) == 10
         and all(item['video_and_audio'] is True and item['frames'] > 0 for item in case['decoders'])
         and case['shared']['clients'] == 10 and case['shared']['provider_slots'] == 1
         and case['after']['clients'] == case['after']['provider_slots'] == 0, f'{tag}: shared viewing')

    samples = counters['samples']
    need(counters['backend'] == row['backend'] and counters['fixed_clock_passed'] is True
         and counters['live_state_passed'] is True and counters['actual_VLC_generation_stable'] is True
         and counters['warmup_seconds'] == 15 and row['measurement_seconds'] == counters['elapsed_seconds']
         and [s['index'] for s in samples] == list(range(61)), f'{tag}: sample window')
    need(counters['final_origin']['maximum_active'] == counters['final_origin']['connections'] == 1,
         f'{tag}: upstream sharing')
    need(media == row['useful_media_complete'] and media['passed'] is True
         and len(media['readers']) == 10 and media['fixed_drain_derivation_seconds'] == 5
         and all(viewer['passed'] is True for viewer in media['readers']), f'{tag}: useful media')

    def primary(sample, metric):
        values = sample['simultaneous_aggregate']
        return values['backend_and_infrastructure'][metric]+values['origin'][metric]

    cpu = (primary(samples[-1], 'cpu_usage_usec')-primary(samples[0], 'cpu_usage_usec'))/1e6/row['measurement_seconds']
    memory = max(primary(sample, 'working_set_estimate_bytes') for sample in samples)
    reported = row['metrics']['primary']
    need(cpu == reported['cpu_mean_cores'] and memory == reported['working_set_estimate_max_simultaneous_bytes'],
         f'{tag}: primary accounting')
    latencies = sorted(viewer['first_byte_seconds']*1000 for viewer in case['readers'])
    return {
        'CPU': cpu, 'memory': memory, 'first_byte_p95': max(50, latencies[math.ceil(.95*len(latencies))-1]),
        'throughput': sum(viewer['window_bytes'] for viewer in media['readers']),
        'encoded_useful_throughput': sum(viewer['window_encoded_useful_bytes'] for viewer in media['readers']),
    }


def verify(root, queue_path, log_path):
    result = json.loads(pinned(root/'result.json', REPORT))
    frozen = json.loads(pinned(root/'frozen-workload.json', FROZEN))
    queue = json.loads(pinned(queue_path, QUEUE))
    launcher = pinned(log_path, LAUNCHER_LOG)
    need(queue['state'] == 'settled' and queue['pilot-2_actual_exit'] == 0 and queue['viewing10-2_actual_exit'] == 1
         and launcher.rstrip().endswith(b"AttributeError: 'dict' object has no attribute 'qualification_only'"),
         'Historical launcher exit and finalization error')
    need(result['frozen_sha256'] == FROZEN and result['candidate_image_id'] == frozen['physical_image'] == IMAGE
         and frozen['levels'] == [10] and frozen['planned_phases'] == result['actual_new_phase_count'] == 6
         and frozen['backend_order'] == [backend for _, backend in ORDER]
         and all(result[name] == frozen[name] for name in ('phase_sha256', 'protocol_sha256', 'scope'))
         and result['retained_phase_count'] == 0 and result['changed_inputs'] == [],
         'Frozen workload identity')
    need(all(result[name] is False for name in ('protocol_qualification_only', 'preflight_only', 'gain_accepted',
                'live_only_gain_accepted', 'full_M235_resource_accepted', 'active_DVR_gain_accepted', 'full_migration_accepted'))
         and frozen['full_campaign_not_run'] is True, 'Narrow scope flags')
    phases = result['phases']
    need([(row['trial'], row['backend']) for row in phases] == ORDER
         and [row['phase'] for row in phases] == list(range(1, 7))
         and all(row['viewers'] == 10 for row in phases), 'Exact six-phase trial order')
    measured = {(row['trial'], row['backend']): phase(root, row) for row in phases}
    media_rows = [row['useful_media_complete'] for row in phases]
    threshold = min(row['one_batch_PTS_seconds'] for row in media_rows)
    for stream in ('0', '1'):
        spans = [reader['streams'][stream]['useful_duration_seconds'] for row in media_rows for reader in row['readers']]
        need(max(spans)-min(spans) <= threshold, f'Cross-phase decoded duration, stream {stream}')
    need(result['cross_phase_useful_duration_equivalence'] == [True, True] and result['all_phases_valid'] is True,
         'Phase qualification')
    need([pair['trial'] for pair in result['pairs']] == [1, 2, 3] and result['scoped_comparison_budgets_passed'] is True,
         'Scoped comparison verdict')
    for pair in result['pairs']:
        source, native = (measured[pair['trial'], backend] for backend in ('baseline', 'candidate'))
        ratios = {metric: native[metric]/source[metric] for metric in source}
        need(pair['viewers'] == 10 and pair['ratios'] == ratios, f"Trial {pair['trial']}: ratio reproduction")
        need(pair['passed'] is True and pair['failed'] == [] and ratios['CPU'] <= .9 and ratios['memory'] <= .95
             and ratios['first_byte_p95'] <= 1.1 and ratios['throughput'] >= .95
             and ratios['encoded_useful_throughput'] >= .95, f"Trial {pair['trial']}: comparison budgets")
    return {
        'scope': result['scope'], 'candidate_image_id': IMAGE, 'result_sha256': REPORT,
        'frozen_workload_sha256': FROZEN, 'historical_launcher_actual_exit': queue['viewing10-2_actual_exit'],
        'historical_launcher_finalization_error': "AttributeError: 'dict' object has no attribute 'qualification_only'",
        'historical_launcher_log_sha256': LAUNCHER_LOG, 'historical_queue_sha256': QUEUE,
        'actual_successful_phase_exits': [row['actual_exit'] for row in phases],
        'verified_raw_phase_receipts': 3*len(phases), 'pair_budgets_reproduced': True,
        'pairs': result['pairs'], 'gain_accepted': result['gain_accepted'],
        'full_M235_resource_accepted': result['full_M235_resource_accepted'],
        'active_DVR_gain_accepted': result['active_DVR_gain_accepted'],
        'full_migration_accepted': result['full_migration_accepted'],
        'credit': 'Original Dispatcharr authors and contributors retain full application credit.',
    }


if __name__ == '__main__':
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument('--run', required=True, type=Path)
    args.add_argument('--queue', required=True, type=Path)
    args.add_argument('--launcher-log', required=True, type=Path)
    opts = args.parse_args()
    print(json.dumps(verify(opts.run, opts.queue, opts.launcher_log), indent=2))
