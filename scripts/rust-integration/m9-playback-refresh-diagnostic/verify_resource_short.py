"""Reconcile actual playback, media, one-slot and resource proof for the short trial."""
import argparse
import hashlib
import json
from pathlib import Path


CANDIDATE = 'fc5562a37d91ed6673e6ee9a740be4e0dd6c7c5f'
IMAGE = 'sha256:b309daadc687ed0bb8f627373e4c34c202f995bb91ab782ae65ee2d32b712c74'
FROZEN = '28ab9bcee1a1cdd245f6f98728db92f23011a42e32d948e5df674cfd8e7ab184'
ROLES = {'baseline', 'candidate', 'origin', 'relay'}


def require(value, description):
    if not value:
        raise ValueError(description)


def load(path):
    return json.loads(path.read_text())


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def verify(root, actual_exit):
    paths = {name: root / filename for name, filename in {
        'binding': 'binding.json', 'execution': 'execution.json', 'cleanup': 'independent-cleanup.json',
        'receipt': 'receipt.json', 'compose': 'evidence/integration.json', 'roles': 'evidence/owned-role-custody.json',
        'counters': 'evidence/owned-role-counters.jsonl', 'resource_summary': 'evidence/owned-role-summary.json',
        'viewers': 'evidence/soak120-h264/readers.json',
    }.items()}
    binding, execution, cleanup, proof, compose = (load(paths[name]) for name in (
        'binding', 'execution', 'cleanup', 'receipt', 'compose'))
    require(int(actual_exit.read_text()) == execution['actual_exit'] == proof['tracked_runner_actual_exit'] == 0
            and all(execution[name] is True for name in ('process_group_settled', 'stdout_EOF', 'stderr_EOF'))
            and execution['failure'] is None and not execution['output_bytes_discarded'], 'actual successful, settled commands')
    require(binding['candidate_revision'] == proof['candidate_revision'] == CANDIDATE
            and binding['image_id'] == proof['image_id'] == IMAGE and binding['frozen_inputs_sha256'] == FROZEN,
            'exact qualified package and current short fixture')
    require(cleanup['passed'] and not any(cleanup['inventories'].values()) and not proof['full_M9_accepted']
            and not proof['two_hour_soak_verified'] and proof['resource_short_verified']
            and {row['service'] for row in cleanup['owned_container_generations']}
            == {'baseline', 'candidate', 'origin', 'soak-relay', 'data-init'}
            and all(row['container_absent'] is True and row['host_generation_absent'] is True
                    for row in cleanup['owned_container_generations']), 'qualified scope and exact retirement')
    checks = {row['name']: row['details'] for row in compose['checks']}
    require(compose['passed'] and len(checks) == 28 and all(row['passed'] for row in compose['checks'])
            and compose['project'] == cleanup['project'] == proof['project'], 'complete exact Compose checks')
    media = checks['soak120-h264:shared-live-and-two-programmes']
    require(media['seconds'] == 120 and media['simultaneous_viewers'] == media['closed_viewer_transports'] == 10
            and media['exact_new_upstream_connections'] == 1 and media['encoder_generation_retired']
            and media['observer_samples'] == 24 and media['decoded_clips'] == 20, 'long-origin bounded decode custody')
    for index in (1, 2):
        task = checks[f'soak120:xmltv-refresh-{index}']
        require(task['ordinal'] == index - 1 and task['status'] == 200 and task['exact_readback_and_result']
                and task['within_schedule_and_terminal_budget'], 'two distinct in-flight guide terminals')
    seed = checks['soak7200:viewer-profile-seeded']
    retirement = checks['soak7200:viewer-owned-redis-retired']
    require(seed['provider_max_streams'] == 1 and seed['idle_keys_clear'] and retirement['provider_slots'] == 0
            and retirement['retired'] and retirement['former_viewer_count'] == 10
            and retirement['channel_scoped_keys_remaining'] == retirement['remaining_grants'] == 0,
            'one-slot provider and exact Redis retirement')
    observations = load(paths['viewers'])
    require([row['offset_seconds'] for row in observations['samples']] == list(range(5, 121, 5))
            and all(row['live_readers'] == 10 and row['one_origin_generation'] for row in observations['samples'])
            and len(observations['reader_terminals']) == 10
            and all(row['status'] == 200 and row['read_owner_joined'] and row['body_and_transport_closed']
                    and row['error_type'] is None and len(row['clips']) == 2
                    for row in observations['reader_terminals']), 'continuous active window and settled media clients')
    custody = load(paths['roles'])
    summary = load(paths['resource_summary'])
    counters = [json.loads(line) for line in paths['counters'].read_text().splitlines()]
    require(set(custody['roles']) == ROLES and summary['idle']['passed'] and summary['active']['counter_ticks'] == 25
            and summary['active']['census_ticks'] == 3 and len(counters) == 47, 'exact role metrics and sample count')
    retries = []
    for row in counters:
        phase, offset, sample = (row[name] for name in ('phase', 'offset', 'sample'))
        if phase != 'active' or offset in (0, 60, 120):
            require(sample['census_complete'] and set(sample['roles']) == ROLES, 'every scheduled whole-role census completed')
        for role, status in sample['roles'].items():
            census = status.get('census', {})
            attempts = census.get('census_attempts', 0)
            if attempts > 1:
                previous = census['discarded_unstable_attempts']
                require(census['coverage_complete'] and len(previous) == attempts - 1, 'discarded unstable attempts remain literal')
                retries.append({'phase': phase, 'second': offset, 'role': role, 'attempts': attempts,
                                'unstable_attempts': previous})
    return {'candidate_revision': CANDIDATE, 'image_id': IMAGE, 'actual_exit': 0, 'exact_cleanup': True,
            'checks_passed': len(checks), 'simultaneous_viewers': 10, 'single_upstream': True,
            'decoded_media_clips': 20, 'viewing_seconds': 120, 'scope': 'short owned resource and provider qualification',
            'xmltv_refresh_terminals': 2, 'provider_slot_limit': 1, 'post_playback_slots': 0,
            'resource_counter_ticks': summary['active']['counter_ticks'], 'resource_active_censuses': 3,
            'idle_censuses_per_window': 11, 'observed_idle_working_set_growth_bytes': summary['idle']['working_memory_growth_bytes'],
            'all_idle_growth_gates_passed': True, 'recovered_disposable_process_census_races': retries,
            'full_milestone_passed': False, 'two_hour_soak_verified': False,
            'evidence_sha256': {name: sha(path) for name, path in paths.items()}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run', type=Path, required=True)
    parser.add_argument('--actual-exit', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args.run, args.actual_exit), sort_keys=True, indent=2))


if __name__ == '__main__':
    main()
