"""Independently verify published functional churn; no Docker/private inputs."""
import argparse
import hashlib
import json
from pathlib import Path


def require(value, message):
    if not value:
        raise ValueError(message)


def verify(data):
    report, protocol = data['report'], data['protocol']
    require(report['passed'] and report['performance_verified'] is False, 'functional scope')
    require(report['candidate_image'] == 'sha256:3368457d42d2cab599001d8af562ff66133be4ae56f4ab7aee62d5ee7a66b467', 'image47 binding')
    require(report['source_revision'] == '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430', 'Original binding')
    require(hashlib.sha256((json.dumps(protocol, indent=2) + '\n').encode()).hexdigest() == report['protocol_sha256'], 'protocol binding')
    require(protocol['churn_schedule'] == [1, 10, 1, 10, 1, 10], 'fixed rounds')
    require(protocol['restarts_between_rounds'] is False and protocol['Redis_resets_between_rounds'] is False, 'unchanged runtime policy')
    require(report['fixture_provider_max_streams'] == 1 and report['cleanup'] == 'owned project removed', 'provider/cleanup')
    require(set(report['backend_starts']) == {'source', 'candidate'} and len(report['phases']) == 12, 'complete backends')
    require(len({row['id'] for row in report['backend_starts'].values()}) == 2, 'distinct backend containers')
    readers = 0
    for backend in ['source', 'candidate']:
        phases = [phase for phase in report['phases'] if phase['backend'] == backend]
        require([p['count'] for p in phases] == protocol['churn_schedule'] and [p['trial'] for p in phases] == list(range(1, 7)), 'complete round order')
        for phase in phases:
            count = phase['count']
            require(phase['passed'] and phase['runtime'] == report['backend_starts'][backend], 'same runtime')
            expected_image = ('sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
                              if backend == 'source' else report['candidate_image'])
            require(phase['source_bound_image'] == expected_image, 'phase image')
            require(phase['warmup_seconds'] == phase['measure_seconds'] == 5 and phase['resources'] is None, 'functional window')
            require(phase['authenticated_identity'] == {'clients': count, 'all_owned_user_id': True}, 'identity')
            require(phase['cleanup_state'] == {'owner_absent': True, 'provider_slots': 0, 'clients': 0, 'metadata_fields': 0} and phase['cleanup_upstream_active'] == 0, 'round cleanup')
            require(phase['continuous_connections'] and phase['requests'] == count and len(phase['viewers']) == count, 'actual readers')
            for row in phase['viewers']:
                require(row['status'] == 200 and row['requests'] == 1 and not row['errors'] and not row['unexpected_eof'] and row['post_window_barrier'], 'reader outcome')
                require(row['prefix_bytes'] == 2097152 and row['measured_bytes'] > 0 and row['decoder_exit_code'] == 0 and row['decoder_frames'] >= 25, 'decoded work')
                readers += 1
    review = data['parent_review']
    require(review['passed'] and review['independent_owned_cleanup_checked'] and review['initial_primers'] == 2, 'parent audit')
    require(all(not any(resources.values()) for resources in review['remaining_owned_resources'].values()), 'owned projects')
    require(readers == review['readers'] == 66 and review['rounds'] == 12, 'summary')
    return {'passed': True, 'rounds': 12, 'readers': readers, 'performance_verified': False}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--projection', type=Path, required=True)
    print(json.dumps(verify(json.loads(parser.parse_args().projection.read_text()))))
