"""Verify portable sanitized Candidate87 rollback publication receipts."""
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
AUTHORITY = 'd24ea9b9e01f18a92d9dda39170f8ca81f44cdd6ff09c58d53bc47669d7fe4b4'
EXPECTED = [
    ['core_systemnotification', 'updated', ['updated_at'], 1],
    ['django_celery_beat_periodictask', 'updated', ['date_changed'], 2],
    ['django_celery_beat_periodictasks', 'updated', ['last_update'], 1],
]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read(path, expected):
    data = path.read_bytes()
    require(hashlib.sha256(data).hexdigest() == expected, f'Exact evidence changed: {path.name}')
    return json.loads(data)


def verify(root):
    authority = read(root/'authority.json', AUTHORITY)
    names = ('original-control.json', 'paired-rollback.json', 'sandbox-refusal.json')
    require(set(authority['public_projections']) == set(names), 'Exact evidence package')
    source, paired, refusal = (read(root/name, authority['public_projections'][name]) for name in names)
    require(source['source_control_result_sha256'] == paired['Original_control_result_sha256']
            == authority['source_control_approved_result_sha256'], 'Independently approved Original control')
    require(paired['paired_result_sha256'] == authority['paired_result_sha256']
            and paired['launcher_sha256'] == authority['paired_launcher_execution_sha256']
            and paired['actual_launcher_exit'] == 0 and paired['exact_cleanup'] is True
            and paired['owned_retired_CID_count'] == 5 and source['exact_cleanup'] is True,
            'Actual paired terminal and owned absence')
    require(refusal['execution_sha256'] == authority['first_refusal_launcher_execution_sha256']
            and refusal['actual_launcher_exit'] == 1 and refusal['launcher_failure'] is None
            and refusal['stdout_EOF'] is refusal['stderr_EOF'] is refusal['host_process_group_settled'] is True
            and refusal['remaining_host_group_members'] == 0 and refusal['result_or_compose_project_created'] is False,
            'Historical sandbox inventory failure retained separately')
    require(source['original_revision'] == authority['Original_revision']
            and source['historical_Candidate87_result_sha256'] == paired['historical_Candidate87_result_sha256']
            == authority['historical_Candidate87_result_sha256'], 'Original and historical result identity')
    require(source['historical_unchanged_after_original_restart'] is False
            and paired['historical_Candidate87_post_restart_fingerprint_equal'] is False
            and paired['actual_strict_Original_SQL_fingerprint_equal_after_restart'] is False
            and paired['actual_Original_SQL_unchanged_through_Native'] is True
            and paired['actual_Original_restart_delta_matches_Source_only_control'] is True,
            'Strict fingerprint boundary and independent Original-only comparison')
    shape = {
        'quiescent_to_ready': EXPECTED,
        'ready_to_authenticated_read': [],
        'quiescent_to_authenticated_read': EXPECTED,
    }
    require(paired['actual_Original_restart_change_shapes'] == source['sanitized_change_shapes'] == shape,
            'Four exact timestamp edits and no post-read changes')
    require(paired['full_M9_accepted'] is source['full_M9_passed'] is authority['full_M9_accepted'] is False
            and paired['performance_verified'] is source['performance_verified'] is authority['performance_verified'] is False
            and authority['production_rollout_performed'] is False, 'Scoped evidence boundary')
    return {'passed':True,'authority_sha256':AUTHORITY,'first_launcher_actual_exit':1,
            'paired_launcher_actual_exit':0,'cleaned_exact_paired_containers':5,
            'Original_SQL_unchanged_through_Native':True,'post_restart_changed_rows':4,
            'strict_SQL_fingerprint_equal_after_restart':False,'full_M9_accepted':False,'performance_verified':False}


if __name__=='__main__':
    print(json.dumps(verify(HERE),indent=2))
