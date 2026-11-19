"""Project the six retained Original Comskip task observations using public files."""
import argparse
import ast
import base64
import hashlib
import json
from pathlib import Path

from framework_auth_contracts import ast_fingerprint


IDENTIFIER = 'candidate-79333cd4d7e842b2d97b'
KIND = 'retained_original_comskip_task_observations'
BINDING_PATH = 'scripts/rust-contracts/retained-comskip-task/bindings.json'
TASK = 'apps.channels.tasks.comskip_process_recording'
SOURCE = {'kind': 'task_candidate', 'path': 'apps/channels/tasks.py', 'line': 3374,
          'name': 'comskip_process_recording', 'callable': 'celery.shared_task'}
ORIGINAL_REVISION = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
CASES = (
    ('positive-mark', 1, 'ok', 2), ('mark-repeat', 1, 'already_processed', 0),
    ('positive-cut', 2, 'ok', 2), ('cut-repeat', 3, 'already_processed', 0),
    ('natural-automatic-mark', 5, 'ok', 2), ('natural-automatic-cut', 6, 'ok', 2),
)
RUNS = {1: 1, 2: 1, 3: 1, 5: 1, 6: 0}
SOURCE_COPY = 'scripts/rust-contracts/scanner-tasks/original/apps/channels/tasks.py'
LIFECYCLE = 'scripts/rust-integration/comskip-positive-lifecycle-source1'
FENCES = 'scripts/rust-integration/comskip-positive-journal-fences1'


def load(path):
    return json.loads(path.read_bytes())


def sha(data):
    return hashlib.sha256(data).hexdigest()


def project(root):
    source = (root/SOURCE_COPY).read_bytes()
    source_digest = '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279'
    assert sha(source) == source_digest
    [function] = [node for node in ast.parse(source).body
                  if isinstance(node, ast.FunctionDef) and node.name == SOURCE['name']]
    assert [(decorator.lineno, ast.unparse(decorator)) for decorator in function.decorator_list] == [(3374, 'shared_task')]
    assert [parameter.arg for parameter in function.args.args] == ['recording_id']

    positive = root/LIFECYCLE
    index_path = positive/'index.json'
    index_sha = '325f5f56fe112b850ce203905e9cff9a408cc0ebd54eb37aba4190bfd46f1772'
    assert sha(index_path.read_bytes()) == index_sha
    index = load(index_path)
    tracked = {item['path']: item for item in index['files']}

    def checked(relative):
        data = (positive/relative).read_bytes()
        assert len(data) == tracked[relative]['bytes'] and sha(data) == tracked[relative]['sha256'], relative
        return json.loads(data)

    fence_path = root/FENCES/'index.json'
    fence_sha = 'cb25f31228fa90c2553a50ce63781b7a4661d96b1246428989b03c772fed4b2b'
    assert sha(fence_path.read_bytes()) == fence_sha
    fence = load(fence_path)
    assert fence['original_Source_index'] == {'path': LIFECYCLE+'/index.json', 'sha256': index_sha}
    assert fence['Source_six_business_projections_unchanged'] and fence['clock_slack_added'] == 0
    assert fence['Source_business_replayed'] is False
    for item in fence['files']:
        data = (root/FENCES/item['path']).read_bytes()
        assert len(data) == item['bytes'] and sha(data) == item['sha256']

    identity = checked('identity.json')
    assert identity['Original_revision'] == ORIGINAL_REVISION
    assert identity['actual_Source_image'] == 'sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
    assert index['Source_actual_business_leaves'] == 6
    assert index['manual_global_producer_graph_qualified'] is False
    assert index['Source_MARK_artwork_backend_observed'] is False and index['Source_CUT_artwork_backend_observed']
    assert index['natural_whole_media_equivalence_qualified'] is index['Native_acceptance'] is False
    projection = checked('business-projection.json')['cells']
    controllers = {}
    for run, expected_exit in RUNS.items():
        paths = identity['controllers'][str(run)]
        terminal = checked(paths['terminal'])
        cleanup = checked(paths['independent_cleanup'])
        assert terminal['actual_exit'] == paths['literal_exit'] == expected_exit
        assert all(terminal[key] for key in ('process_group_settled', 'stdout_EOF', 'stderr_EOF'))
        assert not terminal['remaining_group_members']
        assert cleanup['passed'] and all(row['passed'] for row in cleanup['checks'])
        assert {row['kind'] for row in cleanup['checks']} == {
            'exact-CID', 'original-host-generation', 'project-container', 'project-network', 'project-volume'}
        controllers[str(run)] = {'actual_exit': expected_exit, 'exact_cleanup_passed': True,
                                 'receipt_paths': [paths['terminal'], paths['independent_cleanup']]}

    cases = []
    unique_tasks = set()
    for label, run, expected_result, event_count in CASES:
        def cell(file):
            return checked(f'cells/{label}/{file}.json')

        assert identity['Source_cells'][label]['Source_run'] == run
        natural = label.startswith('natural-')
        frame = cell('comskip-frame' if natural else 'frame')
        backend = cell('comskip-backend' if natural else 'backend')
        publications = cell('journal')['publications']
        headers = frame['headers']
        [published] = [row for row in publications if row['task'] == TASK]
        assert headers['task'] == TASK and published['raw_frame'] == frame
        task_id = headers['id']
        assert published['queue'] == frame['properties']['delivery_info']['routing_key'] == 'celery'
        assert task_id == published['id'] == backend['task_id'] and task_id not in unique_tasks
        unique_tasks.add(task_id)
        args, kwargs, callbacks = json.loads(base64.b64decode(frame['body'], validate=True))
        rows_before = cell('SQL-before')
        rows_after = cell('SQL-after')
        row = rows_after['recordings'][0] if natural else rows_after
        assert args == [row['id']] and not kwargs and not any(callbacks.values())
        assert backend['status'] == 'SUCCESS' and backend['result'] == expected_result
        assert backend['traceback'] is None and backend['children'] == []
        assert len(projection[label]['Comskip_WS']) == event_count
        if expected_result == 'already_processed':
            assert rows_before == rows_after
        if natural:
            recording = cell('recording-frame')['headers']
            parent_backend = cell('recording-backend')
            assert [row['task'] for row in publications] == [TASK.rsplit('.', 1)[0]+'.prefetch_recording_artwork',
                                                            TASK.rsplit('.', 1)[0]+'.run_recording', TASK]
            assert headers['root_id'] == headers['parent_id'] == recording['id'] == backend['parent_id']
            assert parent_backend['task_id'] == recording['id']
            assert parent_backend['children'] == [[[task_id, None], None]]
        else:
            assert headers['root_id'] == task_id and headers['parent_id'] is None
        cases.append({'case': label, 'controller': run, 'queue': 'celery', 'result': expected_result,
                      'observed_task_relationship': 'recording child' if natural else 'own root',
                      'recording_id_matches_SQL': True, 'repeat_preserves_full_recording_row': expected_result == 'already_processed',
                      'Comskip_websocket_events': event_count, 'retained_artifact_directory': f'{LIFECYCLE}/cells/{label}'})
    projection = {
        'candidate_id': IDENTIFIER, 'source': SOURCE, 'Source_revision': ORIGINAL_REVISION, 'origins': {
            SOURCE_COPY: source_digest, LIFECYCLE+'/index.json': index_sha, FENCES+'/index.json': fence_sha,
        }, 'controller_receipts': controllers, 'actual_Original_task_leaves': cases,
        'register_assignment_added': False, 'fully_verified': False, 'new_Source_or_Native_executions': 0,
        'whole_milestone_accepted': False,
        'credit': 'Original Dispatcharr, Celery, Kombu, Comskip, FFmpeg and dependency authors retain full credit.',
    }
    definition = {'path': SOURCE_COPY, 'original_file_sha256': source_digest, 'start_line': SOURCE['line'],
                  'end_line': function.end_lineno, 'ast_sha256': ast_fingerprint(function)}
    return projection, definition


def dependencies():
    files = {BINDING_PATH, SOURCE_COPY, FENCES+'/index.json'}
    for name in ('index.json', 'identity.json', 'business-projection.json'):
        files.add(f'{LIFECYCLE}/{name}')
    files.add(f'{FENCES}/business.py')
    for run in RUNS:
        for name in ('terminal.json', 'Root-independent-cleanup.json'):
            files.add(f'{LIFECYCLE}/runs/{run}/{name}')
    for label, *_ in CASES:
        files.add(f'{FENCES}/Source-fences/{label}.json')
        names = ['SQL-before', 'SQL-after', 'journal']
        if label.startswith('natural-'):
            names += ['comskip-frame', 'comskip-backend', 'recording-frame', 'recording-backend']
        else:
            names += ['frame', 'backend']
        files.update(f'{LIFECYCLE}/cells/{label}/{name}.json' for name in names)
    return files


def derive(root):
    observed, definition = project(Path(root))
    fixture_id = 'retained-Original-Comskip-lifecycle-six'
    actor = {'producer': 'owned fixture admin HTTP and observed periodic recording workflow',
             'executor': 'actual Original Celery worker', 'complete_role_matrix': False}
    fixture = {
        'id': fixture_id, 'baseline_revision': ORIGINAL_REVISION,
        'execution_kind': 'historical_actual_Original_capture_offline_assertion_replay',
        'observed_actor': actor, 'literal_controller_receipts': observed['controller_receipts'],
        'Source_replayed_for_mapping': False, 'manual_global_producer_graph_qualified': False,
        'natural_MARK_artwork_backend_observed': False, 'natural_whole_media_equivalence_qualified': False,
    }
    contract = {
        'candidate_id': IDENTIFIER, 'source': SOURCE, 'owning_milestone': 'M6',
        'definition': definition, 'task_name': TASK, 'fixture_ids': [fixture_id], 'observed_actor': actor,
        'assertions': [
            'Exact retained broker task, root and parent identities, recording SQL argument and backend result',
            'Both repeat outcomes preserve the complete recording row and emit zero Comskip events',
            'Preserve the five actual Original controller exits and independent exact cleanup checks',
        ],
        'observations': observed['actual_Original_task_leaves'],
        'result': {
            'state': 'partial_historical_Original_task_observations', 'actual_worker_observations': 6,
            'received_signals': 0, 'raw_backend_wire_asserted': False, 'new_Source_cases_this_task': 0,
            'Native_coverage_increased': False, 'complete_role_matrix': False, 'fully_verified': False,
        },
        'fully_verified': False,
    }
    return {
        'format_version': 1, 'kind': KIND, 'Original_revision': ORIGINAL_REVISION,
        'credit': observed['credit'], 'authorities': observed['origins'], 'contracts': [],
        'non_HTTP_contracts': [contract], 'fixtures': {fixture_id: fixture},
        'actual_Original_worker_observations': 6, 'new_Source_execution': False,
        'Native_coverage_increased': False, 'fully_verified_added': 0,
    }


def validate(root, value, bindings=None):
    assert value == derive(root), 'exact retained Original Comskip task, fixtures, assertions and observations'


def restore_pre_comskip_task(root, rows):
    [contract] = load(Path(root)/BINDING_PATH)['non_HTTP_contracts']
    [row] = [row for row in rows if row['candidate_id'] == IDENTIFIER]
    assert row.pop('executable_Source_contract') == contract
    assert row.pop('owning_milestone') == contract['owning_milestone']
    assert row.pop('assertions') == contract['assertions']
    row.pop('setup_fixture')
    row.update(state='static_candidate_requires_runtime_contract', result=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--binding', action='store_true', help='Output the canonical register binding.')
    args = parser.parse_args()
    if args.binding:
        result = derive(args.root)
    else:
        register = load(args.root/'docs/rust-contract-register.json')
        assert register['baseline_ref'] == ORIGINAL_REVISION
        [entry] = [row for row in register['non_HTTP_candidates'] if row['candidate_id'] == IDENTIFIER]
        assert entry['source'] == SOURCE and entry['fully_verified'] is False
        result, _ = project(args.root)
    print(json.dumps(result, indent=2))
