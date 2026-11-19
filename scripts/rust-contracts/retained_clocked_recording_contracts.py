"""Reproduce the two retained Original clocked-recording task assignments."""
import argparse
import ast
import base64
import gzip
import hashlib
import json
from pathlib import Path

from framework_auth_contracts import ast_fingerprint

KIND = 'retained_original_clocked_recording_tasks'
BINDING_PATH = 'scripts/rust-contracts/retained-clocked-recording/bindings.json'
BASE = 'scripts/rust-job-contracts/clocked-recording-source/evidence/'
SOURCE_COPY = 'scripts/rust-contracts/scanner-tasks/original/apps/channels/tasks.py'
SOURCE_REVISION = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
RAW_REPORT = '323c396c00b88fa48459a61a63799c5f2529919554b529778aa3b7a221b0a7b8'
INPUTS = {
    'integration.json.gz': '4964a230e0f9a8a113215bd111c0888fd60215e43f8e065ae0fda65616c9fde0',
    'source-clocked-recording-private.json.gz': '88030e2124b312ede0df72e4efc4971a76f705afeea47636d8532254b08e7199',
    'independent-cleanup.json.gz': 'f9854bfe15a4b06303677d5ffb76b60eed2d737b64c38395bc9286e098f1ad14',
    'clocked-prerun.jsonl.gz': 'd324737629835c303b7b7f71911125e31032466f90d0715c3e6a7b74e3c50f83',
    'clocked-postrun.jsonl.gz': '9dd01371dde0562eef085337e5367d4df1f60e25222dc9f62500ecd606d9efff',
}
IDENTITIES = (
    ('candidate-390f50055508e3581055', 'prefetch_recording_artwork', 3869, 'artwork', 'celery', 'ok'),
    ('candidate-0f29552aefd3a49114ee', 'run_recording', 2063, 'recording_task', 'dvr', None),
)
SOURCE_HASH = '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279'


def dependencies():
    return {BINDING_PATH, SOURCE_COPY, *(BASE+name for name in INPUTS)}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def derive(root):
    root = Path(root)
    authorities = {SOURCE_COPY: SOURCE_HASH, **{BASE+name: digest for name, digest in INPUTS.items()}}
    for path, digest in authorities.items():
        assert sha((root/path).read_bytes()) == digest, path

    def decoded(name):
        return gzip.decompress((root/BASE/name).read_bytes())

    host = json.loads(decoded('integration.json.gz'))
    raw = decoded('source-clocked-recording-private.json.gz')
    guest = json.loads(raw)
    cleanup = json.loads(decoded('independent-cleanup.json.gz'))
    assert sha(raw) == RAW_REPORT == host['Source_raw_report_sha256']
    assert host['Original_revision'] == SOURCE_REVISION and host['Original_archive_sha256'] == ARCHIVE
    assert host['actual_host_exit'] == host['actual_controller_exit'] == 0 and host['actual_controller_settled']
    assert host['passed'] and host['Source_verified'] and guest['passed'] and guest['Source_verified']
    assert host['source_acceptance']['actual_tasks'] == 2 and host['source_acceptance']['checks'] == len(guest['checks']) == 17
    assert all(check['passed'] for check in guest['checks'])
    assert host['Native_verified'] is guest['Native_verified'] is host['performance_verified'] is False
    assert host['cleanup_budget']['within_budget'] and host['default_database_unchanged']
    assert all(row['physically_settled'] and row['pipes_settled'] and row['collection_invalid'] is None
               for row in host['processes'])
    assert set(cleanup) == {'container', 'network', 'volume'} and all(row['exit'] == 0 and row['empty'] for row in cleanup.values())
    [first_pre, second_pre] = [json.loads(line) for line in decoded('clocked-prerun.jsonl.gz').splitlines()]
    [first_post, second_post] = [json.loads(line) for line in decoded('clocked-postrun.jsonl.gz').splitlines()]
    assert [first_pre, second_pre] == guest['all_prerun'] and [first_post, second_post] == guest['all_postrun']
    [row] = guest['final']['recordings']
    assert row['custom_properties']['status'] == 'completed' and row['custom_properties']['remux_success']
    assert row['custom_properties']['poster_logo_id'] > 0
    assert row['custom_properties']['owned_fixture_marker'] == 'preserve-normal-clocked'
    frames = [guest['admission_celery']['frames'][0]['frame'], guest['recording_dispatch']['frame']]
    assert len(guest['unconsumed_Beat_siblings']['ids']) == 2 and guest['unconsumed_Beat_siblings']['actual_FIFO_order_preserved']
    assert not set(guest['unconsumed_Beat_siblings']['ids']) & {first_pre['id'], second_pre['id']}
    assert row['task_id'] == frames[1]['headers']['periodic_task_name']
    assert not guest['final']['clocked'] and all(periodic['name'] != row['task_id'] for periodic in guest['final']['periodic'])
    definitions = {node.name: node for node in ast.parse((root/SOURCE_COPY).read_bytes()).body if isinstance(node, ast.FunctionDef)}
    fixture_id = 'retained-Original-future-clocked-recording-Source3'
    actor = {'producer': 'Original Recording model and django-celery-beat', 'executor': 'actual Original Celery workers',
             'complete_role_matrix': False}
    fixture = {'id': fixture_id, 'baseline_revision': SOURCE_REVISION, 'baseline_archive_sha256': ARCHIVE,
               'observed_actor': actor, 'actual_controller_exit': 0, 'actual_host_exit': 0, 'exact_cleanup_passed': True,
               'original_checks_passed': 17, 'unconsumed_Beat_siblings': 2, 'Source_replayed_for_mapping': False}
    contracts = []
    for (identifier, name, line, key, queue, result), frame, pre, post in zip(
            IDENTITIES, frames, (first_pre, second_pre), (first_post, second_post), strict=True):
        node = definitions[name]
        assert min(decorator.lineno for decorator in node.decorator_list) == line
        full_name = 'apps.channels.tasks.'+name
        observed = guest[key]
        headers = frame['headers']
        task_id = headers['id']
        assert task_id == headers['root_id'] == observed['id'] == pre['id'] == pre['root_id'] == post['id'] == post['root_id']
        assert headers['parent_id'] is pre['parent_id'] is post['parent_id'] is None
        assert headers['task'] == pre['task'] == post['task'] == full_name
        assert frame['properties']['correlation_id'] == task_id
        assert pre['delivery']['routing_key'] == frame['properties']['delivery_info']['routing_key'] == queue
        assert pre['actual_process_starttime'] == post['actual_process_starttime'] and pre['pid'] == post['pid']
        args, kwargs, _ = json.loads(base64.b64decode(frame['body'], validate=True))
        assert args == pre['args'] == ([row['id']] if key == 'artwork' else [row['id'], row['channel_id'], row['start_time'], row['end_time']])
        assert kwargs == pre['kwargs'] == {} and observed['prerun'] == [pre] and observed['postrun'] == [post]
        backend = observed['raw_result']
        backend_bytes = base64.b64decode(backend['raw_base64'], validate=True)
        assert json.loads(backend_bytes) == backend['metadata']
        assert backend['metadata']['task_id'] == task_id and backend['metadata']['status'] == post['state'] == 'SUCCESS'
        assert backend['metadata']['result'] == post['result'] == observed['Original_AsyncResult_get']['value'] == result
        assert observed['Original_AsyncResult_get']['executed'] and backend['metadata']['children'] == []
        contracts.append({
            'candidate_id': identifier, 'source': {'kind': 'task_candidate', 'path': 'apps/channels/tasks.py',
                                                   'line': line, 'name': name, 'callable': 'celery.shared_task'},
            'task_name': full_name, 'owning_milestone': 'M6', 'fixture_ids': [fixture_id], 'observed_actor': actor,
            'definition': {'path': SOURCE_COPY, 'original_file_sha256': SOURCE_HASH,
                           'start_line': line, 'end_line': node.end_lineno, 'ast_sha256': ast_fingerprint(node)},
            'observations': [{'case': 'normal-future-clocked-recording-Source3', 'task_id': task_id, 'queue': queue,
                              'root_id': task_id, 'parent_id': None, 'arguments': args, 'backend_status': 'SUCCESS',
                              'backend_result': result, 'backend_wire_sha256': sha(backend_bytes),
                              'worker_prerun_and_postrun_observed': True}],
            'assertions': ['Exact natural queue, own-root frame, SQL row arguments and executed worker/backend receipt',
                           'Owned recording row kept its periodic task name and poster through successful remux',
                           'Exact process and owned resource cleanup preserved'],
            'result': {'state': 'partial_historical_Original_task_observations', 'actual_worker_observations': 1,
                       'new_Source_cases_this_task': 0, 'Native_coverage_increased': False, 'fully_verified': False},
            'fully_verified': False,
        })
    return {'format_version': 1, 'kind': KIND, 'Original_revision': SOURCE_REVISION, 'Original_archive_sha256': ARCHIVE,
            'credit': 'Original Dispatcharr, Celery, django-celery-beat, FFmpeg and dependency authors retain all credit.',
            'authorities': [{'path': path, 'sha256': digest} for path, digest in authorities.items()],
            'contracts': [], 'non_HTTP_contracts': contracts, 'fixtures': {fixture_id: fixture},
            'actual_Original_worker_observations': 2, 'new_Source_execution': False, 'Native_coverage_increased': False,
            'fully_verified_added': 0}


def validate(root, value, bindings=None):
    assert value == derive(root), 'exact Original clocked recording task and artwork observations'


def restore_pre_clocked_recording(root, rows):
    path = Path(root)/BINDING_PATH
    contracts = json.loads(path.read_bytes())['non_HTTP_contracts']
    entries = {row['candidate_id']: row for row in rows}
    for contract in contracts:
        row = entries[contract['candidate_id']]
        assert row.pop('executable_Source_contract') == contract
        for field in ('owning_milestone', 'assertions', 'setup_fixture'):
            row.pop(field)
        row.update(state='static_candidate_requires_runtime_contract', result=None)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    print(json.dumps(derive(args.root), indent=2))
