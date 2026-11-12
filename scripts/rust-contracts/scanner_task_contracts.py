"""Original Celery observations, scoped to six exact static task candidates.
Public retained captures are replayed offline; no new worker execution or ACK guarantee.
Original Dispatcharr authors retain full application and algorithm credit.
"""
import ast
import base64
import hashlib
import json
from pathlib import Path
from framework_auth_contracts import ast_fingerprint

KIND = 'captured_original_scanner_task_observations'
IDENTITIES = [{'candidate_id': 'candidate-a5dea1db3aa833bf0187', 'source': {'kind': 'task_candidate', 'path': 'core/tasks.py', 'line': 60, 'name': 'scan_and_process_files', 'callable': 'celery.shared_task'}, 'task_name': 'core.tasks.scan_and_process_files'}, {'candidate_id': 'candidate-442405cebe0695fa65ba', 'source': {'kind': 'task_candidate', 'path': 'apps/epg/tasks.py', 'line': 498, 'name': 'refresh_epg_data', 'callable': 'celery.shared_task'}, 'task_name': 'apps.epg.tasks.refresh_epg_data'}, {'candidate_id': 'candidate-ec9fbf9e83b49892cea2', 'source': {'kind': 'task_candidate', 'path': 'apps/epg/tasks.py', 'line': 3454, 'name': 'build_programme_index_task', 'callable': 'celery.shared_task'}, 'task_name': 'apps.epg.tasks.build_programme_index_task'}, {'candidate_id': 'candidate-1bcc1e26e1c56f58d30a', 'source': {'kind': 'task_candidate', 'path': 'apps/channels/tasks.py', 'line': 943, 'name': 'evaluate_series_rules', 'callable': 'celery.shared_task'}, 'task_name': 'apps.channels.tasks.evaluate_series_rules'}, {'candidate_id': 'candidate-da2be1cbd1c100963945', 'source': {'kind': 'task_candidate', 'path': 'apps/m3u/tasks.py', 'line': 1699, 'name': 'refresh_m3u_groups', 'callable': 'celery.shared_task'}, 'task_name': 'apps.m3u.tasks.refresh_m3u_groups'}, {'candidate_id': 'candidate-b7a0c27875c0e36a3f31', 'source': {'kind': 'task_candidate', 'path': 'apps/m3u/tasks.py', 'line': 3504, 'name': 'refresh_single_m3u_account', 'callable': 'celery.shared_task'}, 'task_name': 'apps.m3u.tasks.refresh_single_m3u_account'}]
AUTHORITIES = [{'path': 'scripts/rust-job-contracts/scanner-logo/children/source-children-4.json', 'sha256': '8603f06456c46816b8b959a48b0d2db72ff166bc096b7d799eb103256634a021'}, {'path': 'scripts/rust-job-contracts/scanner-logo/children/verified-children-4.json', 'sha256': '6a6bd332ffe41db93c65cbc1d49e4b4d0f66972f8832c31c31c389972e34b5a2'}, {'path': 'scripts/rust-job-contracts/scanner-logo/children/run_pair.py', 'sha256': 'ad0ce732ab55f42646d5ba049d55c876a47dd3a453143f7a960addb9b8bc4912'}, {'path': 'scripts/rust-job-contracts/scanner-logo/children/source_pair.py', 'sha256': '724c558e448c1971d46f1088876635966e4f38720f4774b953ad3809f90f5640'}, {'path': 'scripts/rust-job-contracts/scanner-logo/children/periodic_worker_observer.py', 'sha256': 'ac2f24a0507e73dbdedc816eaf01e4d39732f867f1a03d5bea0ef86a1d17b935'}, {'path': 'scripts/rust-job-contracts/scanner-logo/m3u-reference/source-m3u-4.json', 'sha256': 'd726946de24020938d815b0ca9b8d39d412d8c110a2226e61e7bcfd38b3ccb72'}, {'path': 'scripts/rust-job-contracts/scanner-logo/m3u-reference/verified-m3u-4.json', 'sha256': 'c47d138967d5d586396bd82bbeeac00813f57955a9920eb4717cd53e81ce6723'}, {'path': 'scripts/rust-job-contracts/scanner-logo/m3u-reference/run_pair.py', 'sha256': 'ad0ce732ab55f42646d5ba049d55c876a47dd3a453143f7a960addb9b8bc4912'}, {'path': 'scripts/rust-job-contracts/scanner-logo/m3u-reference/source_pair.py', 'sha256': '3a9426ed8c2c0fb9ec0e7043496221bff4f8d018e602202356d5a74df06d7ae9'}, {'path': 'scripts/rust-job-contracts/scanner-logo/m3u-reference/periodic_worker_observer.py', 'sha256': '366d7a7efd61330a37ee251fb84a7b21d4bed437bef6a917390db879000ac557'}, {'path': 'scripts/rust-job-contracts/scanner-logo/file-branches/source-file-branches-2.json', 'sha256': '8aa0689682cbce5f7d57a05ca8932afd146d4bf26d2d8f8c1d40fc278a37c060'}, {'path': 'scripts/rust-job-contracts/scanner-logo/file-branches/verified-file-branches-2.json', 'sha256': 'd3b57ea6aa860849a5b549f7fee7db2e2d1da19ed25a8b3dd1274fc73e6eb66c'}, {'path': 'scripts/rust-job-contracts/scanner-logo/file-branches/run_pair.py', 'sha256': '424d321a1ede95791a49036d06303dcf0b104b84bf65dc714dc160eea0706567'}, {'path': 'scripts/rust-job-contracts/scanner-logo/file-branches/source_pair.py', 'sha256': '5a7e30a686abbd559bd6d09e7ba26cce0d8c0e50bc95c83a473c72930acefa28'}, {'path': 'scripts/rust-job-contracts/scanner-logo/file-branches/periodic_worker_observer.py', 'sha256': '224fb1360f553272b2149a241d106a8c0cd07616af7aa2c760b0a49228bcd603'}]
ORIGINAL_FILES = [{'path': 'scripts/rust-contracts/scanner-tasks/original/apps/channels/tasks.py', 'sha256': '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279'}, {'path': 'scripts/rust-contracts/scanner-tasks/original/apps/epg/tasks.py', 'sha256': 'deb4792a39ed93c57da435ecc2a4fcdf5b4dad8f4e087d3afdb5d4f2b7932ff9'}, {'path': 'scripts/rust-contracts/scanner-tasks/original/apps/m3u/tasks.py', 'sha256': '921e0fea7273c0ce7d1b1380a4f675718fbc5cdb3bb0d38005cdae2b73cce981'}, {'path': 'scripts/rust-contracts/scanner-tasks/original/core/tasks.py', 'sha256': '5e20b3a7395c9d78cc45f3b07abc1becc7d744a3f37e09735eac512ae537dcbc'}]
BINDING_PATH = 'scripts/rust-contracts/scanner-tasks/bindings.json'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def dependencies():
    return [BINDING_PATH] + [r['path'] for r in AUTHORITIES + ORIGINAL_FILES]


def derive(root):
    root = Path(root)
    for reference in AUTHORITIES + ORIGINAL_FILES:
        assert hashlib.sha256((root / reference['path']).read_bytes()).hexdigest() == reference['sha256'], 'exact Original authority bytes'
    definitions = {}
    for row in IDENTITIES:
        relative = 'scripts/rust-contracts/scanner-tasks/original/' + row['source']['path']
        tree = ast.parse((root / relative).read_text())
        function = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == row['source']['name'])
        assert min(n.lineno for n in function.decorator_list) == row['source']['line'], 'decorated task coordinate'
        assert any('shared_task' in ast.unparse(n) for n in function.decorator_list), 'actual task decorator'
        definitions[row['task_name']] = dict(row, definition={'path': relative, 'start_line': function.lineno,
            'end_line': function.end_lineno, 'ast_sha256': ast_fingerprint(function)})
    observed = {name: [] for name in definitions}
    fixtures = {}
    for folder, stem, count in [('children', 'children-4', 100), ('m3u-reference', 'm3u-4', 119), ('file-branches', 'file-branches-2', 217)]:
        prefix = 'scripts/rust-job-contracts/scanner-logo/' + folder + '/'
        raw_path = prefix + 'source-' + stem + '.json'
        data = json.loads((root / raw_path).read_text())
        proof = json.loads((root / (prefix + 'verified-' + stem + '.json')).read_text())
        assert data['passed'] is True and proof['passed'] is True and proof['checks'] == count, 'Original passed scoped observations'
        assert proof['default_database_unchanged'] is True and 'removed' in proof['cleanup'], 'Original isolation and cleanup'
        assert len(data['checks']) == count and all(x['passed'] is True for x in data['checks']), 'actual named checks'
        fixtures[folder] = {'id': folder, 'entrypoint': prefix + 'run_pair.py', 'worker_setup': prefix + 'periodic_worker_observer.py',
            'scenario_setup': prefix + 'source_pair.py', 'raw_path': raw_path, 'cases': [x['name'] for x in data['cases']],
            'execution_kind': 'historical_real_Original_worker_capture_offline_assertion_replay',
            'checks': data['checks'], 'scope': proof['scope'], 'new_Source_execution': False}
        if folder == 'file-branches':
            assert len(data['cases'][-1]['after']['queue']) == proof['preserved_default_queue_frames'] == 29
            assert sum(len(c['children']) for c in data['cases']) == 31, 'before-publish includes two denied LPUSH attempts'
        for case_index, case in enumerate(data['cases']):
            posts = case.get('postruns', [case.get('postrun')])
            pres = case.get('preruns', [case.get('prerun')])
            for post in posts:
                name = post['task']; identifier = post['id']
                assert name in observed, 'unexpected task execution'
                pre = next(x for x in pres if x['id'] == identifier)
                assert pre['task'] == name
                if folder == 'file-branches':
                    assert identifier == case['parent_id']
                    metadata = case['metadata']; ttl = None; received = None
                else:
                    received = next(x for x in case['received'] if x['id'] == identifier)
                    assert received['task'] == name
                    assert all(received[k] == pre[k] for k in ['root_id', 'parent_id', 'args', 'kwargs']), 'worker lineage/arguments'
                    result = case['results'][identifier]; metadata = result['metadata']; ttl = result['ttl']
                    assert json.loads(base64.b64decode(result['raw_base64'], validate=True)) == metadata, 'backend wire JSON'
                    assert type(ttl) is int and ttl > 0
                assert metadata['task_id'] == identifier and metadata['status'] == post['state'], 'terminal task identity/state'
                assert metadata['status'] in ['SUCCESS', 'FAILURE']
                if metadata['status'] == 'SUCCESS':
                    assert metadata['result'] == post['result'], 'SUCCESS backend value'
                else:
                    assert isinstance(metadata['result'], dict) and metadata['result']['exc_type'] and metadata['traceback'], 'actual Source typed exception/traceback'
                publications = [case['parent_publication']] + case.get('publications', case.get('children', []))
                publication = next(x for x in publications if x.get('headers', {}).get('id') == identifier)
                assert publication['headers']['task'] == name, 'actual publication task identity'
                observed[name].append({'fixture_id': folder, 'case_index': case_index, 'case_name': case['name'],
                    'task_id': identifier, 'received_signal_retained': received is not None,
                    'received': received, 'prerun': pre, 'postrun': post, 'metadata': metadata, 'observed_ttl': ttl,
                    'publication_sha256': digest(publication), 'before_sha256': digest(case['before']),
                    'after_sha256': digest(case['after']), 'sql_sha256': digest(case['sql'])})
    counts = [31, 6, 9, 6, 12, 10]
    contracts = []
    for name, expected in zip(definitions, counts):
        runs = observed[name]
        assert len(runs) == expected and len({r['task_id'] for r in runs}) == expected
        item = definitions[name]
        contracts.append(dict(item, fixture_ids=sorted({r['fixture_id'] for r in runs}), observations=runs,
            assertions=['publication and named worker-stage task UUID/name agree', 'retained received signal agrees with prerun arguments/lineage when present',
                'terminal state and backend UUID agree; typed FAILURE is preserved', 'raw backend JSON agrees where retained; scoped SQL/cache effects hash-bound'],
            result={'state': 'partial_historical_Original_task_observations', 'actual_worker_observations': len(runs),
                'received_signals': sum(r['received_signal_retained'] for r in runs), 'new_Source_cases_this_task': 0,
                'fully_verified': False}, fully_verified=False,
            remaining=['ACK timing only observed snapshots, not a universal guarantee', 'no HTTP/Main/Native task completion claim',
                'file-branches children publications are unconsumed; no child execution inferred', 'only captured synthetic local-file scenarios and explicit failures/busy branches']))
    return {'format_version': 1, 'kind': KIND, 'Original_revision': '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430',
        'authorities': AUTHORITIES, 'original_files': ORIGINAL_FILES, 'contracts': [], 'non_HTTP_contracts': contracts,
        'fixtures': fixtures, 'fully_verified_added': 0, 'worker_observations': 74, 'received_signals': 57,
        'unconsumed_file_branch_children': 29, 'file_branch_before_publish_observations': 31, 'Native_mapped_parent621': 'related-only; not Original execution authority'}


def validate(root, value, bindings=None):
    assert value == derive(root), 'exact six-task Original projection/fixture/assertions'
