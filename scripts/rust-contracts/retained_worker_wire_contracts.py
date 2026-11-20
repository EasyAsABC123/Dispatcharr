"""Portable typed Original worker and WebSocket route evidence."""
import ast
import hashlib
import json
from pathlib import Path
import re

from framework_auth_contracts import ast_fingerprint

KIND = 'retained_original_worker_wire_non_HTTP'
PREFIX = 'scripts/rust-contracts/retained-worker-wire/'
BINDING = PREFIX + 'bindings.json'
RECEIPT = PREFIX + 'receipt.json'
SOURCE_TASKS = 'scripts/rust-contracts/scanner-tasks/original/'
ORIGINAL = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
JOB = 'scripts/rust-job-contracts/verified-source-native-default-stats-1.json'
WIRE = 'scripts/rust-realtime-contracts/verified-source-wire-1.json'
INPUTS = {
    RECEIPT: '3f3edeb524fa2110cbfc2b7df9d25487e14477c1409ecb845b7f7024e04d30e0',
    JOB: '812433fcf3a200b9f52d1027eabbdbc54f0b55619ea6400ab2246c49a3daeefe',
    WIRE: 'b09b658e0f093dd6948822d5c34908260777d5d4f1e2e8ea51e35e741a2a0cf7',
    SOURCE_TASKS + 'apps/channels/tasks.py': '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279',
    SOURCE_TASKS + 'apps/m3u/tasks.py': '921e0fea7273c0ce7d1b1380a4f675718fbc5cdb3bb0d38005cdae2b73cce981',
    PREFIX + 'original/apps/proxy/tasks.py': 'a4c5e88138ca21424208ff79de398b9345dc76681440c518423e6134678b3326',
    PREFIX + 'original/dispatcharr/routing.py': '567b99437a8292e60913193d47bdf7d4855b40f07f02425eb16611f4097c8b15',
}
STATS_CASES = ('empty', 'static-live-client', 'ghost-client', 'wrongtype-metadata', 'invalid-buffer-index',
               'group-send-wrongtype')
TASKS = (
    ('candidate-28e73dbbafb9d9b259a3', 'apps/channels/tasks.py', 1157, 'maintain_recurring_recordings', SOURCE_TASKS,
     ('recurring-disabled',), 0,
     ('Actual no-argument Original prefork task completed with integer zero for the disabled rules fixture.',
      'The selected recording rows and owned broker reservations remained unchanged.')),
    ('candidate-8699da637444094159da', 'apps/m3u/tasks.py', 4248, 'check_account_expirations', SOURCE_TASKS,
     ('expiration-stale',), None,
     ('Actual no-argument Original worker returned null with the exact stale notification row removed.',
      'A real ChannelsRedis dismissal event matched the removed row on the owned fixture.')),
    ('candidate-ca631f742f76c9d0b508', 'apps/proxy/tasks.py', 14, 'fetch_channel_stats', PREFIX+'original/',
     STATS_CASES, None,
     ('Six real Original prefork roots returned null with exact prerun/postrun and backend joins.',
      'Existing live metadata retains one client and the ghost case removes the stale client.',
      'Five cases emit the scoped statistics event and the group-send error case emits zero.')),
)
ROUTE = 'candidate-d094f56b97e7c1053e60'
CREDIT = 'Original Dispatcharr, Celery, Django Channels, Daphne and dependency authors retain full application and behavior credit.'
WORKER_FIXTURE = 'retained-Original-prefork-default-task-effects'
WIRE_FIXTURE = 'retained-Original-Daphne-ws-wire55'


def dependencies():
    return {BINDING, *INPUTS}


def checked(root, name):
    blob = (root / name).read_bytes()
    assert hashlib.sha256(blob).hexdigest() == INPUTS[name], name
    return blob


def meta(path, node):
    return {'path': path, 'original_file_sha256': INPUTS[path], 'start_line': node.lineno,
            'end_line': node.end_lineno, 'ast_sha256': ast_fingerprint(node)}


def validate_observations(receipt, job, wire):
    assert receipt['original_revision'] == job['bindings']['source']['original_revision'] == wire['baseline_commit'] == ORIGINAL
    assert receipt['archive_sha256'] == job['bindings']['source']['original_archive_sha256'] == ARCHIVE
    assert all(re.fullmatch('[a-f0-9]{64}', digest) for digest in receipt['raw_digests'].values())
    assert receipt['worker_actual_orig_prefork_signals_used'] and receipt['worker_outer_cleanup_recorded'] == 'project removed'
    assert receipt['worker_independent_outer_group_claimed'] is receipt['worker_other_families_counted'] is False
    assert receipt['new_runtime_execution'] is receipt['full_M0_accepted'] is False
    assert receipt['raw_digests']['integration.json'] == job['bindings']['source']['integration_sha256']
    assert receipt['raw_digests']['source-default-effects-worker-private.json'] == job['bindings']['source']['private_report_sha256']
    observed = receipt['websocket']
    assert observed['path'] == '/ws/' and observed['guest_actual_exit'] == 0 and observed['wire_case_count'] == 55
    assert observed['observed_cases'] == [
        {key: row[key] for key in ('case','status','close') if key in row} for row in wire['cases']]
    assert observed['exceptional_source_members_left_before_compose_down'] == wire['source_group_members_after_close'] == 4
    assert observed['exceptional_source_member_cleanup_claimed'] is observed['new_synthetic_consumer_assignment'] is False
    assert observed['project_cleanup_recorded'] is True
    assert receipt['raw_digests']['websocket-source.json'] == wire['artifact_sha256']


def derive(root):
    root = Path(root)
    receipt, job, wire = (json.loads(checked(root, name)) for name in (RECEIPT, JOB, WIRE))
    validate_observations(receipt, job, wire)
    assert wire['wire_contracts_passed'] is True and wire['milestone_complete'] is False
    assert job['passed'] is True and job['main_or_default_consumer_enabled'] is False
    worker = {'producer': 'owned manually published no-argument task',
              'executor': 'actual Original Celery prefork worker',
              'complete_role_matrix': False}
    transport = {'executor': 'actual Original Daphne and Django Channels', 'complete_role_matrix': False}
    fixtures = {
        WORKER_FIXTURE: {'id': WORKER_FIXTURE, 'baseline_revision': ORIGINAL,
                         'execution_kind': 'historical_actual_Original_Celery_prefork_signals',
                         'observed_actor': worker, 'cleanup': receipt['worker_outer_cleanup_recorded'],
                         'independent_outer_group_claimed': False, 'default_scheduler_execution': False},
        WIRE_FIXTURE: {'id': WIRE_FIXTURE, 'baseline_revision': ORIGINAL,
                       'execution_kind': 'historical_actual_Original_Daphne_tcp_wire', 'observed_actor': transport,
                       'observed_wire_path': '/ws/', 'guest_actual_exit': 0, 'project_cleanup_recorded': True,
                       'exceptional_source_members_left_before_compose_down': 4,
                       'exceptional_source_member_cleanup_claimed': False},
    }
    result = {'Native_coverage_increased': False, 'new_Source_cases_this_task': 0,
              'complete_role_matrix': False, 'fully_verified': False}
    contracts = []
    task_cases = receipt['task_cases']
    assert len(task_cases) == 3
    for identifier, original, line, name, source_prefix, names, expected, assertions in TASKS:
        path = source_prefix + original
        [node] = [item for item in ast.parse(checked(root,path)).body
                  if isinstance(item,ast.FunctionDef) and item.name == name
                  and item.lineno == line+1 and [dec.lineno for dec in item.decorator_list] == [line]]
        task = original.removesuffix('.py').replace('/','.')+'.'+name
        cases = task_cases[task]
        assert tuple(row['case'] for row in cases) == names
        assert all(row['actual_prerun_postrun_joined'] is row['root_without_parent'] is True
                   and row['args']==[] and row['kwargs']=={} and row['worker_terminal']=='SUCCESS'
                   and row['backend_children']==0 and type(row['backend_result']) is type(expected)
                   and row['backend_result']==expected for row in cases)
        contracts.append({'candidate_id': identifier, 'source': {'kind':'task_candidate','path':original,
                           'line':line,'name':name,'callable':'celery.shared_task'}, 'owning_milestone':'M6',
                          'definition':{**meta(path,node), 'decorator_line':line}, 'task_name':task, 'fixture_ids':[WORKER_FIXTURE],
                          'observed_actor':worker, 'assertions':list(assertions), 'observations':cases,
                          'result':dict(result,state='partial_historical_Original_task_observations',
                                        actual_worker_observations=len(cases)), 'fully_verified':False})
    path = PREFIX + 'original/dispatcharr/routing.py'
    [node] = [item for item in ast.walk(ast.parse(checked(root,path)))
              if isinstance(item,ast.Call) and isinstance(item.func,ast.Name) and item.func.id=='path'
              and item.lineno==5 and isinstance(item.args[0],ast.Constant) and item.args[0].value=='ws/']
    assert wire['source_sha256']['dispatcharr/routing.py']==INPUTS[path]
    contracts.append({'candidate_id':ROUTE,'source':{'kind':'websocket_route','path':'dispatcharr/routing.py',
                      'line':5,'callable':'django.urls.path'},'owning_milestone':'M7','definition':meta(path,node),
                      'fixture_ids':[WIRE_FIXTURE],'observed_actor':transport,
                      'assertions':['Actual pinned Daphne admits or denies the selected /ws/ handshakes and reports literal close statuses.',
                                    'Preserve the observed four exceptional stale memberships separately from Compose removal.'],
                      'observations':receipt['websocket']['observed_cases'],
                      'result':dict(result,state='partial_historical_Original_websocket_route_observations',
                                    actual_wire_cases=55),'fully_verified':False})
    return {'format_version':1,'kind':KIND,'Original_revision':ORIGINAL,'credit':CREDIT,'contracts':[],
            'fixtures':fixtures,'non_HTTP_contracts':contracts,'fully_verified_added':0,
            'new_Source_execution':False,'Native_coverage_increased':False}


def validate(root, value, bindings=None):
    # Dict equality treats False and zero as equal. The retained Celery value
    # is an actual integer in the disabled recurring case.
    expected = derive(root)
    assert json.dumps(value, sort_keys=True)==json.dumps(expected, sort_keys=True), \
        'exact retained worker and websocket typed Original authority'
