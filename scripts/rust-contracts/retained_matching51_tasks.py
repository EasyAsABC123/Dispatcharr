"""Typed Original Source51 all-match and two matching child task contracts."""
import ast
import hashlib
import json
from pathlib import Path
import re

from framework_auth_contracts import ast_fingerprint

REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
KIND='retained_original_Source51_all_match_and_guide_children_tasks'
FOLDER='scripts/rust-contracts/retained-matching51-tasks/'
PUBLIC='scripts/rust-integration/channel-matching-source51/Source51/'
SOURCE='scripts/rust-contracts/scanner-tasks/original/'
INPUT=FOLDER+'bindings.json'
RECEIPT=FOLDER+'receipt.json'
FILES={
    RECEIPT:'72c19e284ad1e9007652360463696a1acacbf97108b1a183476ee010c7adbcf5',
    FOLDER+'sd-batch-definition.py':'92a9b0416084e93ed6fb73f247507350c999bff52767eec031efb925a1f2ec86',
    PUBLIC+'cases.json':'5e5bbd3023cb563043cd6a45e8dcd38b4cbaf59176e4ce4afa12e389f00774a3',
    PUBLIC+'cleanup.json':'bf84729148064e378e0e92807967686485aceaf0b95a620cacf2278a65fbb79f',
    PUBLIC+'execution.json':'799095effd305dbb0f2e436e7efcc28e3b288bb24fb20d22a724c1db9a01871d',
    PUBLIC+'qualification.json':'7ffd10806cb2407432d21953387b85832e6d8a07afeeee899fd746c3ac66be6d',
    PUBLIC+'availability.json':'6addf40504684d054ab4c1c56464d819b6e320794b163fa46754d49ccaf3f065',
    SOURCE+'apps/channels/tasks.py':'92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279',
    SOURCE+'apps/epg/tasks.py':'deb4792a39ed93c57da435ecc2a4fcdf5b4dad8f4e087d3afdb5d4f2b7932ff9',
}
ROOT='apps.channels.tasks.match_epg_channels'
PARSER='apps.epg.tasks.parse_programs_for_tvg_id'
BATCH='apps.epg.tasks.fetch_sd_mapped_guide_batch'
ROW_NAMES=(
    'all-exact-gracenote-priority-inactive-unassigned','all-excludes-nameless-catalog',
    'selected-mixed-SD-batch-XML-dummy','single-SD-direct-parse-cache-group',
)
TASKS=(
    ('candidate-f461943e677d63b54628','apps/channels/tasks.py',245,ROOT,SOURCE+'apps/channels/tasks.py',
     'Exact all-unassigned matching updates the two qualifying channel rows and excludes nameless catalogue rows.'),
    ('candidate-872650ecefd8633593b5','apps/epg/tasks.py',1779,PARSER,SOURCE+'apps/epg/tasks.py',
     'Mixed selected bulk matching publishes one XMLTV parser; single SD match also publishes through the parser.'),
    ('candidate-5190afcd5e590be9738a','apps/epg/sd_tasks.py',452,BATCH,FOLDER+'sd-batch-definition.py',
     'Selected bulk matching publishes one Schedules Direct batch; terminal result does not qualify provider import.'),
)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def dependencies():
    return {INPUT,*FILES}


def check_rows(receipt, cases):
    assert receipt['original_revision']==REVISION and receipt['format_version']==1
    assert receipt['raw_report_sha256']==cases['raw_helper_report_sha256']
    assert [row['name'] for row in receipt['rows']]==list(ROW_NAMES)
    by_task={}
    ids=set()
    for row,children_expected in zip(receipt['rows'],((),(),(PARSER,BATCH),(PARSER,)),strict=True):
        [public]=[case for case in cases['cases'] if case['name']==row['name']]
        assert public['status']=='passed' and public['actual_HTTP_status']==row['actual_parent_HTTP']==202
        assert public['root_task']==row['parent_task_name']
        assert row['parent_frame_sha256']==public['root_frame_sha256']
        assert row['parent_backend_result_sha256']==public['backend_result_sha256']
        assert row['DB1_journal_terminal_durable'] is public['DB1_journal_terminal_durable'] is True
        assert row['actual_changed_assignments']==public['actual_changed_assignments']
        assert row['actual_parent_path']==('/api/channels/channels/1/match-epg/' if row['name']==ROW_NAMES[3]
                                          else '/api/channels/channels/match-epg/')
        children=row['children']
        assert tuple(child['task_name'] for child in children)==tuple(public['actual_direct_child_tasks'])==children_expected
        if not children:
            assert row['root']['task_name']==ROOT and row['root']['positional_args']==[]
            assert row['root']['task_id_sha256']==row['parent_task_id_sha256']
            assert row['root']['parent_task_id_sha256'] is None
            assert row['root']['frame_sha256']==row['parent_frame_sha256']
        else:
            assert row['root'] is None and public['actual_child_parent_and_root_match'] is True
            for child in children:
                assert child['parent_task_id_sha256']==row['parent_task_id_sha256']
                [object_id]=child['positional_args']
                assert type(object_id) is int and object_id>0
            assert [child['backend_status'] for child in children]==public['child_terminal_statuses']
        selected=children if children else [row['root']]
        for observation in selected:
            assert observation['task_id_sha256'] not in ids
            ids.add(observation['task_id_sha256'])
            assert observation['queue']=='celery' and observation['verified_parent_root_correlation'] is True
            assert observation['kwargs']=={} and observation['backend_status']=='SUCCESS'
            for key in ('task_id_sha256','frame_sha256','backend_result_sha256'):
                assert re.fullmatch('[a-f0-9]{64}',observation[key])
            assert observation['backend_result_sha256']==digest(json.dumps(observation['backend_result'],sort_keys=True).encode())
            by_task.setdefault(observation['task_name'],[]).append({
                **observation,'Source51_case':row['name'],'actual_parent_HTTP_path':row['actual_parent_path'],
                'actual_parent_HTTP_status':202,'actual_changed_assignments':row['actual_changed_assignments'],
                'direct_worker_signals_observed':False,
            })
    assert [row['backend_result'] for row in by_task[PARSER]]==[None,'SD guide fetch complete']
    assert [row['backend_result'] for row in by_task[BATCH]]==['SD mapped guide batch complete']
    assert [row['actual_changed_assignments'] for row in by_task[ROOT]]==[2,0]
    assert len(ids)==5
    return by_task


def derive(root):
    inputs={}
    for name,expected in FILES.items():
        raw=(Path(root)/name).read_bytes()
        assert digest(raw)==expected
        inputs[name]=json.loads(raw) if name.endswith('.json') else raw
    receipt=inputs[RECEIPT]
    observations=check_rows(receipt,inputs[PUBLIC+'cases.json'])
    execution=inputs[PUBLIC+'execution.json']
    qualification=inputs[PUBLIC+'qualification.json']
    cleanup=inputs[PUBLIC+'cleanup.json']
    availability=inputs[PUBLIC+'availability.json']
    assert execution['actual_exit']==qualification['actual_exit']==0 and execution['remaining_group_members']==[]
    assert all(execution[k] is True for k in ('stdout_EOF','stderr_EOF','process_group_settled'))
    assert qualification['natural_HTTP_cases']==14 and qualification['actual_cleanup_passed'] is True
    assert cleanup['passed'] is cleanup['all_owned_resources_absent'] is cleanup['original_positive_host_PIDs_absent'] is True
    assert cleanup['exact_CID_count']==2 and qualification['actual_model_inference_qualified'] is False
    assert all(availability[k] is False for k in ('model_cache_path_present','model_loaded','downloads_attempted'))
    fixture={'id':'Original-Source51-quiet-matching','Original_revision':REVISION,'actual_controller_exit':0,
             'settled_with_EOF':True,'exact_cleanup':True,'scope':qualification['Source_supported_config_domain'],
             'actual_model_inference_qualified':False,'authority_paths':FILES}
    contracts=[]
    for identifier,path,line,task_name,definition,assertion in TASKS:
        name=task_name.rsplit('.',1)[-1]
        [node]=[item for item in ast.parse(inputs[definition]).body if isinstance(item,ast.FunctionDef) and item.name==name]
        start=min(decorator.lineno for decorator in node.decorator_list)
        assert any(ast.unparse(decorator).startswith('shared_task') for decorator in node.decorator_list)
        if task_name==BATCH:
            source=receipt['sd_batch_definition']
            assert source['original_path']==path and source['start_line']==line and start==1
            assert source['end_line']==line+node.end_lineno-1 and source['slice_sha256']==FILES[definition]
            assert any(kw.arg=='name' and kw.value.value==task_name for kw in node.decorator_list[0].keywords)
        else:
            assert start==line
        contracts.append({
            'candidate_id':identifier,'source':{'kind':'task_candidate','path':path,'line':line,'name':name,
                                               'callable':'celery.shared_task'},
            'task_name':task_name,'owning_milestone':'M6','fixture_ids':[fixture['id']],
            'definition':{'path':definition,'start_line':line,'ast_sha256':ast_fingerprint(node)},
            'observed_actor':{'producer':'Original authenticated matching view' if task_name==ROOT
                              else 'Original matching worker parent',
                              'executor':'Original normal Celery queue celery','complete_role_matrix':False},
            'observations':observations[task_name],
            'assertions':['Actual task, arguments, parent/root identity, backend SUCCESS/result and SQL assignment count match retained scope.',
                          assertion,'No successful external guide import or model inference was measured.'],
            'result':{'state':'partial_historical_Original_task_observations',
                      'actual_task_terminal_observations':len(observations[task_name]),
                      'new_Source_cases_this_task':0,'Native_coverage_increased':False,'fully_verified':False},
            'fully_verified':False,
        })
    return {'format_version':1,'kind':KIND,'Original_revision':REVISION,
            'credit':'Original Dispatcharr, Django, Celery, CPython, RapidFuzz and dependency authors retain full credit.',
            'fixtures':{fixture['id']:fixture},'contracts':[],'non_HTTP_contracts':contracts,
            'new_Source_executions':0,'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False}


def validate(root,value,bindings=None):
    assert value==derive(root),'Retained Original Source51 matching task observations mismatch'
