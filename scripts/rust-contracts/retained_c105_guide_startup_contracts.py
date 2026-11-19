"""Typed Original startup tasks from the untouched C105 date-guide epoch."""
import ast
import hashlib
import json
from pathlib import Path
import re

from framework_auth_contracts import ast_fingerprint

KIND='retained_original_C105_guide_startup_tasks'
INPUT='scripts/rust-contracts/retained-c105-guide-startup/bindings.json'
RECEIPT='scripts/rust-integration/retained-c105-guide-startup/receipt.json'
RECEIPT_SHA='11b9a0a4b330027b9ba425b1525ce0b0b7e8222be9a8d49548240b47b293efba'
BASELINE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
CANDIDATE='fc2c81c2d52bbc17299d6900534d9564432d8e89'
PLUGIN='scripts/rust-pluginrepo-contracts/metadata-component/authority/Original/apps/plugins/tasks.py'
SCANNER='scripts/rust-contracts/scanner-tasks/original/'
PRODUCERS='scripts/rust-contracts/backup-source16/executed-definition/inputs/Original/'
INPUTS={
    PLUGIN:'19bf9f0d06491280b331615cd17c76b0ce56c0e5ee67f91f10a512823d1284ab',
    SCANNER+'apps/channels/tasks.py':'92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279',
    SCANNER+'core/tasks.py':'5e20b3a7395c9d78cc45f3b07abc1becc7d744a3f37e09735eac512ae537dcbc',
    PRODUCERS+'apps/plugins/apps.py':'27b3b52fad5c8ca2aaf1912ac4165b1b5c58ac438cb489ce1d6182c55ef32679',
    PRODUCERS+'dispatcharr/celery.py':'e478611b17bb46583e6104f0bef3cae04b16b67d0c8e6ada0babef9fc814eeb6',
}
IDENTITIES=(
    ('candidate-3b52cc39641faa6dda72','apps/plugins/tasks.py',9,'refresh_plugin_repos',PLUGIN,
     PRODUCERS+'apps/plugins/apps.py','refresh_plugin_repos.apply_async(countdown=10)',None),
    ('candidate-20cb0ac54bf6fbde0fd0','apps/channels/tasks.py',3107,'recover_recordings_on_startup',
     SCANNER+'apps/channels/tasks.py',PRODUCERS+'dispatcharr/celery.py','recover_recordings_on_startup.delay()','Recovery complete'),
    ('candidate-3d0b2d3fb3f98d68738d','core/tasks.py',1030,'check_for_version_update',
     SCANNER+'core/tasks.py',PRODUCERS+'dispatcharr/celery.py','check_for_version_update.delay()',None),
)
FIXTURE='retained-Original-C105-guide-startup-epoch'


def dependencies():
    return {INPUT,RECEIPT,*INPUTS}


def checked(root,path,expected):
    raw=(Path(root)/path).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==expected,path
    return raw


def derive(root):
    receipt=json.loads(checked(root,RECEIPT,RECEIPT_SHA))
    sources={path:checked(root,path,checksum) for path,checksum in INPUTS.items()}
    assert receipt['source_revision']==BASELINE and receipt['source_archive_sha256']==ARCHIVE
    assert receipt['candidate_revision']==CANDIDATE and receipt['actual_original_exit']==0
    assert receipt['monitor_UID']==65532 and receipt['owned_containers_retired']==2
    assert receipt['broker_ready_and_reserved_counts']==[0,0,0,0]
    assert receipt['enabled_plugin_repositories']==receipt['plugin_periodic_interval']==0
    assert receipt['worker']=='Original normal-default@owned Celery prefork worker for celery queue'
    assert all(receipt[key] is True for key in ('settled_exit_and_EOF','exact_original_cleanup',
                  'retained_date_only_output_passed','worker_master_generation_settled_with_EOF',
                  'monitor_settled_and_flushed','owned_closed_https_proxy_confirmed'))
    assert all(receipt[key] is False for key in ('per_task_worker_prerun_postrun_witness',
                  'full_milestone_accepted','native_coverage_increased','performance_verified'))
    assert len({row['task_id_sha256'] for row in receipt['rows']})==3
    actors={'producer':'pinned Original application or worker startup callback',
            'executor':receipt['worker'],'complete_role_matrix':False}
    fixture={'id':FIXTURE,'baseline_revision':BASELINE,'baseline_archive_sha256':ARCHIVE,
             'actual_controller_exit':0,'settled_exit_and_EOF':True,'exact_cleanup_passed':True,
             'worker_master_generation_settled_with_EOF':True,'per_task_worker_prerun_postrun_witness':False,
             'scope':'quiet owned schedules-direct startup before selected work; plugin and network paths fenced',
             'receipt_path':RECEIPT,'receipt_sha256':RECEIPT_SHA}
    contracts=[]
    for (identifier,path,line,name,source,publisher,call,result), row in zip(IDENTITIES,receipt['rows'],strict=True):
        task=path.removesuffix('.py').replace('/','.')+'.'+name
        assert row['task_name']==task and row['backend_status']=='SUCCESS' and row['backend_result']==result
        assert row['args']==[] and row['kwargs']=={} and row['backend_children']==0 and row['own_root_no_parent'] is True
        assert (row['routing_key'],row['broker_database'],row['result_database'],row['hard_soft_time_limits'])==('celery',1,2,[None,None])
        assert all(re.fullmatch('[a-f0-9]{64}',row[key]) for key in (
            'task_id_sha256','canonical_frame_sha256','canonical_result_metadata_sha256'))
        if name=='refresh_plugin_repos':
            assert 0<row['countdown_seconds_observed']<=10.1
            boundary='Repositories disabled with periodic interval zero; the startup delayed call still settles.'
        elif name=='check_for_version_update':
            assert row['countdown_seconds_observed'] is None
            boundary='The owned external HTTP proxy is closed; no successful upstream version query or notification is claimed.'
        else:
            assert row['countdown_seconds_observed'] is None
            boundary='Literal Recovery complete without children; no nonempty recording recovery phenotype is inferred.'
        [node]=[n for n in ast.parse(sources[source]).body if isinstance(n,ast.FunctionDef) and n.name==name]
        assert min(d.lineno for d in node.decorator_list)==line
        producer=sources[publisher].decode().splitlines()
        [producer_line]=[index for index,contents in enumerate(producer,1) if contents.strip()==call]
        contracts.append({
            'candidate_id':identifier,'source':{'kind':'task_candidate','path':path,'line':line,'name':name,'callable':'celery.shared_task'},
            'owning_milestone':'M6','task_name':task,'fixture_ids':[FIXTURE],'observed_actor':actors,
            'definition':{'path':source,'original_file_sha256':INPUTS[source],'start_line':line,
                          'end_line':node.end_lineno,'ast_sha256':ast_fingerprint(node)},
            'producer_definition':{'path':publisher,'original_file_sha256':INPUTS[publisher],'line':producer_line,'call':call},
            'observations':[row],
            'assertions':['Natural own-root zero-argument publication on the exact queue and matched actual Original backend result.',
                          'Owned worker master and broker observer settle with EOF and exact namespace cleanup.',boundary],
            'result':{'state':'partial_historical_Original_task_observations','actual_worker_observations':1,
                      'new_Source_cases_this_task':0,'Native_coverage_increased':False,'fully_verified':False},
            'fully_verified':False,
        })
    return {'format_version':1,'kind':KIND,'Original_revision':BASELINE,'Original_archive_sha256':ARCHIVE,
            'credit':'Original Dispatcharr, Celery and dependency authors retain full application and behavior credit.',
            'fixtures':{FIXTURE:fixture},'contracts':[],'non_HTTP_contracts':contracts,
            'actual_Original_worker_observations':3,'new_Source_executions':0,'fully_verified_operations':0,
            'Native_coverage_increased':False,'performance_verified':False}


def validate(root,packet,_bindings=None):
    assert packet==derive(root)


def restore_pre_c105_startup(rows):
    selected={item[0] for item in IDENTITIES}
    for row in rows:
        if row['candidate_id'] in selected:
            assert row['executable_Source_contract']['candidate_id']==row['candidate_id']
            for key in ('setup_fixture','owning_milestone','assertions','executable_Source_contract'):
                del row[key]
            row.update(result=None,state='static_candidate_requires_runtime_contract')
