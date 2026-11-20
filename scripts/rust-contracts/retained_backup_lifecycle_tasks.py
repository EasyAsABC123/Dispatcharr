"""Bind the actual scoped Original scheduled and ordinary restore task observations."""
import ast
import json
from pathlib import Path
import re

from framework_auth_contracts import ast_fingerprint
from retained_backup_create_task import AUTHORITY, REVISION, SOURCE, digest

KIND='retained_original_Backup_Source12_and_41_lifecycle_tasks'
PREFIX='scripts/rust-contracts/retained-backup-lifecycle/'
INPUT=PREFIX+'bindings.json'
RECEIPT=PREFIX+'receipt.json'
RECEIPT_SHA='f5891f39893bdee887582708b705fb588707abd5e258836c525c53383a351d35'
SOURCES={
    'api_views.py':'92dced3d20743862066dfeccb1728ca4f1fd508aecb7489686dd03c66939972a',
    'scheduler.py':'910fe81b17eeea2963c1a3dd242068729618decc8145ac46c31ab58679233e7b',
    'tasks.py':'b3376d03f807fbb2540e28d44f2a9c8f1982fb317953bef800a2d42691bf3742',
}
CONTEXTS=(
    ('scheduled','candidate-8981d161337a42b1cec6','scheduled_backup_task',81,'scheduler.py',127,
     'Original Beat periodic task backup-scheduled-task','Original normal Celery worker queue celery',
     ['Two actual default Beat queue publications with no positional args and retention_count 1.',
      'Authenticated status on the published identities reports UTC completed and Honolulu failed.',
      'UTC PostgreSQL archive survives one retention deletion; Honolulu fresh archive is deleted by filename retention.']),
    ('restore','candidate-f503ee82e11bba950149','restore_backup_task',56,'api_views.py',326,
     'Original authenticated admin filename restore view','Original owned normal Celery worker',
     ['Actual restore 202 identities match independently polled Redis backend and terminal HTTP status.',
      'Ordinary current-schema restore holds a real SQL lock; migration restores permissions and the same Main reads the recovered data.',
      'Invalid owned archive reaches backend SUCCESS with a caught failed result and unchanged SQL.',
      'The filename publication call is Source-defined; broker frame and worker signals were not captured in this context.']),
)


def dependencies():
    return {INPUT,RECEIPT,*(AUTHORITY+SOURCE+name for name in SOURCES)}


def check_observations(receipt):
    assert receipt['source_revision']==REVISION and receipt['format_version']==1
    for key,count,cids in (('scheduled',12,3),('restore',5,2)):
        context=receipt[key]
        assert context['source_named_contexts_passed']==count
        assert context['whole_controller_exit']==1 and context['whole_run_accepted'] is False
        assert context['EOF_and_process_group_settled'] is context['exact_cleanup'] is True
        assert context['owned_CIDs_absent']==cids and len(context['observations'])==2
        assert len({row['task_id_sha256'] for row in context['observations']})==2
        assert all(re.fullmatch('[a-f0-9]{64}',context[k]) for k in ('cleanup_sha256','execution_sha256','report_sha256'))
    scheduled=receipt['scheduled']
    utc,honolulu=scheduled['observations']
    for row,zone,state in ((utc,'UTC','completed'),(honolulu,'Pacific/Honolulu','failed')):
        assert row['timezone']==zone and row['actual_authenticated_terminal_status']==state
        assert row['args']==[] and row['kwargs']=={'retention_count':1} and row['own_root'] is True
        assert (row['queue'],row['periodic_task_name'])==('celery','backup-scheduled-task')
        assert all(re.fullmatch('[a-f0-9]{64}',row[k]) for k in (
            'task_id_sha256','publication_sha256','terminal_body_sha256'))
    assert utc['deleted_count']==1 and utc['archive']['database_signature']=='PGDMP'
    assert utc['archive']['members']==['database.dump','metadata.json'] and utc['archive']['archive_bytes']>0
    assert honolulu['fresh_scheduled_file_deleted'] is honolulu['prior_UTC_archive_preserved'] is True
    assert honolulu['raw_backend_status_observed'] is False and scheduled['schedule_disabled_after'] is True
    restore=receipt['restore']
    assert restore['restored_migrated_builtin_permissions']==4
    assert all(restore[k] is True for k in ('actual_SQL_lock_and_retirement',
        'restored_same_Main_login_settings_channel','invalid_owned_archive_SQL_unchanged'))
    for row,state in zip(restore['observations'],('completed','failed'),strict=True):
        assert (row['producer_method'],row['producer_status'],row['backend_state'],row['task_result_state'])==(
            'POST',202,'SUCCESS',state)
        assert row['broker_frame_captured'] is False
        assert all(re.fullmatch('[a-f0-9]{64}',row[k]) for k in ('task_id_sha256','producer_body_sha256'))


def derive(root):
    root=Path(root)
    raw=(root/RECEIPT).read_bytes()
    assert digest(raw)==RECEIPT_SHA
    receipt=json.loads(raw)
    check_observations(receipt)
    source={}
    for name,expected in SOURCES.items():
        raw=(root/(AUTHORITY+SOURCE+name)).read_bytes()
        assert digest(raw)==expected
        source[name]=ast.parse(raw)
    fixtures={}
    contracts=[]
    for key,identifier,name,line,producer_file,producer_line,actor,worker,assertions in CONTEXTS:
        [task]=[node for node in source['tasks.py'].body if isinstance(node,ast.FunctionDef) and node.name==name]
        assert min(node.lineno for node in task.decorator_list)==line
        assert ast.unparse(task.decorator_list[0])=='shared_task(bind=True)'
        if key=='scheduled':
            [call]=[node for node in ast.walk(source[producer_file]) if isinstance(node,ast.Call)
                    and isinstance(node.func,ast.Name) and node.func.id=='create_or_update_periodic_task']
            assert any(kw.arg=='celery_task_path' and kw.value.value=='apps.backups.tasks.'+name
                       for kw in call.keywords)
            assert any(kw.arg=='kwargs' and ast.unparse(kw.value)=="{'retention_count': settings['retention_count']}"
                       for kw in call.keywords)
        else:
            [call]=[node for node in ast.walk(source[producer_file]) if isinstance(node,ast.Call)
                    and ast.unparse(node)=='restore_backup_task.delay(filename)']
        assert call.lineno==producer_line
        fixture_id='retained-Original-backup-'+key
        actual=receipt[key]
        fixtures[fixture_id]={
            'id':fixture_id,'baseline_revision':REVISION,'receipt_path':RECEIPT,'receipt_sha256':RECEIPT_SHA,
            **{key:value for key,value in actual.items() if key!='observations'},
        }
        contracts.append({
            'candidate_id':identifier,
            'source':{'kind':'task_candidate','path':'apps/backups/tasks.py','line':line,'name':name,
                      'callable':'celery.shared_task'},
            'owning_milestone':'M8','task_name':'apps.backups.tasks.'+name,'fixture_ids':[fixture_id],
            'observed_actor':{'producer':actor,'executor':worker,'complete_role_matrix':False},
            'definition':{'path':AUTHORITY+SOURCE+'tasks.py','start_line':line,
                          'end_line':task.end_lineno,'ast_sha256':ast_fingerprint(task)},
            'producer_definition':{'path':AUTHORITY+SOURCE+producer_file,'line':producer_line,
                                   'call':ast.unparse(call)},
            'observations':actual['observations'],'assertions':assertions,
            'result':{'state':'partial_historical_Original_task_observations','actual_task_terminal_observations':2,'direct_worker_signals_observed':False,
                      'new_Source_cases_this_task':0,'Native_coverage_increased':False,'fully_verified':False},
            'fully_verified':False,
        })
    return {'format_version':1,'kind':KIND,'Original_revision':REVISION,
            'credit':'Original Dispatcharr, Celery, PostgreSQL and dependency authors retain full application and behavior credit.',
            'fixtures':fixtures,'contracts':[],'non_HTTP_contracts':contracts,'new_Source_executions':0,
            'fully_verified_operations':0,'Native_coverage_increased':False,'performance_verified':False}


def validate(root,value,bindings=None):
    assert value==derive(root),'Retained Original backup lifecycle observations mismatch'
