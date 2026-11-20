"""Typed Original Auto Sync task with exact bounded no-change queue evidence."""
import ast
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re

from framework_auth_contracts import ast_fingerprint

KIND='retained_original_Source28_autosync_standalone_task'
FOLDER='scripts/rust-contracts/retained-autosync28-task/'
INPUT=FOLDER+'bindings.json'
RECEIPT=FOLDER+'receipt.json'
RECEIPT_SHA='ba3d269f503d2f981d46e9e2eb93ff2fc378d518b3d70e21225aee608bc3e348'
DEFINITION='scripts/rust-contracts/scanner-tasks/original/apps/m3u/tasks.py'
SOURCE_SHA='921e0fea7273c0ce7d1b1380a4f675718fbc5cdb3bb0d38005cdae2b73cce981'
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IDENTIFIER='candidate-9a57d4d8a58c627531e8'
TASK='apps.m3u.tasks.sync_auto_channels'


def dependencies():
    return {INPUT,RECEIPT,DEFINITION}


def check_receipt(receipt):
    assert receipt['format_version']==1 and receipt['original_revision']==REVISION
    assert receipt['named_original_cells_passed']==5 and receipt['whole_controller_exit']==1
    assert receipt['exact_cleanup'] is receipt['EOF_group_settled'] is True and receipt['owned_CIDs_absent']==2
    raw=receipt['raw_sha256']
    assert raw['SQL_before']==raw['SQL_after']
    assert all(re.fullmatch('[a-f0-9]{64}',checksum) for checksum in raw.values())
    task=receipt['standalone']
    assert task['task']==TASK and task['queue']=='celery' and task['own_root_no_parent'] is True
    account,scope=task['args']
    assert type(account) is int and account>0 and datetime.fromisoformat(scope).utcoffset() is not None
    assert task['kwargs']=={} and task['backend_state']=='SUCCESS' and task['SQL_no_change'] is True
    assert task['production_HTTP_or_Beat_created_standalone_root'] is task['reexecution_or_restart_observed'] is False
    assert task['backend_result']=={'status':'ok','channels_created':0,'channels_updated':0,
                                  'channels_deleted':0,'channels_failed':0,'failed_stream_details':[]}
    inline=receipt['inline']
    assert inline['production_refresh_root_observed'] is True and inline['separate_auto_sync_frame_observed'] is False
    assert inline['channels_after']==6001 and inline['children_after']==3
    assert all(re.fullmatch('[a-f0-9]{64}',inline[key]) for key in ('parent_id_sha256','normalized_SQL_sha256'))


def derive(root):
    raw=(Path(root)/RECEIPT).read_bytes()
    code=(Path(root)/DEFINITION).read_bytes()
    assert hashlib.sha256(raw).hexdigest()==RECEIPT_SHA and hashlib.sha256(code).hexdigest()==SOURCE_SHA
    receipt=json.loads(raw)
    check_receipt(receipt)
    tree=ast.parse(code)
    [node]=[item for item in tree.body if isinstance(item,ast.FunctionDef) and item.name=='sync_auto_channels']
    assert ast.unparse(node.decorator_list[0])=='shared_task' and node.decorator_list[0].lineno==2187
    [refresh]=[item for item in ast.walk(tree) if isinstance(item,ast.Call) and isinstance(item.func,ast.Name)
               and item.func.id=='sync_auto_channels' and item.lineno==3958]
    fixture={'id':'Original-batch28-auto-sync','original_revision':REVISION,
             'receipt_path':RECEIPT,'receipt_sha256':RECEIPT_SHA,'whole_controller_actual_exit':1,
             'all_five_named_cells_passed':True,'exact_cleanup':True,'standalone_after_prune_policy_noop':True}
    contract={'candidate_id':IDENTIFIER,'source':{'kind':'task_candidate','path':'apps/m3u/tasks.py',
                                                'line':2187,'name':'sync_auto_channels','callable':'celery.shared_task'},
              'owning_milestone':'M6','task_name':TASK,'fixture_ids':[fixture['id']],
              'observed_actor':{'producer':receipt['standalone']['producer'],'executor':receipt['standalone']['executor'],
                                'complete_role_matrix':False},
              'definition':{'path':DEFINITION,'start_line':2187,'ast_sha256':ast_fingerprint(node)},
              'inline_source_call':{'path':DEFINITION,'line':refresh.lineno,'call':ast.unparse(refresh)},
              'observations':[receipt['standalone']],'separate_inline_workflow_scope':receipt['inline'],
              'assertions':['Actual two positional arguments and own-root broker/frame/backend identities join.',
                            'SUCCESS and exact no-change task result with byte-identical owned SQL snapshot.',
                            'The normal refresh workflow calls the same task body inline and publishes guide children.',
                            'The standalone root came from an owned management fixture; no restart evidence is inferred.'],
              'result':{'state':'partial_historical_Original_task_observations','actual_task_terminal_observations':1,
                        'new_Source_cases_this_task':0,'Native_coverage_increased':False,'fully_verified':False},
              'fully_verified':False}
    return {'format_version':1,'kind':KIND,'Original_revision':REVISION,
            'credit':'Original Dispatcharr, Celery, Kombu and dependency authors retain full credit.',
            'fixtures':{fixture['id']:fixture},'contracts':[],'non_HTTP_contracts':[contract],
            'new_Source_executions':0,'fully_verified_operations':0,'Native_coverage_increased':False,
            'performance_verified':False}


def validate(root,value,bindings=None):
    assert value==derive(root),'Retained Original Auto Sync task observations mismatch'
