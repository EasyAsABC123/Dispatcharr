"""Qualify one natural Original create-backup worker execution from Source16."""
import ast
import base64
import hashlib
import io
import json
from pathlib import Path
import tarfile

from framework_auth_contracts import ast_fingerprint

KIND='retained_original_Source16_backup_create_task'
INPUT='scripts/rust-contracts/retained-backup-create-task/bindings.json'
AUTHORITY='scripts/rust-contracts/backup-source16/'
SOURCE='executed-definition/inputs/Original/apps/backups/'
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
TASK='apps.backups.tasks.create_backup_task'
IDENTIFIER='candidate-b745b2ca10c3763b5a5a'
FIXTURE='retained-Original-Source16-backup-create'
FILES={
    'Source16-projection.json': '5f0ce76f0d308da14cce1c0f57899577dd14732e1bc818ee916864d2d1902760',
    'Root-actual-exit.json': '04158922ff0bd9c4d15ec2b626db01867fd63b248aeca3e8eea45db9fd46b183',
    'Root-actual-review.json': 'f9b252b69122e8d9cc0f6e0f9688583ea81ac06d49bba01727fcbb7b36b92f83',
    'archive-proof.json': '32336e35e3b1decab3bb0be850e82502d3f474369f31ec95dcaf8a10656cdf12',
    'lifecycle-projection.tar.gz': '15a18a2d7c0017c00aeab0250bba6652a2e866152aa533799bcd8fb69f4daeba',
    'lifecycle-projection-inventory.json': '2760beb7c10f23da3b8d4abbb7d66d334f2bf7e3c88354dd2a55dcf837d9ac4b',
    'executed-definition/inputs/Original/apps/backups/tasks.py': 'b3376d03f807fbb2540e28d44f2a9c8f1982fb317953bef800a2d42691bf3742',
    'executed-definition/inputs/Original/apps/backups/api_views.py': '92dced3d20743862066dfeccb1728ca4f1fd508aecb7489686dd03c66939972a',
}


def dependencies():
    return {INPUT,*(AUTHORITY+name for name in FILES)}


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def files(root):
    result={}
    for name,checksum in FILES.items():
        data=(Path(root)/(AUTHORITY+name)).read_bytes()
        assert digest(data)==checksum,name
        result[name]=data
    return result


def json_member(archive, inventory, name):
    member=archive.getmember(name)
    assert member.isfile() and 0<member.size<=4000000
    with archive.extractfile(member) as reader:
        raw=reader.read()
    recorded=inventory[name]
    assert (len(raw),digest(raw))==(recorded['projected_bytes'],recorded['projected_sha256'])
    return json.loads(raw)


def frames(snapshot):
    broker=snapshot['redis'][1]
    assert broker['database']==2
    return [(bytes.fromhex(row['key_hex']).decode(),json.loads(bytes.fromhex(raw).decode()))
            for row in broker['rows'] if row['type']=='list' for raw in row['list_hex']]


def worker_observation(inputs):
    report=json.loads(inputs['Source16-projection.json'])
    assert report['Original']==REVISION and report['capture_valid'] is True
    assert report['actual_request_count']==16 and report['Source_TCP_or_Native'] is False
    assert report['startup_skip'] is True and report['force_authenticate_used'] is False
    create=report['requests'][3]
    assert create['name']=='admin_create_ignores_malformed_JSON_enqueues_one'
    assert create['method']=='POST' and create['status']==202 and create['capture_complete'] is True
    assert frames(create['before'])==[]
    [(queue,frame)]=frames(create['after'])
    before=report['before_Celery_configuration']
    after=report['after_Celery_configuration']
    assert queue==before['actual_create_route_queue']==before['owned_queue']==after['owned_queue']
    assert before['producer_only_owned_route_added'] is True and after['producer_only_owned_route_added'] is False
    assert after['actual_create_route_queue']=='celery'
    assert before['full_startup_or_default_routing_claim'] is after['full_startup_or_default_routing_claim'] is False
    task_id=create['json']['task_id']
    assert frame['headers']['id']==frame['headers']['root_id']==task_id
    assert frame['headers']['task']==TASK and frame['headers']['parent_id'] is None
    assert frame['properties']['delivery_info']['routing_key']==queue
    arguments=json.loads(base64.b64decode(frame['body'],validate=True))
    assert arguments[:2]==[[],{}]

    inventory=json.loads(inputs['lifecycle-projection-inventory.json'])
    with tarfile.open(fileobj=io.BytesIO(inputs['lifecycle-projection.tar.gz']),mode='r:gz') as archive:
        ready=json_member(archive,inventory,'worker-ready.json')
        pre=json_member(archive,inventory,'worker-prerun.json')
        post=json_member(archive,inventory,'worker-postrun.json')
        returned=json_member(archive,inventory,'worker-returned.json')
        execution=json_member(archive,inventory,'execution.json')
        cleanup=json_member(archive,inventory,'independent/cleanup.json')
    assert ready['consumer_queues']==[queue] and pre['delivery_info']['routing_key']==queue
    assert ready['passive_observer'] is pre['passive_observer'] is post['passive_observer'] is True
    assert pre['task_id']==pre['request']['id']==pre['request']['root_id']==post['task_id']==task_id
    assert pre['request']['is_eager'] is False and pre['request']['parent_id'] is None
    assert pre['task_name']==post['task_name']==TASK and post['state']=='SUCCESS'
    assert returned['consumed_task_ids']==[task_id] and returned['connections_close_called'] is True
    backend_raw=bytes.fromhex(report['actual_completed_backend']['raw_hex'])
    backend=json.loads(backend_raw)
    result=post['retval']
    assert backend['task_id']==task_id and backend['status']=='SUCCESS' and backend['children']==[]
    assert backend['result']==result and result['status']=='completed'

    archive_proof=json.loads(inputs['archive-proof.json'])
    download=report['requests'][-1]
    assert archive_proof['raw_archive_omitted'] is archive_proof['derived_from_private_original_reviewed_by_Root'] is True
    assert archive_proof['original_stream_bytes']==result['size']==download['body_bytes']
    assert archive_proof['original_stream_sha256']==download['body_sha256']
    assert archive_proof['database_dump_magic_hex']=='5047444d50'
    assert [row['name'] for row in archive_proof['members']]==['database.dump','metadata.json']
    assert archive_proof['metadata']['database_type']=='postgresql'
    assert execution['Original_revision']==REVISION and execution['controller_exit']==0
    assert execution['passed'] is execution['owned_cleanup_independently_verified'] is True
    assert execution['host_children'] and all(row['exit']==0 and row['physically_settled'] is True
                                              and row['pipes_settled'] is True for row in execution['host_children'])
    assert cleanup['passed'] is True and len(cleanup['queries'])==4 and cleanup['PIDs']
    assert all(row['original_process_absent'] is True for row in cleanup['PIDs'])
    host=json.loads(inputs['Root-actual-exit.json'])
    review=json.loads(inputs['Root-actual-review.json'])
    assert host['exit']==review['actual_host_wait_exit']==0 and host['wait_completed'] is True
    assert review['passed'] is True and review['real_worker_tasks']==1 and review['pure_effect_checks']==144
    assert all(review[key] is True for key in ('worker_actual_exit0_wait_pipes','actual_PGDMP_ZIP_stream_validated',
              'request_SQL11_users_events_and_default11_bytes_exact','owned_SQL_backends_absent_after_worker',
              'all_host_children_actual0_settled','independent_project_CID_and_host_PIDs_absent'))
    return {
        'task_name':TASK,'args':[],'kwargs':{},'task_id_sha256':digest(task_id.encode()),
        'routing_scope':'producer-only owned worker queue','broker_database':2,'own_root_no_parent':True,
        'pre_and_post_passive_signals_match':True,'backend_status':'SUCCESS','backend_result':'completed',
        'backend_result_sha256':digest(backend_raw),'archive_format':'PostgreSQL PGDMP in Dispatcharr V2 ZIP',
        'archive_bytes':result['size'],'archive_sha256':archive_proof['original_stream_sha256'],
        'retained_archive_members':archive_proof['members'],'raw_archive_redecoded':False,
        'default11_SQL_users_and_events_unchanged':True,'owned_cleanup_passed':True,
    }


def derive(root):
    inputs=files(root)
    observation=worker_observation(inputs)
    definition=SOURCE+'tasks.py'
    producer=SOURCE+'api_views.py'
    [task]=[node for node in ast.parse(inputs[definition]).body
            if isinstance(node,ast.FunctionDef) and node.name=='create_backup_task']
    assert min(node.lineno for node in task.decorator_list)==35
    assert ast.unparse(task.decorator_list[0])=='shared_task(bind=True)'
    [call]=[node for node in ast.walk(ast.parse(inputs[producer])) if isinstance(node,ast.Call)
            and ast.unparse(node)=='create_backup_task.delay()']
    fixture={'id':FIXTURE,'baseline_revision':REVISION,'actual_controller_exit':0,
             'owned_cleanup_passed':True,'APIClient_Original_TCP':False,
             'normal_worker_backend_and_archive_effect':True,'normal_startup_and_default_queue':False,
             'authority_paths':{AUTHORITY+path:sha for path,sha in FILES.items()}}
    contract={'candidate_id':IDENTIFIER,'source':{'kind':'task_candidate','path':'apps/backups/tasks.py',
              'line':35,'name':'create_backup_task','callable':'celery.shared_task'},
              'owning_milestone':'M8','task_name':TASK,'fixture_ids':[FIXTURE],
              'observed_actor':{'producer':'Original authenticated admin create view',
                                'executor':'Original owned Celery solo worker','complete_role_matrix':False},
              'definition':{'path':AUTHORITY+definition,'start_line':35,'end_line':task.end_lineno,
                            'ast_sha256':ast_fingerprint(task)},
              'producer_definition':{'path':AUTHORITY+producer,'line':call.lineno,'call':ast.unparse(call)},
              'observations':[observation],
              'assertions':['Exact natural empty-argument publication from the create view and one matched owned worker execution.',
                            'Actual backend SUCCESS and independently reviewed PostgreSQL V2 archive with unchanged SQL, users and events.',
                            'Exact owned lifecycle and cleanup. Queue override and startup skips bound this result.'],
              'result':{'state':'partial_historical_Original_task_observations','actual_worker_observations':1,
                        'new_Source_cases_this_task':0,'Native_coverage_increased':False,'fully_verified':False},
              'fully_verified':False}
    return {'format_version':1,'kind':KIND,'Original_revision':REVISION,
            'credit':'Original Dispatcharr, Celery, PostgreSQL and dependency authors retain full application and behavior credit.',
            'fixtures':{FIXTURE:fixture},'contracts':[],'non_HTTP_contracts':[contract],
            'new_Source_executions':0,'fully_verified_operations':0,
            'Native_coverage_increased':False,'performance_verified':False}


def validate(root, value, bindings=None):
    assert value==derive(root),'Retained Original Source16 backup task observation mismatch'
