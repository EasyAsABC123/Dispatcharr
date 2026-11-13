"""SYNTHETIC pure-control specimen, never an application observation."""
import base64,copy,datetime,hashlib,io,json,zipfile
from pathlib import Path
from validate_native import token,SOURCE_SHA,ORIGINAL_SHA,TABLES
ROOT=Path(__file__).resolve().parents[1]
def sample():
 source=json.loads((ROOT/'inputs/Source16-projection.json').read_text());plan=json.loads((ROOT/'inputs/plan.json').read_text());nonce='abcdef1234';db='rust_probe_backupjob_native_'+nonce;base=datetime.datetime(2026,11,13,4,tzinfo=datetime.timezone.utc);stamp=lambda n:(base+datetime.timedelta(seconds=n)).isoformat();users=[];actors={}
 for role,row in zip(plan['roles'],source['seed_users']):
  alias=role['alias'];n=dict(row,username='rust-probe-backupjob-native-'+nonce+'-'+alias,api_key='backup-native-'+nonce+'-'+alias,password='!',date_joined=stamp(0));users.append(n);actors[alias]=n
 SQL={'tables':{t:users if t=='accounts_user'else []for t in TABLES},'sequences':{t:{'last_value':3 if t=='accounts_user'else 1,'is_called':t=='accounts_user'}for t in TABLES}}
 id='12345678-1234-4234-8234-123456789abc';name='dispatcharr-backup-2026.11.13.04.00.24.zip';metadata={'format':'dispatcharr-backup','version':2,'database_type':'postgresql','database_file':'database.dump','created_at':stamp(25)};b=io.BytesIO()
 with zipfile.ZipFile(b,'w')as z:z.writestr('database.dump',b'PGDMP-SYNTHETIC-NOT-A-REAL-DATABASE');z.writestr('metadata.json',json.dumps(metadata))
 archive=b.getvalue();result={'status':'completed','filename':name,'size':len(archive)};backend={'status':'SUCCESS','result':result,'traceback':None,'children':[],'date_done':stamp(26),'task_id':id};frame={'headers':{'id':id,'task':'apps.backups.tasks.create_backup_task'},'properties':{'delivery_info':{'exchange':'','routing_key':'celery'}},'body':base64.b64encode(b'[[],{},{}]').decode()}
 def state(src,queue=False,done=False):
  scopes=copy.deepcopy(src['redis']);scopes[1]['rows']=([{'key_hex':b'celery'.hex(),'type':'list','dump_hex':'00','pttl':-1,'list_hex':[json.dumps(frame).encode().hex()]}]if queue else []);
  if queue or done:scopes[1]['rows'].append({'key_hex':b'_kombu.binding.celery'.hex(),'type':'set','dump_hex':'010203','pttl':-1})
  scopes[2]['rows']=([{'key_hex':('celery-tasks:celery-task-meta-'+id).encode().hex(),'type':'string','dump_hex':'00','pttl':3599000,'value_hex':json.dumps(backend).encode().hex()}]if done else []);return{'SQL':copy.deepcopy(SQL),'users':copy.deepcopy(users),'events':[],'redis':scopes,'files':[{'name':name,'bytes':len(archive),'sha256':hashlib.sha256(archive).hexdigest()}]if done else []}
 seed=state(source['seed_snapshot']);native={'capture_complete':True,'passed':False,'capture_only_passed_field':True,'actual_request_count':16,'Source_application_runs':0,'Main_application_runs':0,'Source_reference_mode':'projected-backup-source16-reference-v1','Source_reference_sha256':SOURCE_SHA,'Original_Source_raw_sha256':ORIGINAL_SHA,'database':db,'native_actual_queue':'celery','Source_fixture_queue_override':source['before_Celery_configuration']['owned_queue'],'seed':seed,'seed_clock':stamp(0),'Source_cache_context_seed':{'before_owned_cache_keys':0,'strict_three_key_allowlist':True,'auth_history_or_events_reset':False,'before_any_request_or_auth_worker':True,'rows':[{'key_hex':r['key_hex'],'Source_dump_sha256':hashlib.sha256(bytes.fromhex(r['dump_hex'])).hexdigest(),'restored_dump_sha256':hashlib.sha256(bytes.fromhex(r['dump_hex'])).hexdigest(),'Source_seed_pttl':r['pttl'],'RESTORE_ttl':0 if r['pttl']==-1 else r['pttl'],'actual_readback_pttl':r['pttl']}for r in source['seed_snapshot']['redis'][0]['rows']]},'requests':[],'worker_began_utc':stamp(24),'worker_completed_utc':stamp(26),'actual_result_metadata':{'task_id':id,'result':result,'date_done':stamp(26)},'worker_run':'Completed('+id+')','HTTP_listener_joined':True,'backups_shutdown':True}
 native['initial']=copy.deepcopy(seed);native['initial']['redis'][0]['rows']=[]
 aliases={'unknown_task':'never-created-'+nonce};from validate_native import expected_path
 for spec,src in zip(plan['cases'],source['requests']):
  i=spec['ordinal'];path=expected_path(spec,aliases);headers={}
  if 'x_api_key'in spec['auth']:headers['x-api-key']=actors[spec['auth']['x_api_key']]['api_key'].encode().hex()
  if 'bearer_literal'in spec['auth']:headers['authorization']=('Bearer '+spec['auth']['bearer_literal']).encode().hex()
  value=copy.deepcopy(src['json'])
  if i==4:value={'detail':'Backup started','task_id':id,'task_token':token(id)};aliases['created_task']=id
  if i in(13,14):value={'state':'completed','result':result};aliases['completed_filename']=name
  if i==15:value=[{'name':name,'size':len(archive),'created':stamp(25)}]
  raw=archive if i==16 else json.dumps(value,separators=(',',':')).encode();challenge={k.lower():v.encode().hex()for k,v in src['headers'].items()if k.lower()=='www-authenticate'};native['requests'].append({'ordinal':i,'name':spec['name'],'method':spec['method'],'path':path,'auth_selectors':spec['auth'],'raw_headers_private':headers,'status':src['status'],'body_hex':raw.hex(),'body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest(),'json':value,'headers_hex':challenge,'began_utc':stamp(2*(i-1)),'completed_utc':stamp(2*i-1),'before':state(src['before'],queue=5<=i<=12,done=i>=13),'after':state(src['after'],queue=4<=i<=12,done=i>=13)})
 native['native_only_family_guards']={'controls':5,'all_UnsupportedTask':True,'before':copy.deepcopy(native['requests'][-1]['after']),'after':copy.deepcopy(native['requests'][-1]['after'])};native['final']=copy.deepcopy(native['requests'][-1]['after']);return native,source,plan
