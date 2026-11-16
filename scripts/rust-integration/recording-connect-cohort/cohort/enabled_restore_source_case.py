"""New Source authority cell inside existing operations batch/Fixture only.
No Source body calls, monkeypatch, nested backend or schema/migrate replay.
Original Dispatcharr and dependencies retain complete application/library credit.
"""
import base64,hashlib,json,uuid
from pathlib import Path
import backup_restore_cases as normal
from normal_backup_case import private_api,archive_gate
CASE='normal-backup-restore-enabled-source'
KEY='owned_restore_probe'
def prepare_context(f):
 if f.case!=CASE or f.backend!='Original':raise ValueError('IndependentOriginalRestoreContextRequired')
 source=Path(__file__).with_name('restore_probe_plugin.py').read_bytes();target='/shared/'+f.case
 guest="from pathlib import Path;import os,base64,json,hashlib;root=Path("+repr(target)+");b=root/'backups';b.mkdir();os.chown(b,65532,65532);b.chmod(0o755);a=Path('/data/backups');\nif a.is_symlink():a.unlink()\nelif a.exists():\n if any(a.iterdir()):raise RuntimeError('UnownedBackupDirectory')\n a.rmdir()\na.symlink_to(b,target_is_directory=True);p=root/'plugins'/"+repr(KEY)+";p.mkdir();s=p/'plugin.py';s.write_bytes(base64.b64decode("+repr(base64.b64encode(source).decode())+"));[(os.chown(x,65532,65532),x.chmod(0o755 if x.is_dir()else 0o444))for x in(p,s)];print(json.dumps({'sha256':hashlib.sha256(s.read_bytes()).hexdigest()}))"
 assert json.loads(f.python(guest,secret=True))['sha256']==hashlib.sha256(source).hexdigest()
 f.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;from apps.plugins.models import PluginConfig;PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();s,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});s.value={**(s.value or {}),'schedule_enabled':False};s.save();PluginConfig.objects.update_or_create(key='owned_restore_probe',defaults={'name':'Owned restore probe','enabled':False,'ever_enabled':False,'owned_tasks':[],'suspended_schedules':[]});\nwith connection.cursor()as c:c.execute('CREATE TABLE owned_restore_probe(ordinal bigserial PRIMARY KEY,phase text NOT NULL,payload jsonb NOT NULL)')\nprint(json.dumps({'owned_plugin_disabled_before_backend':True}))")
def decoded_jsonb_object(value):
 # Pinned Django raw PostgreSQL cursor returns JSONB text; ORM JSONField
 # values may already be decoded. Decode one string layer only.
 if isinstance(value,str):value=json.loads(value)
 if not isinstance(value,dict):raise TypeError('ActualJSONBObjectRequired')
 return value
def run(f):
 if f.case!=CASE or f.backend!='Original':raise ValueError('IndependentOriginalRestoreContextRequired')
 out=Path(f.output)/'enabled-restore-source';out.mkdir();(out/'private').mkdir(mode=0o700)
 report={'state':'running','full_restore_parity':False,'performance_verified':False,'consumer_scope':'explicit user-module import, prefork4; default plugin bootstrap parity not claimed'};normal.save(out/'report.json',report)
 # Standard Celery include registers the USER task before strategies are built.
 # No task function or backend/migrate body is called by this controller.
 report['prior_worker_stop']=f.role('celery',[],stop=True)
 f.role('celery',['/dispatcharrpy/bin/celery','-A','dispatcharr','worker','--pool=prefork','--concurrency=4','--include','owned_restore_probe.plugin','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned'],extra={'PYTHONPATH':'/app:/data/plugins'})
 def registered():
  value=json.loads(f.python("import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';from dispatcharr.celery import app;print(json.dumps(app.control.inspect(destination=['normal-default@owned'],timeout=1).registered()))"));names=value.get('normal-default@owned')if isinstance(value,dict)else None
  return {'registered':True,'actual_count':len(names)}if isinstance(names,list)and len(names)<=512 and'owned_restore_probe.physical_child'in names else False
 report['actual_prepublication_registration']=f.wait(registered,20,'ActualSourceRestoreUserTaskRegistered')
 def actual_all_children_initialized():
  stats=json.loads(f.python("import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';from dispatcharr.celery import app;print(json.dumps(app.control.inspect(destination=['normal-default@owned'],timeout=1).stats()))",secret=True));item=stats.get('normal-default@owned')if isinstance(stats,dict)else None;pool=item.get('pool',{})if isinstance(item,dict)else{};pids=pool.get('processes')
  if not isinstance(pids,list)or len(pids)!=4 or any(not isinstance(x,int)for x in pids):return False
  observed=f.sql("with connection.cursor()as c:c.execute(\"SELECT payload FROM owned_restore_probe WHERE phase='worker_process_init_seen' ORDER BY ordinal\");rows=c.fetchall()\nprint(json.dumps([r[0]for r in rows]))")
  observed=[decoded_jsonb_object(x)for x in observed]
  seen={x['worker_pid']for x in observed};return {'actual_inspect_pool_pids':pids,'actual_user_init_SQL_markers':[x for x in observed if x['worker_pid']in pids],'before_archived_enabled_setup':True}if set(pids)<=seen else False
 report['actual_all_prefork_children_initialized_disabled']=f.wait(actual_all_children_initialized,30,'ActualFourSourceChildrenInitializedWhileDisabled')
 normal.save(out/'report.json',report)
 # Declared fixture input: archived enabled state differs from the live disabled
 # registry. Only the REAL restore task/migrate receiver may perform discovery.
 f.sql("from apps.plugins.models import PluginConfig;PluginConfig.objects.filter(key='owned_restore_probe').update(enabled=True,ever_enabled=True);print(json.dumps(True))")
 prefix=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task('owned-prefix-calibration');k=k.decode()if isinstance(k,bytes)else k;print(json.dumps(k[:-len('owned-prefix-calibration')]))")
 code,value=private_api(f,out,'actual-create','POST','/api/backups/create/',{});assert code==202
 created=normal.terminal(f,out,value['task_id'],value['task_token'],prefix);assert created['metadata']['status']=='SUCCESS'and created['metadata']['result']['status']=='completed'
 filename=created['metadata']['result']['filename'];report['causal_archive']=archive_gate(f,filename)
 f.sql("from apps.plugins.models import PluginConfig;PluginConfig.objects.filter(key='owned_restore_probe').update(enabled=False);print(json.dumps(True))")
 task,token=normal.begin(f,out,filename,'actual-enabled-restore');report['actual_restore_task']=task;normal.save(out/'report.json',report)
 terminal=normal.terminal(f,out,task,token,prefix);private=out/'private'/'actual-terminal.json';normal.save(private,terminal);private.chmod(0o600);report['actual_terminal']={'backend_status':terminal['metadata'].get('status'),'result_status':terminal['metadata'].get('result',{}).get('status'),'private_receipt_sha256':hashlib.sha256(private.read_bytes()).hexdigest()}
 def observed_after_body():
  value=f.sql("with connection.cursor()as c:c.execute('SELECT ordinal,phase,payload FROM owned_restore_probe ORDER BY ordinal');rows=c.fetchall()\nprint(json.dumps(rows))");value=[[r[0],r[1],decoded_jsonb_object(r[2])]for r in value]
  matches=[r for r in value if r[1]=='post_migrate_receiver_liveness'and r[2].get('observed_task_id')==task]
  return value if len(matches)==1 else False
 rows=f.wait(observed_after_body,20,'ActualSourceRestorePostrunLivenessObserved')
 report['actual_plugin_SQL_observations']=rows;normal.save(out/'report.json',report)
 assert terminal['metadata']['status']=='SUCCESS'and terminal['metadata']['result']=={'status':'completed','filename':filename}
 scoped=[r for r in rows if isinstance(r[2].get('current_task'),dict)and r[2]['current_task'].get('id')==task]
 imports=[r[2]for r in scoped if r[1]=='module_import_seen'];constructors=[r[2]for r in scoped if r[1]=='constructor_seen']
 branch='module_reexecuted_and_constructor_in_restore_scope'if imports and constructors else'constructor_without_observed_module_reexecution'if constructors else'no_restore_constructor_observed'
 report['actual_discovery_branch']={'branch':branch,'restore_module_imports':imports,'restore_constructors':constructors,'Source_default_use_cache':False,'Source_force_reload_requested':False,'private_cache_access_or_clearing':False};normal.save(out/'report.json',report)
 phases=[r[1]for r in rows];required=('constructor_begin','constructor_child_published','physical_child','constructor_child_joined','constructor_end')
 if not all(p in phases for p in required):
  liveness=next(r[2]for r in rows if r[1]=='post_migrate_receiver_liveness'and r[2].get('observed_task_id')==task)
  report['actual_post_migrate_receiver_liveness']=liveness
  before=liveness.get('before_restore_body');after=liveness.get('after_restore_body');cfg=liveness.get('restored_plugin_config')
  absent_branch=not constructors and not imports and isinstance(before,dict)and isinstance(after,dict)and before.get('receiver_alive')is False and after.get('receiver_alive')is False and cfg=={'enabled':True,'ever_enabled':True}
  report.update(state='passed'if absent_branch else'observed_unqualified',characterization_scope='Actual restored enabled config with observed absent/dead weak receiver before and after ordinary restore; no constructor/child execution claimed'if absent_branch else'Constructor/receiver branch remains unqualified',missing_observed_phases=[p for p in required if p not in phases],Native_enabled_restore_accepted=False)
  normal.save(out/'report.json',report);return report
 assert constructors
 if imports:assert any(x['module_nonce']==constructors[0]['module_nonce']for x in imports)

 begin=next(r[2]for r in rows if r[1]=='constructor_begin');child=next(r[2]for r in rows if r[1]=='physical_child');joined=next(r[2]for r in rows if r[1]=='constructor_child_joined');end=next(r[2]for r in rows if r[1]=='constructor_end')
 assert begin['current_task']['id']==task==end['current_task']['id']and begin['current_task']['name']=='apps.backups.tasks.restore_backup_task'
 assert child['current_task']['parent_id']==task and child['current_task']['root_id']==task and child['expected_parent']==task and child['current_task']['id']==joined['actual_child_id']
 assert begin['SQL']['pid']==end['SQL']['pid']and begin['SQL']['backend_start']==end['SQL']['backend_start']and child['SQL']['pid']!=begin['SQL']['pid']
 child_meta=f.wait(lambda:f.redis(2,"return redis.call('GET',ARGV[1])or false",prefix+joined['actual_child_id']),20,'ActualSourceRestoreChildBackend');child_meta=json.loads(child_meta);assert child_meta['status']=='SUCCESS'
 assert any(x[0][0]==joined['actual_child_id']for x in end['current_task']['children'])
 report.update(state='passed',actual_child_backend=child_meta,unqualified=['cold default plugin registration','warm already-enabled module reuse','force-token changes during restore','children with held locks/timeouts/caller loss','Native enabled restore still guarded','performance']);normal.save(out/'report.json',report);return report
