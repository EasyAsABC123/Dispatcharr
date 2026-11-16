"""Actual paired restore API/task in one EXISTING independent owned context.
No nested fixture/app scheduler or Source backend task-body invocation.
Original Dispatcharr/Django/PostgreSQL authors retain all application credit.
"""
import hashlib,json,secrets,time,traceback,urllib.parse,uuid
from pathlib import Path
from normal_backup_case import private_api,archive_gate
CASE='normal-backup-restore'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True,default=str)+'\n')
def prepare_context(f):
 if f.case not in(CASE,'normal-backup-restore-partial-admission'):return
 target='/shared/'+f.case+'/backups'
 f.python("from pathlib import Path;import os;p=Path("+repr(target)+");p.mkdir();os.chown(p,65532,65532);p.chmod(0o755);a=Path('/data/backups');\nif a.is_symlink():a.unlink()\nelif a.exists():\n if any(a.iterdir()):raise RuntimeError('UnownedBackupDirectory')\n a.rmdir()\na.symlink_to(p,target_is_directory=True)")
 before=secrets.token_urlsafe(24);after=secrets.token_urlsafe(24);username='owned-restore-'+uuid.uuid4().hex[:8]
 setup=f.sql("from apps.accounts.models import User;from core.models import CoreSettings;from apps.channels.models import Channel;from django_celery_beat.models import PeriodicTask,PeriodicTasks;u=User.objects.create(username="+repr(username)+",user_level=10,is_active=True,is_staff=True,is_superuser=True);u.set_password("+repr(before)+");u.save();s,_=CoreSettings.objects.get_or_create(key='owned_restore_marker',defaults={'name':'Owned restore marker'});s.value={'phase':'before','unicode':'日本語 ångström'};s.save();b,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});b.value={**(b.value or {}),'schedule_enabled':False};b.save();PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed();Channel.objects.filter(pk="+str(f.channel_id)+").update(name='Owned restore before');print(json.dumps({'user_id':u.pk,'marker_id':s.pk,'actual_database':connection.settings_dict['NAME']}))",secret=True)
 assert setup['actual_database']==f.database
 f._backup_restore={'username':username,'password_before':before,'password_after':after,**setup}
 save(Path(f.output)/'declared-backup-restore-inputs.json',{'context':f.case,'actual_owned_database':f.database,'schema_scope':'current pinned migration graph, no active/history plugins','new_Source_REST_restore_characterization_required':True,'same_Main_required':True,'no_Source_task_body_replay':True,'private_credentials_retained':True})
def snapshot(f):
 return f.sql("from core.models import CoreSettings;from apps.channels.models import Channel;from django.contrib.contenttypes.models import ContentType;from django.contrib.auth.models import Permission;print(json.dumps({'database':connection.settings_dict['NAME'],'marker':CoreSettings.objects.get(key='owned_restore_marker').value,'channel':Channel.objects.get(pk="+str(f.channel_id)+").name,'pluginrepo_ctypes':ContentType.objects.filter(app_label='plugins',model='pluginrepo').count(),'pluginrepo_permissions':list(Permission.objects.filter(content_type__app_label='plugins',content_type__model='pluginrepo').order_by('codename').values_list('codename',flat=True))}))")
def pg(f):
 return f.sql("with connection.cursor()as c:c.execute(\"SELECT pid,backend_start::text,client_port,state,wait_event_type,md5(query) FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() ORDER BY pid\");rows=c.fetchall()\nprint(json.dumps(rows))")
def terminal(f,out,task,token,prefix):
 def check():
  code,value=private_api(f,out,'status','GET','/api/backups/status/'+task+'/?token='+urllib.parse.quote(token),admin=False)
  if code!=200:raise AssertionError(('ActualRestoreStatusHTTP',code))
  raw=f.redis(2,"return redis.call('GET',ARGV[1])or false",prefix+task)
  if not raw:return False
  meta=json.loads(raw)
  if meta.get('status')not in('SUCCESS','FAILURE','REVOKED'):return False
  # API was sampled before Redis: an older pending response is not a
  # terminal projection merely because the later backend read completed.
  if value.get('state')not in('completed','failed'):return False
  if meta.get('status')=='SUCCESS'and isinstance(meta.get('result'),dict):
   result=meta['result']
   if result.get('status')=='completed':assert value=={'state':'completed','result':result}
   else:assert value=={'state':'failed','error':result.get('error','Unknown error')}
  return {'api':value,'metadata':meta}
 return f.wait(check,180,'ActualRestoreBackendTerminal')
def begin(f,out,filename,label):
 code,value=private_api(f,out,label,'POST','/api/backups/'+urllib.parse.quote(filename,safe='')+'/restore/',{})
 assert code==202;uuid.UUID(value['task_id']);assert value['detail']=='Restore started'
 return value['task_id'],value['task_token']
def run(f):
 if f.case!=CASE:raise ValueError('OwnedRestoreContextRequired')
 out=Path(f.output)/'backup-restore';out.mkdir();(out/'private').mkdir(mode=0o700);state=f._backup_restore;cells=[];data={}
 prefix=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task('owned-prefix-calibration');k=k.decode()if isinstance(k,bytes)else k;assert k.endswith('owned-prefix-calibration');print(json.dumps(k[:-len('owned-prefix-calibration')]))")
 def cell(name,work,requires=()):
  if any(x not in data for x in requires):cells.append({'name':name,'status':'not_run','reason':'failed actual prerequisite'});return
  try:
   value=work();data[name]=value;cells.append({'name':name,'status':'passed','actual':value})
  except Exception as error:
   p=out/'private'/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600);cells.append({'name':name,'status':'failed','error_type':type(error).__name__})
  save(out/'report.json',{'status':'running','cells':cells})
 def make_archive():
  # The actual archive lacks ONE builtin content type and four permissions;
  # Source migrate and Native native reconciliation must recreate natural keys.
  f.sql("from django.contrib.contenttypes.models import ContentType;v=ContentType.objects.filter(app_label='plugins',model='pluginrepo').delete();print(json.dumps({'deleted_rows':v[0]}))")
  before=snapshot(f);assert before['pluginrepo_ctypes']==0 and before['pluginrepo_permissions']==[]
  code,value=private_api(f,out,'create','POST','/api/backups/create/',{});assert code==202
  result=terminal(f,out,value['task_id'],value['task_token'],prefix)
  assert result['metadata']['status']=='SUCCESS'and result['metadata']['result']['status']=='completed'
  filename=result['metadata']['result']['filename'];data['filename']=filename
  return {'filename':filename,'actual_archive':archive_gate(f,filename),'before':before,'actual_create_task':value['task_id']}
 cell('actual_current_schema_archive',make_archive)
 def mutate():
  f.sql("from apps.accounts.models import User;from apps.channels.models import Channel;from core.models import CoreSettings;u=User.objects.get(pk="+str(state['user_id'])+");u.set_password("+repr(state['password_after'])+");u.save(update_fields=['password']);s=CoreSettings.objects.get(key='owned_restore_marker');s.value={'phase':'after'};s.save(update_fields=['value']);Channel.objects.filter(pk="+str(f.channel_id)+").update(name='Owned restore after');print(json.dumps(True))",secret=True)
  value=snapshot(f);assert value['marker']=={'phase':'after'}and value['channel']=='Owned restore after';return value
 cell('actual_intervening_database_mutation',mutate,('actual_current_schema_archive',))
 def restore():
  # A real owned table lock holds actual DROP, not a timer/mock success. This
  # provides a bounded observation window for all Native existing pool slots.
  ready='/shared/'+f.case+'/restore-lock.ready';release='/shared/'+f.case+'/restore-lock.release'
  script="import os,json,time,signal,psycopg;from pathlib import Path;stop=[False];signal.signal(signal.SIGTERM,lambda *_:stop.__setitem__(0,True));c=psycopg.connect(host='/var/run/postgresql',dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ.get('POSTGRES_PASSWORD'));x=c.cursor();x.execute('LOCK TABLE public.accounts_user IN ACCESS SHARE MODE');x.execute('SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()');Path("+repr(ready)+").write_text(json.dumps(x.fetchone()));deadline=time.monotonic()+60\nwhile not stop[0]and not Path("+repr(release)+").exists():\n if time.monotonic()>=deadline:raise RuntimeError('OwnedRestoreLockDeadline')\n time.sleep(.025)\nc.rollback();c.close()"
  f.role('backup-restore-lock',['/dispatcharrpy/bin/python','-c',script]);f.wait(lambda:(f.case_data/'restore-lock.ready').is_file(),15,'OwnedRestoreLockReady')
  try:
   task,token=begin(f,out,data['filename'],'restore');data['restore_task']=task
   # Read-only live waiter and namespace/socket observations precede release.
   # Snapshot query text is only a hash; passwords/query parameters are excluded.
   blocked=f.wait(lambda:next((r for r in pg(f)if r[4]=='Lock'),False),20,'ActualRestoreDDLBlocked');data['actual_DDL_tool_generation']=(blocked[0],blocked[1])
   observations={'actual_lock_waiter':blocked,'actual_PG_rows':pg(f)}
   if f.backend=='Native':
    sockets=f.actual_Main_pool_sockets();assert not sockets['errors']
    pg_sockets=[s for s in sockets['sockets']if s['remote_port']==5432 and s['state']=='01'];assert len(pg_sockets)==8
    by_port={r[2]:r for r in observations['actual_PG_rows']if r[2]is not None};mapped=[by_port.get(s['local_port'])for s in pg_sockets];assert all(mapped)
    data['old_native_pool_generations']={(r[0],r[1])for r in mapped};observations.update(actual_Main_pool_sockets=sockets,actual_same_pool8_backend_generations=mapped)
   save(out/'actual-held-restore.json',observations)
  finally:
   f.python("from pathlib import Path;Path("+repr(release)+").touch()")
   f.role('backup-restore-lock',[],stop=True)
  result=terminal(f,out,task,token,prefix);assert result['metadata']['status']=='SUCCESS'and result['metadata']['result']=={'status':'completed','filename':data['filename']}
  assert result['api']['state']=='completed'
  return {'actual_restore_task':task,'actual_result':result['metadata']['result'],'actual_lock_wait_observed':True,'same_Main_instance':True}
 cell('actual_REST202_restore_and_post_migrate',restore,('actual_intervening_database_mutation',))
 def validate():
  value=snapshot(f);assert value['database']==f.database and value['marker']=={'phase':'before','unicode':'日本語 ångström'}and value['channel']=='Owned restore before'
  assert value['pluginrepo_ctypes']==1 and value['pluginrepo_permissions']==['add_pluginrepo','change_pluginrepo','delete_pluginrepo','view_pluginrepo']
  code,login=private_api(f,out,'restored-login','POST','/api/accounts/token/',{'username':state['username'],'password':state['password_before']},admin=False);assert code==200 and isinstance(login.get('access'),str)
  code,settings=private_api(f,out,'restored-settings','GET','/api/core/settings/');assert code==200
  rows=settings.get('results',[])if isinstance(settings,dict)else settings;assert any(r.get('key')=='owned_restore_marker'and r.get('value')==value['marker']for r in rows)
  code,channel=private_api(f,out,'restored-channel','GET','/api/channels/channels/'+str(f.channel_id)+'/');assert code==200 and channel['name']=='Owned restore before'
  live={(r[0],r[1])for r in pg(f)};assert data['actual_DDL_tool_generation']not in live
  if f.backend=='Native':
   old=data['old_native_pool_generations'];assert not(old&live)
  return {'actual_SQL':value,'restored_login_HTTP200':True,'restored_settings_HTTP200':True,'restored_channel_HTTP200':True,'old_Native_pool_generations_absent':f.backend=='Native','actual_DDL_PG_tool_generation_absent':True}
 cell('same_Main_restored_login_settings_channels_and_pool_retirement',validate,('actual_REST202_restore_and_post_migrate',))
 def invalid():
  name='dispatcharr-backup-owned-invalid-'+uuid.uuid4().hex[:8]+'.zip'
  f.python("from pathlib import Path;import os;p=Path("+repr('/shared/'+f.case+'/backups/'+name)+");p.write_bytes(b'owned non-ZIP archive');os.chown(p,65532,65532);p.chmod(0o444)")
  before=snapshot(f);task,token=begin(f,out,name,'invalid-restore');result=terminal(f,out,task,token,prefix)
  assert result['metadata']['status']=='SUCCESS'and result['metadata']['result']['status']=='failed'and isinstance(result['metadata']['result']['error'],str)
  assert result['api']=={'state':'failed','error':result['metadata']['result']['error']}
  assert snapshot(f)==before
  return {'actual_task':task,'actual_backend_state':'SUCCESS','actual_result_status':'failed','actual_status_projection':'failed with error, backend SUCCESS failed dictionary','database_unchanged':True,'error_string_exact_parity':'unqualified'}
 cell('caught_archive_failure_is_SUCCESS_failed_dict',invalid)
 if f.backend=='Native':
  def stop():
   actual=f.stop_native(expected=0,seconds=55);assert actual['actual_exit']==0;return actual
  cell('actual_normal_Main0',stop)
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','context':CASE,'cells':cells,'full_restore_parity':False,'performance_verified':False,'unmeasured':['historical migrations','enabled/history plugin restore','critical PG failure/nonfatal-warning new cases','cancellation/callerloss/heldMain deadline','live recording overlap during restore','broader cache/Beat restoration races']};save(out/'report.json',report);return report
