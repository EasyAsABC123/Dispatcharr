"""Existing Fixture callback: actual HTTP actions, SQL identities and user tasks.
No service/application starts on import. Root owns bounded cohort dispatch.
"""
import base64,concurrent.futures,hashlib,json,time,urllib.error,urllib.request,uuid
from pathlib import Path
KEY='owned_m8_actions'
def save(f,name,value):
 (Path(f.output)/('m8-actions-'+name+'.json')).write_text(json.dumps(value,indent=2,default=str)+'\n')
def prepare_context(f):
 source=Path(__file__).with_name('plugin.py').read_bytes()
 # Existing owned baseline writer touches ONLY the context's data-only mount.
 relative=f.case_data.relative_to(f.data)
 if f.case!='m8-actions'or relative.parts!=('m8-actions',):raise ValueError('ExactOwnedM8ContextRequired')
 target=str(Path('/shared')/relative/'plugins'/KEY)
 script="import base64,pathlib,os,hashlib,json;root=pathlib.Path("+repr(target)+");parent=root.parent;assert parent.is_dir()and not parent.is_symlink();assert pathlib.Path('/data/plugins').resolve()==parent.resolve();root.mkdir(mode=0o755);file=root/'plugin.py';file.write_bytes(base64.b64decode("+repr(base64.b64encode(source).decode())+"));[(os.chown(p,65532,65532),p.chmod(0o755 if p.is_dir()else 0o444))for p in(root,file)];st=file.stat();assert st.st_uid==65532 and st.st_gid==65532 and st.st_mode&0o777==0o444;print(json.dumps({'guest_path':str(file),'loader_path':'/data/plugins/"+KEY+"/plugin.py','sha256':hashlib.sha256(file.read_bytes()).hexdigest(),'bytes':st.st_size,'uid':st.st_uid,'gid':st.st_gid,'mode':oct(st.st_mode&0o777)}))"
 installed=json.loads(f.python(script,secret=True))
 expected=hashlib.sha256(source).hexdigest();host=f.case_data/'plugins'/KEY/'plugin.py'
 if installed['sha256']!=expected or installed['bytes']!=len(source)or hashlib.sha256(host.read_bytes()).hexdigest()!=expected:raise ValueError('ActualPluginMountBytesMismatch')
 save(f,'installed-plugin',installed)
 f.sql("with connection.cursor()as c:\n c.execute('CREATE TABLE owned_m8_probe(ordinal bigserial PRIMARY KEY,label text NOT NULL,phase text NOT NULL,backend_pid int NOT NULL,backend_start timestamptz NOT NULL,payload jsonb NOT NULL,observed_at timestamptz DEFAULT clock_timestamp())')\n c.execute('CREATE TABLE owned_m8_barrier(label text PRIMARY KEY,released boolean NOT NULL DEFAULT false)')\n c.execute('CREATE TABLE owned_m8_lock(id int PRIMARY KEY,value int NOT NULL)')\n c.execute('INSERT INTO owned_m8_lock VALUES(1,0)')\nprint(json.dumps({'owned_tables_created':True}))")
 f._plugin_environment={**getattr(f,'_plugin_environment',{}),'RUST_PLUGIN_EXECUTION_AUDIT':'1'}
 save(f,'prepared',{'plugin_sha256':hashlib.sha256(source).hexdigest(),'context':f.case,'source_actor_requirement':'Actual existing default worker must use prefork concurrency>=4 before startup; no synthetic task apply/body invocation','native_pool':8,'existing_auth_writer_budget':4,'existing_global_jobs_budget':6})

def request(f,label,action):
 data=json.dumps({'action':action,'params':{'label':label}}).encode();req=urllib.request.Request(f.base_url+'/api/plugins/plugins/'+KEY+'/run/',data=data,headers={'Content-Type':'application/json','Authorization':'Bearer '+f.actual_admin_token()},method='POST')
 record={'label':label,'action':action,'status':None,'body':None,'error':None}
 try:
  try:r=urllib.request.urlopen(req,timeout=f.remaining(35))
  except urllib.error.HTTPError as error:r=error
  with r:
   raw=r.read(1048577);record['status']=r.status;record['raw_sha256']=hashlib.sha256(raw).hexdigest()
   if len(raw)>1048576:raise RuntimeError('ActionResponseCapacity')
   record['body']=json.loads(raw)
 except BaseException as error:record['error']=type(error).__name__
 finally:save(f,'HTTP-'+label,record)
 return record

def snapshot(f):
 return f.sql("with connection.cursor()as c:\n c.execute('SELECT ordinal,label,phase,backend_pid,backend_start::text,payload,observed_at::text FROM owned_m8_probe ORDER BY ordinal');rows=c.fetchall()\n c.execute(\"SELECT pid,backend_start::text,state,wait_event_type,wait_event,client_port FROM pg_stat_activity WHERE datname=current_database() ORDER BY pid\");sessions=c.fetchall()\nprint(json.dumps({'rows':rows,'sessions':sessions},default=str))")
def wait(f,predicate,seconds,label):
 until=time.monotonic()+f.remaining(seconds)
 while time.monotonic()<until:
  value=snapshot(f);save(f,label+'-latest',value)
  if predicate(value):return value
  time.sleep(min(.05,f.remaining(.05)))
 raise TimeoutError(label)
def barrier(f,label):
 f.sql("with connection.cursor()as c:c.execute('INSERT INTO owned_m8_barrier(label)VALUES(%s)',["+repr(label)+"])\nprint(json.dumps({'inserted':True}))")
def run(f):
 report={'context':f.case,'passed':False,'full_M8_passed':False,'Source_consumer_scope':'explicitOwnedUserModuleconsumer'if f.backend=='Original'else'actualNativeSelectedRuntime','Original_default_plugin_bootstrap_parity':False,'Source_execution_audit_not_yet_qualified':True,'cases':{}};save(f,'report',report)
 # Discover/enable use REAL Main/Source routes, never preseed Native DB enabled.
 def api(path,body):
  req=urllib.request.Request(f.base_url+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json','Authorization':'Bearer '+f.actual_admin_token()},method='POST')
  try:r=urllib.request.urlopen(req,timeout=f.remaining(20))
  except urllib.error.HTTPError as e:r=e
  with r:return r.status,r.read(1048576).decode()
 code,body=api('/api/plugins/plugins/reload/',{});save(f,'reload',{'status':code,'body':body});assert code==200
 code,body=api('/api/plugins/plugins/'+KEY+'/enabled/',{'enabled':True});save(f,'enable',{'status':code,'body':body});assert code==200
 if f.backend=='Original':
  # Explicit standard Celery user-module import BEFORE consumer strategies are
  # built, under an owned PYTHONPATH. Original default plugin-bootstrap failure
  # remains unqualified: fork-child discovery alone did not register the parent
  # consumer in Source30. No task methods/Source bodies/strategies are patched.
  receipt=f.role('celery',[],stop=True);save(f,'Source-pre-enable-worker-retired',receipt)
  f.role('celery',['/dispatcharrpy/bin/celery','-A','dispatcharr','worker','--pool=prefork','--concurrency=4','--include','owned_m8_actions.plugin','-Q','celery','--without-gossip','--without-mingle','--without-heartbeat','--loglevel=WARNING','--hostname=normal-default@owned'],extra={'PYTHONPATH':'/app:/data/plugins'})
  def actual_registered():
   script="import os,json;os.environ['CELERY_BROKER_URL']='redis://:playback-fixture-only@localhost:6379/1';from dispatcharr.celery import app;r=app.control.inspect(destination=['normal-default@owned'],timeout=1).registered();print(json.dumps(r))"
   response=json.loads(f.python(script));names=response.get('normal-default@owned')if isinstance(response,dict)else None
   if not isinstance(names,list):return False
   if len(names)>512 or any(not isinstance(n,str)or len(n)>512 for n in names):raise AssertionError('ActualRegisteredTaskReplyBudget')
   return response if'owned_m8_actions.physical_child'in names else False
  registered=f.wait(actual_registered,20,'actual-explicit-Source-consumer-registered-BEFORE-publication')
  save(f,'Source-explicit-consumer-registration',{'scope':'explicitOwnedUserModuleconsumer','default_Original_plugin_bootstrap_parity':False,'CLI_include':'owned_m8_actions.plugin','PYTHONPATH':'/app:/data/plugins','actual_targeted_inspect_registered':registered,'observed_BEFORE_action_publication':True,'previous_default_worker_registration_failure_preserved':True})
 labels=['four-'+uuid.uuid4().hex[:8]for _ in range(4)]
 for label in labels:barrier(f,label)
 # Fixed four owned callers; existing absolute fixture deadline bounds every
 # socket/poll. No additional service/pool/task framework is constructed.
 with concurrent.futures.ThreadPoolExecutor(max_workers=4)as executor:
  futures=[executor.submit(request,f,label,'wait')for label in labels]
  try:
   held=wait(f,lambda s:len({r[1]for r in s['rows']if r[1]in labels and r[2]=='child_started'})==4,20,'four-actual-children-held')
   parents={r[3]for r in held['rows']if r[1]in labels and r[2]=='parent_started'};children={r[3]for r in held['rows']if r[1]in labels and r[2]=='child_started'}
   assert len(parents)==4 and len(children)==4 and parents.isdisjoint(children),(parents,children)
   # The separately owned SQL observer itself adds one known connection; do
   # not call total PG session count<=8 from this snapshot without subtracting
   # that measured observer and separately-classified worker/Beat sessions.
   report['cases']['four_distinct_parent_child_sessions']={'passed':True,'parents':sorted(parents),'children':sorted(children),'snapshot':held}
   if f.backend=='Native':
    actual=f.actual_Main_pool_sockets()
    save(f,'actual-Main-pool-sockets',actual);assert not actual['errors'],actual
    native_pg=[socket for socket in actual['sockets']if socket['remote_port']==5432 and socket['state']=='01']
    assert len(native_pg)<=8,native_pg
    by_port={row[5]:row for row in held['sessions']if row[5]is not None}
    observed=[by_port.get(socket['local_port'])for socket in native_pg]
    assert all(row is not None for row in observed),(native_pg,held['sessions'])
    actual_pids={row[0]for row in observed};assert parents|children<=actual_pids,(parents,children,actual_pids)
    assert len(native_pg)==8,actual
    report['cases']['actual_Main_pool8_socket_to_backend_accounting']={'passed':True,'actual_native_main_sockets':actual,'mapped_backend_generations':observed,'observer_is_separate_baseline_connection':True}

  finally:
   f.sql("with connection.cursor()as c:c.execute('UPDATE owned_m8_barrier SET released=true WHERE label=ANY(%s)',["+repr(labels)+"])\nprint(json.dumps({'released':True}))")
  values=[future.result(timeout=f.remaining(30))for future in futures]
 assert all(v['status']==200 and v['error']is None for v in values),values
 label='timeout-'+uuid.uuid4().hex[:8];barrier(f,label);value=request(f,label,'timeout_continue');assert value['status']==200,value
 actual=snapshot(f);phases=[r[2]for r in actual['rows']if r[1]==label];assert phases.index('child_started')<phases.index('actual_get_timeout')<phases.index('parent_released_child')<phases.index('child_finished'),phases
 report['cases']['timeout_parent_SQL_continues_before_child_terminal']={'passed':True,'actual':value,'phases':phases}
 label='rollback-'+uuid.uuid4().hex[:8];value=request(f,label,'transaction_rollback');assert value['status']==200,value
 assert value['body']['result']['value']==0,value
 failure=value['body']['result']['failure'];assert failure['class']=='OperationalError'and'lock timeout'in failure['message'].lower(),failure
 report['cases']['active_transaction_child_lock_timeout_and_parent_rollback']={'passed':True,'actual':value}
 from .builtin_cases import run as builtin_refresh_case
 report['cases']['exact_builtin_refresh_action']=builtin_refresh_case(f,request,save)
 if f.backend=='Original':
  label='inline-source-'+uuid.uuid4().hex[:8]
  observed=request(f,label,'inline_characterize');save(f,'Source-inline-characterization',observed)
  assert observed['status']==200 and observed['error']is None,observed
  assert observed['body']['result']['characterization_completed']is True,observed
  report['cases']['Source_inline_authority']=observed['body']['result']
 report['final_SQL']=snapshot(f)
 identities={(r[3],r[4])for r in report['final_SQL']['rows']if r[2]in('parent_started','child_started')}
 def collect_identities(value):
  if isinstance(value,dict):
   if isinstance(value.get('pid'),int)and isinstance(value.get('backend_start'),str):identities.add((value['pid'],value['backend_start']))
   for item in value.values():collect_identities(item)
  elif isinstance(value,list):
   for item in value:collect_identities(item)
 collect_identities(report['cases'].get('Source_inline_authority'))
 if f.backend=='Native':
  audits=[v['body']['result']['native_execution_audit']for v in values]+[v['body']['result']['child']['native_execution_audit']for v in values]
  assert all(a and a['execution_audit_enabled']and a['entrypoint_guard_installed']and not a['actual_blocked_calls']for a in audits),audits
  report['actual_native_execution_audits']=audits
  report['normal_Main_stop']=f.stop_native(expected=0,seconds=55)
 else:
  f.stop_roles();report['normal_Source_roles_stop']='actual receipts retained by existing role owner'
 retired=wait(f,lambda s:not(identities&{(r[0],r[1])for r in s['sessions']}),15,'owned-backend-generations-retired')
 report['normal_owned_backend_generations_retired']={'identities':sorted(identities),'actual':retired}
 report['passed']=True;report['required_remaining']=['actual source execution audit origin/function classification','held-query forced-abort quarantine and exact owned backend PID/backend_start retirement','additional non-TCP/default-port database transport characterization'];save(f,'report',report);return report
