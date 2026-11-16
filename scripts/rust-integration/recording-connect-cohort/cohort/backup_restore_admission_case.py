"""Native-only acceptance of stopped, partially admitted real Main restore.
Existing Fixture/cohort only; no backend imports or replacement task bodies.
Original Dispatcharr/PostgreSQL/dependency authors retain all application credit.
"""
import hashlib,json,queue,threading,time,urllib.error,urllib.request,uuid
from pathlib import Path
import backup_restore_cases as normal
from normal_backup_case import private_api,archive_gate
CASE='normal-backup-restore-partial-admission'
def prepare_context(f):
 if f.case!=CASE:return
 normal.prepare_context(f)
def run(f):
 if f.case!=CASE or f.backend!='Native':raise ValueError('IndependentNativeRestoreAdmissionContextRequired')
 out=Path(f.output)/'backup-restore-admission';out.mkdir();(out/'private').mkdir(mode=0o700)
 report={'status':'running','scope':'Native stopped preclaim pool admission only','full_restore_parity':False,'performance_verified':False,'observations':{},'cells':[]};normal.save(out/'report.json',report)
 def record(name,value):report['observations'][name]=value;normal.save(out/'report.json',report)
 prefix=f.sql("from celery import current_app;k=current_app.backend.get_key_for_task('owned-prefix-calibration');k=k.decode()if isinstance(k,bytes)else k;print(json.dumps(k[:-len('owned-prefix-calibration')]))")
 code,value=private_api(f,out,'create','POST','/api/backups/create/',{});assert code==202
 created=normal.terminal(f,out,value['task_id'],value['task_token'],prefix);assert created['metadata']['status']=='SUCCESS'and created['metadata']['result']['status']=='completed'
 filename=created['metadata']['result']['filename'];record('causal_archive',archive_gate(f,filename));before=normal.snapshot(f)
 def schema_identity():return f.sql("with connection.cursor()as c:c.execute(\"SELECT oid::bigint,relname,relkind FROM pg_class WHERE relnamespace='public'::regnamespace ORDER BY oid\");rows=c.fetchall()\nprint(json.dumps(rows))")
 original_schema=schema_identity();record('before_public_relation_identities',original_schema)
 # One ACTUAL Main HTTP SQL owner is held by a real external table lock. It
 # consumes an existing Main socket; it does not add a pool/worker constructor.
 ready='/shared/'+f.case+'/admission-lock.ready';release='/shared/'+f.case+'/admission-lock.release'
 guest="import os,json,time,signal,psycopg;from pathlib import Path;stop=[False];signal.signal(signal.SIGTERM,lambda *_:stop.__setitem__(0,True));c=psycopg.connect(host='/var/run/postgresql',dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ.get('POSTGRES_PASSWORD'));x=c.cursor();x.execute('LOCK TABLE public.dispatcharr_channels_channel IN ACCESS EXCLUSIVE MODE');x.execute('SELECT pg_backend_pid(),backend_start::text FROM pg_stat_activity WHERE pid=pg_backend_pid()');Path("+repr(ready)+").write_text(json.dumps(x.fetchone()));deadline=time.monotonic()+150\nwhile not stop[0]and not Path("+repr(release)+").exists():\n if time.monotonic()>=deadline:raise RuntimeError('OwnedAdmissionLockDeadline')\n time.sleep(.025)\nc.rollback();c.close()"
 results=queue.Queue();threads=[];signalled=False;task=None;old=set()
 def launch(label):
  def actual_request():
   try:
    request=urllib.request.Request(f.base_url+'/api/channels/channels/'+str(f.channel_id)+'/',headers={'Authorization':'Bearer '+f.actual_admin_token()})
    try:response=urllib.request.urlopen(request,timeout=f.remaining(120))
    except urllib.error.HTTPError as error:response=error
    with response:raw=response.read(1048577);assert len(raw)<=1048576;value={'label':label,'status':response.status,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
   except Exception as error:value={'label':label,'error_type':type(error).__name__}
   results.put(value)
  thread=threading.Thread(target=actual_request,name='owned-restore-'+label,daemon=True);thread.start();threads.append(thread)
 def sockets():return f.actual_Main_pool_sockets()
 def frame():
  raw=f.redis(1,"local a=redis.call('LRANGE','celery',0,127);for _,v in ipairs(a)do local ok,e=pcall(cjson.decode,v);if ok and e.headers and e.headers.id==ARGV[1]then return v end end;return false",task)
  assert raw;value=json.loads(raw);assert value['headers']['task']=='apps.backups.tasks.restore_backup_task';return hashlib.sha256(raw.encode()).hexdigest()
 try:
  f.role('backup-restore-admission-lock',['/dispatcharrpy/bin/python','-c',guest]);f.wait(lambda:(f.case_data/'admission-lock.ready').is_file(),10,'ActualAdmissionLockReady')
  launch('held-existing-owner')
  def actual_held():
   ss=sockets();rows=normal.pg(f);ports={x['local_port']for x in ss['sockets']if x['remote_port']==5432 and x['state']=='01'};held=[r for r in rows if r[2]in ports and r[4]=='Lock'];return {'Main':ss,'PG':rows,'held':held}if len(held)==1 else False
  held=f.wait(actual_held,12,'ActualExistingMainSQLSocketHeld');record('actual_held_existing_owner',held)
  task,token=normal.begin(f,out,filename,'restore-preclaim');started=time.monotonic();raw_hash=frame()
  # Idle socket count alone cannot demonstrate admission. A second actual
  # authenticated GET must remain pending while only the FIRST lock waiter is
  # present: the otherwise-idle seven sockets are unavailable to that caller.
  # This is a bounded external capacity witness, not direct actor introspection.
  launch('capacity-probe');f.sleep(2)
  ss=sockets();rows=normal.pg(f);pg_sockets=[x for x in ss['sockets']if x['remote_port']==5432 and x['state']=='01'];assert len(pg_sockets)==8
  by_port={r[2]:r for r in rows if r[2]is not None};mapped=[by_port.get(x['local_port'])for x in pg_sockets];assert all(mapped)
  assert len([r for r in mapped if r[4]=='Lock'])==1 and all(t.is_alive()for t in threads)
  assert frame()==raw_hash and not f.redis(2,"return redis.call('GET',ARGV[1])or false",prefix+task)
  assert time.monotonic()-started<20;old={(r[0],r[1])for r in mapped}
  record('partial_admission_capacity_witness',{'actual_Main':ss,'actual_mapped_pool8':mapped,'same_raw_restore_frame_sha256':raw_hash,'both_HTTP_callers_pending':True,'only_one_actual_PG_lock_waiter':True,'seconds_since_REST202':time.monotonic()-started,'direct_actor_introspection':False})
  # No timer/forced stop is allowed to stand in for the actual warm-drain exit.
  info=json.loads(f.command([f.docker,'inspect',f.native]))[0];assert info['State']['Running'];pid=info['State']['Pid'];generation=f.pid_start(pid);assert generation is not None;record('before_TERM',{'Main_host_pid':pid,'Main_host_generation':generation,'actual_CID':info['Id']})
  f.command([f.docker,'kill','--signal=TERM',f.native],seconds=10);signalled=True
  def retired():
   state=json.loads(f.command([f.docker,'inspect',f.native]))[0]['State'];return state if not state['Running']else False
  state=f.wait(retired,75,'ActualAdmissionMainWarmDrainExit');record('actual_signal_exit',state)
  assert state['ExitCode']!=137 and not state['OOMKilled'];assert f.pid_start(pid)!=generation
  # The lock is STILL held. A server query may remain lock-waiting after
  # its client process exits until PostgreSQL observes that closed socket;
  # process retirement is never evidence of SQL Ready/physical session close.
  remaining=normal.pg(f);still_present=old&{(r[0],r[1])for r in remaining}
  if still_present:assert state['ExitCode']!=0
  record('actual_settlement_classification',{'actual_Main_exit':state['ExitCode'],'server_ownership_survived_exit':bool(still_present),'nonzero_required_if_unsettled':True,'clean_exit_accepted_only_when_all_original_server_generations_absent_under_held_lock':True})
  assert frame()==raw_hash and not f.redis(2,"return redis.call('GET',ARGV[1])or false",prefix+task)
  record('held_lock_exit_boundary',{'original_Main_pool_generations_still_present':sorted(still_present),'restore_still_raw_unclaimed':True,'restore_backend_result_absent':True,'physical_retirement_boundary':'actual Main nonzero process exit; SQL/session settlement must still be observed after external lock release','actual_PG':remaining})
  report['cells'].append({'name':'stopped_partial_admission_never_claims_or_drops_and_reports_actual_settlement','status':'passed'})
  receipt=f.stop_native(expected=None,seconds=5);assert receipt['actual_exit']==state['ExitCode'];record('actual_retirement_receipt',receipt)
 except Exception as error:
  report['cells'].append({'name':'stopped_partial_admission_never_claims_or_drops_and_reports_actual_settlement','status':'failed','error_type':type(error).__name__});raise
 finally:
  # Real held owner and caller sockets are settled even if observation fails.
  f.python("from pathlib import Path;Path("+repr(release)+").touch()")
  f.role('backup-restore-admission-lock',[],stop=True)
  for thread in threads:thread.join(timeout=f.remaining(10))
  record('HTTP_caller_receipts',{'results':list(results.queue),'all_threads_settled':all(not t.is_alive()for t in threads),'TERM_sent':signalled})
  report['status']='passed'if report['cells']and all(c['status']=='passed'for c in report['cells'])else'failed';normal.save(out/'report.json',report)
 try:
  f.wait(lambda:not(old&{(r[0],r[1])for r in normal.pg(f)}),15,'ActualHeldPGGenerationsRetiredAfterRelease')
  after=normal.snapshot(f);assert after==before and schema_identity()==original_schema
  assert all(not t.is_alive()for t in threads)
 except Exception as error:
  report['status']='failed';report['cells'].append({'name':'database_unchanged_after_held_owner_release','status':'failed','error_type':type(error).__name__});normal.save(out/'report.json',report);raise
 report['cells'].append({'name':'database_unchanged_after_held_owner_release','status':'passed','actual':after,'all_public_relation_OIDs_preserved':True});report['unqualified']=['Source parity is not applicable to Native pool admission','unknown physical-close fault injection','direct internal actor phase observation','partial admission settling before normal clean exit','restore cancellation after destructive phase','performance'];normal.save(out/'report.json',report);return report
