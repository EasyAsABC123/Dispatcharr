"""Additive selected-programme scale/rollback cells in the SAME SD fixture.
Original Dispatcharr authors retain application/SQL algorithms. No app on import.
Call prepare after XML download prepare, run after its seven ordinary cells.
"""
import hashlib,json,time,traceback
from pathlib import Path
from backend_terminal import require_backend_dependency,wait_backend_terminal,BlockedBackendDependency
CASE='schedules-direct'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 fixture._xmltv_selected=fixture.sql("from apps.epg.models import EPGSource,EPGData;s=EPGSource.objects.bulk_create([EPGSource(name='Owned streamed selected XMLTV',source_type='xmltv',url='http://127.0.0.1:18097/xmltv/selected.xml',file_path='/data/cached_epg/owned-selected-missing.xml',custom_properties={'auto_apply_epg_logos':False})])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-http-selected',name='Owned selected guide')])[0];print(json.dumps({'source':s.pk,'epg':e.pk}))",secret=True)
 save(Path(fixture.output)/'declared-XMLTV-selected-inputs.json',{'selected_valid':6001,'invalid_sibling':1,'description_bytes_each':1024,'minimum_selected_description_bytes':6001*1024,'parse_page_rows':256,'Source_insert_batch_rows':5000,'Source_and_Native_new_observations_required':True,'physical_cancellation_requires_declared_final_boundary':True})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'xmltv-selected';out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700);cells=[];setup=fixture._xmltv_selected
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;p=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+p+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+p+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 zero='00000000-0000-0000-0000-000000000000';key=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(zero)+').decode()))');assert key.endswith(zero);prefix=key[:-len(zero)]
 def backend(ident):
  raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('declared selected result bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
  return None if raw is None else json.loads(raw)
 def produce(label):
  started=time.monotonic();made=guest('from apps.epg.tasks import parse_programs_for_tvg_id;t=parse_programs_for_tvg_id.apply_async(args=['+str(setup['epg'])+'],kwargs={"force":True});print(json.dumps({"id":t.id}))');save(private/(label+'-producer.json'),made)
  result=wait_backend_terminal(fixture,backend,made['id'],180,'actual-selected-XMLTV-terminal-'+label,private);save(private/(label+'-backend.json'),result)
  return {'task_id':made['id'],'backend':result,'observed_duration_seconds':time.monotonic()-started}
 def snapshot(label):
  # Read-only streamed observation, not a Source task-body replay. At most500
  # selected SQL tuples are held, and the controller receives only a digest.
  script='''from apps.epg.models import ProgramData
import hashlib
q=ProgramData.objects.filter(epg_id=EPG_ID).order_by('id')
sha=hashlib.sha256();identity=hashlib.sha256();count=0;description_bytes=0;names=hashlib.sha256()
for row in q.values('id','title','description','start_time','end_time','tvg_id').iterator(chunk_size=500):
 count+=1;description_bytes+=len((row['description']or'').encode());identity.update(str(row['id']).encode()+b'\\n');names.update((row['title']+'\\n').encode());sha.update(json.dumps({k:v for k,v in row.items()if k!='id'},sort_keys=True,default=str,separators=(',',':')).encode()+b'\\n')
print(json.dumps({'count':count,'description_bytes':description_bytes,'row_content_sha256':sha.hexdigest(),'row_identity_sha256':identity.hexdigest(),'ordered_title_sha256':names.hexdigest()}))
'''.replace('EPG_ID',str(setup['epg']))
  value=fixture.sql(script,secret=True);save(private/(label+'-SQL.json'),value);return value
 def cell(name,work):
  row={'name':name,'status':'running'};cells.append(row)
  try:
   require_backend_dependency(fixture,private,name)
   row.update(status='passed',actual=work())
  except BlockedBackendDependency as error:row.update(status='blocked',reason='ActualNativeCommonCeleryFault',task_produced=False)
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);path=private/(name+'-failure.txt');path.write_text(traceback.format_exc()[-8192:]);path.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 def large():
  value=produce('selected6001');assert value['backend']['status']=='SUCCESS'and value['backend']['result']is None
  state=snapshot('selected6001');expected=hashlib.sha256(''.join('Selected '+str(i).zfill(5)+'\n'for i in range(6001)).encode()).hexdigest();assert state['count']==6001 and state['description_bytes']==6001*1024 and state['ordered_title_sha256']==expected
  return {'backend':value,'SQL':state,'invalid_sibling_skipped':True,'whole_input_vector_required':False}
 def rollback():
  before=snapshot('rollback-before');assert before['count']==6001
  # A genuine PostgreSQL trigger fails the SECOND Source5000-row insert page,
  # exercising rollback after prior DELETE and5000 inserts, not a mocked sink.
  fixture.sql("with connection.cursor()as c:\n c.execute(\"CREATE FUNCTION owned_selected_insert_refusal() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN IF NEW.epg_id="+str(setup['epg'])+" AND NEW.title='Selected 05000' THEN RAISE EXCEPTION USING ERRCODE='P0001',MESSAGE='owned selected second page refusal';END IF;RETURN NEW;END$$\")\n c.execute(\"CREATE TRIGGER owned_selected_insert_refusal BEFORE INSERT ON epg_programdata FOR EACH ROW EXECUTE FUNCTION owned_selected_insert_refusal()\")\nprint(json.dumps({'owned_trigger_installed':True}))",secret=True)
  try:
   value=produce('second-page-refusal');assert value['backend']['status']=='FAILURE';after=snapshot('rollback-after');assert before==after
   return {'backend':value,'SQL_before':before,'SQL_after':after,'actual_original_rows_and_ids_preserved':True,'Source_exception_text_parity_claimed':False}
  finally:fixture.sql("with connection.cursor()as c:\n c.execute('DROP TRIGGER IF EXISTS owned_selected_insert_refusal ON epg_programdata');c.execute('DROP FUNCTION IF EXISTS owned_selected_insert_refusal()')\nprint(json.dumps({'owned_trigger_removed':True}))",secret=True)
 cell('actual-forced-TVG6001-selected-above4MiB-perrow-skip',large)
 cell('actual-second5000-insert-page-refusal-preserves-old-guide',rollback)
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'performance_verified':False,'unqualified':['2GiB decoded limit','per-node4MiB native extraction bound','same-family Source6 concurrency','global XML syntax/transport error terminal contracts','Main held-parser/SQL physical cancellation final-boundary cohort']};save(out/'report.json',report);return report

def run_held_deadline(fixture):
 """Native ONLY, LAST callback before this SD context closes; no new context.
 A real row lock holds the atomic replacement through Main's declared deadline.
 Its required nonzero exit is an honest unknown accepted-task result, not a
 Source successful shutdown or a completed programme-import claim.
 """
 if fixture.case!=CASE or fixture.backend!='Native':raise ValueError('NativeFinalSDContextBoundaryRequired')
 setup=fixture._xmltv_selected;out=Path(fixture.output)/'xmltv-selected-held';out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 ready='/shared/'+CASE+'/selected-held.ready'
 script="import os,signal,time,pathlib,psycopg;running=[True];signal.signal(signal.SIGTERM,lambda *unused:running.__setitem__(0,False));c=psycopg.connect(dbname=os.environ['POSTGRES_DB'],user=os.environ['POSTGRES_USER'],password=os.environ['POSTGRES_PASSWORD'],host='/var/run/postgresql');x=c.cursor();x.execute('SELECT id FROM epg_programdata WHERE epg_id="+str(setup['epg'])+" ORDER BY id LIMIT 1 FOR UPDATE');assert x.fetchone();pathlib.Path("+repr(ready)+").write_text(str(os.getpid()));\nwhile running[0]:time.sleep(.1)\nc.rollback();c.close()"
 fixture.role('xml-selected-held-row',['/dispatcharrpy/bin/python','-c',script]);fixture.wait(lambda:(fixture.case_data/'selected-held.ready').is_file(),15,'actual-selected-row-lock')
 try:
  # Real task publication on the fixture's explicitly configured broker/backend.
  produced=fixture.sql("from apps.epg.tasks import parse_programs_for_tvg_id;t=parse_programs_for_tvg_id.apply_async(args=["+str(setup['epg'])+"],kwargs={'force':True});from celery import current_app;print(json.dumps({'id':t.id,'backend_key':current_app.backend.get_key_for_task(t.id).decode()}))",secret=True);save(private/'actual-producer.json',produced)
  def pending():
   return fixture.sql("with connection.cursor()as c:\n c.execute(\"SELECT pid,backend_start::text,wait_event_type FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query ILIKE 'DELETE FROM epg_programdata WHERE epg_id=%'\");print(json.dumps(c.fetchall()))",secret=True)
  blocked=fixture.wait(pending,90,'actual-staged-programme-atomic-delete-held');save(private/'actual-pending-SQL.json',blocked)
  terminal=fixture.redis(2,"return redis.call('GET',ARGV[1])",produced['backend_key']);save(private/'backend-before-stop.json',terminal);assert terminal is None
  # stop_native sends real TERM, records literal exit/OOM/logs, and rejects
  # Docker's SIGKILL fallback137. The blocker remains held until AFTER exit.
  result=fixture.stop_native(expected=1,seconds=55);save(private/'actual-Main-stop.json',result);assert result['actual_exit']==1 and not result['oom_killed']
 finally:receipt=fixture.role('xml-selected-held-row',[],stop=True);save(private/'actual-lock-role-stop.json',receipt)
 def absent():
  rows=fixture.sql("with connection.cursor()as c:\n c.execute(\"SELECT pid,backend_start::text FROM pg_stat_activity WHERE datname=current_database()\");print(json.dumps(c.fetchall()))",secret=True);return {'all_observed_SQL_generations_absent':True}if all((r[0],r[1])not in[(v[0],v[1])for v in rows]for r in blocked)else False
 settled=fixture.wait(absent,15,'actual-held-Main-SQL-generation-retired');save(private/'actual-SQL-retirement.json',settled)
 report={'status':'passed','actual_Main_exit':result,'physical_SQL_retirement':settled,'accepted_task_outcome':'unknown; no replay or synthetic terminal','Source_normal_shutdown_equivalence_claimed':False,'performance_verified':False};save(out/'report.json',report);return report
