"""Source/Native XML refresh deferral extension within the existing SD context.
No applications or task bodies run on import. Original authors retain full credit.
"""
import base64,datetime,json,traceback,uuid
from pathlib import Path
CASE='schedules-direct'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 # Controlled DATA input. This is a valid force=True parse of an unmapped EPG,
 # preserving the Source distinction from ordinary mapped-only imports.
 source=fixture.sql("from apps.epg.models import EPGSource,EPGData;s=EPGSource.objects.bulk_create([EPGSource(name='Owned XML deferred',source_type='xmltv',file_path='/data/epgs/xml-deferred.xml')])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-xml-deferred',name='Owned XML deferred')])[0];print(json.dumps({'source':s.pk,'epg':e.pk}))",secret=True)
 fixture._xml_tvg_deferred=source
 day=(datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=1)).strftime('%Y%m%d')
 xml=('<?xml version="1.0"?><tv><channel id="owned-xml-deferred"><display-name>Owned XML deferred</display-name></channel><programme channel="owned-xml-deferred" start="'+day+'100000 +0000" stop="'+day+'103000 +0000"><title>Owned deferred programme</title></programme></tv>').encode()
 script="import base64,os;from pathlib import Path;p=Path('/shared/"+CASE+"/epgs/xml-deferred.xml');p.write_bytes(base64.b64decode("+repr(base64.b64encode(xml).decode())+"));os.chown(p,65532,65532);p.chmod(0o644);print('owned XML input written')"
 fixture.python(script,secret=True)
 save(Path(fixture.output)/'declared-XML-defer-input.json',{'source':source['source'],'epg':source['epg'],'force':True,'mapped':False,'countdown_seconds':15,'maximum_deferrals':480,'file_bytes':len(xml)})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 setup=fixture._xml_tvg_deferred;out=Path(fixture.output)/'xml-tvg-deferral';out.mkdir(exist_ok=False);private=out/'private';private.mkdir(mode=0o700);cells=[]
 def guest(script):
  prefix="import os,sys,json;from urllib.parse import quote;password=quote(os.environ['REDIS_PASSWORD'],safe='');os.environ['CELERY_BROKER_URL']='redis://:'+password+'@localhost:6379/1';os.environ['CELERY_RESULT_BACKEND']='redis://:'+password+'@localhost:6379/2';sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from celery import current_app;\n"
  return json.loads(fixture.python(prefix+script,secret=True))
 calibration='00000000-0000-0000-0000-000000000000';key=guest('print(json.dumps(current_app.backend.get_key_for_task('+repr(calibration)+').decode()))');assert key.endswith(calibration);prefix=key[:-len(calibration)]
 def terminal(ident):
  raw=fixture.redis(2,"if redis.call('STRLEN',ARGV[1])>1048576 then return redis.error_reply('declared XML result bound')end;return redis.call('GET',ARGV[1])",prefix+ident)
  return None if raw is None else json.loads(raw)
 def wait_terminal(ident,label,seconds=45):
  value=fixture.wait(lambda:terminal(ident),seconds,'actual-XML-terminal-'+label);save(private/(label+'-backend.json'),value);return value
 def publish(force=True,defer=0,label='xml'):
  made=guest('from apps.epg.tasks import parse_programs_for_tvg_id;t=parse_programs_for_tvg_id.apply_async(args=['+str(setup['epg'])+'],kwargs='+repr({'force':force,'_defer_retry':defer})+');print(json.dumps({"id":t.id}))');save(private/(label+'-producer.json'),made);return made['id']
 def frames():
  p=fixture.case_data/'sd-publications.jsonl'
  if p.stat().st_size>1048576:raise AssertionError('DeclaredSharedSDXMLLedgerBudget')
  return [json.loads(line)for line in p.read_text().splitlines()if line.strip()]
 def receipt(ident):
  return fixture.redis(1,"local h=redis.call('HGETALL','unacked');for i=1,#h,2 do if string.find(h[i+1],ARGV[1],1,true)then return {h[i],h[i+1]}end end;return false",ident)
 def lock():
  token='owned-XML-refresh-'+uuid.uuid4().hex;key='task_lock_refresh_epg_data_'+str(setup['source']);assert fixture.redis(0,"return redis.call('SET',ARGV[1],ARGV[2],'NX','EX',120)",key,token)=='OK';return key,token
 def unlock(key,token):return fixture.redis(0,"if redis.call('GET',ARGV[1])==ARGV[2]then return redis.call('DEL',ARGV[1])end;return 0",key,token)
 def snapshot(label):
  rows=fixture.sql('from apps.epg.models import ProgramData;print(json.dumps(list(ProgramData.objects.filter(epg_id='+str(setup['epg'])+').values("id","title","start_time","end_time")),default=str))',secret=True);save(private/(label+'-SQL.json'),rows);return rows
 def cell(name,body):
  row={'name':name,'status':'running'};cells.append(row)
  try:row.update(status='passed',actual=body())
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 def busy():
  key,token=lock()
  try:
   ident=publish(label='busy');parent=wait_terminal(ident,'busy');assert parent['status']=='SUCCESS'and parent['result']=='Deferred';children=[v[0][0]for v in parent.get('children',[])];assert len(children)==1;child=children[0]
   frame=fixture.wait(lambda:next((v for v in frames()if(v.get('headers')or{}).get('id')==child),None),10,'actual-XML-deferred-publication')
   assert frame['headers']['task']=='apps.epg.tasks.parse_programs_for_tvg_id'and frame['headers']['parent_id']==ident and frame['headers']['root_id']==ident
   save(private/'busy-actual-publication-row.json',frame);actual_frame=json.loads(frame['raw_frame']);body=json.loads(base64.b64decode(actual_frame['body']));assert body[0]==[setup['epg']]and body[1]=={'force':True,'_defer_retry':1}
   eta=datetime.datetime.fromisoformat(frame['headers']['eta']);assert 14<(eta.timestamp()-frame['observed_epoch'])<=16
   actual=fixture.wait(lambda:receipt(child),10,'actual-XML-unacked-receipt');save(private/'busy-unacked.json',actual);assert unlock(key,token)==1
   completed=wait_terminal(child,'busy-child');rows=snapshot('busy-child');assert completed['status']=='SUCCESS'and completed['result']is None and len(rows)==1 and rows[0]['title']=='Owned deferred programme'and not receipt(child)
   return {'actual_busy_parent_and_fifteen_second_child':True,'actual_durable_receipt_and_due_forced_unmapped_parse':True}
  finally:unlock(key,token)
 def exhausted():
  before=snapshot('exhausted-before');key,token=lock()
  try:
   ident=publish(False,480,'exhausted');value=wait_terminal(ident,'exhausted');assert value['status']=='SUCCESS'and value['result']=='Deferred too many times: source refresh in progress'and not value.get('children');assert snapshot('exhausted')==before
   return {'actual480_limit_no_child_no_parse':True}
  finally:unlock(key,token)
 def revoked():
  before=snapshot('revoked-before');key,token=lock()
  try:
   ident=publish(label='revoked-parent');value=wait_terminal(ident,'revoked-parent');assert value['status']=='SUCCESS'and value['result']=='Deferred';child=value['children'][0][0][0]
   fixture.wait(lambda:receipt(child),10,'actual-XML-revocable-reservation');guest('current_app.control.revoke('+repr(child)+',terminate=False);print(json.dumps({"actual_control_sent":True}))')
   result=wait_terminal(child,'revoked-child');save(private/'revoked-initial-terminal-receipt.json',{'backend':result,'actual_unacked':receipt(child)});assert result['status']=='REVOKED'
   fixture.wait(lambda:not receipt(child),45,'actual-XML-revoked-reservation-retirement');assert snapshot('revoked-child')==before
   return {'actual_pidbox_revoke_terminal_and_reservation_absence':True}
  finally:unlock(key,token)
 def warm():
  key,token=lock();argv=None
  try:
   ident=publish(label='warm-parent');value=wait_terminal(ident,'warm-parent');assert value['status']=='SUCCESS'and value['result']=='Deferred';child=value['children'][0][0][0]
   fixture.wait(lambda:receipt(child),10,'actual-XML-warm-reservation')
   if fixture.backend=='Native':stop=fixture.stop_native(expected=0)
   else:
    record=fixture.roles['celery'];status=json.loads(fixture.python("import json,sys;sys.path.insert(0,'/normal-dvr-tools');from role_owner import rpc;print(json.dumps(rpc("+repr(record)+",'status')))"));argv=status['actual_argv'];save(private/'warm-Source-worker-status.json',status);stop=fixture.role('celery',[],stop=True)
   save(private/'warm-actual-stop.json',stop)
   queued=fixture.redis(1,"local q=redis.call('LRANGE','celery',0,999);local n=0;for _,v in ipairs(q)do n=n+#v end;if n>1048576 then return redis.error_reply('declared XML warm queue bound')end;for _,v in ipairs(q)do if string.find(v,ARGV[1],1,true)then return v end end;return false",child)
   save(private/'warm-restored-frame.json',json.loads(queued)if queued else None);assert queued and not receipt(child)
   frame=json.loads(queued);assert frame['headers']['id']==child and frame['headers']['redelivered']is True and frame['properties']['delivery_info']['redelivered']is True
   assert unlock(key,token)==1
   if fixture.backend=='Native':fixture.start_native()
   else:fixture.role('celery',argv)
   value=wait_terminal(child,'warm-child');assert value['status']=='SUCCESS'and value['result']is None and not receipt(child)
   return {'actual_normal_stop0_and_owner_receipt':True,'actual_restored_pair_and_same_child_consumed_after_restart':True}
  finally:
   unlock(key,token)
   # Do not hide a failed restart; the existing fixture still owns exact cleanup.
 cell('source-refresh-busy-durable-retry',busy);cell('source-refresh-defer-limit480',exhausted);cell('source-refresh-deferred-revoked',revoked);cell('source-refresh-warm-restored-restart',warm)
 report={'status':'passed'if all(v['status']=='passed'for v in cells)else'failed','cases':cells,'unqualified':['cold visibility/revoke races beyond this warm restart','held physical shutdown','missing-file download and incomplete-file policies']};save(out/'report.json',report);return report
