"""One existing-Fixture Source/Native HLS promotion→TS failover workflow.
No nested application bootstrap; Original Dispatcharr authors retain credit.
"""
import hashlib,json,urllib.request
from pathlib import Path
from live_reader import Reader
UA='dispatcharr-promotion-owned-account'
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def origin(fixture,path):
 script="import urllib.request,urllib.error,json;u='http://127.0.0.1:18098/"+path+"';\ntry:r=urllib.request.urlopen(u,timeout=2)\nexcept urllib.error.HTTPError as e:r=e\nexcept (urllib.error.URLError,TimeoutError):r=None\nif r is None:print(json.dumps([0,'not-ready']))\nelse:\n with r:print(json.dumps([r.status,r.read(1048577).decode()]))"
 status,body=json.loads(fixture.python(script))
 if path=='health':return status==200
 if status!=200:raise RuntimeError('ActualOwnedHLSOriginHTTP:'+str(status))
 return body
def stats(fixture):return json.loads(origin(fixture,'stats'))
def prepare_context(fixture):
 started=False
 try:
  root='/shared/'+fixture.case+'/promotion'
  fixture.python("import os;from pathlib import Path;p=Path("+repr(root)+");p.mkdir(mode=0o700,exist_ok=False);os.chown(p,65532,65532)")
  fixture.role('promotion-origin',['/dispatcharrpy/bin/python','-u','/normal-dvr-tools/promotion_origin.py'],extra={'PROMOTION_ROOT':root});started=True
  fixture.wait(lambda:origin(fixture,'health'),30,'actual-owned-HLS-origin-ready')
  # Primitive bulk setup of owned rows; not replacing Source signal/model code.
  script="from core.models import StreamProfile,UserAgent;from apps.channels.models import Channel,Stream;from apps.m3u.models import M3UAccountProfile;from django_celery_beat.models import PeriodicTask,PeriodicTasks;proxy=StreamProfile.get_locked('Proxy');a=UserAgent.objects.create(name='owned-promotion-account',user_agent="+repr(UA)+");b=UserAgent.objects.create(name='owned-promotion-profile',user_agent='must-not-win-input-profile-UA');StreamProfile.objects.filter(pk=proxy.pk).update(user_agent=b);p=M3UAccountProfile.objects.get(pk="+str(fixture.provider)+");type(p.m3u_account).objects.filter(pk=p.m3u_account_id).update(user_agent=a);c=Channel.objects.get(pk="+str(fixture.channel_id)+");Channel.objects.filter(pk=c.pk).update(stream_profile=proxy);rows=list(c.channelstream_set.order_by('order','pk'));Stream.objects.filter(pk=rows[0].stream_id).update(url='http://127.0.0.1:18098/primary/index.m3u8');n=PeriodicTask.objects.update(enabled=False);PeriodicTasks.update_changed();print(json.dumps({'actual_profile':proxy.name,'primary_stream_id':rows[0].stream_id,'backup_stream_id':rows[1].stream_id,'account_agent':a.user_agent,'conflicting_profile_agent':b.user_agent,'disabled_beat_rows':n}))"
  observed=fixture.sql(script);fixture._promotion=observed;save(fixture.output/'input-promotion-prepare.json',observed)
 except BaseException:
  if started:
   try:fixture.role('promotion-origin',[],stop=True)
   except Exception as error:save(fixture.output/'input-promotion-preparation-cleanup-error.json',{'type':type(error).__name__})
  raise
def run(fixture):
 out=fixture.output/'input-promotion-capture';out.mkdir(mode=0o755);report={'status':'running','scope':'Source lockedProxy HLS URL→actual FFmpeg, shared two viewers, real TS failover; no resource gains','cells':[],'full_M3_passed':False,'performance_verified':False};readers=[]
 def persist():save(out/'report.json',report)
 def processes():
  cid=fixture.native if fixture.backend=='Native'else fixture.command(fixture.compose+['ps','-q','baseline']).strip()
  return [line for line in fixture.command([fixture.docker,'top',cid,'-eo','pid,args']).splitlines()[1:]if'ffmpeg'in line and'http://127.0.0.1:18098/primary/index.m3u8'in line]
 try:
  for label in('owner','follower'):
   reader=Reader(fixture.base_url+'/proxy/ts/stream/'+fixture.channel_uuid,out/(label+'.ts'),fixture.actual_admin_token());readers.append(reader)
   fixture.wait(lambda:reader.snapshot()['media_bytes']>=750000,40,'actual-promoted-HLS-media-'+label)
  active=fixture.snapshot('input-promotion-active');observed=stats(fixture);report['active']=active;report['actual_input_process_count']=len(processes());report['origin']=observed;persist()
  if len(processes())!=1 or active['actual_provider_slots']!=1:raise AssertionError('OneActualSharedPromotedInputRequired')
  requests=[r for r in observed['requests']if r['path'].startswith('/primary/')]
  if not requests or any(r['user_agent']!=UA for r in requests):raise AssertionError('ActualAccountUserAgentPrecedenceRequired')
  report['cells'].append({'name':'actual-shared-promotion-and-account-agent','passed':True})
  before=[r.snapshot()['media_bytes']for r in readers];baseline=json.loads(fixture.get(fixture.origin+'/stats')[1]);origin(fixture,'control/fail-primary')
  fixture.wait(lambda:json.loads(fixture.get(fixture.origin+'/stats')[1])['connections']>baseline['connections'],120,'actual-HLS-to-TS-alternate-request')
  fixture.wait(lambda:all(r.snapshot()['media_bytes']>=n+188000 for r,n in zip(readers,before)),40,'actual-post-failover-useful-media')
  if processes():raise AssertionError('OldPromotedProcessRetirementRequired')
  report['cells'].append({'name':'actual-promotion-failover-retirement-and-useful-media','passed':True});report['after_failover']=fixture.snapshot('input-promotion-after-failover');persist()
  for reader in readers:reader.close()
  for label in('owner','follower'):
   path=out/(label+'.ts');guest='/evidence/'+str(path.relative_to(fixture.output));text=fixture.command(fixture.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-v','error','-xerror','-threads','1','-filter_threads','1','-filter_complex_threads','1','-i',guest,'-t','3','-map','0:v:0','-map','0:a:0','-threads:v','1','-threads:a','1','-f','null','-'],30,allow=True);receipt=fixture.cli[-1];error=Path(receipt['stderr']).read_bytes();cell={'name':label+'-actual-AV-prefix-decode','passed':receipt['actual_exit']==0 and not error and receipt['stdout_EOF']and receipt['stderr_EOF']and receipt['process_group_settled'],'stderr_sha256':hashlib.sha256(error).hexdigest(),'file_sha256':hashlib.sha256(path.read_bytes()).hexdigest()};report['cells'].append(cell);persist()
  if fixture.backend=='Native':report['actual_Main_stop']=fixture.stop_native(expected=0)
  else:
   for name in['beat','dvr','celery','web']:fixture.role(name,[],stop=True)
  fixture.role('promotion-origin',[],stop=True)
  report['status']='passed'if all(c['passed']for c in report['cells'])else'failed'
 except Exception as error:report.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:256])
 finally:
  for reader in readers:
   try:reader.close()
   except Exception as cleanup:report.update(status='failed',reader_cleanup_error=type(cleanup).__name__)
  report['readers']=[r.snapshot()for r in readers]
  if any(r['errors']or not r['ended']or r['http_status']!=200 or r['sync_errors']for r in report['readers']):report['status']='failed'
  persist()
 return report
