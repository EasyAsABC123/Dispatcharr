"""One genuine Source/Native common Redirect context in the existing Fixture.
All alias/counter observations come from actual Redis and real Main HTTP calls.
Seeded metadata states are declared branch inputs, not fabricated media owners.
"""
import hashlib,json,re,threading,time,urllib.error,urllib.request,uuid
from output_profile_reader import Reader
import preview_cases
from pathlib import Path
def save(p,v):Path(p).write_text(json.dumps(v,indent=2)+'\n')
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None
def prepare_context(f):
 state='/tmp/owned-redirect-'+uuid.uuid4().hex
 f.python("import os,pathlib;p=pathlib.Path("+repr(state)+");p.mkdir(mode=0o755);os.chown(p,65532,65532)")
 media=f.sql("from apps.channels.models import Channel;c=Channel.objects.get(pk="+str(f.channel_id)+");print(json.dumps(c.streams.order_by('channelstream__order').first().url))")
 if not re.fullmatch(r'http://origin:8080/live/[0-9]{1,20}\.ts',media):raise AssertionError('OwnedExistingMediaOriginOnly')
 upstream='11.254.'+('252'if f.backend=='Native'else'251')+'.3'
 origin=f.command(f.compose+['ps','-q','origin']).strip();inspection=json.loads(f.command([f.docker,'inspect',origin]))[0]
 if not any(row.get('IPAddress')==upstream for row in inspection['NetworkSettings']['Networks'].values()):raise AssertionError('ActualOwnedMediaOriginIP')
 f.role('redirect-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/redirect_origin.py','--state',state,'--upstream',upstream,'--media-path',media.split(':8080',1)[1]])
 record=f.roles['redirect-origin']
 ready=f.wait(lambda:json.loads(f.python("from pathlib import Path;p=Path("+repr(state+'/ready.json')+");print(p.read_text() if p.exists() else 'null')")),10,'actual-redirect-origin-ready')
 if ready['pid']!=record['pid']or ready['listener']!=['127.0.0.1',18099]:raise AssertionError('OwnedRedirectOriginIdentity')
 f._redirect_state=state
 seed=f.sql("from apps.channels.models import Channel;from core.models import StreamProfile,CoreSettings;c=Channel.objects.get(pk="+str(f.channel_id)+");profile=StreamProfile.objects.get(name='Redirect',locked=True);c.stream_profile=profile;c.save(update_fields=['stream_profile']);s=c.streams.order_by('channelstream__order').first();media=s.url;s.url='http://127.0.0.1:18099/ok';s.save(update_fields=['url']);p=s.m3u_account.profiles.get(is_default=True);p.max_streams=1;p.save(update_fields=['max_streams']);print(json.dumps({'stream':s.pk,'profile':p.pk,'account':s.m3u_account_id,'Redirect_profile':profile.pk,'media_url':media}))")
 f.sql("from apps.accounts.models import User;u=User.objects.get(pk="+str(f.accounts['admin']['id'])+");v=dict(u.custom_properties or {});v.pop('allowed_m3u_profile_ids',None);u.custom_properties=v;u.save(update_fields=['custom_properties']);print(json.dumps({'actual_owned_admin_unrestricted':True}))")
 f._redirect_seed=seed;save(f.output/'redirect-seed.json',{'actual_origin':ready,'actual_seed':{key:value for key,value in seed.items()if key!='media_url'},'media_url_sha256':hashlib.sha256(seed['media_url'].encode()).hexdigest()})
def run(f):
 out=f.output/'redirect-capture';out.mkdir(mode=0o755);report={'status':'running','cells':[],'full_M3_passed':False,'performance_verified':False,'unversioned_foreign_Source_ABA_unqualified':True}
 pending=[]
 def persist():save(out/'report.json',report)
 def cell(name,operation):
  row={'name':name,'status':'running'};report['cells'].append(row);persist()
  try:row['observation']=operation();row['status']='passed'
  except Exception as error:row.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:180])
  persist()
 def settings(validate):
  f.sql("from core.models import CoreSettings;s,_=CoreSettings.objects.get_or_create(key='proxy_settings',defaults={'name':'Proxy Settings','value':{}});s.value={**(s.value or {}),'validate_redirect_urls':"+repr(validate)+"};s.save();print(json.dumps({'actual_validate':s.value['validate_redirect_urls']}))")
 def request(label):
  item={'status':None,'settled':False};save(out/(label+'.json'),item)
  req=urllib.request.Request(f.base_url+'/proxy/ts/stream/'+f.channel_uuid,headers={'Authorization':'Bearer '+f.actual_admin_token()})
  try:
   try:reply=urllib.request.build_opener(NoRedirect()).open(req,timeout=f.remaining(12))
   except urllib.error.HTTPError as error:reply=error
   with reply:
    body=reply.read(65537)
    if len(body)>65536:raise ValueError('RedirectResponse64KiBBound')
    item.update(status=reply.status,location=reply.headers.get('Location'),body_bytes=len(body),body_sha256=hashlib.sha256(body).hexdigest())
    if reply.status>=400:
     try:item['body']=json.loads(body)
     except ValueError:item['body_not_JSON']=True
   item['settled']=True
  except Exception as error:item.update(error_type=type(error).__name__)
  save(out/(label+'.json'),item);return item
 def snapshot(label):
  seed=f._redirect_seed
  value=f.redis(0,"local a=redis.call('GET','channel_stream:'..ARGV[1]);local p=redis.call('GET','stream_profile:'..ARGV[2]);local m='live:channel:'..ARGV[3]..':metadata';return {a or'',p or'',redis.call('GET','profile_connections:'..ARGV[4])or'0',redis.call('EXISTS',m),redis.call('HGET',m,'state')or'',redis.call('SCARD','live:channel:'..ARGV[3]..':clients'),redis.call('GET','live:channel:'..ARGV[3]..':owner')or''}",str(f.channel_id),str(seed['stream']),f.channel_uuid,str(seed['profile']))
  observed={'stream_alias':value[0],'profile_alias':value[1],'profile_count':int(value[2]),'metadata_exists':value[3],'state':value[4],'clients':value[5],'raw_owner':value[6]};save(out/(label+'.json'),observed);return observed
 def held(label,operation):
  ident=uuid.uuid4().hex;seed=f._redirect_seed
  f.sql("from apps.channels.models import Stream;Stream.objects.filter(pk="+str(seed['stream'])+").update(url="+repr('http://127.0.0.1:18099/held/'+ident)+");print(json.dumps({'actual_held_URL_seeded':True}))")
  settings(True);result={};thread=threading.Thread(target=lambda:result.update(request(label+'-first')),daemon=True);pending.append(thread);thread.start()
  try:
   f.wait(lambda:json.loads(f.python("from pathlib import Path;import json;print(json.dumps(Path("+repr(f._redirect_state+'/'+ident+'-entered.json')+").exists()))")),4,'actual-redirect-HEAD-held')
   before=snapshot(label+'-while-held')
   if before['profile_count']!=1 or before['stream_alias']!=str(seed['stream'])or before['profile_alias']!=str(seed['profile'])or before['clients']or before['raw_owner']:raise AssertionError('ActualSourceAliasesOneReservedSlotNoWorker')
   value=operation(before,label)
   save(out/(label+'-parent-still-held-proof.json'),{'actual_request_thread_pending':thread.is_alive()})
   if not thread.is_alive():raise AssertionError('BorrowObservationMustPrecedeParentFinalization')
  finally:
   f.python("from pathlib import Path;Path("+repr(f._redirect_state+'/'+ident+'.release')+").write_text('release')")
   thread.join(f.remaining(13))
   save(out/(label+'-first-final.json'),dict(result,host_thread_settled=not thread.is_alive()))
   if thread.is_alive():raise RuntimeError('ActualHeldRedirectHTTPNotSettled')
  if result.get('status')!=302 or not result.get('settled'):raise AssertionError('ActualHeldFirstHandoff')
  final=snapshot(label+'-final')
  if final['stream_alias']or final['profile_alias']or final['profile_count']!=0 or final['clients']or final['raw_owner']:raise AssertionError('ActualRedirectFinalAliasesCounterNoPhantomClient')
  return {'before':before,'operation':value,'first':result,'final':final}
 def borrow(before,label):
  answer=request(label+'-borrowed');after=snapshot(label+'-after-borrow')
  if answer['status']!=302 or after!=before:raise AssertionError('ActualBorrowDoesNotReserveOrRelease')
  return answer
 def inverse():
  seed=f._redirect_seed;reader=None;ident=uuid.uuid4().hex;result={};
  try:
   f.sql("from apps.channels.models import Channel,Stream;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");c.stream_profile=StreamProfile.objects.get(name='Redirect',locked=True);c.save(update_fields=['stream_profile']);Stream.objects.filter(pk="+str(seed['stream'])+").update(url="+repr('http://127.0.0.1:18099/held/'+ident)+");print(json.dumps({'actual_inverse_parent_Redirect':True}))")
   settings(True);thread=threading.Thread(target=lambda:result.update(request('inverse-parent')),daemon=True);pending.append(thread);thread.start()
   try:
    f.wait(lambda:json.loads(f.python("from pathlib import Path;import json;print(json.dumps(Path("+repr(f._redirect_state+'/'+ident+'-entered.json')+").exists()))")),4,'actual-inverse-parent-HEAD-held')
    initial=snapshot('inverse-parent-reserved')
    if initial['profile_count']!=1 or not initial['stream_alias']:raise AssertionError('ActualInverseParentSourceSlot')
    f.sql("from apps.channels.models import Channel;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");c.stream_profile=StreamProfile.objects.get(name='Proxy',locked=True);c.save(update_fields=['stream_profile']);print(json.dumps({'actual_inverse_now_Proxy':True}))")
    f.sql("from apps.m3u.models import M3UAccount,M3UAccountProfile;M3UAccount.objects.filter(pk="+str(seed['account'])+").update(is_active=False);M3UAccountProfile.objects.filter(pk="+str(seed['profile'])+").update(is_active=False);print(json.dumps({'actual_retained_assignment_inactive_before_raw':True}))")
    before_origin=json.loads(f.get(f.origin+'/stats')[1]);reader=Reader(f.base_url+'/proxy/ts/stream/'+f.channel_uuid,out/'inverse-raw.ts',f.actual_admin_token())
    adopted=f.wait(lambda: (lambda observed: observed if observed['profile_count']==1 and observed['clients']==1 and observed['raw_owner']and json.loads(f.get(f.origin+'/stats')[1]).get('active')==1 else None)(snapshot('inverse-raw-admission-observation')),3,'actual-Proxy-borrows-pending-Redirect')
    save(out/'inverse-pre-release-proof.json',{'initial':initial,'adopted':adopted,'parent_still_pending':thread.is_alive(),'actual_owned_media_Origin_connected':True,'buffered_media_readiness_not_required_before_parent_5s_timeout':True})
    if not thread.is_alive()or adopted['profile_count']!=1 or adopted['clients']!=1 or not adopted['raw_owner']:raise AssertionError('ActualInverseMustAdoptNoSecondReservation')
   finally:
    f.python("from pathlib import Path;Path("+repr(f._redirect_state+'/'+ident+'.release')+").write_text('release')")
    thread.join(f.remaining(13));save(out/'inverse-parent-final.json',dict(result,host_thread_settled=not thread.is_alive()))
   if thread.is_alive()or result.get('status')!=302:raise AssertionError('ActualInverseParentHandoff')
   after=snapshot('inverse-after-parent-finally');base=reader.snapshot()['media_bytes']
   f.wait(lambda:reader.snapshot()['media_bytes']>=base+188000 and not reader.snapshot()['errors']and not reader.snapshot()['ended'],40,'actual-borrowed-Proxy-survives-parent-finally')
   samples=[]
   for i in range(3):
    f.sleep(1.1);samples.append(snapshot('inverse-parent-released-metadata-tick-'+str(i)))
   origin=json.loads(f.get(f.origin+'/stats')[1]);proof={'parent':result,'adopted':adopted,'after_parent':after,'samples':samples,'origin_new_connections':origin['connections']-before_origin['connections'],'origin_active':origin['active'],'Source_slot_timing_claim':'parent finally may consume slot while borrowed media remains; no lifetime-quota guarantee','raw_fresh_advance':True}
   save(out/'inverse-parent-raw-proof.json',proof)
   if after['profile_count']!=0 or after['stream_alias']or after['profile_alias']or any(row['profile_count']!=0 or row['clients']!=1 for row in samples)or proof['origin_new_connections']!=1 or proof['origin_active']!=1:raise AssertionError('ActualSourceParentSlotConsumedOnceWhileProxyMediaContinues')
   return proof
  except BaseException as error:
   save(out/'inverse-primary-failure.json',{'error_type':type(error).__name__,'error_label':str(error)[:180]});raise
  finally:
   f.sql("from apps.m3u.models import M3UAccount,M3UAccountProfile;M3UAccount.objects.filter(pk="+str(seed['account'])+").update(is_active=True);M3UAccountProfile.objects.filter(pk="+str(seed['profile'])+").update(is_active=True);print(json.dumps({'restored_retained_assignment_activity':True}))")
   if reader:reader.close();save(out/'inverse-raw-final.json',reader.snapshot())
   if reader:
    stopped=f.api('POST','/proxy/ts/stop/'+f.channel_uuid,{},'admin');save(out/'inverse-dependency-stop.json',stopped)
    if stopped['status']!=200:raise AssertionError('ActualInverseStopForNextIndependentCells')
    f.wait(lambda:json.loads(f.get(f.origin+'/stats')[1]).get('active')==0 and f.redis(0,"return {redis.call('GET','live:channel:'..ARGV[1]..':owner')or'',redis.call('EXISTS','live:channel:'..ARGV[1]..':stopping')}",f.channel_uuid)==['',0],30,'inverse-actual-retirement-before-remaining-cells')
    f.sql("from apps.channels.models import Channel;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");c.stream_profile=StreamProfile.objects.get(name='Redirect',locked=True);c.save(update_fields=['stream_profile']);print(json.dumps({'restored_actual_Redirect_for_next_cells':True}))")
 cell('pending-Redirect-to-Proxy-reuses-inactive-retained-assignment',inverse)
 cell('reserved-before-metadata-concurrent-borrow-max1',lambda:held('missing-metadata',borrow))
 def inactive(before,label):
  seed=f._redirect_seed
  f.sql("from apps.m3u.models import M3UAccount,M3UAccountProfile;M3UAccount.objects.filter(pk="+str(seed['account'])+").update(is_active=False);M3UAccountProfile.objects.filter(pk="+str(seed['profile'])+").update(is_active=False);print(json.dumps({'declared_inactive_SQL_input':True}))")
  try:return borrow(before,label)
  finally:f.sql("from apps.m3u.models import M3UAccount,M3UAccountProfile;M3UAccount.objects.filter(pk="+str(seed['account'])+").update(is_active=True);M3UAccountProfile.objects.filter(pk="+str(seed['profile'])+").update(is_active=True);print(json.dumps({'restored':True}))")
 cell('borrowed-assignment-survives-current-inactive-SQL',lambda:held('inactive-profile',inactive))
 def stale(before,label):
  seed=f._redirect_seed
  f.redis(0,"local key='live:channel:'..ARGV[1]..':metadata';redis.call('HSET',key,'state','stopped','m3u_profile',ARGV[2],'stream_id',ARGV[3]);return 1",f.channel_uuid,str(seed['profile']),str(seed['stream']))
  seeded=snapshot(label+'-declared-stopped-input');answer=request(label+'-new-after-stale');after=snapshot(label+'-after-new')
  if answer['status']!=302 or after['profile_count']!=0 or after['stream_alias']or after['profile_alias']:raise AssertionError('ActualStaleClaimAndNewGrantCleanup')
  return {'seeded_metadata_is_branch_input_not_media_owner':seeded,'response':answer,'after':after}
 cell('existing-stopped-metadata-claims-stale-slot-once',lambda:held('stale-assignment',stale))
 def full():
  seed=f._redirect_seed;settings(False)
  f.redis(0,"redis.call('DEL','live:channel:'..ARGV[1]..':metadata');redis.call('SET','profile_connections:'..ARGV[2],'1');return 1",f.channel_uuid,str(seed['profile']))
  try:
   answer=request('foreign-full');after=snapshot('foreign-full-after')
   if answer['status']!=503 or after['profile_count']!=1 or after['stream_alias']or after['profile_alias']or after['clients']:raise AssertionError('ForeignReservationMustRemainUntouched')
   return {'foreign_counter_is_declared_input':True,'response':answer,'after':after}
  finally:f.redis(0,"redis.call('DEL','profile_connections:'..ARGV[1]);return 1",str(seed['profile']))
 cell('foreign-full-counter-refuses-without-private-grant',full)
 def previews():
  observed=preview_cases.run(f)
  if observed['status']!='passed':raise AssertionError('RequiredDirectPreviewCellsFailed')
  return observed
 cell('direct-default-and-scoped-profile-preview',previews)

 def active_proxy():
  seed=f._redirect_seed;media=seed['media_url']
  if not re.fullmatch(r'http://origin:8080/live/[0-9]{1,20}\.ts',media):raise AssertionError('OwnedExistingMediaOriginOnly')
  settings(False)
  f.sql("from apps.channels.models import Channel,Stream;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");c.stream_profile=StreamProfile.objects.get(name='Proxy',locked=True);c.save(update_fields=['stream_profile']);Stream.objects.filter(pk="+str(seed['stream'])+").update(url="+repr(media)+");print(json.dumps({'actual_existing_channel_now_Proxy':True}))")
  before_origin=json.loads(f.get(f.origin+'/stats')[1]);reader=Reader(f.base_url+'/proxy/ts/stream/'+f.channel_uuid,out/'existing-proxy.ts',f.actual_admin_token())
  try:
   f.wait(lambda:reader.snapshot()['media_bytes']>=750000 and not reader.snapshot()['errors'],30,'actual-Proxy-media-before-Redirect')
   before=snapshot('real-active-proxy-before-profile-edit')
   if not(before['stream_alias']==str(seed['stream'])and before['profile_alias']==str(seed['profile'])and before['profile_count']==1 and before['metadata_exists']and before['state']in('active','waiting_for_clients','buffering','connecting','initializing')and before['clients']==1 and before['raw_owner']):raise AssertionError('ActualRunningProxyPublicAssignment')
   f.sql("from apps.channels.models import Channel;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");c.stream_profile=StreamProfile.objects.get(name='Redirect',locked=True);c.save(update_fields=['stream_profile']);print(json.dumps({'actual_live_Channel_profile_changed_to_Redirect':True}))")
   answer=request('Proxy-to-Redirect-handoff');after=snapshot('real-active-proxy-after-Redirect');base=reader.snapshot()['media_bytes']
   f.wait(lambda:reader.snapshot()['media_bytes']>=base+188000 and not reader.snapshot()['errors']and not reader.snapshot()['ended'],10,'actual-Proxy-survives-Redirect-handoff')
   origin=json.loads(f.get(f.origin+'/stats')[1]);proof={'before':before,'response':answer,'after':after,'ordinary_raw_fresh_advance':True,'origin_new_connections':origin['connections']-before_origin['connections'],'origin_active':origin['active'],'validation_disabled_for_this_declared_slice':True,'private_Proxy_lease_or_provider_reserve_for_Redirect':False}
   save(out/'real-proxy-Redirect-proof.json',proof)
   if answer['status']!=302 or answer.get('location')!=media or any(before[key]!=after[key]for key in ('stream_alias','profile_alias','profile_count','clients','raw_owner'))or not after['metadata_exists']or proof['origin_new_connections']!=1 or proof['origin_active']!=1:raise AssertionError('ActualRedirectMustBorrowRunningProxyNoNewClientOrUpstream')
   return proof
  finally:
   reader.close();save(out/'real-proxy-reader-final.json',reader.snapshot())
   if not reader.snapshot()['ended']or reader.snapshot()['errors']:raise RuntimeError('ActualProxyViewerNotSettled')
 # Last business cell: actual Source post-viewer retention is observed literally;
 # normal Main/role drain and exact fixture cleanup remain outer requirements.
 def extensions():
  import redirect_extensions,inactive_input_cases,channel_delete_cases,direct_redirect_cases,account_custom_cases,redirect_provider_framing
  observed=redirect_extensions.run(f,[('account-custom-scalars',account_custom_cases.run),('inactive-input-profile',inactive_input_cases.run),('Channel-Recording-Collector',channel_delete_cases.run),('saved-Redirect-characterization',direct_redirect_cases.run),('pooled-Redirect-fixed-probe-fallback',redirect_provider_framing.run)])
  if observed['status']!='passed':raise AssertionError('RequiredRedirectExtensionsFailed')
  return observed
 cell('same-context-inactive-input-Collector-and-characterization',extensions)
 cell('running-Proxy-to-Redirect-reuses-actual-raw-Lease-aliases',active_proxy)

 try:
  for thread in pending:
   if thread.is_alive():thread.join(f.remaining(13))
  report['all_owned_request_threads_settled']=all(not thread.is_alive()for thread in pending)
 except Exception as error:report['request_cleanup_error']=type(error).__name__
 try:
  report['actual_Origin_stop']=f.role('redirect-origin',[],stop=True)
  report['actual_Origin_final']=json.loads(f.python("from pathlib import Path;print(Path("+repr(f._redirect_state+'/final.json')+").read_text())"))
  if not(report['actual_Origin_final']['threads_joined']and report['actual_Origin_final']['active']==0):raise AssertionError('ActualOriginSocketThreadDrain')
 except Exception as error:report['Origin_cleanup_error']=type(error).__name__
 report['status']='passed'if len(report['cells'])==8 and all(row['status']=='passed'for row in report['cells'])and report.get('all_owned_request_threads_settled')and report.get('actual_Origin_stop')and not report.get('Origin_cleanup_error')else'failed';persist();return report
