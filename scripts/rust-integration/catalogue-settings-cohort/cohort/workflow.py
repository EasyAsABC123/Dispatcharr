"""One normal DVR Source/Native workflow. No application starts on import.
Original Dispatcharr authors retain application/algorithm/protocol credit.
Actual fixture adapter must own Docker Compose, SQL/cache/worker/media evidence.
This workflow has no synthetic producer, frame normalization or green fallback.
"""
import datetime,json,time,urllib.request,urllib.error
from pathlib import Path
from budget import bounded_read
UTC=datetime.timezone.utc

def utc():return datetime.datetime.now(UTC)
def iso(value):return value.isoformat()
def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
class Unsupported(Exception):pass
class Workflow:
 def __init__(self,fixture,output):
  self.fixture=fixture;self.out=Path(output);self.out.mkdir(exist_ok=False);self.serial=0
  self.report={'status':'running','cases':[],'full_milestone_passed':False,'performance_verified':False,'no_Source_model18_replay':True};self.flush()
 def flush(self):save(self.out/'report.json',self.report)
 def request(self,method,path,body=None):
  # Token comes only from actual owned fixture authentication, never a static
  # key or the internal FFmpeg URL. No token/header is written to evidence.
  token=self.fixture.actual_admin_token();headers={'Authorization':'Bearer '+token}
  encoded=None
  if body is not None:encoded=json.dumps(body).encode();headers['Content-Type']='application/json'
  request=urllib.request.Request(self.fixture.base_url+path,data=encoded,headers=headers,method=method)
  self.serial+=1;label=self.fixture.case+'-http-'+str(self.serial).zfill(4);record={'context':self.fixture.case,'method':method,'path':path,'body':body,'began_utc':iso(utc()),'status':None,'failure':None}
  save(self.out/(label+'.json'),record)
  try:
   try:response=urllib.request.urlopen(request,timeout=self.fixture.remaining(15))
   except urllib.error.HTTPError as error:response=error
   with response:
    raw=bounded_read(response,1048576,self.fixture.remaining);record['status']=response.status
    if len(raw)>1048576:raise ValueError('ActualHTTPResponse1MiBBudget')
    (self.out/(label+'.body')).write_bytes(raw);record['response_bytes']=len(raw)
    value=json.loads(raw)if raw else None
    return response.status,value
  except BaseException as error:record['failure']=type(error).__name__;raise
  finally:record['finished_utc']=iso(utc());save(self.out/(label+'.json'),record)
 def snapshot(self,label):
  # Adapter persists raw queue/SQL/Redis/process/origin/file observations BEFORE
  # deriving this narrow projection. These are real outputs, not permissions.
  observed=self.fixture.snapshot(label);save(self.out/(label+'.json'),observed);return observed
 def recording(self,ident):
  code,row=self.request('GET',f'/api/channels/recordings/{ident}/')
  if code==404:return None
  if code!=200:raise AssertionError(('ActualRecordingRead',ident,code,row))
  return row
 def wait(self,predicate,seconds,label):
  deadline=time.monotonic()+self.fixture.remaining(seconds)
  while time.monotonic()<deadline:
   self.fixture.remaining(1)
   actual=predicate()
   if actual:return actual
   time.sleep(min(.5,max(0,deadline-time.monotonic())))
  self.snapshot(label+'-deadline');raise TimeoutError(label)
 def book(self,start,end,title):
  code,row=self.request('POST','/api/channels/recordings/',{'channel':self.fixture.channel_id,'start_time':iso(start),'end_time':iso(end),'custom_properties':{'program':{'title':self.fixture.channel_name},'fixture_case_label':title}})
  if code==501:raise Unsupported('normal production Booking route/effect not assembled')
  if code!=201 or not isinstance(row,dict)or not isinstance(row.get('id'),int):raise AssertionError(('ActualBooking',code,row))
  return row['id']
 def active(self,ids):
  values=[self.recording(i)for i in ids]
  return values if all(r and r.get('custom_properties',{}).get('status')=='recording'for r in values)else None
 def terminal(self,ident,status):
  row=self.recording(ident)
  cp={}if row is None else row.get('custom_properties')or{}
  return row if cp.get('status')==status and 'remux_success'in cp else None
 def demand(self,observed,recorders):
  assert observed['actual_upstream_connections']==1,observed
  assert observed['actual_provider_slots']==1,observed
  assert observed['actual_recording_ffmpeg_count']==recorders,observed
 def decode(self,ident):
  result=self.fixture.decode_complete_recording(ident)
  save(self.out/(f'{self.fixture.case}-decoder-{ident}.json'),result)
  assert result['actual_exit']==0 and result['process_group_settled']and result['stdout_EOF']and result['stderr_EOF'],result
  assert result['video_frames']>0 and result['audio_frames']>0 and result['actual_file_bytes']>0,result
  assert result['internal_damage_detected']is False,result
 def overlap(self):
  start=(utc()+datetime.timedelta(seconds=10)).replace(microsecond=0)
  ids=[self.book(start,start+datetime.timedelta(seconds=95),'Normal overlap'),self.book(start+datetime.timedelta(seconds=1),start+datetime.timedelta(seconds=92),'Normal overlap')]
  self.wait(lambda:self.active(ids),35,'two-real-active-recorders');self.fixture.wait_for_hls_segments(ids,2,25)
  self.demand(self.snapshot('overlap-active'),2)
  import playback_cases
  active_playback=playback_cases.run(self.fixture,ids[0],active=True);save(self.out/'overlap-active-playback.json',active_playback);assert active_playback['executed_slice_passed'],active_playback
  active_m6=self.fixture.active_viewer_cases(self.out);save(self.out/'active-viewer-M6.json',active_m6)
  assert active_m6.get('status')=='passed',active_m6
  for ident in ids:self.wait(lambda ident=ident:self.terminal(ident,'completed'),140,'overlap-terminal-'+str(ident));self.decode(ident)
  playback=playback_cases.run(self.fixture,ids[0]);save(self.out/'overlap-completed-playback.json',playback);assert playback['executed_slice_passed'],playback
  self.report['required_playback_unmeasured']=playback['required_unmeasured']
  final=self.snapshot('overlap-final');assert final['all_owned_recording_children_retired']and final['owned_hls_directories_absent'],final
  assert final['maximum_actual_segment_count']>16,final
  slow=self.fixture.slow_reader_cases(ids[0]);save(self.out/'slow-reader.json',slow)
  assert slow['executed_slice_passed'],slow
  if self.fixture.backend=='Native':assert slow['physical_reader_shutdown_qualified'],slow
  self.report['required_Source_shutdown_unmeasured']=slow['required_unmeasured']
  if slow.get('physical_reader_shutdown_qualified')or slow.get('Source_held_TERM_characterized'):
   self.report['required_playback_unmeasured']=[v for v in self.report['required_playback_unmeasured']if v!='physical slow-reader shutdown']
 def controls(self):
  start=(utc()+datetime.timedelta(seconds=10)).replace(microsecond=0)
  ids=[self.book(start+datetime.timedelta(seconds=i),start+datetime.timedelta(seconds=75+i),'Normal controls')for i in range(3)]
  self.fixture.start_ordinary_viewer(recording_prefix=ids[0])
  self.wait(lambda:self.active(ids),40,'three-real-active-recorders');self.fixture.wait_for_hls_segments(ids,2,25)
  before=self.snapshot('controls-active');self.demand(before,3)
  code,result=self.request('POST',f'/api/channels/recordings/{ids[1]}/extend/',{'extra_minutes':1});assert code==200,(code,result)
  code,result=self.request('POST',f'/api/channels/recordings/{ids[0]}/stop/',{});assert code==200 and result=={'success':True,'status':'stopped'},(code,result)
  self.wait(lambda:self.terminal(ids[0],'stopped'),50,'stopped-partial-finalized');self.decode(ids[0])
  self.demand(self.snapshot('controls-after-selective-stop'),2)
  code,result=self.request('DELETE',f'/api/channels/recordings/{ids[2]}/');assert code==204 and result is None,(code,result)
  self.wait(lambda:self.recording(ids[2])is None,10,'deleted-row-absent')
  self.fixture.wait_for_recording_absence(ids[2],30)
  self.demand(self.snapshot('controls-after-selective-delete'),1)
  self.fixture.wait_past_recording_original_deadline(ids[1],10)
  alive=self.recording(ids[1]);assert alive['custom_properties']['status']=='recording',alive
  self.wait(lambda:self.terminal(ids[1],'completed'),100,'extended-recorder-completed');self.decode(ids[1])
  result=self.snapshot('controls-final');assert result['cancelled_recording_end_count']==1 and result['late_duplicate_cancelled_end_count']==0,result
  assert result['other_viewers_preserved']and result['all_owned_recording_children_retired'],result
  self.fixture.stop_ordinary_viewer()
 def nonempty_restart(self):
  # Actual Source HTTP producer executes while its actual DVR consumer is off.
  # Native startup adopts those real rows/raw frames, not a fake minted registry.
  proof=self.fixture.produce_original_bookings_with_dvr_stopped()
  save(self.out/'restart-actual-source-producer.json',proof)
  assert proof['actual_HTTP_statuses']and all(c==201 for c in proof['actual_HTTP_statuses']),proof
  assert proof['actual_dvr_queue_length']>0 and proof['actual_source_frames_retained']and proof['no_frame_rewrite'],proof
  self.fixture.start_backend_on_preserved_nonempty_database_and_queue()
  result=self.fixture.observe_complete_restart_recovery()
  save(self.out/'restart-recovery.json',result)
  for field in ['old_frame_actual_cas','named_and_UUID_keys_preserved','due_actual_media','expired_actual_remux','expired_missing_playlist_interrupted','future_periodic_repaired','queued_deleted_row_consumed','revoked_deleted_row_consumed','original_recording_window_preserved','all_owned_recording_children_retired']:
   assert result[field]is True,(field,result)
  for ident in result['actual_media_recording_ids']:self.decode(ident)
 def streams_denial(self):
  self.fixture.apply_actual_streams_deny_policy_for_loopback()
  try:
   start=utc()+datetime.timedelta(seconds=2);ident=self.book(start,start+datetime.timedelta(seconds=24),'Actual STREAMS denial')
   self.wait(lambda:self.terminal(ident,'interrupted'),80,'streams-denied-capture-terminal')
   result=self.snapshot('streams-denied');assert result['actual_internal_TS_HTTP_denied']and result['actual_upstream_connections']==0 and result['actual_provider_slots']==0,result
   assert result['invented_internal_credentials']is False and result['loopback_policy_bypass']is False,result
  finally:self.fixture.restore_actual_streams_policy()
 def authorization(self):
  result=self.fixture.authorization_cases();save(self.out/'authorization.json',result);assert result['passed'],result
 def artwork(self):
  result=self.fixture.artwork_cases();save(self.out/'artwork.json',result);assert result['passed'],result
 def grouped_m6(self):
  result=self.fixture.run_selected_families(self.out);save(self.out/'grouped-m6.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def core_produced(self):
  result=self.fixture.run_core_context(self.out);save(self.out/'core-produced.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def idle_viewing(self):
  result=self.fixture.idle_viewing_cases();save(self.out/'idle-viewing.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def xc_live(self):
  result=self.fixture.xc_live_cases();save(self.out/'xc-live.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def plugin_eta(self):
  import eta_cases
  result=eta_cases.run(self.fixture);save(self.out/(self.fixture.case+'.json'),result)
  if not result['passed']:raise AssertionError(result)
 def provider_artwork(self):
  result=self.fixture.provider_artwork_cases();save(self.out/'recording-provider-artwork.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def restore(self):
  result=self.fixture.restore_cases();save(self.out/(self.fixture.case+'.json'),result)
  if result.get('status',result.get('state'))!='passed':raise AssertionError(result)
 def recording_plugin_characterization(self):
  import recording_connect_cases
  actual=recording_connect_cases.run(self.fixture);save(self.out/'recording-plugin-characterization.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def recording_connect_boundaries(self):
  import recording_connect_boundaries
  actual=recording_connect_boundaries.run(self.fixture);save(self.out/'recording-connect-boundaries.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def normal_backup(self):
  result=self.fixture.normal_backup_cases();save(self.out/'normal-backup.json',result)
  if result.get('status')!='passed':raise AssertionError(result)
 def backup_physical(self):
  result=self.fixture.backup_physical_cases();save(self.out/(self.fixture.case+'.json'),result)
  if result.get('status')!='passed':raise AssertionError(result)
 def channel_maintenance(self):
  result=self.fixture.channel_maintenance_cases();save(self.out/'channel-maintenance.json',result)
  if result.get('passed')is not True:raise AssertionError(result)
 def source_slow_reader(self):
  result=self.fixture.source_retained_slow_reader();save(self.out/'source-slow-reader.json',result)
  if not result.get('executed_slice_passed')or result.get('Source_held_TERM_characterized')is not True:raise AssertionError(result)
  self.report['required_Source_shutdown_unmeasured']=result.get('required_unmeasured',[])
 def held_drain(self):
  result=self.fixture.native_held_drain();save(self.out/'held-drain.json',result)
  assert result['native_exit']['actual_exit']==1,result
 def auto_sync(self):
  import auto_sync_cases,compact_cases,compact_api_cases,regex_cases
  actual=auto_sync_cases.run(self.fixture);save(self.out/'auto-sync.json',actual)
  large=next((r for r in actual.get('cases',[])if r.get('name',r.get('cell'))=='actual-large-parent-5001-descendants-full-metadata'),None)
  results={}
  for name,module in [('compact',compact_cases),('compact-api',compact_api_cases),('regex',regex_cases)]:
   retained=self.fixture.binding.get('retained_Source_subcontexts',{}).get(name)if self.fixture.backend=='Original'else None
   if retained is not None:
    results[name]={'status':'retained_source_authority','satisfies_dependency':True,'reference':retained,'scope':'qualified Source23 leaf, no application replay; current Native must execute'}
   elif large and large.get('status')=='passed':
    try:results[name]=module.run(self.fixture)
    except Exception as error:results[name]={'status':'failed','failure':type(error).__name__+':'+str(error)[:1024]}
   else:results[name]={'status':'blocked','reason':'actual large descendants not settled'}
   save(self.out/(name+'.json'),results[name])
  if actual.get('status')!='passed'or any(not(r.get('status')=='passed'or(r.get('status')=='retained_source_authority'and r.get('satisfies_dependency')is True))for r in results.values()):
   self.fixture.auto_failure_snapshot('auto-dependent-failure')
   raise AssertionError({'auto':actual.get('status'),**{k:v.get('status')for k,v in results.items()}})
 def input_promotion(self):
  import input_promotion_cases
  actual=input_promotion_cases.run(self.fixture);save(self.out/'input-promotion.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def output_profile(self):
  import output_profile_cases
  actual=output_profile_cases.run(self.fixture);save(self.out/'output-profile.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vlc_live(self):
  import vlc_live_cases
  actual=vlc_live_cases.run(self.fixture);save(self.out/'vlc-live.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def schedules_direct(self):
  import sd_guide_cases,xml_tvg_cases,xmltv_download_cases,xmltv_selected_cases,xmltv_properties_output_cases,epg_connect_cases,xml_hook_cases,xml_remote_hook_cases,dummy_fallback_cases,stream_name_cases,custom_guide_cases
  results=[]
  for name,module in [('schedules-direct',sd_guide_cases),('xml-tvg-deferred',xml_tvg_cases),('xmltv-download',xmltv_download_cases),('xmltv-selected',xmltv_selected_cases),('xmltv-property-output',xmltv_properties_output_cases),('epg-connect',epg_connect_cases),('xml-local-hooks',xml_hook_cases),('xml-remote-hooks',xml_remote_hook_cases),('dummy-fallback',dummy_fallback_cases),('stream-name',stream_name_cases),('custom-guide',custom_guide_cases)]:
   try:actual=module.run(self.fixture)
   except Exception as error:actual={'status':'failed','error_type':type(error).__name__,'error_label':str(error)[:1024]}
   save(self.out/(name+'.json'),actual);results.append(actual)
  if self.fixture.backend=='Native'and all(x.get('status')=='passed'for x in results):
   held=xmltv_selected_cases.run_held_deadline(self.fixture);save(self.out/'xmltv-selected-held-deadline.json',held);results.append(held)
  if any(x.get('status')!='passed'for x in results):raise AssertionError({str(n):x.get('status')for n,x in enumerate(results)})
 def custom_guide_held(self):
  import custom_guide_cases
  actual=custom_guide_cases.run_held_native(self.fixture);save(self.out/'custom-guide-held.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def m8_actions(self):
  from m8_actions import cases as m8_actions_cases
  outcomes={};errors={}
  try:
   actual=m8_actions_cases.run(self.fixture);save(self.out/'m8-actions.json',actual)
   outcomes['actions']='passed'if actual.get('passed')is True else'failed'
  except Exception as error:
   outcomes['actions']='failed';errors['actions']={'error_type':type(error).__name__,'reason':str(error)[:1024]}
  if self.fixture.backend=='Native':
   # A failed action observation cannot erase independent negative-domain cells.
   # Retire any surviving Main before fresh startup, preserving literal status.
   if outcomes['actions']!='passed'and self.fixture.native is not None:
    try:save(self.out/'m8-actions-failed-observer-Main-stop.json',self.fixture.stop_native(expected=None))
    except Exception as error:
     outcomes['boundary_prerequisite']='failed';outcomes['domain_boundaries']='blocked';errors['boundary_prerequisite']={'error_type':type(error).__name__,'reason':str(error)[:1024]}
   if 'boundary_prerequisite'not in errors:
    try:
     import dvr_domain_cases
     boundaries=dvr_domain_cases.run(self.fixture);save(self.out/'dvr-domain-boundaries.json',boundaries)
     outcomes['domain_boundaries']=boundaries.get('status')
    except Exception as error:
     outcomes['domain_boundaries']='failed';errors['domain_boundaries']={'error_type':type(error).__name__,'reason':str(error)[:1024]}
  save(self.out/'m8-independent-workflows.json',{'outcomes':outcomes,'errors':errors,'failed_action_not_relabelled':True,'full_M8_acceptance':False})
  if any(value!='passed'for value in outcomes.values()):raise AssertionError({'outcomes':outcomes,'errors':errors})
 def stream_file_crud(self):
  import stream_file_cases
  actual=stream_file_cases.run(self.fixture);save(self.out/'stream-file-crud.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def process_transports(self):
  import process_transport_cases
  actual=process_transport_cases.run(self.fixture);save(self.out/'process-transports.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def recording_output_profile(self):
  import recording_profile_cases
  actual=recording_profile_cases.run(self.fixture);save(self.out/'recording-output-profile.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def redirect_common(self):
  import redirect_cases
  actual=redirect_cases.run(self.fixture);save(self.out/'redirect-common.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def channel_matching(self):
  import matching_cases
  actual=matching_cases.run(self.fixture);save(self.out/'channel-matching.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def comskip_manual(self):
  import comskip_cases
  actual=comskip_cases.run(self.fixture);save(self.out/'comskip-manual.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
  import comskip_positive_epochs
  positive=comskip_positive_epochs.run(self.fixture);save(self.out/'comskip-positive-epochs.json',positive)
  if positive.get('status')!='passed':raise AssertionError(positive)
 def live_vod_group_settings(self):
  import live_vod_settings_cases
  actual=live_vod_settings_cases.run(self.fixture);save(self.out/'live-vod-group-settings.json',actual)
  if actual['status']!='passed':raise AssertionError(actual)
 def vod_category_language(self):
  import vod_category_language_cases
  actual=vod_category_language_cases.run(self.fixture);save(self.out/'vod-category-language-lifecycle.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_catalogue_scale(self):
  import vod_catalogue_scale_cases
  actual=vod_catalogue_scale_cases.run(self.fixture);save(self.out/'vod-catalogue-scale.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_grouped_catalogue(self):
  import vod_grouped_catalogue_cases
  actual=vod_grouped_catalogue_cases.run(self.fixture);save(self.out/(self.fixture.case+'.json'),actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_external_identity(self):
  import vod_external_identity_cases
  actual=vod_external_identity_cases.run(self.fixture);save(self.out/'vod-external-identity.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_shared_metadata(self):
  import vod_shared_metadata_cases
  actual=vod_shared_metadata_cases.run(self.fixture);save(self.out/'vod-shared-metadata.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_dense_list(self):
  import vod_dense_list_cases
  actual=vod_dense_list_cases.run(self.fixture);save(self.out/'vod-dense-list.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_series_advanced(self):
  import vod_series_advanced_cases
  actual=vod_series_advanced_cases.run(self.fixture);save(self.out/'vod-series-advanced.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_series_info(self):
  import vod_series_info_cases
  actual=vod_series_info_cases.run(self.fixture);save(self.out/'vod-series-info.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_series(self):
  import vod_series_cases
  actual=vod_series_cases.run(self.fixture);save(self.out/'vod-series.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def vod_repeat(self):
  import vod_repeat_cases
  actual=vod_repeat_cases.run(self.fixture);save(self.out/'vod-repeat.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def m3u_refresh_all(self):
  import m3u_refresh_all_cases
  actual=m3u_refresh_all_cases.run(self.fixture);save(self.out/'m3u-refresh-all.json',actual)
  if actual.get('status')!='passed':raise AssertionError(actual)
 def run(self):
  cases=[('authorization',self.authorization),('artwork',self.artwork),('grouped-m6',self.grouped_m6),('channel-maintenance',self.channel_maintenance),('normal-backup',self.normal_backup),('core-produced',self.core_produced),('overlap',self.overlap),('controls',self.controls),('nonempty-restart',self.nonempty_restart),('streams-denial',self.streams_denial)]
  if self.fixture.backend=='Native':cases.append(('held-drain',self.held_drain))
  else:cases=[('source-slow-reader',self.source_slow_reader),('nonempty-restart',self.nonempty_restart),('normal-backup',self.normal_backup)]
  physical=[('backup-held-graceful',self.backup_physical),('backup-delete-refusal',self.backup_physical)]
  if self.fixture.backend=='Native':physical.append(('backup-held-deadline',self.backup_physical))
  cases.extend(physical)
  cases.append(('idle-viewing',self.idle_viewing))
  cases.append(('recording-provider-artwork',self.provider_artwork))
  cases.extend((name,self.plugin_eta)for name in ['plugin-eta-fifo','plugin-eta-warm','plugin-eta-crash'])
  cases.append(('xc-live',self.xc_live))
  if self.fixture.backend=='Native':
   # Surface the three observed failures first, with the same independent
   # SQL/cache/process reset and the full fourteen-case acceptance scope.
   first=['core-produced','normal-backup','nonempty-restart']
   by_name=dict(cases)
   cases=[(name,by_name[name])for name in first]+[(name,method)for name,method in cases if name not in first]
  cases.extend([('auto-sync',self.auto_sync),('vlc-live',self.vlc_live),('input-promotion',self.input_promotion),('output-profile',self.output_profile)])
  cases.extend([('schedules-direct',self.schedules_direct),('recording-output-profile',self.recording_output_profile)])
  cases.append(('redirect-common',self.redirect_common))
  cases.append(('m8-actions',self.m8_actions))
  cases.append(('process-transports',self.process_transports))
  cases.append(('channel-matching',self.channel_matching))
  cases.append(('m3u-refresh-all',self.m3u_refresh_all))
  cases.append(('vod-repeat',self.vod_repeat))
  cases.append(('vod-series',self.vod_series))
  cases.append(('vod-series-info',self.vod_series_info))
  cases.append(('vod-series-advanced',self.vod_series_advanced))
  cases.append(('vod-dense-list',self.vod_dense_list))
  cases.append(('vod-shared-metadata',self.vod_shared_metadata))
  cases.append(('vod-external-identity',self.vod_external_identity))
  cases.append(('vod-international-catalogue',self.vod_grouped_catalogue))
  cases.append(('vod-ordered-title-identity',self.vod_grouped_catalogue))
  cases.append(('vod-catalogue-scale',self.vod_catalogue_scale))
  cases.append(('vod-category-language-lifecycle',self.vod_category_language))
  cases.append(('live-vod-group-settings',self.live_vod_group_settings))
  cases.append(('comskip-manual',self.comskip_manual))
  cases.append(('stream-file-crud',self.stream_file_crud))
  if self.fixture.backend=='Native':cases.append(('custom-guide-held',self.custom_guide_held))
  selected=['vlc-live','auto-sync','input-promotion']
  if self.fixture.binding.get('include_output_profile'):selected.append('output-profile')
  if self.fixture.backend=='Native':selected.extend(['channel-maintenance','controls','xc-live'])
  cases.append(('normal-backup-restore',self.restore))
  if self.fixture.backend=='Original':cases.extend([('normal-backup-restore-enabled-source',self.restore),('recording-plugin-characterization',self.recording_plugin_characterization)])
  else:cases.extend([('normal-backup-restore-partial-admission',self.restore),('recording-plugin-characterization',self.recording_plugin_characterization),('recording-connect-boundaries',self.recording_connect_boundaries)])
  selected=self.fixture.binding.get('execution_contexts',{}).get(self.fixture.backend,selected)
  if not selected or len(selected)!=len(set(selected)):raise ValueError('ExplicitNonemptyUniqueContextSelectionRequired')
  if 'custom-guide-held'in selected and(self.fixture.backend!='Native'or selected[-1]!='custom-guide-held'):raise ValueError('HeldGuideMustBeFinalNativeContext')
  by_name=dict(cases)
  cases=[(name,by_name[name])for name in selected]
  for name,method in cases:
   if self.fixture.functional_budget_expired():self.report['remaining_contexts_blocked']='whole-cohort absolute deadline expired';break
   retained=self.fixture.retained_native_case(name)if self.fixture.backend=='Native'else(None if name=='core-produced'else self.fixture.retained_source_case(name))
   if retained is not None:
    if self.fixture.backend=='Original'and name in('plugin-eta-warm','plugin-eta-crash'):self.report['required_Source_ETA_unmeasured']=['Source warm/crash subset only; historical wholefalse, periodic previousrow/delete/orphan transition not measured.']
    if self.fixture.backend=='Original'and name=='nonempty-restart':self.report['required_Source_recovery_media_unmeasured']=['Old Source full restart/media remains false: decoder3 equal audio DTS warning fails original no-stderr; decoder4/5 not reached. Only task-state10 and revoked ordering reused.']
    self.report['cases'].append({'case':name,**retained});self.flush();continue
   row={'case':name,'status':'not_run'};self.report['cases'].append(row);self.flush()
   try:
    # One backend fixture, explicit actual idle/reset boundary between contexts.
    with self.fixture.independent_context(name,start_consumers=name!='source-slow-reader'and not name.startswith('plugin-eta-')):
     row['status']='running';self.flush();method();row['status']='passed'
   except Unsupported as error:row.update(status='blocked',reason=str(error))
   except Exception as error:
    if getattr(error,'common_candidate_startup_failure',False):
     row.update(status='failed_application_startup',reason=str(error));self.report['remaining_contexts_blocked']='Common production registry initialization failed; dependent Native contexts not repeated';break
    if getattr(error,'batch_infrastructure_failure',False):
     row.update(status='failed_infrastructure',reason=type(error).__name__+':'+str(error)[:1024]);self.report['infrastructure_failure']={'case':name,'type':type(error).__name__};self.report['remaining_contexts_blocked']='Actual fixture infrastructure failed before further business contexts';break
    row.update(status='failed',reason=type(error).__name__+':'+str(error)[:1024])
   finally:self.flush()
   if self.fixture.functional_budget_expired():
    self.report['remaining_contexts_blocked']='whole-cohort absolute deadline expired';break
   if not self.fixture.actual_isolation_valid():
    self.report['remaining_contexts_blocked']='actual prior context retains resources';break
  executed=len(self.report['cases'])==len(cases) and all(c['status']=='passed'or(c['status']in('retained_source_authority','retained_native_authority')and c.get('satisfies_dependency')is True)for c in self.report['cases'])
  self.report['executed_slice_passed']=executed
  self.report['status']=('partial'if self.report.get('required_playback_unmeasured')else'passed')if executed else'failed'
  self.flush();return self.report
