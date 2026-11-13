"""Socket-free comparator corruptions. Synthesized Native fields are NOT runs."""
import base64,copy,datetime,importlib.util,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT/'tools'))
from native_contract import validate_native_capture
REFERENCE=ROOT/'tools/Source3-reference.json'
def sample():
 value=json.loads(REFERENCE.read_text());task=value['recording_dispatch']['frame']['headers']['id'];art=value['artwork']['id']
 value.update(Source_verified=False,Native_verified=True,native_command_began_utc='2026-11-13T16:06:43.9+00:00',native_command_finished_utc='2026-11-13T16:07:13.6+00:00')
 value['all_prerun']=[row for row in value['all_prerun']if row['id']==art];value['all_postrun']=[row for row in value['all_postrun']if row['id']==art]
 value['recording_task']['prerun']=[];value['recording_task']['postrun']=[]
 processes=[]
 source_commands=[row['argv']for row in value['all_subprocess']if row['argv'][0]=='ffmpeg']
 for index,code in enumerate([255,0]):
  processes.append({'argv':source_commands[index],'pid':9100+index,'identity':{'pid':9100+index,'pgid':9100+index,'start_ticks':123456},'code':code,'signal':None,'started_utc':'2026-11-13T16:06:44+00:00','settled_utc':'2026-11-13T16:07:13.4+00:00','stderr':{'read_error':None,'total_bytes':0,'tail':[]},'reader_error':None,'command_error':None,'wait_error':None,'kill_error':None,'emergency':False})
 value['recordings_root_context_before_native']={'preprovisioned_by_fixture':False}
 value['native_dvr']={'recording_root_before_admission':{'exists':False},'capture_only':True,'capture_complete':True,'passed':True,'Source_task_execution':False,'Main_execution':False,'command_began_utc':'2026-11-13T16:06:44+00:00','command_returned_utc':'2026-11-13T16:07:13.5+00:00','actual_run':{'kind':'Completed','task':task,'metadata_written':True,'bytes_written':value['final']['recordings'][0]['custom_properties']['bytes_written'],'file_size':value['retained_media']['files'][0]['bytes'],'processes':processes},'drain':{'ok':True},'diagnostics_after_drain':{'available_slots':1},'caller_weak_alive_after_run':False,'event_connections_before':4,'event_connections_after':4,'event_bytes_before':4096,'event_bytes_after':4096,'pool_size_after_explicit_close':0}
 # Explicit synthetic PG observer receipts, never represented as actual work.
 contracts=json.loads((ROOT/'tools/normal-pg-contracts.json').read_text())['contracts'];queries=[row['sql']for row in contracts.values()]
 pick=lambda prefix:next(query for query in queries if query.startswith(prefix))
 lookup=pick('SELECT ident::bigint');touch=pick('UPDATE django_celery_beat_periodictasks');epg=pick('UPDATE epg_');m3u=pick('UPDATE m3u_');delete_periodic=pick('DELETE FROM django_celery_beat_periodictask ');delete_clocked=pick('DELETE FROM django_celery_beat_clockedschedule ');unused=pick('SELECT EXISTS(SELECT 1 FROM django_celery_beat_periodictask WHERE clocked_id')
 order=['BEGIN','SAVEPOINT native_dvr_predelete',lookup,touch,'RELEASE SAVEPOINT native_dvr_predelete',epg,m3u,delete_periodic,'COMMIT','BEGIN',lookup,touch,'COMMIT',unused,'BEGIN',delete_clocked,'SAVEPOINT native_dvr_clocked_postdelete',lookup,touch,'RELEASE SAVEPOINT native_dvr_clocked_postdelete','COMMIT']
 periodic=next(row for row in value['after_beat']['periodic']if row['name']=='dvr-recording-1');bigint=lambda number:number.to_bytes(8,'big',signed=True).hex()
 stamp=datetime.datetime.fromisoformat(value['final']['sentinel'][0]['last_update']);epoch=datetime.datetime(2000,1,1,tzinfo=datetime.timezone.utc);delta=stamp-epoch;stamp_wire=bigint((delta.days*86400+delta.seconds)*1000000+delta.microseconds)
 events=[]
 for query in order:
  parameters=[]
  if query==touch:parameters=[stamp_wire]
  elif query in(epg,m3u):parameters=[bigint(periodic['id'])]
  elif query in(unused,delete_clocked):parameters=[bigint(periodic['clocked_id'])]
  elif query==delete_periodic:parameters=[bigint(periodic['id']),b'dvr-recording-1'.hex(),bigint(periodic['clocked_id'])]
  events.append({'kind':'execute','connection':1,'sql':query,'parameter_hex':parameters})
  command={epg:'UPDATE 0',m3u:'UPDATE 0',delete_periodic:'DELETE 1',delete_clocked:'DELETE 1'}.get(query)
  if command:events.append({'kind':'actual-target-backend-frame','connection':1,'backend_pid':99,'sql':query,'kind_hex':'43','body_hex':(command.encode()+b'\0').hex()})
 for index,event in enumerate(events):event.update(epoch=stamp.timestamp()+index/1000,monotonic=1000+index/1000)
 value['native_pg']={'errors':[],'held':None,'listener_alive':False,'connections':[{'closed':True,'up_joined':True}],'events':events}
 value['native_pg_before_listener_stop']={'errors':[],'connections':[{'closed':True,'up_joined':True}]}
 value['worker_exit_receipts']=[row for row in value['worker_exit_receipts']if row['role']in('artwork','beat')]
 value['worker_exit_receipts'].append({'role':'native-dvr','actual_exit':0,'actual_poll_exit':0,'actual_wait_completed':True,'pipes_settled':True,'writer_settled':True,'forced':False,'invalid':None,'pid':9199,'pgid':9199,'actual_initial_pgid':9199,'starttime':'111111','original_process_absent':True})
 active_row={'key_base64':base64.b64encode(b'dvr:recording-active:1').decode(),'type':'string','value_base64':base64.b64encode(b'1').decode(),'ttl':44}
 value['native_functional_observations']=[{'utc':'2026-11-13T16:06:45+00:00','monotonic':1000.0,'active_rows':[active_row]},{'utc':'2026-11-13T16:06:46+00:00','monotonic':1001.0,'active_rows':[copy.deepcopy(active_row)]}]
 value['native_functional_observations'][0]['active_rows'][0]['ttl']=30
 trace=[]
 for kind,observed in [('before-active',[0,-2]),('before-auxiliary',None),('acquired',True),('renewed',1),('after-auxiliary',None),('removed',1)]:
  trace.append({'kind':kind,'observation':{'before_utc':'2026-11-13T16:06:45+00:00','after_utc':'2026-11-13T16:06:45.1+00:00',**({'value':observed}if observed is not None else{'absent':True})}})
 value['native_dvr']['actual_run']['active_observations']=trace
 return value
class ComparatorControls(unittest.TestCase):
 def test_full_Source_derived_synthetic_baseline_not_actual(self):
  accepted=validate_native_capture(0,sample(),REFERENCE);self.assertTrue(accepted['passed']);self.assertTrue(accepted['all_temporal_fields_retained']);self.assertGreater(len(accepted['temporal_inventory']),100)
 def reject(self,change):
  value=sample();change(value)
  with self.assertRaises((ValueError,KeyError,TypeError)):validate_native_capture(0,value,REFERENCE)
 def test_bool_exit_not_zero(self):
  with self.assertRaises(ValueError):validate_native_capture(False,sample(),REFERENCE)
 def test_missing_explicit_null_backend_field(self):self.reject(lambda v:v['recording_task']['raw_result']['metadata'].pop('traceback'))
 def test_not_Original_source_execution(self):self.reject(lambda v:v.update(Source_verified=True))
 def test_fixture_preprovisioning_cannot_claim_fresh_root_parity(self):self.reject(lambda v:v['recordings_root_context_before_native'].update(preprovisioned_by_fixture=True))
 def test_root_already_present_before_actor_not_fresh_root_case(self):self.reject(lambda v:v['native_dvr']['recording_root_before_admission'].update(exists=True))
 def test_wrong_own_task_identity(self):self.reject(lambda v:v['native_dvr']['actual_run'].update(task='00000000-0000-4000-8000-000000000000'))
 def test_new_unqualified_property_not_silently_ignored(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].update(unknown='not omitted'))
 def test_missing_source_property(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].pop('owned_fixture_marker'))
 def test_changed_existing_property(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].update(owned_fixture_marker='changed'))
 def test_clock_outside_actual_command(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].update(started_at='2026-11-13 16:06:43.99'))
 def test_naive_source_UTC_end_does_not_allow_early_completion(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].update(ended_at='2026-11-13 16:07:12.9'))
 def test_backend_date_done_outside_actual_command(self):self.reject(lambda v:v['recording_task']['raw_result']['metadata'].update(date_done='2026-11-13T16:07:14+00:00'))
 def test_false_Source_postrun_not_fabricated_for_native(self):self.reject(lambda v:v['recording_task']['postrun'].append({'state':'SUCCESS'}))
 def test_unknown_started_task(self):self.reject(lambda v:v['all_prerun'].append({'id':'unknown'}))
 def test_unknown_unfinished_task_not_discarded(self):self.reject(lambda v:v['all_prerun'].append(copy.deepcopy(v['all_prerun'][0])))
 def test_unknown_original_role_PID(self):self.reject(lambda v:v['worker_exit_receipts'][0].update(starttime=None))
 def test_unsettled_native_input_writer(self):self.reject(lambda v:v['worker_exit_receipts'][2].update(writer_settled=False))
 def test_Source_signal_not_owned_prefork_child(self):self.reject(lambda v:v['all_prerun'][0].update(pid=9199))
 def test_raw_sibling_removed(self):self.reject(lambda v:v['final']['queues']['celery'].pop())
 def test_raw_sibling_reordered(self):self.reject(lambda v:v['final']['queues']['celery'].reverse())
 def test_missing_null_metadata_result(self):self.reject(lambda v:v['recording_task']['raw_result']['metadata'].pop('result'))
 def test_prior_artwork_backend_not_rewritten(self):self.reject(lambda v:v['final']['result_backend_raw'][0].update(DUMP_base64='AA=='))
 def test_final_tracking_row_not_from_last_wire_update(self):self.reject(lambda v:v['final']['sentinel'][0].update(last_update='2026-11-13 16:06:44.000000+00:00'))
 def test_wrong_literal_active_value(self):self.reject(lambda v:v['native_functional_observations'][0]['active_rows'][0].update(value_base64='Mg=='))
 def test_nonpersistent_active_key_TTL_is_real(self):self.reject(lambda v:v['native_functional_observations'][0]['active_rows'][0].update(ttl=-1))
 def test_instantaneous_active_sample_not_sustained(self):self.reject(lambda v:v['native_functional_observations'][1].update(monotonic=1000.1))
 def test_missing_real_active_renewal(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'].pop(3))
 def test_no_observed_TTL_reset_for_long_capture(self):self.reject(lambda v:v['native_functional_observations'][1]['active_rows'][0].update(ttl=29))
 def test_bool_byte_count(self):self.reject(lambda v:v['final']['recordings'][0]['custom_properties'].update(bytes_written=True))
 def test_wrong_slot_retirement(self):self.reject(lambda v:v['native_dvr']['diagnostics_after_drain'].update(available_slots=0))
 def test_held_owner_not_retired(self):self.reject(lambda v:v['native_dvr'].update(caller_weak_alive_after_run=True))
 def test_process_emergency_not_normal(self):self.reject(lambda v:v['native_dvr']['actual_run']['processes'][0].update(emergency=True))
 def test_process_signal_exit_not_observed255(self):self.reject(lambda v:v['native_dvr']['actual_run']['processes'][0].update(code=None,signal=2))
 def test_wrong_owned_process_group(self):self.reject(lambda v:v['native_dvr']['actual_run']['processes'][0]['identity'].update(pgid=1))
 def test_missing_actual_tracking_touch(self):self.reject(lambda v:v['native_pg']['events'].pop(next(i for i,row in enumerate(v['native_pg']['events'])if row['kind']=='execute'and row['sql'].startswith('UPDATE django_celery_beat_periodictasks'))))
 def test_unlisted_cleanup_statement_inside_transaction(self):self.reject(lambda v:v['native_pg']['events'].insert(2,{'kind':'execute','connection':1,'sql':'UPDATE unknown_table SET x=1','parameter_hex':[]}))
 def test_nonfinite_proxy_clock(self):self.reject(lambda v:v['native_pg']['events'][0].update(monotonic=float('nan')))
 def test_bool_proxy_clock(self):self.reject(lambda v:v['native_pg']['events'][0].update(epoch=True))
 def test_reversed_causal_proxy_clock(self):self.reject(lambda v:v['native_pg']['events'][0].update(monotonic=9000))
 def test_wrong_periodic_bound_identity(self):self.reject(lambda v:next(row for row in v['native_pg']['events']if row['kind']=='execute'and row['sql'].startswith('UPDATE epg_')).update(parameter_hex=['0000000000000063']))
 def test_missing_real_backend_CommandComplete(self):self.reject(lambda v:v['native_pg']['events'].pop(next(i for i,row in enumerate(v['native_pg']['events'])if row['kind']=='actual-target-backend-frame')))
 def test_wrong_backend_affected_rows(self):self.reject(lambda v:next(row for row in v['native_pg']['events']if row['kind']=='actual-target-backend-frame').update(body_hex=b'UPDATE 1\0'.hex()))
 def test_unsettled_sql_connection_not_forceclosed_to_pass(self):self.reject(lambda v:v['native_pg_before_listener_stop']['connections'][0].update(closed=False))
 def test_wrong_unobserved_ffmpeg_argument(self):self.reject(lambda v:v['native_dvr']['actual_run']['processes'][0]['argv'].append('-unsupported'))
 def test_missing_audit_detail(self):self.reject(lambda v:v['final']['system_events'][1]['details'].pop('failure_reason'))
 def test_unsettled_reader(self):self.reject(lambda v:v['native_dvr']['actual_run']['processes'][0].update(reader_error='pending'))
 def test_native_fake_pool_close(self):self.reject(lambda v:v['native_dvr'].update(pool_size_after_explicit_close=1))
 def test_video_prefix_not_full_decode(self):self.reject(lambda v:v['media_commands'][1].update(actual_exit=1))
 def test_successful_prefix_command_cannot_claim_full_decode(self):self.reject(lambda v:v['media_commands'][1]['argv'].extend(['-t','1']))
 def test_zero_video_duration_refused(self):self.reject(lambda v:v['ffprobe']['format'].update(duration='0'))
 def test_different_positive_video_duration_has_no_equal_delivery_claim(self):
  value=sample();value['ffprobe']['format']['duration']='25.0';result=validate_native_capture(0,value,REFERENCE);self.assertFalse(result['Native_duration_equals_Source_qualified']);self.assertEqual(result['actual_Native_video_duration_seconds'],25.0)
 def test_video_nonfinite(self):self.reject(lambda v:v['ffprobe']['format'].update(duration='NaN'))
 def test_wrong_origin_headers(self):self.reject(lambda v:v['final']['origin']['rows'][0]['headers'].pop())
 def test_changed_cache_key_not_silently_dropped(self):self.reject(lambda v:v['final']['cache'].append({'key_base64':'Zm9yZWlnbg==','type':'string','DUMP_base64':'AA==','ttl':1}))

 def test_integer_acquire_is_not_bool_reply(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][2]['observation'].update(value=1))
 def test_bool_renewal_is_not_Lua_int(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][3]['observation'].update(value=True))
 def test_float_renewal_is_not_Lua_int(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][3]['observation'].update(value=1.0))
 def test_zero_renewal_not_success(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][3]['observation'].update(value=0))
 def test_bool_remove_is_not_Lua_int(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][-1]['observation'].update(value=True))
 def test_float_remove_is_not_Lua_int(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][-1]['observation'].update(value=1.0))
 def test_zero_remove_not_success(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][-1]['observation'].update(value=0))
 def test_bool_absence_is_not_integer_pair(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][0]['observation'].update(value=[False,-2]))
 def test_float_absence_is_not_integer_pair(self):self.reject(lambda v:v['native_dvr']['actual_run']['active_observations'][0]['observation'].update(value=[0,-2.0]))

if __name__=='__main__':unittest.main()
