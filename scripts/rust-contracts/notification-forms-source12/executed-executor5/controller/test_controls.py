"""Pure Source12 effect/definition corruption controls. No Source/SQL/Docker/app imports."""
import ast,copy,hashlib,json,unittest
from pathlib import Path
from effects import compare,normalized
from synthetic import example,state
from definition import ROOT,validate

def body(row,value):
 raw=json.dumps(value,separators=(',',':')).encode();row.update(json=value,response_body_hex=raw.hex(),response_body_bytes=len(raw),response_body_sha256=hashlib.sha256(raw).hexdigest())
def rebind(row,side='after',warm=True):
 old=row[side];row[side]=state(old['notifications'],old['users'],old['audit']['sequences'],warm)

class Controls(unittest.TestCase):
 def reject(self,mutate):
  actual,plan=example();mutate(actual)
  with self.assertRaises((ValueError,KeyError,TypeError,StopIteration,AssertionError)):compare(actual,plan)
 def test_complete_SYNTHETIC_not_actual_Source(self):
  actual,plan=example();result=compare(actual,plan);self.assertTrue(result['passed']);self.assertTrue(actual['_synthetic_only']);self.assertEqual(result['actual_Source_requests'],12)
 def test_missing_case(self):self.reject(lambda r:r['requests'].pop())
 def test_missing_seed_role(self):self.reject(lambda r:r['seed']['users'].pop())
 def test_seed_notice_field(self):self.reject(lambda r:r['seed']['notifications']['notifications'][0].update(title='different'))
 def test_seed_dismissal_wrong_pair(self):self.reject(lambda r:r['seed']['notifications']['dismissals'][0].update(notification_id=11))
 def test_seed_false_not_null(self):self.reject(lambda r:r['seed']['notifications']['dismissals'][0].update(action_taken=False))
 def test_swapped_actual_alias_target(self):self.reject(lambda r:r['requests'][7].update(path='/api/core/notifications/12/'))
 def test_wrong_resolved_PK(self):self.reject(lambda r:r['requests'][7]['resolver']['kwargs'].update(pk='12'))
 def test_wrong_resolved_action(self):self.reject(lambda r:r['requests'][7]['resolver'].update(action='update'))
 def test_wrong_resolved_URL_name(self):self.reject(lambda r:r['requests'][7]['resolver'].update(url_name='wrong'))
 def test_wrong_request_body_hash(self):self.reject(lambda r:r['requests'][0].update(body_sha256='0'*64))
 def test_wrong_content_type(self):self.reject(lambda r:r['requests'][6].update(content_type='application/json'))
 def test_wrong_actor(self):self.reject(lambda r:r['requests'][9].update(actor='admin'))
 def test_wrong_raw_APIkey(self):self.reject(lambda r:r['requests'][0]['raw_headers_private'].update(HTTP_X_API_KEY=b'not-the-owned-user-key'.hex()))
 def test_force_auth_relabel(self):self.reject(lambda r:r.update(force_authenticate_used=True))
 def test_wrong_backend(self):self.reject(lambda r:r['requests'][0]['actual_auth_observation'].update(backend_class='forced'))
 def test_wrong_status(self):self.reject(lambda r:r['requests'][2].update(status=200))
 def test_wrong_response_body_hash(self):self.reject(lambda r:r['requests'][0].update(response_body_sha256='0'*64))
 def test_framework_exception_not_HTTP(self):self.reject(lambda r:r['requests'][0].update(transport='framework_exception'))
 def test_missing_raw_SQL_table(self):self.reject(lambda r:r['requests'][0]['after']['audit']['tables'].pop('vod_movie'))
 def test_missing_raw_SQL_text(self):self.reject(lambda r:r['requests'][0]['after']['audit']['tables']['accounts_user'].update(raw_json_text=[]))
 def test_erase_cache(self):self.reject(lambda r:r['requests'][0]['after']['cache'].update(rows=[]))
 def test_extra_cache_budget(self):
  def mutate(r):r['requests'][1]['after']['cache']['rows'].append({'key_hex':b':1:unexpected'.hex(),'type':'string','dump_hex':'00','value_hex':'00','pttl':100})
  self.reject(mutate)
 def test_failed_auth_history_not_masked(self):self.reject(lambda r:r['requests'][9]['after']['cache'].update(api_key_failed_auth_history=[1.]))
 def test_cascade_cannot_delete_foreign(self):
  def mutate(r):
   row=r['requests'][11];row['after']['notifications']['notifications']=[n for n in row['after']['notifications']['notifications']if n['id']!=11];rebind(row)
  self.reject(mutate)
 def test_cascade_cannot_drop_foreign_dismissal(self):
  def mutate(r):
   row=r['requests'][11];row['after']['notifications']['dismissals']=[];rebind(row)
  self.reject(mutate)
 def test_delete_does_not_consume_sequence(self):
  def mutate(r):
   row=r['requests'][11];row['after']['audit']['sequences']['public.core_notificationdismissal_id_seq']['last_value']=4;rebind(row)
  self.reject(mutate)
 def test_PUT_cannot_retain_checked_active(self):
  def mutate(r):
   row=r['requests'][3];n=next(n for n in row['after']['notifications']['notifications']if n['id']==12);n['is_active']=True;rebind(row);value=dict(row['json'],is_active=True);body(row,value)
  self.reject(mutate)
 def test_duplicate_title_first_wins_rejected(self):
  def mutate(r):
   row=r['requests'][7];n=next(n for n in row['after']['notifications']['notifications']if n['id']==13);n['title']='first';rebind(row);body(row,dict(row['json'],title='first'))
  self.reject(mutate)
 def test_nested_bool_int_not_equal(self):
  def mutate(r):
   row=r['requests'][1];n=next(n for n in row['after']['notifications']['notifications']if n['id']==12);n['action_data']['flag']=1;rebind(row);value=copy.deepcopy(row['json']);value['action_data']['flag']=1;body(row,value)
  self.reject(mutate)
 def test_own_update_clock_bounded(self):
  def mutate(r):
   row=r['requests'][7];n=next(n for n in row['after']['notifications']['notifications']if n['id']==13);n['updated_at']='2099-01-01T00:00:00Z';rebind(row)
  self.reject(mutate)
 def test_empty204(self):self.reject(lambda r:body(r['requests'][11],{}))
 def test_missing_memory_file_metadata(self):self.reject(lambda r:r['requests'][6]['parsed_request_metadata']['cached']['_files'].update(fields=[]))
 def test_file_observer_must_not_read(self):self.reject(lambda r:r['requests'][6]['parsed_request_metadata']['cached']['_files']['fields'][0]['values'][0].update(file_content_read=True))
 def test_denied_body_not_force_parsed(self):self.reject(lambda r:r['requests'][9]['parsed_request_metadata'].update(forced_parse=True))
 def test_wrong_duplicate_input_order(self):self.reject(lambda r:r['requests'][7]['parsed_request_metadata']['cached']['_data']['fields'][0]['values'].reverse())
 def test_missing_full62_table(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['tables'].pop('django_session'))
 def test_extra_unreviewed_public_table(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['tables'].update(unreviewed={'count':0,'sha_like_md5':'0'*32}))
 def test_missing_full59_sequence(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['sequences'].pop('auth_group_id_seq'))
 def test_changed_schema_column(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['columns'].pop())
 def test_wrong_full_named_md5(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['tables']['core_systemnotification'].update(sha_like_md5='0'*32))
 def test_other_public_table_modified(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['tables']['django_session'].update(count=1,sha_like_md5='1'*32))
 def test_other_public_sequence_modified(self):self.reject(lambda r:r['requests'][0]['after']['public_manifest']['sequences'].update(auth_group_id_seq=[2,True]))
 def test_raw_broker_extra_frame(self):self.reject(lambda r:r['requests'][0]['after']['raw_Redis'][2]['rows'].append({'key_hex':b'celery'.hex(),'type':'list','dump_hex':'00','pttl':-1,'list_hex':[b'unknown'.hex()]}))
 def test_unknown_setup_frame_dropped_both_sides(self):
  def mutate(r):r['seed']['raw_Redis'][2]['rows'].append({'key_hex':b'celery'.hex(),'type':'list','dump_hex':'00','pttl':-1,'list_hex':[b'unconsumed-setup'.hex()]})
  self.reject(mutate)
 def test_final_raw_result_dropped(self):self.reject(lambda r:r['final_before_probe_cleanup']['raw_Redis'][3]['rows'].append({'key_hex':b'backend'.hex(),'type':'string','dump_hex':'00','value_hex':'00','pttl':3600000}))
 def test_channels_pop_not_allowed(self):self.reject(lambda r:r['requests'][0]['after']['Channels'].update(receive_pop_or_ack_calls=1))
 def test_channels_receiver_swap(self):self.reject(lambda r:r['requests'][0]['after']['Channels'].update(receiver='specific.other!receiver'))
 def test_channels_probe_is_context(self):self.reject(lambda r:r['Channels_probe_context'].update(group_registration_is_fixture_context_not_Default_graph=False))
 def test_channels_cleanup_not_dequeue(self):self.reject(lambda r:r['Channels_probe_cleanup'].update(frames_consumed=1))
 def test_SQL_timestamp_format_same_instant(self):self.assertEqual(normalized({'date_joined':'2026-11-13T00:00:00.100000Z'}),normalized({'date_joined':'2026-11-13T00:00:00.1+00:00'}))
 def test_actual_datetime_rewind_not_normalized(self):self.assertNotEqual(normalized({'updated_at':'2026-11-13T00:00:00.1Z'}),normalized({'updated_at':'2026-11-13T00:00:00.100001Z'}))
 def test_Source_view_functions_not_patched(self):
  tree=ast.parse((ROOT/'controller/source_fixture.py').read_text());self.assertFalse(any(isinstance(n,(ast.Assign,ast.AnnAssign))and any(isinstance(t,ast.Attribute)and isinstance(t.value,ast.Name)and t.value.id=='SystemNotificationViewSet'for t in getattr(n,'targets',[]))for n in ast.walk(tree)))
 def test_observer_never_reads_file_or_forces_request_data(self):
  tree=ast.parse((ROOT/'controller/source_fixture.py').read_text());fn=next(x for x in tree.body if isinstance(x,ast.FunctionDef)and x.name=='parsed_metadata');self.assertFalse(any(isinstance(x,ast.Attribute)and x.attr in('read','seek','chunks','data','FILES')for x in ast.walk(fn)))
if __name__=='__main__':unittest.main(verbosity=2)
