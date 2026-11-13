"""Pure corruption controls for SHA-bound credential projections and semantics."""
import ast,copy,json,sys,tempfile,unittest,tarfile,io,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT))
import verify
from validate_source_projection import compare
from validate_native_projection import validate as native_validate,Clocks
import cache_binding
SOURCE=verify.load_source_projection();NATIVE=json.loads((ROOT/'Native20-projection.json').read_text());PLAN=verify.load_plan()
class Controls(unittest.TestCase):
 def test_all40_retained_observations(self):self.assertTrue(verify.validate()['passed'])
 def reject_source(self,fn):
  value=copy.deepcopy(SOURCE);fn(value)
  with self.assertRaises((ValueError,KeyError,AssertionError)):compare(value,PLAN)
 def reject_native(self,fn):
  value=copy.deepcopy(NATIVE);fn(value)
  with self.assertRaises((ValueError,KeyError,AssertionError)):native_validate(value,reference_mode='retained-original-observation')
 def test_Source_wrong_role(self):self.reject_source(lambda s:s['seed_users'][3].update(user_level=10))
 def test_Source_wrong_seed_notice(self):self.reject_source(lambda s:s['seed_notifications']['notifications'][0].update(source='developer'))
 def test_Source_target_swap(self):self.reject_source(lambda s:s['requests'][1]['resolver']['kwargs'].update(pk='2'))
 def test_Source_wrong_action(self):self.reject_source(lambda s:s['requests'][2]['resolver'].update(action='list'))
 def test_Source_auth_header_hash_corruption(self):self.reject_source(lambda s:s['requests'][0]['raw_headers_private']['HTTP_AUTHORIZATION'].update(sha256='0'*64))
 def test_Source_api_key_hash_corruption(self):self.reject_source(lambda s:s['seed_users'][0]['api_key'].update(sha256='0'*64))
 def test_Source_backend_precedence(self):self.reject_source(lambda s:s['requests'][12]['actual_auth_observation'].update(backend_class='apps.accounts.authentication.ApiKeyAuthentication'))
 def test_Source_forceauth_scope(self):self.reject_source(lambda s:s.update(force_authenticate_used=True))
 def test_Source_failure_budget_reset(self):self.reject_source(lambda s:s['requests'][16]['before_cache'].update(api_key_failed_auth_history=None))
 def test_Source_user_sideeffect(self):self.reject_source(lambda s:s['requests'][14]['after_users'][0].update(api_key={'sha256':'0'*64,'bytes':10}))
 def test_Source_event_sideeffect(self):self.reject_source(lambda s:s['requests'][14]['after_events'].append({'id':1}))
 def test_Source_raw_body_corruption(self):self.reject_source(lambda s:s['requests'][0].update(body_hex='7b7d'))
 def test_Source_skipped_sequence(self):self.reject_source(lambda s:s['requests'][3]['after_audit']['sequences']['public.core_notificationdismissal_id_seq'].update(last_value=10))
 def test_Native_deniedUI_read_regression(self):self.reject_native(lambda n:n['requests'][0].update(status=403))
 def test_Native_adminwrite_bypass(self):self.reject_native(lambda n:n['requests'][5].update(status=201))
 def test_Native_recorder_false_cannot_be_relabelled(self):self.reject_native(lambda n:n.update(passed=True))
 def test_Native_fabricated_backend(self):self.reject_native(lambda n:n.update(actual_auth_backend_observed=True))
 def test_Native_user_sideeffect(self):self.reject_native(lambda n:n['requests'][14]['after_SQL']['tables']['accounts_user'][0].update(api_key={'sha256':'0'*64,'bytes':10}))
 def test_Native_event_sideeffect(self):self.reject_native(lambda n:n['requests'][14]['after_events'].append({'id':1}))
 def test_Native_wrong_credential_header(self):self.reject_native(lambda n:n['requests'][0]['raw_headers_private']['authorization'].update(sha256='0'*64))
 def test_Native_history_cannot_lie_about_rawGET(self):
  value=copy.deepcopy(NATIVE['requests'][14]['after_cache']);value['api_key_failed_auth_raw_hex']='00'
  with self.assertRaisesRegex(ValueError,'NativeGETBytesMatchActualDump'):cache_binding.validate(value,native=True)
 def test_Source_history_cannot_lie_about_actualDump(self):
  value=copy.deepcopy(SOURCE['requests'][14]['after_cache']);value['api_key_failed_auth_history'][0]+=.000001
  with self.assertRaisesRegex(ValueError,'DumpPickleMatchesDecodedHistory'):cache_binding.validate(value,native=False)
 def test_Native_drop_actual_history_cachekey(self):
  value=copy.deepcopy(NATIVE['requests'][14]['after_cache']);value['rows']=[r for r in value['rows']if r['key_hex']!=cache_binding.KEY]
  with self.assertRaisesRegex(ValueError,'ExactlyOneActualHistoryRedisKey'):cache_binding.validate(value,native=True)
 def test_Source_actual_twoentry_LZF_decodes(self):cache_binding.validate(copy.deepcopy(SOURCE['requests'][15]['after_cache']),native=False)
 def test_LZF_declared_size_corruption(self):
  value=copy.deepcopy(SOURCE['requests'][15]['after_cache']);row=next(r for r in value['rows']if r['key_hex']==cache_binding.KEY);raw=bytearray.fromhex(row['dump_hex']);raw[3]=1;row['dump_hex']=raw.hex()
  with self.assertRaises(ValueError):cache_binding.validate(value,native=False)
 def test_pickle_executable_opcode_rejected_before_load(self):
  raw=b'cos\nsystem\n(S"false"\ntR.';value={'api_key_failed_auth_history':[1.0],'rows':[{'key_hex':cache_binding.KEY,'dump_hex':(b'\0'+bytes([len(raw)])+raw+b'\0'*10).hex()}]}
  with self.assertRaisesRegex(ValueError,'HarmlessExactPickleOpcodesOnly'):cache_binding.validate(value,native=False)
 def test_Native_no_failure_budget(self):self.reject_native(lambda n:n['requests'][14]['after_cache'].update(api_key_failed_auth_history=None))
 def test_Native_budget_reset(self):self.reject_native(lambda n:n['requests'][16]['before_cache'].update(api_key_failed_auth_history=None))
 def test_Native_badjwt_goodkey_fallback(self):self.reject_native(lambda n:n['requests'][11].update(status=200))
 def test_Native_sequence_advance(self):self.reject_native(lambda n:n['requests'][3]['after_SQL']['sequences']['core_notificationdismissal'].update(last_value=10))
 def test_Native_foreign_dismissal(self):self.reject_native(lambda n:n['requests'][3]['after_SQL']['tables']['core_notificationdismissal'][0].update(user_id=1))
 def test_Native_missing_SQLtable(self):self.reject_native(lambda n:n['requests'][0]['after_SQL']['tables'].pop('vod_movie'))
 def test_Native_crossrequest_rewind(self):self.reject_native(lambda n:n['requests'][4].update(before_SQL=n['seed_SQL']))
 def test_Native_rawbody_corruption(self):self.reject_native(lambda n:n['requests'][0].update(body_hex='7b7d'))
 def test_Native_auth_challenge_missing(self):self.reject_native(lambda n:n['requests'][10]['headers_hex'].pop('www-authenticate'))
 def test_mapped_clock_rewind_before_equality(self):
  c=Clocks(['2026-11-12T20:00:00Z']);row={'began_utc':'2026-11-12T21:00:00Z','completed_utc':'2026-11-12T21:00:01Z'};c.compare('2026-11-12T20:00:00Z','2026-11-12T21:00:00Z',row)
  with self.assertRaises(ValueError):c.compare('2026-11-12T20:00:00Z','2026-11-12T20:00:00Z',row)
 def test_coincident_generated_clock_is_registered(self):
  c=Clocks(['2026-11-12T20:00:00Z']);row={'began_utc':'2026-11-12T20:00:00Z','completed_utc':'2026-11-12T20:00:01Z'};c.compare('2026-11-12T20:00:00Z','2026-11-12T20:00:00Z',row);self.assertEqual(len(c.mapping),1)
 def test_explicit_reference_mode_required(self):
  with self.assertRaisesRegex(ValueError,'ExplicitReferenceModeRequired'):native_validate(copy.deepcopy(NATIVE))
 def test_no_silent_projected_or_original_hash_fallback(self):
  with self.assertRaisesRegex(ValueError,'ExactRetainedAuthorityHash'):native_validate(copy.deepcopy(NATIVE),reference_mode='new-projected-reference')
 def test_new_projected_reference_never_claimed_measured(self):
  binding=json.loads((ROOT/'projected-native-reference/Source-binding.json').read_text());self.assertFalse(binding['projected_reference_executed']);self.assertNotEqual(binding['Source20_sha256'],binding['Original_Source20_sha256'])
 def test_originalSource_and_projectionSHA_distinct(self):
  r=verify.load_reference();self.assertNotEqual(r['Source20']['original_sha256'],r['Source20']['projection_sha256']);self.assertNotEqual(r['Native20']['original_sha256'],r['Native20']['projection_sha256'])
 def corrupt_lifecycle(self,label,name,mutate):
  with tempfile.TemporaryDirectory()as directory:
   root=Path(directory);values={}
   with tarfile.open(ROOT/(label+'-lifecycle.tar.gz'))as bundle:
    for member in bundle.getmembers():values[member.name]=bundle.extractfile(member).read()
   values[name]=mutate(values[name]);inventory={n:{'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}for n,data in values.items()};(root/(label+'-lifecycle-inventory.json')).write_text(json.dumps(inventory))
   with tarfile.open(root/(label+'-lifecycle.tar.gz'),'w:gz')as bundle:
    for n,data in values.items():member=tarfile.TarInfo(n);member.size=len(data);bundle.addfile(member,io.BytesIO(data))
   old=verify.ROOT
   try:
    verify.ROOT=root
    with self.assertRaises(ValueError):verify.validate_lifecycle(label)
   finally:verify.ROOT=old
 def test_default_audit_byte_change_not_normalized(self):self.corrupt_lifecycle('source','default-after-private.json',lambda data:data+b' ')
 def test_actual_SQL_backend_not_absent(self):self.corrupt_lifecycle('native','backend-after-ELF-exit.json',lambda data:json.dumps([{'actual_backends':[{'pid':1}]}]).encode())
 def test_actual_wait_negative_not_hidden(self):self.corrupt_lifecycle('native','command-015.json',lambda data:json.dumps(dict(json.loads(data),actual_exit=101)).encode())
 def test_actual_original_PID_not_absent(self):
  def mutate(data):
   value=json.loads(data);value['PID_observations'][0]['original_process_absent']=False;return json.dumps(value).encode()
  self.corrupt_lifecycle('source','independent-cleanup-query-receipt.json',mutate)
 def test_JWT_literal_not_present_in_projection(self):
  for value in(SOURCE,NATIVE):
   for token in value['tokens_private'].values():self.assertNotIn('raw',token);self.assertEqual(set(token['claim_summary']),{'user_id','token_type','lifetime_seconds'})
 def test_python_syntax(self):
  for p in ROOT.glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))
if __name__=='__main__':unittest.main()
