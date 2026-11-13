"""Finite synthetic corruption controls. No Docker/application/socket work."""
import copy,datetime,hashlib,importlib.util,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
import validate_native as V
ROOT=Path(__file__).resolve().parents[1];SOURCE=json.loads((ROOT/'inputs/Source24-private.json').read_bytes())
def native():
 rows=[]
 for a in SOURCE['requests']:
  b={k:copy.deepcopy(a[k])for k in ['index','name','actor','method','path','body_hex','body_sha256','body_bytes','before','after','began_utc','completed_utc']};b.update(actual_request_invoked=True,capture_complete=True,physical_close={'fd_after':-1,'original_inode_absent':True,'timer_joined':True})
  if a['index']==24:
   body=b'{"detail":"Plugin repository request processing failed."}';b.update(status=500,json=json.loads(body),headers=[['content-type','application/json']],response_body_hex=body.hex(),response_body_bytes=len(body),response_body_sha256=hashlib.sha256(body).hexdigest())
  else:
   for k in ['status','json','headers','response_body_hex','response_body_bytes','response_body_sha256']:b[k]=copy.deepcopy(a[k])
   if type(b['headers'])is dict:b['headers']=list(b['headers'].items())
  rows.append(b)
 return {'_synthetic_only':True,'capture_only':True,'passed':False,'capture_complete':True,'actual_request_count':24,'completed_capture_count':24,'raw_body_byte_parity_claimed':False,'final_Source_framework_vs_Native500_gap':True,'Main_or_fullM8':False,'before_requests':copy.deepcopy(SOURCE['seed']),'requests':rows}
class Effects(unittest.TestCase):
 def rejects(self,change):
  n=native();change(n)
  with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):V.validate(SOURCE,n)
 def test_synthetic_positive_effect_logic_only(self):self.assertTrue(V.validate(SOURCE,native())['passed'])
 def test_missing_request(self):self.rejects(lambda n:n['requests'].pop())
 def test_swapped_name(self):self.rejects(lambda n:n['requests'][0].update(name=n['requests'][1]['name']))
 def test_wrong_target(self):self.rejects(lambda n:n['requests'][8].update(path='/api/plugins/repos/802/'))
 def test_changed_input_body(self):self.rejects(lambda n:n['requests'][8].update(body_hex='00'))
 def test_wrong_role(self):self.rejects(lambda n:n['requests'][2].update(actor='admin'))
 def test_recorder_promoted(self):self.rejects(lambda n:n.update(passed=True))
 def test_body_bytes_claimed(self):self.rejects(lambda n:n.update(raw_body_byte_parity_claimed=True))
 def test_final_framework_gap_erased(self):self.rejects(lambda n:n.update(final_Source_framework_vs_Native500_gap=False))
 def test_Main_promoted(self):self.rejects(lambda n:n.update(Main_or_fullM8=True))
 def test_request_not_invoked(self):self.rejects(lambda n:n['requests'][0].update(actual_request_invoked=False))
 def test_unclosed_fd(self):self.rejects(lambda n:n['requests'][0]['physical_close'].update(fd_after=8))
 def test_unjoined_timer(self):self.rejects(lambda n:n['requests'][0]['physical_close'].update(timer_joined=False))
 def test_body_sha_corrupt(self):self.rejects(lambda n:n['requests'][0].update(response_body_sha256='0'*64))
 def test_stale_decoded_body(self):self.rejects(lambda n:n['requests'][0].update(json={}))
 def test_status_changed(self):self.rejects(lambda n:n['requests'][0].update(status=200))
 def test_missing_auth_challenge(self):self.rejects(lambda n:n['requests'][0].update(headers=[['content-type','application/json']]))
 def test_final_not500(self):self.rejects(lambda n:n['requests'][-1].update(status=200))
 def test_readonly_repo_field_mutated(self):self.rejects(lambda n:n['requests'][11]['after']['repos'][1].update(name='wrong'))
 def test_config_setnull_omitted(self):self.rejects(lambda n:n['requests'][17]['after']['configs'][0].update(source_repo_id=801))
 def test_config_clock_touched(self):self.rejects(lambda n:n['requests'][17]['after']['configs'][0].update(updated_at='2099-01-01T00:00:00Z'))
 def test_foreign_row_mutated(self):self.rejects(lambda n:n['requests'][8]['after']['repos'][0].update(enabled=False))
 def test_bool_to_int(self):self.rejects(lambda n:n['requests'][8]['after']['repos'][1].update(enabled=1))
 def test_sequence_consumption(self):self.rejects(lambda n:n['requests'][8]['after']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'].__setitem__(0,9999))
 def test_other_table_digest_changed(self):self.rejects(lambda n:n['requests'][8]['after']['public_manifest']['tables']['accounts_user'].update(sha_like_md5='0'*32))
 def test_schema_metadata_changed(self):self.rejects(lambda n:n['requests'][8]['after']['public_manifest']['columns'].pop())
 def test_update_outside_own_clock(self):self.rejects(lambda n:n['requests'][8]['after']['repos'][1].update(updated_at='2099-01-01T00:00:00Z'))
 def test_APIkey_normalization_not_allowed(self):self.rejects(lambda n:n['requests'][0]['after']['users'][0].update(api_key='new-not-original'))
 def test_cache_key_dropped(self):self.rejects(lambda n:n['requests'][0]['after']['raw_Redis'][1]['rows'].pop())
 def test_cache_extra_history(self):self.rejects(lambda n:n['requests'][0]['after']['raw_Redis'][1]['rows'].append({'key_hex':b':1:throttle_api_key_auth_127.0.0.1'.hex()}))
 def test_channels_dump_changed(self):
  def change(n):
   row=next(r for r in n['requests'][0]['after']['raw_Redis'][1]['rows']if r['type']=='zset');row['dump_hex']='00'
  self.rejects(change)
 def test_persistent_TTL_changed(self):
  def change(n):
   row=next(r for r in n['requests'][0]['after']['raw_Redis'][1]['rows']if r['pttl']==-1);row['pttl']=1000
  self.rejects(change)
 def test_TTL_bool_is_not_int(self):self.rejects(lambda n:n['requests'][0]['after']['raw_Redis'][1]['rows'][0].update(pttl=True))
 def test_own_TTL_reset(self):
  before=copy.deepcopy(SOURCE['seed']['raw_Redis']);after=copy.deepcopy(before);row=next(r for d in after for r in d['rows']if r['pttl']>=0);row['pttl']+=1000
  with self.assertRaises(ValueError):V.own_cache(before,after)
 def test_existing_clock_map_precedes_equality(self):
  clocks=V.Clocks(SOURCE);s=next(iter(clocks.generated));n=s+datetime.timedelta(seconds=1);clocks.compare(s.isoformat(),n.isoformat())
  with self.assertRaises(ValueError):clocks.compare(s.isoformat(),s.isoformat())
 def test_coincident_generated_clock_registers(self):
  clocks=V.Clocks(SOURCE);s=next(iter(clocks.generated));clocks.compare(s.isoformat(),s.isoformat());self.assertIn(s,clocks.mapping)
 def test_imported_seed_clock_cannot_change(self):
  clocks=V.Clocks(SOURCE);s=SOURCE['seed']['repos'][0]['created_at']
  with self.assertRaises(ValueError):clocks.compare(s,'2099-01-01T00:00:00Z')
 def test_database_guard(self):
  from native_state import owned
  for bad in ['dispatcharr','postgres','rust_probe_pluginrepo_123','rust_probe_pluginrepo_aaaaaaaaaa;DROP DATABASE dispatcharr']:
   with self.assertRaises(ValueError):owned(bad)
  self.assertEqual(owned('rust_probe_pluginrepo_0123456789'),'rust_probe_pluginrepo_0123456789')
 def test_no_Django_import_on_module_import(self):
  import native_state
  self.assertFalse(any(n=='django'or n.startswith(('django.','apps.','dispatcharr.'))for n in sys.modules))
class BootstrapEnvironment(unittest.TestCase):
 def test_actual_helper_references_covered(self):
  from bootstrap_env import audit_helpers,storage_environment
  result=audit_helpers(ROOT,storage_environment());self.assertTrue(result['passed']);self.assertEqual(len(result['external_required']),9)
 def test_every_required_fixture_variable_missing_refused(self):
  from bootstrap_env import validate_environment,storage_environment
  for key in storage_environment():
   with self.subTest(key=key):
    value=storage_environment();value.pop(key)
    with self.assertRaises(ValueError):validate_environment(value)
 def test_modular_wrongcontext_refused(self):
  from bootstrap_env import validate_environment,storage_environment
  value=storage_environment();value['DISPATCHARR_ENV']='modular'
  with self.assertRaises(ValueError):validate_environment(value)
 def test_unclassified_helper_variable_refused(self):
  import bootstrap_env as E
  original=Path.read_bytes
  def altered(path):
   raw=original(path)
   return raw+b'\necho "$UNDECLARED_REQUIRED_CONTEXT"\n'if path.name=='02-postgres.sh'else raw
  with patch.object(Path,'read_bytes',altered):
   with self.assertRaises(ValueError):E.audit_helpers(ROOT,E.storage_environment())
 def test_guard_must_precede_sourcing(self):
  import bootstrap_env as E
  original=Path.read_text
  with patch.object(Path,'read_text',lambda path,*a,**k:original(path,*a,**k).replace('FixtureRequiredEnvironment','missingguard')if path.name=='start.sh'else original(path,*a,**k)):
   with self.assertRaises(ValueError):E.audit_helpers(ROOT,E.storage_environment())
if __name__=='__main__':unittest.main(verbosity=2)
