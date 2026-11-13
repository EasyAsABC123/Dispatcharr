"""Pure Source2 seed/body/exception checks; zero app imports/requests."""
import copy,hashlib,json,unittest
from pathlib import Path
import effects as E
ROOT=Path(__file__).resolve().parents[1]
PLAN=json.loads((ROOT/'inputs/plan.json').read_text())
def seed():
 nonce='a'*10
 users=[{'id':r['id'],'username':'owned-pluginrepo-'+nonce+'-'+r['alias'],'user_level':r['user_level'],'is_active':True,'custom_properties':r['custom_properties'],'api_key':'owned-rendered-errors-'+nonce+'-'+r['api_key_alias'].replace('_','-')}for r in PLAN['roles']]
 settings=[{'key':r['key'],'name':r['name'],'value':r['value']}for r in PLAN['seed']['CoreSettings']]
 return {'database':'rust_probe_pluginrepo_'+nonce,'seed':{'users':users,'repos':[{'id':1},PLAN['seed']['custom_repos'][0]],'configs':[],'settings':settings,'public_manifest':{'sequences':{k:[v,True]for k,v in PLAN['seed']['three_called_sequence_states'].items()}}}}
class Controls(unittest.TestCase):
 def test_exact_seed_positive(self):self.assertTrue(E.validate_seed(seed(),PLAN)['passed'])
 def test_unique_duplicate_roles_are_actual_seed_contract(self):
  for path,value in[(('seed','users',1,'api_key'),'other-key'),(('seed','users',2,'id'),201),(('seed','users',2,'user_level'),True),(('seed','users',0,'is_active'),False),(('seed','repos',1,'id'),801),(('seed','public_manifest','sequences','accounts_user_id_seq'),[203,True])]:
   v=seed();d=v
   for k in path[:-1]:d=d[k]
   d[path[-1]]=value
   with self.assertRaises(ValueError):E.validate_seed(v,PLAN)
 def test_complete_exception_literal(self):
  v={'type':'owned.synthetic.Exception','complete':True,'truncated':False}
  for field in('message','traceback'):
   raw=b'owned pure exception';v.update({field+'_hex':raw.hex(),field+'_bytes':len(raw),field+'_original_bytes':len(raw),field+'_sha256':hashlib.sha256(raw).hexdigest(),field+'_original_sha256':hashlib.sha256(raw).hexdigest(),field+'_truncated':False})
  E.raw_exception(v)
  for field,new in[('complete',False),('truncated',True),('message_original_bytes',999),('traceback_original_sha256','0'*64),('traceback_truncated',True)]:
   bad=copy.deepcopy(v);bad[field]=new
   with self.assertRaises(ValueError):E.raw_exception(bad)
 def test_false_observer_literal_and_separate_response_exception(self):
  code=(ROOT/'controller/source_fixture.py').read_text();self.assertIn('client.raise_request_exception=False',code);self.assertNotIn('client.raise_request_exception=True',code);self.assertIn("row['response_exc_info']",code);self.assertIn('exception(response.exc_info)',code);self.assertIn("'framework_exception','response_exc_info'",code);self.assertIn("need(len(dependencies)==67",code)
 def test_actual_observed_identity_guard_variable(self):
  code=(ROOT/'controller/run.py').read_text();self.assertIn("type(observed)is str and observed.isdigit()and int(observed)>0",code);self.assertNotIn("type(starttime)is str",code)
 def test_no_stale_execution_seal(self):
  self.assertEqual(json.loads((ROOT/'execution-plan.json').read_text())['state'],'COMPLETE_FRESH_SOURCE2_UNEXECUTED');
  if(ROOT/'seal.json').exists():self.assertEqual(json.loads((ROOT/'seal.json').read_text())['state'],'SEALED_UNEXECUTED_SOURCE2_RENDERED_ERRORS')
 def test_no_function_patches_or_forceauth(self):
  code=(ROOT/'controller/source_fixture.py').read_text();self.assertNotIn('.force_authenticate(',code);self.assertNotIn('mock.patch',code);self.assertIn("should_skip_initialization()is True",code);self.assertIn("User._meta.get_field('api_key').unique is False",code);self.assertIn('response.content',code)
if __name__=='__main__':unittest.main()
