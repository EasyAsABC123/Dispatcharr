"""Synthetic no-socket Source2 binding/optional-JSON corruption controls."""
import copy,hashlib,json,unittest
from pathlib import Path
import validate_native as V
ROOT=Path(__file__).resolve().parents[1]
S=json.loads((ROOT/'inputs/Source2-private.json').read_text())
def fixture():
 rows=[]
 for a in S['requests']:
  r={k:copy.deepcopy(a[k])for k in('index','name','actor','method','path','body_hex','body_sha256','body_bytes','status','response_body_hex','response_body_bytes','response_body_sha256','before','after')};r.update(actual_request_invoked=True,capture_complete=True,headers=list(a['headers'].items()),physical_close={'fd_after':-1,'original_inode_absent':True,'timer_joined':True},json=None,json_parse_succeeded=False,json_parse_error={'type':'JSONDecodeError','message':'synthetic HTML parse error'});rows.append(r)
 return {'_synthetic_only':True,'capture_only':True,'passed':False,'capture_complete':True,'actual_request_count':2,'completed_capture_count':2,'requests':rows,'before_requests':copy.deepcopy(S['seed']),'raw_body_byte_parity_predeclared':True,'Source_rendered_HTML_reference':True,'Main_or_fullM8':False}
class Controls(unittest.TestCase):
 def test_two_synthetic_shape_baseline(self):self.assertTrue(V.validate(S,fixture())['passed'])
 def test_optional_json_capture_is_actual_body_not_reencoding(self):
  code=(ROOT/'controller/native_http.py').read_text();self.assertIn("row['json_parse_succeeded']=False",code);self.assertIn('response_body_hex=data.hex()',code);self.assertIn('response_body_sha256',S['requests'][0])
 def test_actual_unique_and_duplicate_seed_key_alias_binding(self):
  plan=json.loads((ROOT/'inputs/plan.json').read_text());users={r['id']:r for r in S['seed']['users']};keys={}
  for role in plan['roles']:
   key=users[role['id']]['api_key'];alias=role['api_key_alias'];self.assertTrue(alias not in keys or keys[alias]==key);keys[alias]=key
  self.assertNotEqual(keys['unique_admin'],keys['duplicate_pair']);self.assertEqual([u['id']for u in users.values()if u['api_key']==keys['duplicate_pair']],[201,202])
 def test_reference_hash_only_selected_original(self):
  auth=json.loads((ROOT/'inputs/authority.json').read_text());self.assertEqual(hashlib.sha256((ROOT/'inputs/Source2-private.json').read_bytes()).hexdigest(),auth['source_raw_sha256']);self.assertNotIn('Source24-private.json',(ROOT/'controller/native_http.py').read_text());self.assertIn('ExactOriginalSource2NoFallback',(ROOT/'controller/native_http.py').read_text())
def negative(name,path,new):
 def test(self):
  v=fixture();d=v
  for key in path[:-1]:d=d[key]
  d[path[-1]]=new
  with self.assertRaises((ValueError,TypeError,KeyError)):V.validate(S,v)
 setattr(Controls,'test_negative_'+name,test)
for name,path,value in [('wrong_count',['actual_request_count'],24),('missing_capture',['capture_complete'],False),('recorder_pass',['passed'],True),('wrong_role',['requests',1,'actor'],'unique_admin'),('wrong_target',['requests',0,'path'],'/api/plugins/repos/801/'),('wrong_body',['requests',0,'body_hex'],'7b7d'),('wrong_status',['requests',1,'status'],401),('changed_source_body',['requests',0,'response_body_hex'],'7b7d'),('wrong_body_hash',['requests',1,'response_body_sha256'],'0'*64),('wrong_content_type',['requests',0,'headers'],[['Content-Type','application/json']]),('forced_json',['requests',0,'json_parse_succeeded'],True),('lost_parse_observation',['requests',0,'json_parse_error'],None),('FDstill_open',['requests',0,'physical_close','fd_after'],1),('timer_alive',['requests',0,'physical_close','timer_joined'],False),('credential_event',['requests',1,'after','events'],[{}]),('wrong_property_type',['requests',0,'after','users',0,'custom_properties'],False),('user_clock_change',['requests',0,'after','users',0,'date_joined'],'2001-01-01T00:00:00Z'),('sequence_mutation',['requests',1,'after','public_manifest','sequences','accounts_user_id_seq'],[999,True]),('table_mutation',['requests',0,'after','public_manifest','tables','plugins_pluginrepo'],{'count':999,'sha_like_md5':'0'*32}),('dropped_cache',['requests',1,'after','raw_Redis'],[]),('Mainclaim',['Main_or_fullM8'],True)]:negative(name,path,value)
if __name__=='__main__':unittest.main()
