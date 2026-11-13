"""Portable corruption controls on actual projected captures, no apps/sockets."""
import copy,json,tempfile,unittest
from pathlib import Path
import verify as V
class Portable(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.refs=[V.load_projection(n)for n in('Source24','Native24','Source2','Native2')]
 def test_all_four_distinct_epochs(self):self.assertTrue(V.verify()['passed'])
 def bad(self,epoch,path,new):
  refs=copy.deepcopy(self.refs);d=refs[epoch]
  for k in path[:-1]:d=d[k]
  d[path[-1]]=new
  with self.assertRaises((ValueError,TypeError,KeyError)):V.qualify(*refs)
 def test_wrong_Native23_status(self):self.bad(1,['requests',0,'status'],200)
 def test_old_Native24_JSONgap_not_relabelled(self):self.bad(1,['requests',23,'status'],200)
 def test_old_Source24_not_invented_HTTP(self):self.bad(0,['requests',23,'transport'],'http_response')
 def test_Source2_no_suppressed_exc_info(self):self.bad(2,['requests',1,'response_exc_info','complete'],False)
 def test_Source2_duplicate_key_equality_class(self):self.bad(2,['seed','users',2,'api_key'],'other-public-key')
 def test_Source24_role_header_alias_swap(self):self.bad(0,['requests',2,'raw_headers_private'],{})
 def test_Native2_wrong_HTML_hash(self):self.bad(3,['requests',0,'response_body_sha256'],'0'*64)
 def test_Native2_wrong_CT(self):self.bad(3,['requests',1,'headers'],[['content-type','application/json']])
 def test_Native2_force_JSON(self):self.bad(3,['requests',0,'json_parse_succeeded'],True)
 def test_Native2_authfailure_sideeffect(self):self.bad(3,['requests',1,'after','events'],[{}])
 def test_Native24_clock_rewind(self):
  refs=copy.deepcopy(self.refs);r=refs[1]['requests'][9];r['after']['repos'][1]['updated_at']='2000-01-01T00:00:00Z'
  with self.assertRaises(ValueError):V.qualify(*refs)
 def test_Native2_sequencechange(self):self.bad(3,['requests',1,'after','public_manifest','sequences','accounts_user_id_seq'],[999,True])
 def test_unknown_reference_hash_rejected_not_fallback(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'wrong.gz';p.write_bytes(b'notselectedreference')
   with self.assertRaises(ValueError):V.load_projection('Source24',p)
 def test_recorderflag_not_promoted(self):self.bad(3,['passed'],True)
if __name__=='__main__':unittest.main()
