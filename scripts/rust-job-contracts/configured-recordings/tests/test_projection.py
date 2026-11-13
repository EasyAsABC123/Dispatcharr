import base64,copy,hashlib,json,sys,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).resolve().parents[1];sys.path.insert(0,str(HERE))
from project_auth import project
from projection_walk import verify
import materialize_evidence

def frame(direction,kind,payload):return {'connection':7,'direction':direction,'kind_hex':kind,'payload_base64':base64.b64encode(payload).decode(),'epoch':1234.56789}
class AuthProjection(unittest.TestCase):
 def test_password_projection_hash_length_and_clock_preserved(self):
  raw=frame('frontend','70',b'synthetic-auth-fixture');out,receipts=project(raw);self.assertIsNone(out['payload_base64']);self.assertEqual(out['epoch'],raw['epoch']);self.assertEqual(verify(raw,out),1);self.assertEqual(receipts[0]['raw_sha256'],hashlib.sha256(b'synthetic-auth-fixture').hexdigest())
 def test_only_SASL_challenge_verifier_redacted(self):
  for code in(0,10,11,12):
   raw=frame('backend','52',code.to_bytes(4,'big')+(b'synthetic-fixture'if code else b''));out,receipts=project(raw);self.assertEqual(len(receipts),int(code in(11,12)));self.assertEqual(verify(raw,out),int(code in(11,12)))
 def test_application_frame_order_and_bytes_unchanged(self):
  data=[frame('frontend','50',b'SELECT NULL AS private_password_column'),frame('frontend','42',b'actual Bind bytes'),frame('backend','45',b'CONTEXT retained'),frame('backend','5a',b'I')];out,receipts=project({'frames':data,'nullable':None,'timestamp':'2026-11-13T00:00:00Z'});self.assertEqual(out,{'frames':data,'nullable':None,'timestamp':'2026-11-13T00:00:00Z'});self.assertEqual(receipts,[])
 def test_unknown_auth_method_refused(self):
  with self.assertRaisesRegex(ValueError,'UnqualifiedPGAuthenticationMethod'):project(frame('backend','52',(5).to_bytes(4,'big')+b'unknown'))
 def test_unknown_frame_field_refused(self):
  raw=frame('frontend','70',b'synthetic');raw['other_auth_copy']='must not bypass projection'
  with self.assertRaisesRegex(ValueError,'ExactRawPGAuthenticationFrameShapeRequired'):project(raw)
 def test_independent_walk_rejects_changed_SQL_clock_or_order(self):
  raw={'frames':[frame('frontend','70',b'synthetic'),frame('frontend','50',b'SELECT 1')],'result':{'status':'SUCCESS','result':None}};out,_=project(raw)
  for mutate in [lambda p:p['frames'][1].update(payload_base64='eA=='),lambda p:p['frames'][0].update(epoch=0),lambda p:p['frames'].reverse(),lambda p:p['result'].pop('result')]:
   changed=copy.deepcopy(out);mutate(changed)
   with self.assertRaises(AssertionError):verify(raw,changed)
 def test_independent_walk_rejects_bad_payload_receipt(self):
  raw=frame('frontend','70',b'synthetic');out,_=project(raw);out['authentication_payload_redacted']['raw_sha256']='0'*64
  with self.assertRaises(AssertionError):verify(raw,out)
 def test_projected_binding_rejects_original_raw_even_if_semantics_usable(self):
  original=frame('frontend','70',b'synthetic');projected,_=project(original)
  with tempfile.TemporaryDirectory()as tmp:
   p=Path(tmp);package=p/'package';package.mkdir();proofs=p/'proofs';proofs.mkdir();expected=(json.dumps(projected,indent=2)+'\n').encode();(package/'projected-proof-map.json').write_text(json.dumps({'report.json':hashlib.sha256(expected).hexdigest()}));file=proofs/'report.json';file.write_bytes(expected);old=materialize_evidence.HERE;materialize_evidence.HERE=package
   try:
    materialize_evidence.validate_projected_tree(proofs);file.write_text(json.dumps(original))
    with self.assertRaisesRegex(ValueError,'OnlyExactProjectedProofsAccepted_NoRawFallback'):materialize_evidence.validate_projected_tree(proofs)
   finally:materialize_evidence.HERE=old
if __name__=='__main__':unittest.main()

class SQLOnlyClone(unittest.TestCase):
 def test_stored_frames_times_and_SQL_unchanged(self):
  import review
  private={'native_pg':{'frames':[frame('frontend','70',b'synthetic'),frame('frontend','50',b'SELECT 1'),frame('frontend','42',b'actual Bind bytes'),frame('backend','45',b'actual SQL error'),frame('backend','5a',b'I')]}}
  stored,_=project(private);original=copy.deepcopy(stored);clone=copy.deepcopy(stored);self.assertEqual(review.sql_comparison_view(clone),1);self.assertEqual(stored,original);self.assertEqual(clone['native_pg']['frames'],stored['native_pg']['frames'][1:]);self.assertEqual(stored['native_pg']['frames'][0]['epoch'],private['native_pg']['frames'][0]['epoch'])
 def test_cannot_label_application_Parse_as_auth(self):
  import review
  value,_=project(frame('frontend','70',b'synthetic'));value['kind_hex']='50'
  with self.assertRaisesRegex(ValueError,'ExactProjectedAuthReceiptRequired'):review.sql_comparison_view({'native_pg':{'frames':[value]}})
