"""Offline publication controls; no application, Docker, build or media work."""
import copy,gzip,hashlib,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import materialize,review
import inspect_image
ROOT=Path(__file__).resolve().parent

class Materialize(unittest.TestCase):
 def fixture(self,base):
  root=base/'root';(root/'evidence').mkdir(parents=True);data=b'{"actual":true}\n';packed=gzip.compress(data,mtime=0);(root/'evidence/one.json.gz').write_bytes(packed);m={'one.json':{'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest(),'gzip_sha256':hashlib.sha256(packed).hexdigest()}};(root/'evidence-map.json').write_text(json.dumps(m));return root,m
 def test_exact_bytes_and_fresh_output(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);out=base/'out';self.assertTrue(materialize.restore(out,root)['passed']);self.assertEqual((out/'one.json').read_bytes(),b'{"actual":true}\n')
   with self.assertRaises(FileExistsError):materialize.restore(out,root)
 def test_bad_gzip_hash(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);(root/'evidence/one.json.gz').write_bytes(b'corrupt')
   with self.assertRaises(ValueError):materialize.restore(base/'out',root)
   self.assertFalse((base/'out').exists())
 def test_wrong_decoded_hash(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);m['one.json']['sha256']='0'*64;(root/'evidence-map.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):materialize.restore(base/'out',root)
 def test_boolean_declared_size(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);m['one.json']['bytes']=True;(root/'evidence-map.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):materialize.restore(base/'out',root)
 def test_traversal_rejected_before_output(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);(root/'evidence-map.json').write_text(json.dumps({'../escape':m['one.json']}))
   with self.assertRaises(ValueError):materialize.restore(base/'out',root)
 def test_short_or_extra_inflation_rejected(self):
  with tempfile.TemporaryDirectory()as d:
   base=Path(d);root,m=self.fixture(base);m['one.json']['bytes']-=1;(root/'evidence-map.json').write_text(json.dumps(m))
   with self.assertRaises(ValueError):materialize.restore(base/'out',root)

class AuthenticationProjection(unittest.TestCase):
 def item(self):return {'direction':'frontend','kind_hex':'70','payload_base64':None,'payload_bytes':123,'payload_sha256':'a'*64,'authentication_payload_projected':True}
 def test_projected_password_length_hash(self):self.assertEqual(review.projected_auth(self.item()),1)
 def test_raw_password_rejected(self):
  value=self.item();value['payload_base64']='c2VjcmV0'
  with self.assertRaises(ValueError):review.projected_auth(value)
 def test_missing_explicit_null_rejected(self):
  value=self.item();value.pop('payload_base64')
  with self.assertRaises(ValueError):review.projected_auth(value)
 def test_boolean_length_rejected(self):
  value=self.item();value['payload_bytes']=True
  with self.assertRaises(ValueError):review.projected_auth(value)
 def test_backend_challenge_rejected_unprojected(self):
  value=self.item();value.update(direction='backend',kind_hex='52',authentication_payload_projected=False)
  with self.assertRaises(ValueError):review.projected_auth(value)
 def test_cancel_secret_projection(self):
  value=self.item();value.update(direction='backend',kind_hex='4b',cancel_secret_projected=True);self.assertEqual(review.projected_auth(value),1)
 def test_application_frames_are_not_redacted(self):
  self.assertEqual(review.projected_auth({'direction':'frontend','kind_hex':'50','payload_base64':'YWJj'}),0)

class ImageReceipt(unittest.TestCase):
 def fake(self,ident):
  class Process:
   stdout=None
  class Job:
   def __init__(self,argv,environment,limit,stop):
    if argv[1:4]!=['image','inspect','--format']or limit!=65536:raise AssertionError('Read-only bounded command')
    self.output={'stdout':bytearray(json.dumps(ident).encode()),'stderr':bytearray()};self.threads=[];self.physically_settled=True;self.invalid=None
   def wait(self,seconds):
    if seconds!=15:raise AssertionError('Fixed readonly timeout')
   def poll(self):return 0
  return Job
 def test_real_function_constructor_resolution_and_typed_receipt(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);docker=root/'docker';docker.write_bytes(b'fixture executable bytes');out=root/'receipt.json'
   with patch.object(inspect_image,'Collected',self.fake(inspect_image.EXPECTED)):
    value=inspect_image.inspect(docker,out)
   self.assertEqual(value['image']['Id'],inspect_image.EXPECTED);self.assertIs(value['process_settled'],True);self.assertEqual(value['actual_exit'],0)
 def test_wrong_image_cannot_become_positive_receipt(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d);docker=root/'docker';docker.write_bytes(b'fixture');out=root/'receipt.json'
   with patch.object(inspect_image,'Collected',self.fake('sha256:'+'0'*64)):
    with self.assertRaises(ValueError):inspect_image.inspect(docker,out)
   self.assertEqual(json.loads(out.read_text())['image'],{})
 def test_existing_receipt_never_overwritten_or_dispatched(self):
  with tempfile.TemporaryDirectory()as d:
   out=Path(d)/'receipt.json';out.write_text('preserved')
   with patch.object(inspect_image,'Collected',side_effect=AssertionError('No dispatch')):
    with self.assertRaises(FileExistsError):inspect_image.inspect(Path(d)/'missing-docker',out)
   self.assertEqual(out.read_text(),'preserved')

class DatasetAssignments(unittest.TestCase):
 def rows(self):return json.loads((ROOT/'datasets.json').read_text())
 def validate(self,rows):
  with tempfile.TemporaryDirectory()as d:
   path=Path(d);(path/'datasets.json').write_text(json.dumps(rows));return review.dataset_rows(path)
 def test_exact_observed_assignments(self):self.assertEqual(len(self.validate(self.rows())),12)
 def test_missing_physical_case_rejected(self):
  rows=self.rows();rows.pop(2)
  with self.assertRaises(ValueError):self.validate(rows)
 def test_reusing_positive_domain_does_not_cover_error_case(self):
  rows=self.rows();rows[2]['mode']=rows[0]['mode']
  with self.assertRaises(ValueError):self.validate(rows)
 def test_negative_cannot_be_relabeled_pass(self):
  rows=self.rows();rows[-1]['passed']=True
  with self.assertRaises(ValueError):self.validate(rows)
 def test_boolean_pass_cannot_be_numeric_one(self):
  rows=self.rows();rows[0]['passed']=1
  with self.assertRaises(ValueError):self.validate(rows)
 def test_corrected_case_cannot_use_old_fixture_bundle(self):
  rows=self.rows();rows[-2]['tools_bundle']='refusal'
  with self.assertRaises(ValueError):self.validate(rows)

if __name__=='__main__':unittest.main()
