"""Negative controls for the approved Source-invalid204 protocol exception."""
import ast,copy,hashlib,json,unittest
from pathlib import Path
source=ast.parse(Path(__file__).with_name('backup_api_packaged_probe.py').read_text());scope={'hashlib':hashlib,'json':json};exec(compile(ast.Module(body=[node for node in source.body if isinstance(node,ast.FunctionDef)and node.name in ['digest','declared_204_delta']],type_ignores=[]),'bounded-backup-protocol-oracle','exec'),scope)
class ProtocolException(unittest.TestCase):
 def setUp(self):
  self.expected={'status':204,'headers':{'content-type':'application/json','content-length':'40','x-content-type-options':'nosniff'},'body_length':0,'body_sha256':hashlib.sha256(b'').hexdigest()};self.actual=copy.deepcopy(self.expected);self.actual['headers'].pop('content-length')
 def allowed(self,name='delete-name-amp&plus+=.zip',failures=None):return scope['declared_204_delta'](name,self.actual,self.expected,['response']if failures is None else failures)
 def test_only_five_named_cases(self):
  for name in ['delete-admin','delete-name-dispatcharr-backup-rust-probe-a.zip','delete-name-arbitrary-upload.txt','delete-name-café.zip','delete-name-amp&plus+=.zip']:self.assertTrue(self.allowed(name))
  self.assertFalse(self.allowed('unknown-delete'))
 def test_other_header_and_filesystem_mismatch_rejected(self):
  self.assertFalse(self.allowed(failures=['response','filesystem']));self.actual['headers']['content-type']='application/zip';self.assertFalse(self.allowed())
 def test_wrong_length_status_or_body_rejected(self):
  for field,value in [('status',200),('body_length',1),('body_length',False),('body_sha256','not-empty')]:
   with self.subTest(field=field,value=value):
    original=self.actual[field];self.actual[field]=value;self.assertFalse(self.allowed());self.actual[field]=original
  self.expected['headers']['content-length']='41';self.assertFalse(self.allowed())
 def test_extra_json_or_retained_length_rejected(self):
  self.actual['json']={};self.assertFalse(self.allowed());self.actual.pop('json');self.actual['headers']['content-length']='40';self.assertFalse(self.allowed())
if __name__=='__main__':unittest.main()
