import json,shutil,tempfile,unittest
from pathlib import Path
import verify
class Public(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)/'public';shutil.copytree(Path(__file__).parent,self.root,ignore=shutil.ignore_patterns('__pycache__'))
 def bad(self,name,fn):
  p=self.root/name;v=json.loads(p.read_text());fn(v);p.write_text(json.dumps(v))
  with self.assertRaises((AssertionError,ValueError)):verify.verify(self.root)
 def test_public_only(self):self.assertEqual(verify.verify(self.root)['typed'],154)
 def test_wrong_image(self):self.bad('image-binding.json',lambda v:v.update(image_id='other'))
 def test_Source_case_binding(self):self.bad('safe-operator17.json',lambda v:v.update(Original_source_case_binding_sha256='0'*64))
 def test_missing_safe_case(self):self.bad('safe-operator17.json',lambda v:v['cases'].pop())
 def test_integer_is_not_boolean_check(self):self.bad('safe-operator17.json',lambda v:v['cases'][0]['checks'].update(network_rows=1))
 def test_default_DB_false(self):self.bad('createsuperuser22.json',lambda v:v.update(default_database_unchanged=False))
 def test_runtime_caps_missing(self):self.bad('createsuperuser22.json',lambda v:v.update(runtime_UID65532_readonly_capdropALL_inspected=False))
 def test_transcript_bytes_forbidden(self):self.bad('createsuperuser22.json',lambda v:v['cases'][0].update(stdout='mustnotpublish'))
 def test_invalid_transcript_hash(self):self.bad('createsuperuser22.json',lambda v:v['cases'][0].update(stdout_sha256='bad'))
 def test_typed_comparison_missing(self):self.bad('createsuperuser22.json',lambda v:v['strict_comparison'].update(strict_typed_comparisons=153))
 def test_duplicate_create_id(self):self.bad('createsuperuser22.json',lambda v:v['cases'][1].update(id=v['cases'][0]['id']))
 def test_cleanup_not_empty(self):self.bad('safe-operator17.json',lambda v:v['cleanup'].update(all_empty=False))
 def test_no_performance_promotion(self):self.bad('image-binding.json',lambda v:v.update(performance_verified=True))
 def test_deep_missing_not_fallback(self):
  with self.assertRaisesRegex(ValueError,'DeepRaw'):verify.verify(self.root,deep_repo=Path(self.tmp.name)/'missing')
if __name__=='__main__':unittest.main()
