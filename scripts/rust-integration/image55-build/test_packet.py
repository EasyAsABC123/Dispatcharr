"""Offline corruption checks after resealing the changed raw evidence."""
import hashlib,importlib.util,json,shutil,tempfile,unittest
from pathlib import Path
HERE=Path(__file__).parent;spec=importlib.util.spec_from_file_location('image55_build',HERE/'validate.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Controls(unittest.TestCase):
 def reject(self,name,change):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d)
   for name0 in [*json.loads((HERE/'manifest.json').read_text()),'manifest.json']:shutil.copyfile(HERE/name0,root/name0)
   p=root/name;v=json.loads(p.read_text());change(v);p.write_text(json.dumps(v));manifest=json.loads((root/'manifest.json').read_text());manifest[name]={'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()};(root/'manifest.json').write_text(json.dumps(manifest))
   with self.assertRaises((ValueError,KeyError,AssertionError)):m.validate(root)
 def test_positive(self):self.assertTrue(m.validate(HERE)['passed'])
 def test_failed_build(self):self.reject('build.json',lambda v:v.update(passed=False))
 def test_unwaited_host(self):self.reject('actual-exit.json',lambda v:v.update(wait_completed=False))
 def test_changed_runtime_source(self):self.reject('build.json',lambda v:v['source_sha256'].update({'rust/Cargo.toml':'0'*64}))
 def test_wrong_image(self):self.reject('Root-image-inspect.json',lambda v:v[0].update(Id='sha256:'+'0'*64))
 def test_performance_promotion(self):self.reject('Root-review.json',lambda v:v.update(performance_gain_claimed=True))
 def test_lost_installed_credit(self):self.reject('Root-credit-frontend-probe.json',lambda v:v.update(stdout=v['stdout'].replace('/usr/share/doc/dispatcharr/LICENSE','/removed/LICENSE')))
 def test_leftover_probe(self):self.reject('Root-probe-absent.json',lambda v:v.update(stdout='leftover\n'))
if __name__=='__main__':unittest.main()
