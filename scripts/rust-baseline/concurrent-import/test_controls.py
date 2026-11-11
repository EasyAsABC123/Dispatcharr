import importlib.util,json,pathlib,sys,unittest,hashlib
from unittest import mock
HERE=pathlib.Path(__file__).resolve().parent
ROOT=HERE.parents[2]
for rel,sha in [('scripts/rust-guide-perf/cgroup.py','83221c96647611c22367fc37859431bba1533334a33b1415c631e039bad0a401'),('scripts/rust-guide-perf/docker_stats.py','bc554c7c30ba004dc5b8a6e78758ca1431696d0ce94302180b6cc5aa693ddb25')]:
 assert hashlib.sha256((ROOT/rel).read_bytes()).hexdigest()==sha
sys.path[:0]=[str(ROOT/'scripts/rust-guide-perf'),str(ROOT/'scripts/rust-integration')]
def load(name):
 s=importlib.util.spec_from_file_location(name,HERE/(name+'.py'));m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m
observer=load('observer');policy=json.loads((HERE/'policy.json').read_text())
class Controls(unittest.TestCase):
 def test_source_only(self):self.assertFalse(policy['resource_acceptance']);self.assertEqual(policy['Source_repetitions_now'],1)
 def test_meaningful_fixed_schedule(self):self.assertEqual(policy['imports_at_seconds'],[5,60]);self.assertEqual(policy['measure_seconds'],120);self.assertLess(60+policy['import_deadline_seconds'],120)
 def test_finite_profile(self):self.assertEqual(policy['provider_max_streams'],1)
 def test_bounded_media(self):self.assertEqual(policy['prefix_bytes'],2097152);self.assertEqual(policy['decode_seconds'],5);self.assertEqual(policy['minimum_video_frames'],25)
 def test_four_services_required(self):
  with self.assertRaises(ValueError):observer.Observer({'baseline':{},'origin':{},'source':{}},ROOT)
 def test_four_services_and_unique_ids(self):
  selected={n:{'id':n,'limits':v}for n,v in policy['caps'].items()}
  with mock.patch.object(observer,'load_perf',return_value=None),mock.patch.object(observer.docker_stats,'Stats'):
   self.assertEqual(len(observer.Observer(selected,ROOT).selected),4)
   for v in selected.values():v['id']='duplicate'
   with self.assertRaises(ValueError):observer.Observer(selected,ROOT)
 def test_finite_caps(self):
  self.assertEqual(sum(v['nano_cpus']for v in policy['caps'].values()),6000000000)
  self.assertEqual(sum(v['memory_bytes']for v in policy['caps'].values()),2684354560)
 def test_no_candidate_cli(self):self.assertNotIn("add_argument('--manifest'",(HERE/'run.py').read_text())
if __name__=='__main__':unittest.main()
