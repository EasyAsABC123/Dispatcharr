"""Probe failures must remain visible without exposing response credentials."""
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('core_utility_probes',Path(__file__).with_name('core_utility_probes.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class CoreUtilityProbeTests(unittest.TestCase):
    def test_transport_error_is_recorded_without_exception_text(self):
        accounts={role:{'access':'sensitive-token'} for role in ['admin','standard','viewer','inactive']};records=[]
        def http(*args,**kwargs):raise RuntimeError('sensitive-token sensitive-response')
        module.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',accounts,lambda *args:records.append(args))
        self.assertGreater(len(records),20);self.assertTrue(all(not row[1] for row in records))
        self.assertNotIn('sensitive',repr(records));self.assertTrue(all(row[2]=={'error_type':'RuntimeError'} for row in records))
    def test_empty_matching_success_bodies_cannot_satisfy_fixture_witness(self):
        accounts={role:{'access':'token'} for role in ['admin','standard','viewer','inactive']};records=[]
        module.run(lambda *args,**kwargs:(200,{}),'http://127.0.0.1:1','http://127.0.0.1:2',accounts,lambda *args:records.append(args))
        witnesses=[row for row in records if 'dataset:' in row[0] or 'events-fixture:' in row[0] or 'known-display:' in row[0]]
        self.assertEqual(len(witnesses),6);self.assertTrue(all(not row[1] for row in witnesses))
if __name__=='__main__':unittest.main()
