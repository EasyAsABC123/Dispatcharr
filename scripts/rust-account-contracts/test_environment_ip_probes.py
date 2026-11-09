"""Enabled package proof cannot accept unsupported responses or leak failures."""
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('environment_ip_probes',Path(__file__).with_name('environment_ip_probes.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class EnvironmentIpProbeTests(unittest.TestCase):
    def accounts(self):return {name:{'access':'secret-token'} for name in ['admin','standard','viewer','inactive']}
    def test_matching_unsupported_is_not_a_pass(self):
        records=[];actions=[]
        module.run(lambda *args,**kwargs:(501,{'error':'unsupported'}),'http://127.0.0.1:1','http://127.0.0.1:2',self.accounts(),lambda *args:records.append(args),lambda data:actions.append(data['action']))
        self.assertEqual(len(records),15);self.assertTrue(all(not row[1] for row in records));self.assertEqual(actions[-1],'environment_ip_restore')
    def test_restore_and_redaction_on_transport_exception(self):
        records=[];actions=[]
        def http(*args,**kwargs):raise RuntimeError('secret-token private-response')
        module.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',self.accounts(),lambda *args:records.append(args),lambda data:actions.append(data['action']))
        self.assertEqual(actions[-1],'environment_ip_restore');self.assertFalse(records[0][1]);self.assertEqual(records[0][2],{'error_type':'RuntimeError'});self.assertNotIn('secret-token',repr(records))
if __name__=='__main__':unittest.main()
