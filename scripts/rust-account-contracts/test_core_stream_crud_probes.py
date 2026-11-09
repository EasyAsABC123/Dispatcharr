"""Unit evidence for strictly limited identity normalization."""
import copy
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('core_stream_crud_probes',Path(__file__).with_name('core_stream_crud_probes.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
class CoreStreamProbeTests(unittest.TestCase):
    def test_only_created_identity_is_normalized(self):
        original={'id':3,'name':'rust-probe-stream-profile-test','user_agent':7,'locked':False}
        snapshot=copy.deepcopy(original);normalized=module.normalize(original)
        self.assertEqual(original,snapshot);self.assertEqual(normalized['id'],'<created-stream-profile-id>');self.assertEqual(normalized['user_agent'],7)
        self.assertEqual(module.normalize({'name':['error']}),{'name':['error']})
    def test_digest_preserves_integer_float_and_boolean_differences(self):
        self.assertNotEqual(module.digest({'value':1}),module.digest({'value':1.0}))
        self.assertNotEqual(module.digest({'value':1}),module.digest({'value':True}))
if __name__=='__main__':unittest.main()
