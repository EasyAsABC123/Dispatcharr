"""Offline safety controls for the owned application-switch controller."""
import ast
from pathlib import Path
import unittest

class SwitchControls(unittest.TestCase):
    def setUp(self):
        self.text=Path(__file__).with_name('stream_change.py').read_text();self.tree=ast.parse(self.text)
    def test_no_resource_sampler_import_or_thread(self):
        imports=[ast.unparse(n)for n in ast.walk(self.tree)if isinstance(n,(ast.Import,ast.ImportFrom))]
        self.assertFalse(any('perf' in x or 'resources' in x or 'docker_stats' in x for x in imports))
        self.assertNotIn('threading.Thread(',self.text)
    def test_real_application_route_not_fixture_origin_control(self):
        self.assertIn("base+'/proxy/ts/change_stream/'",self.text)
        self.assertNotIn("origin+'/control'",self.text)
    def test_authenticated_mutation_once_not_retried(self):
        self.assertEqual(self.text.count("control_request('POST',True)"),1)
        self.assertIn("channels[0]['backup_stream_pk']",self.text)
    def test_bounded_body_and_same_original_reader(self):
        self.assertIn('response.read(65537)',self.text);self.assertIn('if len(raw)>65536',self.text)
        self.assertEqual(self.text.count("owner=reader(0,'original-body')"),1)
        self.assertIn("count+750000",self.text)
    def test_policy_keeps_unlimited_and_null_growth(self):
        self.assertIn("policy['provider_cap']==0",self.text);self.assertNotIn('p.max_streams=1',self.text)

if __name__=='__main__':unittest.main()
