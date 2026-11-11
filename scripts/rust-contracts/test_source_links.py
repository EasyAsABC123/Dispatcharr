"""Corruption controls for the Original-only stable-identity links."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
spec=importlib.util.spec_from_file_location('_dispatcharr_original_source_links',Path(__file__).with_name('source_links.py'))
links=importlib.util.module_from_spec(spec);spec.loader.exec_module(links)
ROOT=Path(os.environ.get('CONTRACT_SOURCE_LINK_ROOT',Path(__file__).resolve().parents[2]))
class SourceLinks(unittest.TestCase):
 def setUp(self):
  paths={'register':'docs/rust-contract-register.json','plugin':'scripts/rust-plugin-contracts/source-plugin-http-4.json','plugin_proof':'scripts/rust-plugin-contracts/verified-plugin-http-4.json','passive':'scripts/rust-contracts/source-passive-4.json','passive_proof':'scripts/rust-contracts/verified-passive-help-1.json','help_data':'scripts/rust-contracts/source-management-help-2.json'}
  self.paths=paths;self.values={k:links.read(ROOT,p)for k,p in paths.items()}
 def reject(self):
  with self.assertRaises(ValueError):links.bind(**self.values)
 def test_exact_public_inputs(self):
  r=links.build(ROOT);self.assertEqual(r['counts']['Original_TCP_cases'],35);self.assertEqual(r['counts']['fully_verified_operations'],0)
  self.assertTrue(all(x['declared_method']is None and not x['native_evaluated']for x in r['Original_TCP_links']))
 def test_register_unchanged(self):
  before=copy.deepcopy(self.values['register']);links.bind(**self.values);self.assertEqual(before,self.values['register'])
 def test_role(self):self.values['plugin']['cases'][0]['role']='unknown';self.reject()
 def test_fake_TCP(self):self.values['plugin']['cases'][0]['actual_original_TCP']=False;self.reject()
 def test_native_promotion(self):self.values['plugin']['native_verified']=True;self.reject()
 def test_missing_case(self):self.values['plugin']['cases'].pop();self.reject()
 def test_wrong_path(self):self.values['plugin']['cases'][0]['path']='/api/plugins/unknown/';self.reject()
 def test_format_alias_is_separate(self):self.values['plugin']['cases'][0]['path']='/api/plugins/plugins.json';self.reject()
 def test_HEAD_body(self):
  next(c for c in self.values['plugin']['cases']if c['method']=='HEAD')['body_bytes']=1;self.reject()
 def test_passive_route_reordered(self):
  r=self.values['passive']['routes'];r[0],r[1]=r[1],r[0];self.reject()
 def test_method_declaration_drift(self):
  self.values['passive']['routes'][0]['method_declarations_unchanged']=['OPTIONS'];self.reject()
 def test_passive_handler_executed(self):self.values['passive']['handler_invocations']=1;self.reject()
 def test_passive_SQL_attempt(self):self.values['passive']['violations']=[{'kind':'SQL_attempt'}];self.reject()
 def test_help_would_execute_handle(self):self.values['help_data']['cases'][0]['argv']=['proxy','start'];self.reject()
 def test_help_stream_corruption(self):self.values['help_data']['cases'][0]['stdout']+='corruption';self.reject()
 def test_help_candidate_identity(self):
  self.values['register']['non_HTTP_candidates']=[r for r in self.values['register']['non_HTTP_candidates']if r['candidate_id']!='candidate-8738db7f5c7a632f8932'];self.reject()
 def test_input_hash_corruption(self):
  with tempfile.TemporaryDirectory()as d:
   root=Path(d)
   for p in self.paths.values():
    dest=root/p;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/p,dest)
   p=root/self.paths['plugin'];p.write_bytes(p.read_bytes()+b'\n')
   with self.assertRaisesRegex(ValueError,'plugin dataset hash'):links.build(root)
if __name__=='__main__':unittest.main(verbosity=2)
