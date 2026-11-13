"""Actual extracted compiler symbol tables and exact closure, no apps/sockets."""
import ast,builtins,hashlib,json,symtable,sys,unittest,runpy
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
class Closure(unittest.TestCase):
 def test_guest_physical_function_has_no_missing_global_constructor_or_module(self):
  text=(ROOT/'tools/source_configured.py').read_text();table=symtable.symtable(text,'actual_guest','exec');defined=set(table.get_identifiers());defined.update(n for n in runpy.run_path(str(ROOT/'tools/seed_spec.py'))if not n.startswith('_'));builtin=set(dir(builtins));child=next(t for t in table.get_children()if t.get_name()=='physical_artwork')
  missing={s.get_name()for s in child.get_symbols()if s.is_global()and s.is_referenced()and s.get_name()not in defined|builtin};self.assertEqual(missing,set())
 def test_actual_host_requires_every_twenty_mounted_tools(self):
  tree=ast.parse((ROOT/'tools/run_source.py').read_text());required=next(ast.literal_eval(n.value)for n in tree.body if isinstance(n,ast.Assign)and any(isinstance(x,ast.Name)and x.id=='REQUIRED'for x in n.targets));actual={p.name for p in(ROOT/'tools').iterdir()if p.is_file()and p.suffix in('.py','.json')};self.assertEqual(required,actual);self.assertEqual(len(required),20)
 def test_input_tool_hashes_match_actual_before_any_driver(self):
  data=json.loads((ROOT/'historical-proposed-execution-inputs.json').read_text());self.assertFalse(data['authorized']);self.assertEqual(set(data['tools_sha256']),{p.name for p in(ROOT/'tools').iterdir()if p.is_file()and p.suffix in('.py','.json')})
  for n,h in data['tools_sha256'].items():self.assertEqual(hashlib.sha256((ROOT/'tools'/n).read_bytes()).hexdigest(),h)
 def test_plan_retains_actual_physical_case_and_hold_budgets(self):
  p=json.loads((ROOT/'case-plan.json').read_text());self.assertEqual(p['case_seconds'],90);self.assertEqual(p['cleanup_seconds'],90);self.assertEqual(p['fixed_gate_hold_seconds'],6.2);self.assertEqual(p['gate_safety_seconds'],20);self.assertFalse(p['Main_default_graph_enabled']);self.assertFalse(p['Source_failure_result_parity'])
 def test_unknown_mode_and_unauthorized_stop_before_any_host_command(self):
  import importlib.util,tempfile
  from unittest.mock import patch
  spec=importlib.util.spec_from_file_location('owned_artwork_physical_host',ROOT/'tools/run_source.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
  with tempfile.TemporaryDirectory()as name:
   base=Path(name);inputs=base/'inputs.json';inputs.write_text(json.dumps({'authorized':False}));output=base/'uncreated'
   with patch.object(sys,'argv',['run_source.py','--repo',name,'--support-root',name,'--output',str(output),'--execution-inputs',str(inputs)]),patch.object(m.shutil,'which',side_effect=AssertionError('No Docker lookup')):
    with self.assertRaisesRegex(ValueError,'ExplicitRootParentedPairAuthority'):m.main()
   self.assertFalse(output.exists())
 def test_peer_prefix_matches_real_compiled_peer(self):
  from interactive_peer import PREFIX
  source=(ROOT.parents[2]/'historical/08-recording_artwork_peer.rs').read_text();self.assertIn(PREFIX.decode(),source)
if __name__=='__main__':unittest.main()
