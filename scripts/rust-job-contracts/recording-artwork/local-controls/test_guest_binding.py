"""Actual guest-code name resolution and real bounded constructor, no app/child."""
import ast,inspect,sys,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import bounded_process

def extracted(path):
 tree=ast.parse(path.read_text());nodes=[]
 for node in tree.body:
  if isinstance(node,ast.Import)and all(x.name in {'base64','datetime','fcntl','hashlib','json','os','signal','sys','time','uuid'}for x in node.names):nodes.append(node)
  if isinstance(node,ast.ImportFrom)and node.module in {'datetime','pathlib','bounded_process'}:nodes.append(node)
  if isinstance(node,ast.FunctionDef)and node.name=='native_artwork_phase':nodes.append(node)
 ns={};exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),ns);return ns
class Lookup(unittest.TestCase):
 def test_actual_guest_code_resolves_real_constructor_and_datetime_before_app(self):
  ns=extracted(ROOT/'tools/source_configured.py');scope=inspect.getclosurevars(ns['native_artwork_phase']);self.assertIs(scope.globals['Collected'],bounded_process.Collected);self.assertIs(ns['datetime'].datetime,__import__('datetime').datetime);self.assertNotIn('Collected',scope.unbound)
 def test_retained_executed_actual1_code_exposes_the_missing_constructor(self):
  old=ROOT/'inputs/prior-executed-source_configured.py.txt';ns=extracted(old);self.assertIn('Collected',inspect.getclosurevars(ns['native_artwork_phase']).unbound)
 def test_real_bounded_constructor_reaches_exact_popen_without_starting_a_child(self):
  ns=extracted(ROOT/'tools/source_configured.py');argv=['/native-parented-peer','recording_artwork_peer::actual_local_artwork_peer','--exact','--ignored','--nocapture','--test-threads=1']
  class StopBeforeChild(RuntimeError):pass
  with patch.object(bounded_process.subprocess,'Popen',side_effect=StopBeforeChild)as spawned:
   with self.assertRaises(StopBeforeChild):ns['Collected'](argv,{},1048576,lambda:None,body=b'{}\n')
   self.assertEqual(spawned.call_count,1);self.assertEqual(spawned.call_args.args[0],argv)
if __name__=='__main__':unittest.main()
