"""Static/local primitive inferences only, never Source application authority."""
import ast,json,shlex,unittest
from pathlib import Path
from source_workflows import profile_cases as definition
ROOT=Path(__file__).resolve().parent
class Primitive(unittest.TestCase):
 def setUp(self):
  tree=ast.parse((ROOT/'source_workflows/inputs/Original/core/models.py').read_bytes());model=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name=='StreamProfile');methods=[n for n in model.body if isinstance(n,ast.FunctionDef)and n.name in ['is_proxy','build_command','_replace_in_part']];need=ast.ClassDef(name='Profile',bases=[],keywords=[],body=methods,decorator_list=[]);scope={'shlex_split':shlex.split,'PROXY_PROFILE_NAME':'Proxy'};exec(compile(ast.fix_missing_locations(ast.Module(body=[need],type_ignores=[])),'<pinned-Source-method-AST>','exec'),scope);self.cls=scope['Profile']
 def invoke(self,c):
  o=self.cls();o.name='Proxy'if c['use_Source_Proxy_name']else'Owned';o.locked=c['locked'];o.command=c['command'];o.parameters=c['parameters'];return o.build_command(c['stream_url'],c['user_agent'],c['channel_id'])
 def test_finite_new_cases(self):self.assertEqual(len(definition.cases()),18);self.assertEqual(len({x['name']for x in definition.cases()}),18)
 def test_locked_Proxy_shortcircuits_before_malformed_quotes_and_None(self):self.assertEqual(self.invoke(definition.cases()[0]),[])
 def test_insertion_happens_after_split_no_shell(self):
  x=self.invoke(definition.cases()[4]);self.assertEqual(x[2],'http://owned.invalid/two words;$HOME');self.assertIn('$(false)',x[4]);self.assertEqual(len(x),5)
 def test_replacement_order_is_forward_only(self):
  x=self.invoke(definition.cases()[5]);self.assertIn('owned-',x[1]);self.assertIn('{streamUrl}',x[1]);self.assertNotIn('{channelId}',x[1])
 def test_zero_None_and_string_zero(self):self.assertEqual(self.invoke(definition.cases()[6]),['ffmpeg','id=']);self.assertEqual(self.invoke(definition.cases()[7]),['ffmpeg','id=']);self.assertEqual(self.invoke(definition.cases()[8]),['ffmpeg','id=0'])
 def test_command_not_split(self):self.assertEqual(self.invoke(definition.cases()[10])[0],'ffmpeg -nostdin')
 def test_malformed_and_None_not_coerced(self):
  with self.assertRaises(ValueError):self.invoke(definition.cases()[15])
  with self.assertRaises(TypeError):self.invoke(definition.cases()[16])
 def test_NUL_is_only_returned_not_executed(self):self.assertIn('\x00',self.invoke(definition.cases()[17])[2])
 def test_guest_has_no_spawn_or_Source_patch(self):
  s=(ROOT/'source_workflows/profile_fixture.py').read_text();ast.parse(s)
  for word in ['subprocess.','os.system(','.start()', 'mock.','force_authenticate(','.build_command =']:
   self.assertNotIn(word,s)
if __name__=='__main__':unittest.main()
