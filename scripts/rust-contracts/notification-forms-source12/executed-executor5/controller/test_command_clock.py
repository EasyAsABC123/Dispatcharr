"""Execute the actual nested collector with local Python only; no app/Docker."""
import ast,copy,sys,tempfile,unittest
from pathlib import Path
import run
ROOT=Path(__file__).resolve().parent.parent

def command_factory(text,out):
 tree=ast.parse(text);main=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='main');command=copy.deepcopy(next(n for n in main.body if isinstance(n,ast.FunctionDef)and n.name=='command'))
 prefix=ast.parse("def factory(out):\n serial=0\n jobs=[]\n cleanup_deadline=None\n began=time.monotonic()\n environment=dict(os.environ)\n def stop_owned():\n  raise AssertionError('UnexpectedLocalStop')\n").body[0]
 prefix.body.extend([command,ast.Return(ast.Tuple(elts=[ast.Name(id='command',ctx=ast.Load()),ast.Name(id='jobs',ctx=ast.Load())],ctx=ast.Load()))])
 namespace=dict(vars(run));exec(compile(ast.fix_missing_locations(ast.Module(body=[prefix],type_ignores=[])),'actual-controller-command','exec'),namespace)
 return namespace['factory'](out)
class Clock(unittest.TestCase):
 def test_actual_command_local_child_settles_and_clock_is_utc(self):
  with tempfile.TemporaryDirectory(prefix='source12-command-control-')as d:
   out=Path(d);command,jobs=command_factory((ROOT/'controller/run.py').read_text(),out)
   self.assertEqual(command([sys.executable,'-B','-c',"print('local-collector-control')"],timeout=5),'local-collector-control\n')
   receipt=run.json.loads((out/'command-001.json').read_text());self.assertEqual(receipt['exit'],0);self.assertTrue(receipt['physically_settled']);self.assertTrue(receipt['pipe_threads_settled']);self.assertIn('+00:00',receipt['began_utc']);self.assertEqual(len(jobs),1)
 def test_literal_executor3_shadowing_fails_before_child(self):
  old=ROOT/'inputs/prior-static-negatives/executor3-pre-Docker/executor3-run.py'
  with tempfile.TemporaryDirectory(prefix='source12-old-clock-control-')as d:
   command,jobs=command_factory(old.read_text(),Path(d))
   with self.assertRaises(UnboundLocalError):command([sys.executable,'-c','raise AssertionError()'])
   self.assertEqual(jobs,[]);self.assertEqual(list(Path(d).iterdir()),[])
if __name__=='__main__':unittest.main()
