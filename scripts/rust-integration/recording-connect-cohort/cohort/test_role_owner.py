"""Exercise actual supervisor settlement function with synthetic OS peers.
This is an offline regression, never Source process/runtime acceptance evidence.
"""
import ast,signal,threading,time,types,unittest
from pathlib import Path
class SettlementRegression(unittest.TestCase):
 def test_actual_shutdown_body_reaps_and_is_idempotent(self):
  source=ast.parse(Path(__file__).with_name('role_owner.py').read_text());serve=next(n for n in source.body if isinstance(n,ast.FunctionDef)and n.name=='serve');shutdown=next(n for n in serve.body if isinstance(n,ast.FunctionDef)and n.name=='shutdown')
  class Child:
   ended=False
   def poll(self):return 0 if self.ended else None
   def wait(self,timeout):self.ended=True;return 0
  child=Child();calls=[]
  class Peer:
   parent=types.SimpleNamespace(name='321')
   def read_text(self):
    fields=['S','1','321']+['0']*16+['original'];return '321 (owned) '+' '.join(fields)
  class Proc:
   def glob(self,pattern):return[Peer()]
  def reaped(*args):raise ChildProcessError()
  thread=types.SimpleNamespace(join=lambda seconds:None,is_alive=lambda:False)
  names={'threading':threading,'signal':signal,'time':time,'subprocess':__import__('subprocess'),'Path':lambda value:Proc(),'os':types.SimpleNamespace(kill=lambda *args:calls.append(args),waitpid=reaped,WNOHANG=1),'child':child,'pid':321,'starttime':'original','identity':lambda pid:None if child.ended else'original','shutdown_lock':threading.Lock(),'shutdown_started':threading.Event(),'signal_settled':threading.Event(),'shutdown_settled':threading.Event(),'term_sent':[False],'stopped':[None],'threads':[thread,thread],'failures':[],'status':lambda:{'synthetic_unit_only':True}}
  exec(compile(ast.Module(body=[shutdown],type_ignores=[]),'actual-role-shutdown','exec'),names)
  result=names['shutdown']();self.assertEqual(result['actual_exit'],0);self.assertTrue(result['all_original_process_generations_absent']);self.assertTrue(result['stdout_EOF']and result['stderr_EOF']);self.assertTrue(names['shutdown_settled'].is_set());self.assertEqual(len(calls),1)
  self.assertIs(names['shutdown'](),result);self.assertEqual(len(calls),1)
if __name__=='__main__':unittest.main()
