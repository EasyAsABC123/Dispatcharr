"""Execute the real nested backend hold block with owned synthetic handles.
No socket, Native application, Source worker or database is started.
"""
import ast,sys,threading,time,types,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from pg_reply_proxy import Exchanges
QUERY=b'SELECT id FROM epg_programdata WHERE epg_id=$1 ORDER BY start_time LIMIT 33'
def block(path):
 tree=ast.parse(path.read_text());proxy=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name=='Proxy');init=next(n for n in proxy.body if isinstance(n,ast.FunctionDef)and n.name=='__init__');handler=next(n for n in init.body if isinstance(n,ast.ClassDef)and n.name=='Handler');handle=next(n for n in handler.body if isinstance(n,ast.FunctionDef)and n.name=='handle')
 loop=next(n for n in ast.walk(handle)if isinstance(n,ast.While)and any(isinstance(x,ast.Call)and isinstance(x.func,ast.Attribute)and x.func.attr=='backend'for x in ast.walk(n)))
 begin=next(i for i,n in enumerate(loop.body)if isinstance(n,ast.With));end=next(i for i,n in enumerate(loop.body)if isinstance(n,ast.Expr)and isinstance(n.value,ast.Call)and isinstance(n.value.func,ast.Attribute)and n.value.func.attr=='sendall')
 fn=ast.FunctionDef(name='actual_backend_hold',args=ast.arguments(posonlyargs=[],args=[ast.arg(arg=n)for n in ('owner','exchanges','kind','payload','ident')],kwonlyargs=[],kw_defaults=[],defaults=[]),body=loop.body[begin:end],decorator_list=[]);module=ast.fix_missing_locations(ast.Module(body=[fn],type_ignores=[]));ns={'time':time};exec(compile(module,str(path),'exec'),ns);return ns['actual_backend_hold']
class Held:
 def __init__(self):self.flag=False
 def is_set(self):return self.flag
 def set(self):self.flag=True
class Release:
 def __init__(self):self.waits=[]
 def wait(self,seconds):self.waits.append(seconds);return True
class BackendBody(unittest.TestCase):
 def state(self):
  t=Exchanges('programme-success-Ready',2);t.frontend(b'P',b's\0'+QUERY+b'\0\0\0');t.frontend(b'B',b'\0s\0');t.frontend(b'E',b'\0\0\0\0\0');t.frontend(b'S',b'');t.backend(b'D',b'row1');t.backend(b'D',b'row2');t.backend(b'C',b'SELECT 2\0')
  owner=types.SimpleNamespace(lock=threading.RLock(),held=Held(),release=Release(),timeline=[]);return t,owner
 def test_old_actual_body_reproduces_NameError_before_held(self):
  t,o=self.state()
  with self.assertRaisesRegex(NameError,"name 'state' is not defined"):block(ROOT/'local-controls/pg_reply_proxy-qa1.py.txt')(o,t,b'Z',b'I',2)
  self.assertFalse(o.held.flag);self.assertEqual(len(o.timeline),1);self.assertEqual(o.release.waits,[])
 def test_corrected_actual_body_sets_and_awaits_exact_gate(self):
  t,o=self.state();block(ROOT/'tools/pg_reply_proxy.py')(o,t,b'Z',b'I',2);self.assertTrue(o.held.flag);self.assertEqual(o.release.waits,[20]);self.assertEqual(len(o.timeline),1);self.assertEqual(o.timeline[0]['matching_rows'],2);self.assertEqual(o.timeline[0]['matching_commands_hex'],[b'SELECT 2\0'.hex()])
 def test_queued_ping_does_not_replace_target_hold(self):
  t,o=self.state();t.frontend(b'Q',b';\0');block(ROOT/'tools/pg_reply_proxy.py')(o,t,b'Z',b'I',2);self.assertTrue(o.held.flag);self.assertEqual(o.timeline[0]['identity'],QUERY.decode())
 def test_nonready_frame_does_not_trigger_hold(self):
  t,o=self.state();block(ROOT/'tools/pg_reply_proxy.py')(o,t,b'N',b'notice',2);self.assertFalse(o.held.flag);self.assertEqual(o.release.waits,[])
if __name__=='__main__':unittest.main()
