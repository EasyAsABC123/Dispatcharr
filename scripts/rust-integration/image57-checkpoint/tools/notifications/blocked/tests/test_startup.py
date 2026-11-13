"""Pure startup-order/corruption controls against exact Original code; no app import."""
import ast,copy,logging,os,types,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
TEXT=(ROOT/'tools/db.py').read_text()
def check(text):
    tree=ast.parse(text);fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='setup');nodes=list(ast.walk(fn))
    setup=next(n for n in nodes if isinstance(n,ast.Call)and ast.unparse(n.func)=='django.setup')
    marker=[n for n in nodes if isinstance(n,ast.Call)and ast.unparse(n.func)=='sys.argv.append'and len(n.args)==1 and isinstance(n.args[0],ast.Constant)and n.args[0].value=='shell']
    if len(marker)!=1 or marker[0].lineno>=setup.lineno:raise ValueError('ShellBeforeDjangoSetup')
    imported=[n for n in nodes if isinstance(n,ast.ImportFrom)and n.module=='dispatcharr.app_initialization'and any(a.name=='should_skip_initialization'and a.asname is None for a in n.names)]
    if len(imported)!=1 or imported[0].lineno>=setup.lineno:raise ValueError('RealOriginalSkipImportBeforeSetup')
    guards=[n for n in nodes if isinstance(n,ast.If)and ast.unparse(n.test)=='should_skip_initialization() is not True'and len(n.body)==1 and isinstance(n.body[0],ast.Raise)]
    if not any(n.lineno<setup.lineno for n in guards)or not any(n.lineno>setup.lineno for n in guards):raise ValueError('BeforeAndAfterOriginalSkipAssertions')
    return True

def original_skip(argv,worker=None):
    tree=ast.parse((ROOT/'reference/dispatcharr/app_initialization.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='should_skip_initialization');space={'sys':types.SimpleNamespace(argv=argv),'os':os,'logger':logging.getLogger('pure-startup-control'),'_uwsgi_worker_id':lambda:worker,'_is_worker_process':lambda:False}
    exec(compile(ast.Module(body=[fn],type_ignores=[]),'pinned-Original-should-skip','exec'),space)
    return space['should_skip_initialization']()
class Startup(unittest.TestCase):
    def test_actual_order(self):self.assertTrue(check(TEXT))
    def reject(self,text):
        with self.assertRaises((ValueError,StopIteration)):check(text)
    def test_missing_marker(self):self.reject(TEXT.replace("sys.argv.append('shell')","sys.argv.append('not-shell')"))
    def test_marker_after_setup(self):
        line="    if 'shell' not in sys.argv[1:]:sys.argv.append('shell')\n";self.reject(TEXT.replace(line,'').replace('    import django;django.setup()\n','    import django;django.setup()\n'+line))
    def test_missing_before_guard(self):self.reject(TEXT.replace("    if should_skip_initialization() is not True:raise ValueError('OriginalManagementSkipBeforeSetup')\n",''))
    def test_missing_after_guard(self):self.reject(TEXT.replace("    if should_skip_initialization() is not True:raise ValueError('OriginalManagementSkipAfterSetup')\n",''))
    def test_wrong_helper_authority(self):self.reject(TEXT.replace('from dispatcharr.app_initialization import should_skip_initialization','from invented_patch import should_skip_initialization'))
    def test_real_Original_all_stages_shell_semantics(self):
        for stage in ['default-audit','migrate','seed','snapshot','phase']:
            argv=['/notification-tools/db.py',stage];self.assertTrue(original_skip(argv+['shell']))
            if stage!='migrate':self.assertFalse(original_skip(argv))
    def test_literal_marker_required(self):self.assertFalse(original_skip(['helper.py','snapshot','not-shell']));self.assertTrue(original_skip(['helper.py','snapshot','shell']))
    def test_management_migrate_already_skips(self):self.assertTrue(original_skip(['helper.py','migrate']))
    def test_worker_override_assertion_not_assumed(self):self.assertFalse(original_skip(['helper.py','snapshot','shell'],worker=0));self.assertTrue(original_skip(['helper.py','snapshot','shell'],worker=2))
    def test_ready_returns_before_notification_sync(self):
        tree=ast.parse((ROOT/'reference/core/apps.py').read_text());cls=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name=='CoreConfig');ready=next(n for n in cls.body if isinstance(n,ast.FunctionDef)and n.name=='ready');nodes=list(ast.walk(ready));guard=next(n for n in nodes if isinstance(n,ast.If)and ast.unparse(n.test)=='should_skip_initialization()');self.assertTrue(any(isinstance(n,ast.Return)for n in guard.body));sync=next(n for n in nodes if isinstance(n,ast.Call)and ast.unparse(n.func)=='self._sync_developer_notifications');self.assertLess(guard.lineno,sync.lineno)
    def test_all_helper_actions_delegate_common_setup(self):
        tree=ast.parse(TEXT);main=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='main');self.assertTrue(any(isinstance(n,ast.Call)and ast.unparse(n.func)=='setup'for n in ast.walk(main)));phase=ast.parse((ROOT/'tools/phase.py').read_text());main=next(n for n in phase.body if isinstance(n,ast.FunctionDef)and n.name=='main');self.assertTrue(any(isinstance(n,ast.Call)and ast.unparse(n.func)=='setup'for n in ast.walk(main)))
if __name__=='__main__':unittest.main()
