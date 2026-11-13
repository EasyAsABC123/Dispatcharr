"""Tooling failure ordering and environment propagation; all subprocesses mocked."""
import contextlib,importlib.util,json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch,MagicMock
import tooling_guard
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('tooling_dispatch54',HERE/'scheduler.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Controls(unittest.TestCase):
    def test_failed_tooling_never_locks_claims_creates_output_or_spawns(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp)
            with patch.object(m,'seal_check',return_value='seal'),patch.object(m,'input_check',side_effect=tooling_guard.ToolingRefusal('unreachable')),patch.object(m,'lock')as lock,patch.object(m.subprocess,'Popen')as child,patch.object(m,'LOGS',root/'logs'),patch.object(m,'STATE',root/'state.json'):
                with self.assertRaises(tooling_guard.ToolingRefusal):m.execute('baseline-all',0,'none','none')
                lock.assert_not_called();child.assert_not_called();self.assertFalse((root/'logs').exists());self.assertFalse((root/'state.json').exists())
    def test_same_controlled_environment_passed_to_child(self):
        with tempfile.TemporaryDirectory()as tmp:
            root=Path(tmp);env={'PATH':'controlled','DOCKER_HOST':'owned','DOCKER_CONFIG':'bound'};s=m.fresh_state(m.PLAN);s['started']=True;s['nodes'][m.GUIDE]['status']='passed-clean';process=MagicMock();process.pid=12345;process.wait.return_value=0
            @contextlib.contextmanager
            def lock():yield
            with patch.object(m,'seal_check',return_value='seal'),patch.object(m,'input_check',return_value=env),patch.object(m,'load_clearance',return_value={}),patch.object(m,'lock',lock),patch.object(m,'state',return_value=s),patch.object(m,'HERE',root),patch.object(m,'LOGS',root/'logs'),patch.object(m,'STATE',root/'state.json'),patch.object(m.subprocess,'Popen',return_value=process)as popen:
                result=m.execute('baseline-all',0,'none','digest');self.assertTrue(result['process_settled']);self.assertIs(popen.call_args.kwargs['env'],env)
    def test_every_host_Docker_idle_query_uses_controlled_environment(self):
        env={'PATH':'controlled','DOCKER_HOST':'owned'}
        with patch.object(m.subprocess,'check_output',return_value='')as run,patch.object(Path,'iterdir',return_value=iter([])):
            self.assertTrue(m.heavy_idle(env)['all_Docker_empty']);self.assertIs(run.call_args.kwargs['env'],env)
if __name__=='__main__':unittest.main(verbosity=2)
