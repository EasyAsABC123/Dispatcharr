"""Real Window.close/Observer.finish dispatch; virtual cgroups, no Docker/probe."""
import importlib.util,json,sys,tempfile,types,unittest
from pathlib import Path
from unittest import mock
from supervision import Capacity
from resource_window import Window,SERVICES
from final_counter_authority import FinalCounterCommand
from finalization import finish
HERE=Path(__file__).resolve().parent
POLICY=json.loads((HERE/'policy.json').read_text())
class FinalCounterControls(unittest.TestCase):
 def test_real_close_finishes_four_counters_via_only_cleanup_authority(self):
  cgroup=types.SimpleNamespace(counters=lambda text:{'virtual_counter':1},delta=lambda a,b:{'cpu_percent_mean':0})
  with mock.patch.dict(sys.modules,{'cgroup':cgroup,'docker_stats':types.SimpleNamespace()}):
   spec=importlib.util.spec_from_file_location('observer7_control',HERE/'observer.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  selected={name:{'id':str(i)*64,'limits':{'memory_bytes':5,'nano_cpus':6}}for i,name in enumerate(sorted(SERVICES),1)}
  normal=mock.Mock(side_effect=Capacity('NormalClosed'));registry=mock.Mock();registry.run.return_value=b'{}';final=FinalCounterCommand(registry,selected,module.CGROUP_SCRIPT)
  observer=object.__new__(module.Observer);observer.selected=selected;observer.command=normal
  # Keep the real counters/read_cgroup/finish call stack, with pure parser data.
  cgroup.counters=lambda text:{'memory_max':5,'cpu_quota':6,'cpu_period':1000000000}
  with tempfile.TemporaryDirectory()as d:
   window=Window(observer,selected,normal,POLICY,Path(d)/'window',None,final_counter_command=final);window.before={'services':{s:{}for s in SERVICES}};result=window.close()
  self.assertIn('whole_resource_window',result);self.assertNotIn('counter_finish_error',result);normal.assert_not_called();self.assertEqual(registry.run.call_count,4)
  self.assertTrue(all(call.kwargs=={'timeout':5,'cleanup':True}for call in registry.run.call_args_list))
 def test_cleanup_counter_authority_refuses_arbitrary_command_and_ID(self):
  registry=mock.Mock();selected={'source':{'id':'a'*64}};op=FinalCounterCommand(registry,selected,'script')
  valid=['docker','exec','a'*64,'/dispatcharrpy/bin/python','-c','script']
  for bad in [['docker','ps'],valid[:2]+['b'*64]+valid[3:],valid[:-1]+['arbitrary code']]:
   with self.assertRaisesRegex(ValueError,'Scope'):op(bad)
  with self.assertRaisesRegex(ValueError,'Scope'):op(valid,timeout=30)
  registry.run.assert_not_called()
 def test_unperformed_database_audit_is_null_not_changed(self):
  with tempfile.TemporaryDirectory()as d:
   import threading
   registry=mock.Mock();registry.active={};registry.lock=threading.Lock();registry.drain.return_value={'drained':True}
   result=finish({'passed':True},d,None,registry,lambda:None,lambda:None,lambda:None,lambda k:'',POLICY['resource_draft']['supervision'])
   self.assertIsNone(result['default_database_unchanged']);self.assertEqual(result['terminal_actions']['default_database_audit']['message'],'DefaultDatabaseAuditNotRun');self.assertFalse(result['passed'])
if __name__=='__main__':unittest.main()
