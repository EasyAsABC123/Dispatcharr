"""UNEXECUTED proposed offline controls; no Docker/network/cgroup on import."""
import importlib.util,json,tempfile,unittest,threading,time
from pathlib import Path
from resource_window import Journal,SERVICES,Window
HERE=Path(__file__).resolve().parent

def corrected():
 spec=importlib.util.spec_from_file_location('coverage_reference',HERE/'denied_observer.py')
 result=importlib.util.module_from_spec(spec);spec.loader.exec_module(result);return result

def inventory(pids):
 return dict(process_ids=pids,identities=[[1,1]],raced_processes=0,permission_denied_processes=0,permission_denied_descriptors=0,tcp_states={'06':3})

def part(pids):
 return dict(process_ids=pids,processes=len(pids),threads=len(pids),fds=1,socket_descriptors=1,socket_inodes=['socket:[7]'],raced_processes=0,raced_descriptors=0,permission_denied_processes=0,permission_denied_descriptors=0)

class Controls(unittest.TestCase):
 def test_source_only_unaccepted(self):
  p=json.loads((HERE/'policy.json').read_text());self.assertFalse(p['resource_acceptance']);self.assertEqual(set(p['caps']),SERVICES);self.assertEqual(p['provider_max_streams'],1);self.assertEqual(p['imports_at_seconds'],[5,60]);self.assertEqual(p['resource_draft']['repetitions'],3)
 def test_no_threshold_rescale(self):
  p=json.loads((HERE/'policy.json').read_text())['resource_draft'];self.assertEqual(p['budgets_inherited_without_tuning']['whole_stack_cpu_percent_one_core'],500);self.assertEqual(p['minimum_complete_stable_idle_censuses'],5);self.assertEqual(p['retention_settle_seconds'],70)
 def test_journal_row_overflow_preserves_good_prefix(self):
  with tempfile.TemporaryDirectory()as directory:
   path=Path(directory)/'rows';journal=Journal(path,1,256,256);journal.append({'one':True});before=path.read_bytes()
   with self.assertRaises(ValueError):journal.append({'two':True})
   self.assertEqual(path.read_bytes(),before)
 def test_journal_total_bytes_precedes_write(self):
  with tempfile.TemporaryDirectory()as directory:
   path=Path(directory)/'rows';journal=Journal(path,5,256,25);journal.append({'x':1});before=path.read_bytes()
   with self.assertRaises(ValueError):journal.append({'too-long':'fifteen characters'})
   self.assertEqual(path.read_bytes(),before)
 def test_duplicate_services_reject(self):
  with self.assertRaises(ValueError):Window(None,{s:{'id':'same'}for s in SERVICES},None,{},Path('/unused'),None)
 def test_denial_incomplete_not_zero(self):
  row=part([10]);row['permission_denied_descriptors']=1;value=corrected().merge_census(inventory([10]),[row],inventory([10]));self.assertFalse(value['complete']);self.assertEqual(value['permission_denied_descriptors'],1)
 def test_raced_inventory_incomplete(self):self.assertFalse(corrected().merge_census(inventory([10]),[part([10])],inventory([11]))['complete'])
 def test_tcp_once_and_union_inodes(self):
  value=corrected().merge_census(inventory([10,20]),[part([10]),part([20])],inventory([10,20]));self.assertTrue(value['complete']);self.assertEqual(value['tcp_states']['06'],3);self.assertEqual(value['unique_socket_inodes'],1)
 def test_outer_command_permission_error_is_fatal(self):
  def denied(*args,**kwargs):raise PermissionError('control')
  with tempfile.TemporaryDirectory()as directory:
   window=object.__new__(Window);window.policy={'resource_draft':{'census_service_deadline_seconds':5,'maximum_json_row_bytes':1048576}};window.command=denied;window.lock=threading.Lock();window.errors=[];window.output=Path(directory);window.started=time.monotonic()
   with self.assertRaises(PermissionError):window._service_census({'id':'owned'})
   self.assertEqual(window.errors[0]['error_type'],'PermissionError')
   self.assertTrue((Path(directory)/'census-failed-owned.json').exists())
   with self.assertRaises(RuntimeError):window.check()
if __name__=='__main__':unittest.main()
