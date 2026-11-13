"""Pure ownership/startup/capture-policy controls; no application or Docker action."""
import ast,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
from definition import ROOT
from source_fixture import parsed_metadata
class Empty:pass
class Request:
 def __init__(self):self._data=Empty;self._files=Empty;self._full_data=Empty
class Lifecycle(unittest.TestCase):
 def test_untouched_empty_request_no_parser_access(self):
  request=Request();before=dict(vars(request));value=parsed_metadata(request);self.assertFalse(value['forced_parse']);self.assertEqual(vars(request),before);self.assertEqual(value['cached']['_data']['class_value'],__name__+'.Empty')
 def test_observer_file_metadata_only(self):
  class File:
   name='owned-small.txt';size=20;content_type='text/plain';charset=None
   def read(self,*a):raise AssertionError('must never read')
  request=Request();request._files={'unknown_upload':File()};value=parsed_metadata(request);self.assertFalse(value['cached']['_files']['fields'][0]['values'][0]['file_content_read'])
 def test_capture_exception_vs_framework_exception_separate(self):
  text=(ROOT/'controller/source_fixture.py').read_text();self.assertIn("row['capture_observer_exception']=exception_record",text);self.assertIn("row['transport']='http_response';row['status']=response.status_code;flush()",text)
 def test_request_invocation_count_not_only_complete_captures(self):
  text=(ROOT/'controller/source_fixture.py').read_text();self.assertIn("report['actual_request_count']+=1;flush()",text);self.assertIn("report['completed_capture_count']+=1",text)
 def test_real_startup_shell_scope(self):
  text=(ROOT/'controller/source_fixture.py').read_text();self.assertIn("'shell'in sys.argv and should_skip_initialization()is True",text);self.assertIn('python manage.py migrate --noinput',(ROOT/'controller/start.sh').read_text());self.assertNotIn('django.setup =',text)
 def test_once_output_and_reviewed_hash_precede_mutation(self):
  text=(ROOT/'controller/run.py').read_text();self.assertLess(text.index("'ReviewedSealHashBinding'"),text.index('out.mkdir('));self.assertIn('exist_ok=False',text)
 def test_cleanup_clock_precedes_post_source_work(self):
  text=(ROOT/'controller/run.py').read_text();start=text.index("command(exec_args(db,'exercise'),timeout=240)");self.assertLess(text.index('begin_cleanup()',start),text.index('qualified=compare(actual,PLAN)',start));self.assertIn("if not report['cleanup_within90']:report['passed']=False;status=1",text)
 def test_no_recursive_owned_stop_collector(self):
  text=(ROOT/'controller/bounded_stop.py').read_text();self.assertIn('lambda:None',text);self.assertIn("receipt['acknowledged']=False",text)
 def test_current_host_PID_identity_observed_before_claim(self):
  text=(ROOT/'controller/run.py').read_text();self.assertIn("'DockerTopPIDHostStartNotObserved'",text);self.assertIn("os.readlink('/proc/self/ns/pid')",text)
 def test_serializer9_literal_historical_state_shape(self):
  value=json.loads((ROOT/'inputs/serializer9-proof.json').read_text());self.assertEqual(value['created_inspect']['State']['Pid'],0);self.assertEqual(value['final_before_remove']['Pid'],0);self.assertNotIn('State',value['final_before_remove']);self.assertIn("proof['final_before_remove']['Pid']",(ROOT/'controller/run.py').read_text())
 def test_original62_helper_is_exact_data_not_imported(self):
  p=ROOT/'inputs/provenance/backup_fixture.py';self.assertEqual(hashlib.sha256(p.read_bytes()).hexdigest(),'21afad77d557a15ef365c12b0757df8838f69708ef03cfee7b324514b5154d58');self.assertNotIn('import backup_fixture',(ROOT/'controller/source_fixture.py').read_text())
if __name__=='__main__':unittest.main(verbosity=2)
