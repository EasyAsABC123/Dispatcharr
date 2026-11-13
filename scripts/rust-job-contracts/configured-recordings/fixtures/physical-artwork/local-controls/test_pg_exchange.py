"""Protocol-order correspondence; synthetic actual-format frames, no sockets."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from pg_reply_proxy import Exchanges
QUERY=b'SELECT id FROM epg_programdata WHERE epg_id=$1 ORDER BY start_time LIMIT 33'
def parse(t,name=b's',query=QUERY):t.frontend(b'P',name+b'\0'+query+b'\0\0\0')
def bind_execute(t,name=b's',portal=b''):
 t.frontend(b'B',portal+b'\0'+name+b'\0\0\0\0\0\0\0');t.frontend(b'E',portal+b'\0\0\0\0\0');t.frontend(b'S',b'')
class Exchange(unittest.TestCase):
 def success(self,t):
  t.backend(b'D',b'owned row1');t.backend(b'D',b'owned row2');t.backend(b'C',b'SELECT 2\0');return t.backend(b'Z',b'I')
 def test_parse_describe_sync_is_not_execution(self):
  t=Exchanges('programme-success-Ready',2);parse(t);t.frontend(b'S',b'');t.backend(b'T',b'owned description');self.assertIsNone(t.backend(b'Z',b'I'));t.forwarded_ready();self.assertEqual(t.groups,[])
 def test_bound_execute_matches_exact_two_rows(self):
  t=Exchanges('programme-success-Ready',2);parse(t);t.frontend(b'S',b'');t.forwarded_ready();bind_execute(t);r=self.success(t);self.assertEqual(r['rows'],2);self.assertEqual(r['identity'],QUERY.decode());self.assertEqual(len(r['executions']),1)
 def test_error_then_queued_ping_does_not_overwrite_target(self):
  q=b'SELECT value FROM core_coresettings WHERE key=$1';t=Exchanges('settings-error-Ready');parse(t,query=q);t.frontend(b'S',b'');t.forwarded_ready();bind_execute(t);t.backend(b'E',b'SERROR\0CP0001\0');t.frontend(b'Q',b';\0');r=t.backend(b'Z',b'I');self.assertIsNotNone(r);self.assertEqual(r['identity'],q.decode());t.forwarded_ready();self.assertIsNone(t.backend(b'Z',b'I'))
 def test_prior_unrelated_ready_cannot_satisfy_later_target(self):
  t=Exchanges('programme-success-Ready',2);t.frontend(b'Q',b'SELECT 9\0');parse(t);bind_execute(t);t.backend(b'D',b'other');t.backend(b'C',b'SELECT 1\0');self.assertIsNone(t.backend(b'Z',b'I'));t.forwarded_ready();self.assertIsNotNone(self.success(t))
 def test_wrong_bound_statement_rejected(self):
  t=Exchanges('programme-success-Ready',2)
  with self.assertRaises(ValueError):t.frontend(b'B',b'\0unknown\0')
 def test_ready_without_frontend_sync_cannot_hold(self):
  t=Exchanges('programme-success-Ready',2);parse(t);t.frontend(b'B',b'\0s\0');t.frontend(b'E',b'\0\0\0\0\0');self.assertIsNone(self.success(t))
 def test_two_executions_in_one_group_are_not_single_query_proof(self):
  t=Exchanges('programme-success-Ready',2);parse(t);t.frontend(b'B',b'\0s\0');t.frontend(b'E',b'\0\0\0\0\0');t.frontend(b'E',b'\0\0\0\0\0');t.frontend(b'S',b'');self.assertIsNone(self.success(t))
 def test_commit_binds_recorded_transaction_and_actual_command(self):
  t=Exchanges('Periodic-COMMIT-Ready');t.frontend(b'Q',b'BEGIN\0');t.forwarded_ready();t.frontend(b'Q',b'INSERT INTO django_celery_beat_periodictask (id) VALUES (1)\0');t.forwarded_ready();t.frontend(b'Q',b'COMMIT\0');t.backend(b'C',b'ROLLBACK\0');self.assertIsNone(t.backend(b'Z',b'I'))
if __name__=='__main__':unittest.main()

class ErrorFields(unittest.TestCase):
 def test_actual_error_payload_keeps_severity_code_message_context(self):
  from pg_reply_proxy import error_fields
  self.assertEqual(error_fields(b'SERROR\0CP0001\0Mowned\0Wcontext\0\0'),{'S':'ERROR','C':'P0001','M':'owned','W':'context'})
 def test_missing_terminator_is_not_normal(self):
  from pg_reply_proxy import error_fields
  with self.assertRaises(ValueError):error_fields(b'CP0001\0')
 def test_duplicate_code_is_not_silently_overwritten(self):
  from pg_reply_proxy import error_fields
  with self.assertRaises(ValueError):error_fields(b'CP0001\0C00000\0\0')
 def test_trailing_unknown_bytes_do_not_disappear(self):
  from pg_reply_proxy import error_fields
  with self.assertRaises(ValueError):error_fields(b'CP0001\0\0junk')
