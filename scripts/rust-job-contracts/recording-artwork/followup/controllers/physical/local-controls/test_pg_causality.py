"""Pure exact-byte PG exchange/privacy controls; never opens a socket."""
import base64,hashlib,json,struct,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import pg_reply_proxy as pg
class Causal(unittest.TestCase):
 def prepared(self,mode,query=None):
  t=pg.Exchanges(mode);q=(query or pg.POLICY[{'recording-read-Ready':'recording_read','movie-read-Ready':'movie_read','update-Ready':'update','update-error-Ready':'update'}[mode]]).encode()
  t.frontend(b'P',b's\0'+q+b'\0\0\0');t.frontend(b'S',b'');self.assertIsNone(t.backend(b'Z',b'I'));t.forwarded_ready();return t
 def execute(self,t):t.frontend(b'B',b'p\0s\0\0\0\0\0\0\0');t.frontend(b'E',b'p\0\0\0\0\0');t.frontend(b'S',b'')
 def error(self,code='P0001',message=None):return b'SERROR\0C'+code.encode()+b'\0M'+(message or pg.POLICY['known_owned_trigger_error_message']).encode()+b'\0\0'
 def test_recording_preparse_ready_not_execution_hold(self):self.prepared('recording-read-Ready')
 def test_recording_data_and_matching_command_before_ready(self):
  t=self.prepared('recording-read-Ready');self.execute(t);t.backend(b'D',b'\0\0');t.backend(b'C',b'SELECT 1\0');v=t.backend(b'Z',b'I');self.assertIsNotNone(v);self.assertEqual(v['identity'],pg.POLICY['recording_read']);self.assertEqual(v['rows'],1)
 def test_movie_read_same_subject_identity(self):
  t=self.prepared('movie-read-Ready');self.execute(t);t.backend(b'D',b'\0\0');t.backend(b'C',b'SELECT 1\0');self.assertIsNotNone(t.backend(b'Z',b'I'))
 def test_unrelated_query_is_not_target(self):
  t=self.prepared('recording-read-Ready','SELECT 1');self.execute(t);t.backend(b'D',b'\0\0');t.backend(b'C',b'SELECT 1\0');self.assertIsNone(t.backend(b'Z',b'I'))
 def test_missing_datarow_not_enough(self):
  t=self.prepared('recording-read-Ready');self.execute(t);t.backend(b'C',b'SELECT 1\0');self.assertIsNone(t.backend(b'Z',b'I'))
 def test_wrong_command_count_not_enough(self):
  t=self.prepared('recording-read-Ready');self.execute(t);t.backend(b'D',b'\0\0');t.backend(b'C',b'SELECT 2\0');self.assertIsNone(t.backend(b'Z',b'I'))
 def test_multiple_rows_not_one_row(self):
  t=self.prepared('recording-read-Ready');self.execute(t);t.backend(b'D',b'\0\0');t.backend(b'D',b'\0\0');t.backend(b'C',b'SELECT 1\0');self.assertIsNone(t.backend(b'Z',b'I'))
 def test_update_confirmation_before_ready(self):
  t=self.prepared('update-Ready');self.execute(t);t.backend(b'C',b'UPDATE 1\0');self.assertIsNotNone(t.backend(b'Z',b'I'))
 def test_zero_updated_not_target_success(self):
  t=self.prepared('update-Ready');self.execute(t);t.backend(b'C',b'UPDATE 0\0');self.assertIsNone(t.backend(b'Z',b'I'))
 def test_actual_owned_error_code_message_and_ready(self):
  t=self.prepared('update-error-Ready');self.execute(t);t.backend(b'E',self.error());v=t.backend(b'Z',b'I');self.assertIsNotNone(v);self.assertEqual(v['error_fields']['C'],'P0001')
 def test_other_error_code_not_known_fault(self):
  t=self.prepared('update-error-Ready');self.execute(t);t.backend(b'E',self.error('42501'));self.assertIsNone(t.backend(b'Z',b'I'))
 def test_other_error_message_not_known_fault(self):
  t=self.prepared('update-error-Ready');self.execute(t);t.backend(b'E',self.error(message='other'));self.assertIsNone(t.backend(b'Z',b'I'))
 def test_no_execute_before_error_ready(self):
  t=self.prepared('update-error-Ready');t.frontend(b'P',b's2\0'+pg.POLICY['update'].encode()+b'\0\0\0');t.frontend(b'S',b'');t.backend(b'E',self.error());self.assertIsNone(t.backend(b'Z',b'I'))
 def test_unknown_portal_refuses(self):
  with self.assertRaisesRegex(ValueError,'UnknownExecutedPortal'):pg.Exchanges('update-Ready').frontend(b'E',b'unknown\0\0\0\0\0')
 def test_frontend_password_auth_never_retained(self):
  p=pg.projected_capture('frontend',b'p',b'fixture SCRAM response');self.assertIsNone(p['payload_base64']);self.assertTrue(p['authentication_payload_projected']);self.assertEqual(p['payload_bytes'],22)
 def test_backend_auth_never_retained(self):self.assertIsNone(pg.projected_capture('backend',b'R',b'nonce-and-challenge')['payload_base64'])
 def test_cancel_secret_projected_backendPID_retained(self):
  data=struct.pack('!II',12345,0xdeadbeef);v=pg.projected_capture('backend',b'K',data);self.assertEqual(v['backend_pid'],12345);self.assertTrue(v['cancel_secret_projected']);self.assertIsNone(v['payload_base64']);self.assertEqual(v['payload_sha256'],hashlib.sha256(data).hexdigest())
 def test_bad_backend_keylength_refuses(self):
  with self.assertRaisesRegex(ValueError,'Length8'):pg.projected_capture('backend',b'K',b'short')
 def test_application_bind_bytes_literal(self):
  data=b'actual bound JSON and ID';v=pg.projected_capture('frontend',b'B',data);self.assertEqual(base64.b64decode(v['payload_base64']),data)
 def test_policy_is_exact_frozen_normal_store_source(self):
  file=(ROOT.parents[2]/'historical/15-recording_local_artwork.rs');self.assertEqual(hashlib.sha256(file.read_bytes()).hexdigest(),pg.POLICY['Store_source_sha256']);self.assertIn(pg.POLICY['update'],file.read_text())
if __name__=='__main__':unittest.main()
