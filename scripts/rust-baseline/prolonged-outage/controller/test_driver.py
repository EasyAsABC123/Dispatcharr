"""No HTTP/Docker fixture; verify source entry/URI/physical-close call ordering."""
import unittest,threading
from unittest import mock
import source_controller
from http_adapter import Body,HTTPAdapter
class Driver(unittest.TestCase):
 def test_source_entry_blocked_before_any_process_or_output(self):
  with mock.patch('subprocess.Popen',side_effect=AssertionError('no process')):
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):source_controller.main()
 def test_only_owned_loopback_network_can_be_constructed(self):
  with self.assertRaises(ValueError):Body('http://example.com','/','secret')
  with self.assertRaises(ValueError):HTTPAdapter('http://127.0.0.1','http://example.com','/','secret',{},None)
 def test_body_shutdown_precedes_join_and_close_no_false_drain(self):
  b=object.__new__(Body);b.stopping=False;b.sock=mock.Mock();b.response=mock.Mock();b.thread=mock.Mock();b.conn=mock.Mock();b.thread.is_alive.return_value=False;b.sock.fileno.return_value=-1;b.snapshot=lambda:{'EOF':True};events=[]
  b.sock.shutdown.side_effect=lambda how:events.append('shutdown');b.thread.join.side_effect=lambda timeout:events.append('join');b.response.close.side_effect=lambda:events.append('response-close');b.conn.close.side_effect=lambda:events.append('connection-close')
  self.assertTrue(b.close()['physical_socket_closed']);self.assertEqual(events,['shutdown','join','response-close','connection-close'])
 def test_fresh_decode_cannot_substitute_original_warm_AV_qualification(self):
  good={'prefix':'original-prefix.ts','decoder_exit':0,'frames':25};self.assertTrue(source_controller.original_warm_decode_qualified([good]))
  for value in [dict(good,prefix='fresh-prefix.ts'),dict(good,frames=24),dict(good,decoder_exit=1),{}]:self.assertFalse(source_controller.original_warm_decode_qualified([value]))
 def test_undrained_reader_never_reports_physical_close(self):
  b=object.__new__(Body);b.sock=mock.Mock();b.thread=mock.Mock();b.thread.is_alive.return_value=True
  with self.assertRaisesRegex(RuntimeError,'Undrained'):b.close()
if __name__=='__main__':unittest.main()
