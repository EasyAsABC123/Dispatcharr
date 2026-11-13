"""Pure RESP target/parser controls, explicitly no live Redis/socket fixture."""
import hashlib,threading,unittest
from owned_resp_proxy import Frame,Gate,arguments,GATE_SAFETY_SECONDS
SCRIPT=b"return redis.call('LPUSH',KEYS[1],ARGV[1])"
def frame(args):return b'*'+str(len(args)).encode()+b'\r\n'+b''.join(b'$'+str(len(a)).encode()+b'\r\n'+a+b'\r\n'for a in args)
class Resp(unittest.TestCase):
 def gate(self,kind='get'):
  want={'kind':'get','key_hex':b':1:coresettings:group:system_settings'.hex()}if kind=='get'else{'kind':'lua','sha1':hashlib.sha1(SCRIPT).hexdigest(),'keys_hex':[b'celery'.hex()]}
  gate=Gate({'case':want},None);gate.arm('case');return gate
 def test_exact_get(self):self.assertTrue(self.gate().target([b'GET',b':1:coresettings:group:system_settings']))
 def test_other_key_rejected(self):self.assertFalse(self.gate().target([b'GET',b':1:coresettings:group:network_access']))
 def test_extra_argument_rejected(self):self.assertFalse(self.gate().target([b'GET',b':1:coresettings:group:system_settings',b'other']))
 def test_exact_lua(self):self.assertTrue(self.gate('lua').target([b'EVAL',SCRIPT,b'1',b'celery',b'envelope']))
 def test_exact_sha(self):self.assertTrue(self.gate('lua').target([b'EVALSHA',hashlib.sha1(SCRIPT).hexdigest().encode(),b'1',b'celery',b'envelope']))
 def test_wrong_script(self):self.assertFalse(self.gate('lua').target([b'EVAL',SCRIPT+b' ',b'1',b'celery',b'envelope']))
 def test_wrong_queue(self):self.assertFalse(self.gate('lua').target([b'EVAL',SCRIPT,b'1',b'other',b'envelope']))
 def test_argument_parser(self):self.assertEqual(arguments(frame([b'GET',b'key'])),[b'GET',b'key'])
 def test_trailing_request_rejected(self):
  with self.assertRaises(ValueError):arguments(frame([b'GET',b'key'])+b'extra')
 def test_arm_once(self):
  with self.assertRaises(ValueError):self.gate().arm('case')
 def test_no_script_not_held(self):
  gate=self.gate('lua');gate.response(True,b'-NOSCRIPT no script\r\n');self.assertEqual(gate.seen,0)
 def test_safety_before_measurement(self):self.assertEqual(GATE_SAFETY_SECONDS,45)
 def test_fragmented_frame(self):
  class NoSocket:pass
  f=Frame(NoSocket(),threading.Event());raw=frame([b'GET',b'key']);f.buffer.extend(raw[:-1]);self.assertIsNone(f.end());f.buffer.extend(raw[-1:]);self.assertEqual(f.end(),len(raw))
if __name__=='__main__':unittest.main()
