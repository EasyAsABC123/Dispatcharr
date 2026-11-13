import io,json,sys,unittest,hashlib
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
import redis_reply_proxy as r
P=json.loads((ROOT/'tools/redis-policy.json').read_text());ID='4ca5ec3e-1e73-497d-ba5a-6ee78f91971b';CHANNEL=P['known_channel_key'].encode()
class Target(unittest.TestCase):
 def cmd(self,kind='result',cached=False):
  lua=P['result_write_lua'if kind=='result'else'CHANNELS_PUBLISH'].encode();sha=hashlib.sha1(lua).hexdigest().encode();key=b'celery-tasks:celery-task-meta-'+ID.encode()if kind=='result'else CHANNEL
  return [b'EVALSHA'if cached else b'EVAL',sha if cached else lua,b'1',key,b'owned-payload']
 def test_actual_result_eval(self):self.assertTrue(r.target('result',ID,P,CHANNEL,self.cmd(),1,9))
 def test_actual_result_evalsha(self):self.assertTrue(r.target('result',ID,P,CHANNEL,self.cmd(cached=True),1,9))
 def test_actual_channels_eval(self):self.assertTrue(r.target('events',ID,P,CHANNEL,self.cmd('event'),0,0))
 def test_actual_channels_evalsha(self):self.assertTrue(r.target('events',ID,P,CHANNEL,self.cmd('event',True),0,0))
 def test_wrong_database_never_targets_result(self):self.assertFalse(r.target('result',ID,P,CHANNEL,self.cmd(),1,8))
 def test_wrong_database_never_targets_event(self):self.assertFalse(r.target('events',ID,P,CHANNEL,self.cmd('event'),0,9))
 def test_other_result_task_key(self):self.assertFalse(r.target('result','another',P,CHANNEL,self.cmd(),1,9))
 def test_wrong_script_not_target(self):
  cmd=self.cmd();cmd[1]=b'return 1';self.assertFalse(r.target('result',ID,P,CHANNEL,cmd,1,9))
 def test_oldsuccess_noop_not_new_write_reply(self):self.assertFalse(r.target('result',ID,P,CHANNEL,self.cmd(),0,9))
 def test_noscript_error_not_success(self):self.assertFalse(r.target('result',ID,P,CHANNEL,self.cmd(cached=True),b'NOSCRIPT missing',9))
 def test_bool_not_integer_success(self):self.assertFalse(r.target('result',ID,P,CHANNEL,self.cmd(),True,9))
 def test_channels_drop_not_successful_delivery(self):self.assertFalse(r.target('events',ID,P,CHANNEL,self.cmd('event'),1,0))
 def test_multi_key_query_refused(self):
  cmd=self.cmd();cmd[2]=b'2';self.assertFalse(r.target('result',ID,P,CHANNEL,cmd,1,9))
 def test_unrelated_bulk_key_not_matching_first_key(self):
  cmd=self.cmd();cmd[3]=b'wrong';cmd.append(b'celery-tasks:celery-task-meta-'+ID.encode());self.assertFalse(r.target('result',ID,P,CHANNEL,cmd,1,9))
 def test_wire_parser_preserves_real_integer_and_bulk(self):
  raw=b'*2\r\n:1\r\n$3\r\nabc\r\n';wire,value=r.frame(io.BytesIO(raw));self.assertEqual(raw,wire);self.assertEqual(value,[1,b'abc'])
 def test_truncated_actual_bulk_refused(self):
  with self.assertRaises(ValueError):r.frame(io.BytesIO(b'$3\r\nab'))
if __name__=='__main__':unittest.main()
