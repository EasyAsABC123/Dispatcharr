"""Injected transport/kernel/clock controls, no network, process or /proc sampling."""
import unittest,json
from unittest import mock
import host_callback as host,redis_session as redis,proc_helper
from metrics import redis_info
class Socket:
 def __init__(self,data):self.data=bytearray(data);self.sent=[];self.closed=False
 def sendall(self,data):self.sent.append(data)
 def recv(self,n):chunk=bytes(self.data[:min(n,7)]);del self.data[:len(chunk)];return chunk
 def settimeout(self,n):pass
 def shutdown(self,n):pass
 def close(self):self.closed=True
 def fileno(self):return -1 if self.closed else 5
def info(run='a'*40,commands=10):return ('run_id:'+run+'\r\ntotal_commands_processed:'+str(commands)+'\r\ntotal_net_input_bytes:100\r\ntotal_net_output_bytes:200\r\n').encode()
def bulk(raw):return b'$'+str(len(raw)).encode()+b'\r\n'+raw+b'\r\n'
class RedisControls(unittest.TestCase):
 def test_blocked_before_connect(self):
  connector=mock.Mock();s=redis.RedisInfoSession('127.0.0.1',6379,b'fixture',connector)
  with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):s.open()
  connector.assert_not_called()
 def test_one_auth_one_connection_bounded_info_wire_and_shutdown(self):
  sock=Socket(b'+OK\r\n'+bulk(info())+bulk(info(commands=11)));connect=mock.Mock(return_value=sock);s=redis.RedisInfoSession('127.0.0.1',6379,b'fixture',connect)
  with mock.patch.object(redis,'EXECUTION_BLOCKED',False):s.open()
  row=s.sample('before-reader');self.assertEqual(row['delta_from_previous']['server_protocol_counters_including_observer']['total_commands_processed'],1);self.assertEqual(row['observer_commands_total'],3);self.assertEqual(row['observer_outbound_RESP_bytes'],sum(map(len,sock.sent)));self.assertEqual(s.password,b'');connect.assert_called_once();s.close();self.assertEqual(sock.fileno(),-1)
 def test_oversized_reply_no_payload_transfer_or_reconnect(self):
  sock=Socket(b'+OK\r\n$65537\r\n');connect=mock.Mock(return_value=sock);s=redis.RedisInfoSession('127.0.0.1',6379,b'fixture',connect)
  with mock.patch.object(redis,'EXECUTION_BLOCKED',False):
   with self.assertRaisesRegex(ValueError,'ReplyCapacity'):s.open()
  self.assertTrue(sock.closed);connect.assert_called_once()
 def test_restart_and_unrelated_malformed_reply_fail_permanently(self):
  for raw in [bulk(info(run='b'*40)),b':12\r\n',b'$3\r\nabcXX']:
   sock=Socket(b'+OK\r\n'+bulk(info())+raw);s=redis.RedisInfoSession('127.0.0.1',6379,b'fixture',lambda *a,**kw:sock)
   with mock.patch.object(redis,'EXECUTION_BLOCKED',False):s.open()
   with self.assertRaises(ValueError):s.sample('before-reader')
   with self.assertRaisesRegex(RuntimeError,'Unavailable'):s.sample('before-reader')
   s.close()
 def test_deadline_rejects_before_INFO_write(self):
  sock=Socket(b'+OK\r\n'+bulk(info()));s=redis.RedisInfoSession('127.0.0.1',6379,b'fixture',lambda *a,**kw:sock)
  with mock.patch.object(redis,'EXECUTION_BLOCKED',False):s.open()
  before=len(sock.sent)
  with self.assertRaises(TimeoutError):s.sample('before-reader',deadline=0)
  self.assertEqual(len(sock.sent),before);s.close()
class HostControls(unittest.TestCase):
 def make(self,broken=None,reused=False,huge=False):
  selected={role:{'Id':format(i+1,'064x'),'StartedAt':'stable','Memory':1,'NanoCpus':1}for i,role in enumerate(sorted(host.ROLES))};mapping={v['Id']:k for k,v in selected.items()};calls=[];counts={}
  def inspect(cid,timeout):return selected[mapping[cid]]
  def run(argv,payload,timeout):
   role=mapping[argv[5]];mode=argv[-1];calls.append((role,mode));counts[(role,mode)]=counts.get((role,mode),0)+1
   if role==broken and mode=='inventory':raise ValueError('captured fatal parse')
   if mode=='inventory':return json.dumps({'processes':[{'pid':7,'identity':[1,1],'start_ticks':11 if reused and counts[(role,mode)]>1 else 10}],'coverage_complete':True,'errors':[]}).encode()
   return json.dumps([{'pid':7,'start_ticks':10,'state':'S','coverage_complete':True,'stable':True,'identified_media':False,'rss_bytes':2,'pss_bytes':1,'denied_fields':[],'executable_basename':'x'*300000 if huge else'python'}]).encode()
  session=mock.Mock();session.sample.return_value={};return host.HostCallbacks(selected,inspect,run,session,{r:'python3'for r in host.ROLES}),calls
 def test_complete_four_roles_fixed_order_and_overhead_record(self):
  h,calls=self.make();r=h.sample('before-reader');self.assertTrue(r['coverage_complete']);self.assertEqual(len(calls),12);self.assertGreaterEqual(r['overhead_seconds'],0)
  with self.assertRaisesRegex(ValueError,'PointOrdering'):h.sample('retention')
 def test_fatal_one_role_preserves_all_other_roles(self):
  h,calls=self.make(broken='backend');r=h.sample('before-reader');self.assertFalse(r['coverage_complete']);self.assertEqual(sum(x['coverage_complete']for x in r['roles'].values()),3);self.assertEqual(r['roles']['backend']['errors'],[{'kind':'ValueError'}])
 def test_reused_PID_same_number_never_complete(self):
  h,_=self.make(reused=True);self.assertFalse(h.sample('before-reader')['coverage_complete'])
 def test_total_rows_bound_across_all_roles(self):
  h,_=self.make(huge=True);r=h.sample('before-reader');self.assertFalse(r['coverage_complete']);self.assertLessEqual(sum(len(json.dumps(v.get('processes',[])))for v in r['roles'].values()),1048576)
 def test_helper_main_blocked_before_parse_or_read(self):
  with mock.patch.object(proc_helper,'inventory',side_effect=AssertionError('no proc')):
   with self.assertRaisesRegex(RuntimeError,'ExecutionBlocked'):proc_helper.main()
if __name__=='__main__':unittest.main()
