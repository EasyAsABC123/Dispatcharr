"""Actual controller helper, no Native worker/process/socket execution."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from fixture_endpoints import route_reply_gate,inspect_config
BASE={'broker_url':'redis://:playback-fixture-only@localhost:6379/8','result_url':'redis://:playback-fixture-only@localhost:6379/9','cache_url':'redis://:playback-fixture-only@localhost:6379/0'}
class Endpoints(unittest.TestCase):
 def test_old_one_sided_route_is_rejected_before_worker(self):
  wrong={**BASE,'broker_url':'redis://:playback-fixture-only@127.0.0.1:61603/8'}
  with self.assertRaises(ValueError):inspect_config(wrong)
 def test_broker_gate_routes_both_same_addr_distinct_dbs(self):
  actual=route_reply_gate(BASE,'broker_url');r=inspect_config(actual);self.assertEqual(r['separate_broker_result_databases'],[8,9]);self.assertEqual(r['physical_port'],61603);self.assertEqual(BASE['broker_url'],'redis://:playback-fixture-only@localhost:6379/8')
 def test_result_gate_and_owned_ACL_user_keep_same_addr(self):
  actual=route_reply_gate(BASE,'result_url');actual['result_url']='redis://owned_configured_failure_1:playback-fixture-only@127.0.0.1:61603/9';self.assertTrue(inspect_config(actual)['broker_result_same_address'])
 def test_db_alias_or_external_endpoint_never_supported(self):
  for change in ({'result_url':'redis://:playback-fixture-only@localhost:6379/8'},{'broker_url':'redis://:playback-fixture-only@outside:6379/8'}):
   with self.assertRaises(ValueError):inspect_config({**BASE,**change})
if __name__=='__main__':unittest.main()
