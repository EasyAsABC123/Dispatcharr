"""Finite corruptions of selected public evidence; no application dispatch."""
import copy,unittest
import verify as V
class Public(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.data=V.observations();cls.life=V.load('lifecycle.json')
 def reject(self,change):
  d=copy.deepcopy(self.data);l=copy.deepcopy(self.life);change(d,l)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(d,l)
 def test_actual_trace(self):self.assertEqual(V.validate(self.data,self.life)['public_response_SQL_and_selected_trace_checks'],32)
 def test_missing_request(self):self.reject(lambda d,l:d['requests'].pop())
 def test_wrong_role(self):self.reject(lambda d,l:d['requests'][0].update(role='other'))
 def test_wrong_method(self):self.reject(lambda d,l:d['requests'][0].update(method='DELETE'))
 def test_wrong_status(self):self.reject(lambda d,l:d['requests'][0].update(status=599))
 def test_wrong_body(self):self.reject(lambda d,l:d['requests'][0].update(response_body_hex=''))
 def test_missing_content_type(self):self.reject(lambda d,l:d['requests'][0].update(headers={}))
 def test_missing_SQL_table(self):self.reject(lambda d,l:d['requests'][0]['before']['public_manifest']['tables'].pop('accounts_user'))
 def test_missing_SQL_sequence(self):self.reject(lambda d,l:d['requests'][0]['before']['public_manifest']['sequences'].pop('accounts_user_id_seq'))
 def test_missing_cache_evidence(self):self.reject(lambda d,l:d['requests'][0]['before']['raw_Redis'].clear())
 def test_socket_not_settled(self):self.reject(lambda d,l:d['requests'][0]['socket_settlement'].update(timer_joined=False))
 def test_bad_clock(self):self.reject(lambda d,l:d['requests'][0].update(began_utc='1970-01-01T00:00:00+00:00'))
 def test_raw_recorder_false_cannot_upgrade(self):self.reject(lambda d,l:d.update(passed=True))
 def test_host_failure_cannot_upgrade(self):self.reject(lambda d,l:l.update(actual_controller_exit=0,actual_controller_passed=True))
 def test_default_after_cannot_upgrade(self):self.reject(lambda d,l:l.update(default_after_audit='measured equal'))
 def test_timeout_collector_cannot_upgrade(self):self.reject(lambda d,l:[x.update(physically_settled=True)for x in l['host_children']])
 def test_Main_cannot_upgrade(self):self.reject(lambda d,l:l.update(Main_activated=True))
if __name__=='__main__':unittest.main()
