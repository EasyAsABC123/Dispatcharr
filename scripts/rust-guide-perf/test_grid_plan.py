"""Separate GRID policy identity, unchanged budgets and complete six-pair gates."""
import copy,json,tempfile,unittest
from unittest.mock import MagicMock,patch
import reader
from test_scaffold import Response
from pathlib import Path
import plan,grid_plan,grid_acceptance,grid_qualification
class GridPolicy(unittest.TestCase):
 def manifest(self):return {'passed':True,'image_id':grid_plan.IMAGE,'source_tree_sha256':grid_plan.TREE,'source_archive_sha256':grid_plan.ARCHIVE}
 def qualification(self):return {w['name']:[dict(backend='source',period_seconds=p,errors=0,correct_responses=True,pacing_fraction=1,cpu_percent_mean=100,memory_fraction=.5,throttled_period_fraction=0)for p in w['period_candidates_seconds']]for w in grid_plan.POLICY['workloads']}
 def test_new_policy_preserves_old_gates_limits_and_scope(self):
  before=copy.deepcopy(plan.POLICY)
  for field in ['budgets','service_limits','orders','warmup_seconds','measurement_seconds','qualification']:self.assertEqual(grid_plan.POLICY[field],plan.POLICY[field])
  self.assertEqual([w['users']for w in grid_plan.POLICY['workloads']],[1,10]);self.assertTrue(all(w['kind']=='json'and w['method']=='GET'and w['period_candidates_seconds']==[10,5,2,1]for w in grid_plan.POLICY['workloads']))
  self.assertEqual(grid_plan.POLICY['fixture']['expected_grid_rows'],2250);self.assertEqual(plan.POLICY,before)
 def test_exact_image_binding_complete_qualification_and_no_candidate_tuning(self):
  q=self.qualification()
  with tempfile.TemporaryDirectory()as root:
   path=Path(root)/'plan.json';record=grid_plan.freeze(path,q,self.manifest());grid_plan.validate_frozen(record,q,self.manifest())
   wrong=self.manifest();wrong['image_id']='sha256:replacement'
   with self.assertRaises(ValueError):grid_plan.freeze(path,q,wrong)
   with self.assertRaises(ValueError):grid_plan.validate_frozen(record,q,wrong)
   q[next(iter(q))][0]['backend']='candidate'
   with self.assertRaises(ValueError):grid_plan.freeze(path,q,self.manifest())
 def test_full_six_pair_grid_cannot_reuse_old_fifteen_pair_scope(self):
  phase=dict(cpu_percent_mean=10,peak_working_memory_bytes=1000,ttfb_p95_ms=10,completion_p95_ms=20,throughput_per_second=1,pacing_fraction=1,samples=60,errors=0,sample_errors=0,late_completed=0,oom_kills=0,counter_resets=0,correct_responses=True,cache_state_verified=True,fixture_unchanged=True,limits_verified=True)
  rows=[dict(workload=w['name'],trial=t,order=grid_plan.POLICY['orders'][t-1],source=phase,candidate=phase)for w in grid_plan.POLICY['workloads']for t in [1,2,3]]
  self.assertTrue(grid_acceptance.matrix(rows)['passed'])
  with self.assertRaises(ValueError):grid_acceptance.matrix(rows[:-1])
  rows[0]['workload']=plan.POLICY['workloads'][0]['name']
  with self.assertRaises(ValueError):grid_acceptance.matrix(rows)
class GridFailureDiagnostics(unittest.TestCase):
 def failure(self,data):
  connection=MagicMock();response=Response(data,'application/json');response.status=503;connection.getresponse.return_value=response
  with patch('reader.http.client.HTTPConnection',return_value=connection):
   with self.assertRaises(reader.ResponseFailure)as caught:reader.request('http://127.0.0.1:1',{'kind':'json','method':'GET','path':'/api/epg/grid/'},'private-fixture-token')
  return caught.exception,response
 def test_status_json_class_without_body_values_or_token(self):
  error,_=self.failure(b'{"detail":"private-value"}')
  self.assertEqual((error.status,error.mime,error.body_class,error.body_keys),(503,'application/json','dict',['detail']))
  self.assertNotIn('private-value',str(vars(error)));self.assertNotIn('private-fixture-token',str(vars(error)))
 def test_error_payload_read_is_bounded(self):
  error,response=self.failure(b'x'*10000)
  self.assertEqual(error.body_class,'over4096');self.assertEqual(response.body.tell(),4097)
if __name__=='__main__':unittest.main()
