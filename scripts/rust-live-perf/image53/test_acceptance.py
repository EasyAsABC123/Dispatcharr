import copy,json,unittest
from pathlib import Path
from test_resources import example
from resources import arithmetic
from acceptance import qualify,pair
PLAN=json.loads(Path(__file__).with_name('protocol.json').read_text())
def phase(backend='source'):
 raw=example();raw['window']={'begin':100,'end':160}
 if backend=='candidate':
  raw['selected']['backend']['id']='8'*64
  for row in raw['counters']['after'].values():row['cpu']['usage_usec']=1500000
  for sample in raw['samples']:
   item=sample['value']['containers'][0];item.update(id='8'*64,inactive_file_bytes=1500,memory_bytes=500);sample['value']['memory_bytes']=2500
 return {'backend':backend,'count':1,'requests':1,'continuous_connections':True,'authenticated_identity':{'clients':1,'all_owned_user_id':True},'measure_seconds':60,'warmup_seconds':15,'cleanup_upstream_active':0,'resources':{'raw':raw,'metrics':arithmetic(raw,100,160)},'viewers':[{'status':200,'requests':1,'errors':[],'unexpected_eof':False,'post_window_barrier':True,'prefix_bytes':2097152,'decoder_frames':75,'measured_bytes':15000000,'first_byte_ms':100}]}
class Acceptance(unittest.TestCase):
 def test_qualified_source(self):self.assertTrue(qualify(phase(),PLAN)['passed'])
 def test_passing_pair(self):self.assertTrue(pair(phase(),phase('candidate'),PLAN)['passed'])
 def test_exact_budget_unmodified(self):
  p=copy.deepcopy(PLAN);pair(phase(),phase('candidate'),PLAN);self.assertEqual(p,PLAN)
 def test_no_Bearer_identity(self):
  p=phase();p['authenticated_identity']['all_owned_user_id']=False;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_reconnect(self):
  p=phase();p['viewers'][0]['requests']=2;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_no_barrier(self):
  p=phase();p['viewers'][0]['post_window_barrier']=False;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_slow_delivery(self):
  p=phase();p['viewers'][0]['measured_bytes']=1;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_repeated_excess_delivery(self):
  p=phase();p['viewers'][0]['measured_bytes']=30000000;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_cpu_summary_tamper(self):
  p=phase();p['resources']['metrics']['cpu_percent_mean']=0;self.assertFalse(qualify(p,PLAN)['passed'])
 def test_memory_budget(self):
  c=phase('candidate');raw=c['resources']['raw']
  for sample in raw['samples']:
   sample['value']['containers'][0].update(inactive_file_bytes=0,memory_bytes=2000);sample['value']['memory_bytes']=4000
  c['resources']['metrics']=arithmetic(raw,100,160);self.assertIn('memory_budget',pair(phase(),c,PLAN)['failed'])
 def test_common_infrastructure_mismatch(self):
  c=phase('candidate');c['resources']['raw']['selected']['infrastructure']['limits']['nano_cpus']=1000000000
  # Equal values are accepted; mutate an explicit nonmetric identity field.
  c['resources']['raw']['selected']['infrastructure']['fixture']='wrong'
  self.assertIn('common_infrastructure_identity',pair(phase(),c,PLAN)['failed'])
 def test_latency_budget(self):
  c=phase('candidate');c['viewers'][0]['first_byte_ms']=1000;self.assertIn('first_byte_p95_budget',pair(phase(),c,PLAN)['failed'])
if __name__=='__main__':unittest.main(verbosity=2)
