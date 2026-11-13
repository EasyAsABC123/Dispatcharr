import copy,unittest
from resources import arithmetic

def example():
 selected={name:{'id':str(i)*64,'limits':{'memory_bytes':536870912,'nano_cpus':cpus*1000000000}}for i,(name,cpus)in enumerate([('backend',2),('infrastructure',1),('origin',2)],1)}
 def counters(at,usage):
  return {name:{'cpu_quota':item['limits']['nano_cpus']//10000,'cpu_period':100000,'memory_max':536870912,'memory_events':{'oom':0,'oom_kill':0},'sampled_monotonic':at,'cpu':{'usage_usec':usage,'nr_periods':100,'nr_throttled':0,'throttled_usec':0}}for name,item in selected.items()}
 samples=[]
 for i in range(50):
  items=[{'id':v['id'],'memory_usage_bytes':2000,'inactive_file_bytes':1000,'memory_bytes':1000,'memory_limit_bytes':536870912}for v in selected.values()]
  samples.append({'started_monotonic':100+i*1.2,'completed_monotonic':100+i*1.2+.001,'value':{'containers':items,'memory_bytes':3000}})
 return {'selected':selected,'counters':{'before':counters(100,1000000),'after':counters(160,2000000)},'samples':samples,'errors':[]}
class Arithmetic(unittest.TestCase):
 def setUp(self):self.raw=example()
 def reject(self):
  with self.assertRaises(ValueError):arithmetic(self.raw,100,160)
 def test_whole_group(self):
  r=arithmetic(self.raw,100,160);self.assertEqual(r['cpu_percent_mean'],5);self.assertEqual(r['peak_working_memory_bytes'],3000);self.assertEqual(r['samples'],50)
 def test_no_input_mutation(self):
  saved=copy.deepcopy(self.raw);arithmetic(self.raw,100,160);self.assertEqual(saved,self.raw)
 def test_OOM_before(self):self.raw['counters']['before']['backend']['memory_events']['oom']=1;self.reject()
 def test_OOM_after(self):self.raw['counters']['after']['backend']['memory_events']['oom_kill']=1;self.reject()
 def test_CPU_reset(self):self.raw['counters']['after']['backend']['cpu']['usage_usec']=0;self.reject()
 def test_CPU_nan(self):self.raw['counters']['after']['backend']['cpu']['usage_usec']=float('nan');self.reject()
 def test_clock_nan(self):self.raw['counters']['after']['backend']['sampled_monotonic']=float('nan');self.reject()
 def test_changed_limit(self):self.raw['counters']['after']['backend']['memory_max']=123;self.reject()
 def test_missing_backend(self):del self.raw['counters']['after']['backend'];self.reject()
 def test_duplicate_id(self):self.raw['selected']['backend']['id']=self.raw['selected']['origin']['id'];self.reject()
 def test_memory_sum(self):self.raw['samples'][0]['value']['memory_bytes']=1;self.reject()
 def test_memory_workingset(self):self.raw['samples'][0]['value']['containers'][0]['memory_bytes']=1;self.reject()
 def test_omitted_service(self):self.raw['samples'][0]['value']['containers'].pop();self.reject()
 def test_insufficient(self):self.raw['samples'].pop();self.reject()
 def test_window_drift(self):self.raw['counters']['after']['backend']['sampled_monotonic']=165;self.reject()
 def test_catchup(self):self.raw['samples'][1]['started_monotonic']=100.1;self.raw['samples'][1]['completed_monotonic']=100.101;self.reject()
 def test_missing_tail(self):
  for i,s in enumerate(self.raw['samples']):s.update(started_monotonic=100+i,completed_monotonic=100+i+.001)
  self.reject()
 def test_sample_error(self):self.raw['errors']=['TimeoutError'];self.reject()
if __name__=='__main__':unittest.main(verbosity=2)
