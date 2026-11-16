"""Recompute published descriptive ratios from byte-exact compressed raw captures.
Original Dispatcharr authors retain application/algorithm credit. No workload runs.
"""
import gzip,hashlib,json,math,statistics
from pathlib import Path
H=Path(__file__).parent
summary=json.loads((H/'summary.json').read_text());phases=[]
def near(a,b):assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-12),(a,b)
for row in summary['phase_metrics']:
 raw={}
 for name in ['resource-samples.json','useful-media-complete.json']:
  data=gzip.decompress((H/'raw'/f"{row['phase']}-{name}.gz").read_bytes())
  assert hashlib.sha256(data).hexdigest()==row['raw_sha256'][name]
  raw[name]=json.loads(data)
 s=raw['resource-samples.json'];m=raw['useful-media-complete.json'];metrics={}
 assert m['passed']and s['fixed_clock_passed']and s['actual_VLC_generation_stable']
 for category in row['metrics']:
  names=[n for n,g in s['groups'].items()if category=='whole_fixture'or(category=='primary'and g['category']in ['backend_and_infrastructure','origin'])or g['category']==category]
  ticks=[]
  for sample in s['samples']:
   groups=[sample['groups'][n]for n in names]
   ticks.append((sum(g['cpu_stat']['usage_usec']for g in groups),sum(g['charged_memory_current_bytes']for g in groups),sum(max(0,g['charged_memory_current_bytes']-g['inactive_file_bytes'])for g in groups)))
  cpu=(ticks[-1][0]-ticks[0][0])/1e6
  metrics[category]={'cpu_core_seconds':cpu,'cpu_mean_cores':cpu/s['elapsed_seconds'],'charged_memory_median_bytes':statistics.median(t[1]for t in ticks),'charged_memory_max_simultaneous_bytes':max(t[1]for t in ticks),'working_set_estimate_median_bytes':statistics.median(t[2]for t in ticks),'working_set_estimate_max_simultaneous_bytes':max(t[2]for t in ticks)}
  for k,v in metrics[category].items():near(v,row['metrics'][category][k])
 for key in ['window_bytes','window_encoded_useful_bytes']:assert sum(v[key]for v in m['readers'])==row[key]
 phases.append({**row,'metrics':metrics})
pairs=[]
for expected in summary['pairs']:
 a=next(v for v in phases if v['viewers']==expected['viewers']and v['trial']==expected['trial']and v['backend']=='baseline')
 b=next(v for v in phases if v['viewers']==expected['viewers']and v['trial']==expected['trial']and v['backend']=='candidate')
 # Latency is a declared projection of the hash-bound shared-ts capture;
 # samples also retain each reader's actual first-byte latency independently.
 def latency(row):
  data=json.loads(gzip.decompress((H/'raw'/f"{row['phase']}-resource-samples.json.gz").read_bytes()))
  values=sorted(v['first_byte_seconds']*1000 for v in data['samples'][0]['readers'])
  assert len(values)==row['viewers'];value=max(50,values[math.ceil(.95*len(values))-1]);near(value,row['first_byte_p95_ms']);return value
 ratios={'CPU':b['metrics']['primary']['cpu_mean_cores']/a['metrics']['primary']['cpu_mean_cores'],'memory':b['metrics']['primary']['working_set_estimate_max_simultaneous_bytes']/a['metrics']['primary']['working_set_estimate_max_simultaneous_bytes'],'throughput':b['window_bytes']/a['window_bytes'],'encoded_useful_throughput':b['window_encoded_useful_bytes']/a['window_encoded_useful_bytes'],'first_byte_p95':latency(b)/latency(a)}
 for k,v in ratios.items():near(v,expected['ratios'][k])
 failed=[k for k,limit in [('CPU',.9),('memory',.95),('first_byte_p95',1.1)]if ratios[k]>limit]+[k for k in ['throughput','encoded_useful_throughput']if ratios[k]<.95]
 assert failed==expected['failed']and expected['passed']==(not failed)
 pairs.append({**expected,'ratios':ratios,'passed':not failed})
for level,expected in summary['geometric_mean_ratios'].items():
 for key,value in expected.items():near(math.exp(statistics.mean(math.log(p['ratios'][key])for p in pairs if level=='0'or str(p['viewers'])==level)),value)
assert len(phases)==12 and sum(p['passed']for p in pairs)==5 and summary['gain_accepted']is False
print(json.dumps({'raw_arithmetic_matches':True,'phases':12,'passed_pairs':5,'failed_pairs':1,'gain_accepted':False,'geometric_mean_ratios':summary['geometric_mean_ratios']},indent=2))
