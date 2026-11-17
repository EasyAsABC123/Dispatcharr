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
 data=gzip.decompress((H/'raw'/f"{row['phase']}-metadata-observer-receipt.json.gz").read_bytes())
 assert hashlib.sha256(data).hexdigest()==row['observer_receipt_sha256'];observer=json.loads(data)
 assert observer['cleanup_passed']and observer['normal_STOP']and observer['actual_exit']==0 and observer['completed_requests']==62
 assert observer['forced']==[]and observer['original_guest_generation_absent']and all(observer['EOF'].values())
 assert observer['identity']['CID']==s['groups']['baseline']['CID']and s['groups']['baseline']['category']=='backend_and_infrastructure'
 assert Path('/sys/fs/cgroup'+observer['identity']['cgroup'])==Path(s['groups']['baseline']['path'])

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
 for stream in ['0','1']:
  values=[v['streams'][stream]['useful_duration_seconds']for v in m['readers']]
  assert len(values)==len(row['useful_duration_seconds'][stream])
  for actual,expected in zip(values,row['useful_duration_seconds'][stream]):near(actual,expected)
 near(m['one_batch_PTS_seconds'],row['one_batch_PTS_seconds'])

 phases.append({**row,'metrics':metrics})
for stream,expected in summary['useful_duration_equivalence'].items():
 values=[v for row in phases for v in row['useful_duration_seconds'][stream]]
 spread=max(values)-min(values);bound=min(row['one_batch_PTS_seconds']for row in phases)
 near(spread,expected['spread_seconds']);near(bound,expected['limit_seconds']);assert spread<=bound
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
assert len(phases)==12 and len(pairs)==6 and summary['gain_accepted']is False
assert sum(p['passed']for p in pairs)==summary['passed_pairs']
assert sum(not p['passed']for p in pairs)==summary['failed_pairs']
assert all(p['passed']for p in pairs)==summary['scoped_budgets_passed']
assert summary['live_only_gain_accepted']is False and not any(summary['original_scope_flags'].values())
cleanup_data=(H/'cleanup.json').read_bytes();assert hashlib.sha256(cleanup_data).hexdigest()==summary['cleanup_proof_sha256']
cleanup=json.loads(cleanup_data);assert cleanup['passed']and all(row['passed']for row in cleanup['checks'])
assert cleanup['exact_CIDs']==summary['cleanup_exact_CIDs']and cleanup['projects']==summary['cleanup_projects']
assert cleanup['positive_original_host_generations']==summary['cleanup_captured_original_host_generations']

print(json.dumps({'raw_arithmetic_matches':True,'phases':12,'passed_pairs':sum(p['passed']for p in pairs),'failed_pairs':sum(not p['passed']for p in pairs),'scoped_budgets_passed':all(p['passed']for p in pairs),'gain_accepted':False,'geometric_mean_ratios':summary['geometric_mean_ratios']},indent=2))
