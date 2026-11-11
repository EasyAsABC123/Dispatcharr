"""Fixed authenticated live workload qualification; no measurement on import.
Original Dispatcharr authors retain full application/algorithm credit and licenses.
"""
import math
from resources import arithmetic

def qualify(phase,plan):
 failures=[];count=phase['count'];
 if count not in plan['viewers']:raise ValueError('Unplanned viewer cohort')
 rows=phase['viewers'];budgets=plan['budgets'];resource=phase['resources'];raw=resource['raw'];window=raw['window']
 metrics=arithmetic(raw,window['begin'],window['end'],budgets['minimum_samples'])
 if not math.isclose(metrics['cpu_percent_mean'],resource['metrics']['cpu_percent_mean'],rel_tol=1e-12):failures.append('CPU_recomputation')
 if metrics['peak_working_memory_bytes']!=resource['metrics']['peak_working_memory_bytes']:failures.append('memory_recomputation')
 if len(rows)!=count or phase['requests']!=count or not phase['continuous_connections']or phase['authenticated_identity']!={'clients':count,'all_owned_user_id':True}:failures.append('continuous_authenticated_clients')
 if phase['measure_seconds']!=plan['resource_measure_seconds']or phase['warmup_seconds']!=plan['resource_warmup_seconds']:failures.append('phase_duration')
 expected_rate=250000
 for index,row in enumerate(rows):
  if row['status']!=200 or row['requests']!=1 or row['errors']or row['unexpected_eof']or not row['post_window_barrier']or row['prefix_bytes']!=plan['decoder_prefix_bytes_per_viewer']or row['decoder_frames']<plan['decoder_min_video_frames']:failures.append('reader_'+str(index)+'_delivery')
  rate=row['measured_bytes']/phase['measure_seconds']
  if not expected_rate*budgets['minimum_viewer_rate_fraction']<=rate<=expected_rate*budgets['maximum_viewer_rate_fraction']:failures.append('reader_'+str(index)+'_rate')
  if not math.isfinite(row['first_byte_ms'])or row['first_byte_ms']<0:failures.append('reader_'+str(index)+'_latency')
 if phase.get('cleanup_upstream_active')!=0:failures.append('upstream_cleanup')
 if phase['backend']=='source':
  a=raw['counters']['before']['backend'];b=raw['counters']['after']['backend'];elapsed=b['sampled_monotonic']-a['sampled_monotonic'];cpu=(b['cpu']['usage_usec']-a['cpu']['usage_usec'])/(elapsed*10000)
  periods=b['cpu']['nr_periods']-a['cpu']['nr_periods'];throttled=b['cpu']['nr_throttled']-a['cpu']['nr_throttled'];fraction=throttled/periods if periods else 0
  if cpu>170:failures.append('Source_CPU_headroom')
  if fraction>.05:failures.append('Source_throttle')
  peak=max(c['memory_bytes']for s in raw['samples']if window['begin']<=s['started_monotonic']<=s['completed_monotonic']<=window['end']for c in s['value']['containers']if c['id']==raw['selected']['backend']['id'])
  if peak>.9*raw['selected']['backend']['limits']['memory_bytes']:failures.append('Source_memory_headroom')
 return {'passed':not failures,'failed':failures,'metrics':metrics,'bytes':sum(r['measured_bytes']for r in rows),'first_byte_p95_ms':sorted(r['first_byte_ms']for r in rows)[math.ceil(.95*count)-1]}

def pair(source,candidate,plan):
 a=qualify(source,plan);b=qualify(candidate,plan);failures=[]
 if not a['passed']or not b['passed']:failures.append('phase_qualification')
 for name in ['infrastructure','origin']:
  if source['resources']['raw']['selected'][name]!=candidate['resources']['raw']['selected'][name]:failures.append('common_'+name+'_identity')
 if source['resources']['raw']['selected']['backend']['id']==candidate['resources']['raw']['selected']['backend']['id']:failures.append('same_backend_identity')
 if source['count']!=candidate['count']or source['backend']!='source'or candidate['backend']!='candidate':failures.append('pair_identity')
 budgets=plan['budgets'];ratios={'CPU':b['metrics']['cpu_percent_mean']/max(a['metrics']['cpu_percent_mean'],.000001),'memory':b['metrics']['peak_working_memory_bytes']/max(a['metrics']['peak_working_memory_bytes'],1),'throughput':b['bytes']/max(a['bytes'],1),'first_byte_p95':max(b['first_byte_p95_ms'],budgets['first_byte_floor_ms'])/max(a['first_byte_p95_ms'],budgets['first_byte_floor_ms'])}
 for key,maximum in [('CPU',budgets['max_CPU_ratio']),('memory',budgets['max_memory_ratio']),('first_byte_p95',budgets['max_first_byte_ratio'])]:
  if ratios[key]>maximum:failures.append(key+'_budget')
 if ratios['throughput']<budgets['minimum_throughput_ratio']:failures.append('throughput_budget')
 return {'passed':not failures,'failed':failures,'ratios':ratios}
