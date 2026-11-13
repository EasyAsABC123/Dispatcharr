"""Independent complete live-matrix verifier. Explicit input only; no work on import.
Original Dispatcharr authors retain application/algorithm credit and licensing.
"""
import hashlib,json,math,statistics
from pathlib import Path
IMAGE='sha256:fc5ea2aba28f86a3ebf3672f6bf8d874a6f79b2d9dd4c8a1efb8993de680375a'
ORIGINAL='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def require(v,label):
 if not v:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def close(a,b):return math.isfinite(a+b)and math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-12)
def phase(p,plan):
 n=p['count'];require(n in [1,10,100]and p['trial']in [1,2,3]and p['backend']in ['source','candidate'],'phase identity')
 require(p['passed']and p['qualification']['passed']and not p['qualification']['failed'],'phase status')
 require(p['source_bound_image']==(ORIGINAL if p['backend']=='source'else IMAGE),'backend image')
 require(p['continuous_connections']and p['requests']==n and p['authenticated_identity']=={'clients':n,'all_owned_user_id':True},'authenticated work')
 require(p['warmup_seconds']==15 and p['measure_seconds']==60 and p['cleanup_upstream_active']==0,'work window/cleanup')
 raw=p['resources']['raw'];begin=raw['window']['begin'];end=raw['window']['end'];require(close(end-begin,60)and not raw['errors'],'resource window')
 selected=raw['selected'];require(set(selected)=={'backend','infrastructure','origin'}and len({v['id']for v in selected.values()})==3,'service group')
 cpu=0
 for name,info in selected.items():
  require(info['limits']==plan[name+'_limits'],'service limits');a=raw['counters']['before'][name];b=raw['counters']['after'][name]
  require(a['memory_max']==b['memory_max']==info['limits']['memory_bytes'],'memory cap')
  require(a['cpu_quota']*10**9//a['cpu_period']==b['cpu_quota']*10**9//b['cpu_period']==info['limits']['nano_cpus'],'CPU cap')
  require(all(t['memory_events'][k]==0 for t in [a,b]for k in ['oom','oom_kill']),'absolute OOM')
  elapsed=b['sampled_monotonic']-a['sampled_monotonic'];require(math.isfinite(elapsed)and 59<=elapsed<=61,'counter window')
  require(all(type(v)is int and v>=0 for row in [a,b]for v in row['cpu'].values())and all(b['cpu'].get(k,-1)>=v for k,v in a['cpu'].items()),'counter reset/type')
  cpu+=(b['cpu']['usage_usec']-a['cpu']['usage_usec'])/(elapsed*10000)
  if name=='backend'and p['backend']=='source':
   periods=b['cpu']['nr_periods']-a['cpu']['nr_periods'];throttled=b['cpu']['nr_throttled']-a['cpu']['nr_throttled'];require((throttled/periods if periods else 0)<=.05,'Source throttle')
   require((b['cpu']['usage_usec']-a['cpu']['usage_usec'])/(elapsed*10000)<=170,'Source CPU headroom')
 samples=[s for s in raw['samples']if begin<=s['started_monotonic']<=s['completed_monotonic']<=end];require(len(samples)>=50,'sample count')
 require(samples[0]['started_monotonic']-begin<=2.5 and end-samples[-1]['completed_monotonic']<=2.5,'edge coverage')
 require(all(.9<=b['started_monotonic']-a['started_monotonic']<=2.5 for a,b in zip(samples,samples[1:])),'cadence')
 for s in samples:
  items=s['value']['containers'];require(len(items)==3 and {v['id']for v in items}=={v['id']for v in selected.values()},'sample identities')
  for v in items:
   usage=v['memory_usage_bytes'];inactive=v['inactive_file_bytes'];require(type(usage)is int and type(inactive)is int and min(usage,inactive)>=0,'memory types')
   require(v['memory_bytes']==(usage-inactive if inactive<usage else usage),'working memory')
   item=next(i for i in selected.values()if i['id']==v['id']);require(v['memory_limit_bytes']==item['limits']['memory_bytes'],'sample cap')
   if p['backend']=='source'and v['id']==selected['backend']['id']:require(v['memory_bytes']<=.9*536870912,'Source memory headroom')
  require(sum(v['memory_bytes']for v in items)==s['value']['memory_bytes'],'memory sum')
 peak=max(s['value']['memory_bytes']for s in samples);m=p['resources']['metrics'];require(close(cpu,m['cpu_percent_mean'])and peak==m['peak_working_memory_bytes']and len(samples)==m['samples'],'derived metrics')
 rows=p['viewers'];require(len(rows)==n,'reader count')
 for r in rows:
  require(r['status']==200 and r['requests']==1 and not r['errors']and not r['unexpected_eof']and r['post_window_barrier'],'reader correctness')
  require(r['prefix_bytes']==2097152 and r['decoder_frames']>=25 and r['decoder_exit_code']==0,'reader decoder')
  require(225000<=r['measured_bytes']/60<=275000 and math.isfinite(r['first_byte_ms'])and r['first_byte_ms']>=0,'reader rate/latency')
 return {'CPU':cpu,'memory':peak,'samples':len(samples),'bytes':sum(r['measured_bytes']for r in rows),'first_byte_p95':sorted(r['first_byte_ms']for r in rows)[math.ceil(.95*n)-1]}

def verify(report,plan):
 require(hashlib.sha256((json.dumps(plan,indent=2)+'\n').encode()).hexdigest()==report['protocol_sha256'],'protocol bytes')
 require(plan['viewers']==[1,10,100]and plan['pairs']==3 and plan['orders']==[['source','candidate'],['candidate','source'],['source','candidate']],'fixed protocol matrix')
 require(plan['resource_warmup_seconds']==15 and plan['resource_measure_seconds']==60 and plan['decoder_prefix_bytes_per_viewer']==2097152 and plan['decoder_output_seconds']==5 and plan['decoder_min_video_frames']==25,'fixed workload')
 for key,value in [('max_CPU_ratio',.9),('max_memory_ratio',.95),('minimum_throughput_ratio',.95),('max_first_byte_ratio',1.1),('first_byte_floor_ms',50),('minimum_samples',50)]:require(plan['budgets'][key]==value,'fixed budget '+key)
 require(report['passed']and report['performance_verified']and report['action']=='measure','not complete')
 require(report['source_revision']==REV and report['candidate_image']==IMAGE and report['cleanup']=='owned project removed','report binding')
 require(len(report['phases'])==18 and len(report['pairs'])==9,'matrix count');indexed={}
 for p in report['phases']:
  key=(p['count'],p['trial'],p['backend']);require(key not in indexed,'duplicate phase');indexed[key]=(p,phase(p,plan))
 require(set(indexed)=={(n,t,b)for n in [1,10,100]for t in [1,2,3]for b in ['source','candidate']},'matrix identities');seen=set();summaries=[]
 for pair in report['pairs']:
  n=pair['viewers'];t=pair['trial'];require((n,t)not in seen,'duplicate pair');seen.add((n,t));a,am=indexed[(n,t,'source')];b,bm=indexed[(n,t,'candidate')]
  require(pair['passed']and not pair['failed']and pair['order']==plan['orders'][t-1],'pair status/order')
  require([p['backend']for p in report['phases']if p['count']==n and p['trial']==t]==pair['order'],'observed order')
  for name in ['infrastructure','origin']:require(a['resources']['raw']['selected'][name]==b['resources']['raw']['selected'][name],'common '+name)
  require(a['resources']['raw']['selected']['backend']['id']!=b['resources']['raw']['selected']['backend']['id'],'same backend')
  ratios={'CPU':bm['CPU']/am['CPU'],'memory':bm['memory']/am['memory'],'throughput':bm['bytes']/am['bytes'],'first_byte_p95':max(bm['first_byte_p95'],50)/max(am['first_byte_p95'],50)}
  require(all(close(v,pair['ratios'][k])for k,v in ratios.items()),'pair arithmetic')
  require(ratios['CPU']<=.9 and ratios['memory']<=.95 and ratios['throughput']>=.95 and ratios['first_byte_p95']<=1.1,'fixed paired budgets')
 for n in [1,10,100]:
  pairs=[p for p in report['pairs']if p['viewers']==n];require(len(pairs)==3,'cohort pairs')
  summaries.append({'viewers':n,'pairs':3,'median_paired_CPU_reduction_percent':100*statistics.median(1-p['ratios']['CPU']for p in pairs),'median_paired_working_memory_reduction_percent':100*statistics.median(1-p['ratios']['memory']for p in pairs),'minimum_measured_throughput_ratio':min(p['ratios']['throughput']for p in pairs)})
 return {'passed':True,'pairs':9,'phases':18,'samples':sum(m['samples']for _,m in indexed.values()),'summaries':summaries,'scope':'Image57 authenticated live backend plus commonPGRedis/origin only; no full application/milestone acceptance'}
