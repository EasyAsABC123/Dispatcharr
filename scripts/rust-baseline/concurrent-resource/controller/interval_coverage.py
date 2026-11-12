"""Pure declared interval/coverage validation; no sampling or padding."""
import math
from coverage import SERVICES,eligible_census_row

def intervals(span,values,max_gap,edge):
 begin,end=span
 if not all(type(x)in(int,float)and math.isfinite(x)for x in span)or end<=begin:return {'passed':False,'reason':'InvalidSpan'}
 rows=sorted((a,b)for a,b in values if all(type(x)in(int,float)and math.isfinite(x)for x in[a,b])and begin<=a<b<=end)
 if not rows:return {'passed':False,'reason':'NoCompleteIntervals','count':0}
 if any(b[0]<a[1]for a,b in zip(rows,rows[1:])):return {'passed':False,'reason':'OverlappingIntervals','count':len(rows)}
 gaps=[max(0,b[0]-a[1])for a,b in zip(rows,rows[1:])]
 return {'passed':rows[0][0]-begin<=edge and end-rows[-1][1]<=edge and max(gaps,default=0)<=max_gap,'count':len(rows),'left_edge_seconds':rows[0][0]-begin,'right_edge_seconds':end-rows[-1][1],'maximum_gap_seconds':max(gaps,default=0)}

def evaluate(samples,censuses,phases,policy):
 q=policy['resource_draft']['coverage_quality'];checks=[]
 for phase in phases:
  label=phase['phase']
  if label not in ['idle-before-first-playback','concurrent-import-playback','idle-after-import-playback']:continue
  span=[phase['begin'],phase['end']];rows=[x for x in samples if x['phase']==label and x.get('phase_epoch')==phase['epoch']and x.get('cpu_interval_stable')is True and x.get('phase_stable')is True]
  minimum=policy['resource_draft']['minimum_workload_cpu_samples']if label=='concurrent-import-playback'else q['idle_CPU_minimum_samples']
  for role in sorted(SERVICES):
   ranges=[x['cpu_intervals'][role]for x in rows if set(x.get('cpu_intervals',{}))==SERVICES]
   result=intervals(span,ranges,q['CPU_max_gap_seconds'],q['CPU_edge_slack_seconds']);result['passed']&=result.get('count',0)>=minimum;checks.append({'id':label+'/CPU-intervals/'+role,**result,'minimum':minimum})
  rows=[x for x in censuses if x['phase']==label and x.get('phase_epoch')==phase['epoch']and eligible_census_row(x)]
  result=intervals(span,[(x['monotonic'],x['completed_monotonic'])for x in rows],q['census_max_gap_seconds'],q['census_edge_slack_seconds']);minimum=q['workload_complete_census_minimum']if label=='concurrent-import-playback'else policy['resource_draft']['minimum_complete_stable_idle_censuses'];result['passed']&=result.get('count',0)>=minimum;checks.append({'id':label+'/complete-census-intervals',**result,'minimum':minimum})
 required=['idle-before-first-playback','concurrent-import-playback','idle-after-import-playback']
 if any(sum(x['phase']==label for x in phases)!=1 for label in required):checks.append({'id':'required-phase-spans','passed':False})
 return checks
