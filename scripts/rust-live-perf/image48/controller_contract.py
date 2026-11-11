"""Complete fixed live matrix acceptance; no timers, Docker or imports on load."""
def complete(report,plan):
 phases=report.get('phases',[]);pairs=report.get('pairs',[])
 expected={(v,t,b)for v in plan['viewers']for t in range(1,4)for b in ['source','candidate']}
 actual=[(p['count'],p['trial'],p['backend'])for p in phases]
 if len(actual)!=18 or len(set(actual))!=18 or set(actual)!=expected:raise ValueError('Incomplete/duplicate matrix')
 if len(pairs)!=9 or {(p['viewers'],p['trial'])for p in pairs}!={(v,t)for v in plan['viewers']for t in range(1,4)}:raise ValueError('Incomplete/duplicate pairs')
 for phase in phases:
  if not phase.get('passed')or not phase.get('qualification',{}).get('passed'):raise ValueError('Failed phase')
 for pair in pairs:
  if not pair['passed']or pair['failed']or pair['order']!=plan['orders'][pair['trial']-1]:raise ValueError('Failed pair/order')
  actual_order=[p['backend']for p in phases if p['count']==pair['viewers']and p['trial']==pair['trial']]
  if actual_order!=pair['order']:raise ValueError('Observed pair order')
 return True
