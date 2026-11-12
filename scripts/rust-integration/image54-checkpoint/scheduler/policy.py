"""Pure finite matrix policy. No Docker/process execution or outcome inference."""
import datetime,hashlib,json,math
from pathlib import Path
PLAN_SHA='bf6a26a0422ad9fc199f04852e19063f1710006994faa4727cdd05061897a1d4'
HEAVY=['sharedTS18','sharedTS6-all-down','retention100'];GUIDE='guide-grid-packaged';SOURCE='complete-refresh-original';NATIVE='complete-refresh'
class Refusal(ValueError):pass
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def bounded_number(v):return type(v)in [int,float]and math.isfinite(v)
def validate_clearance(c,seal,now):
 if c.get('authorized')is not True or c.get('scheduler_seal_sha256')!=seal or c.get('plan_sha256')!=PLAN_SHA:raise Refusal('Root exact-seal clearance required')
 if c.get('action')!='image54-package28-correction2-once':raise Refusal('Exact image54 action required')
 if c.get('candidate_manifest_sha256')!='cac7bcdcc60dff6f26e233e78c2737b7f5d9450cf54042719b8d5cd61cf5ff8b' or c.get('candidate_image')!='sha256:8e3c133c0639d60057d8d05a5be2fce66e0f0968c35e29f674c348495a1a9ca0':raise Refusal('Exact image54 candidate clearance required')
 if c.get('support_revision')!='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f':raise Refusal('Exact frozen support clearance required')
 negative=c.get('initial_host_tooling_review',{})
 if negative.get('sha256')!='de1cf855b36982ecf37ccdd6a23b3796ff6123c9ebf9a50870d874581593e7bd' or not negative.get('path') or sha(negative['path'])!=negative['sha256']:raise Refusal('Exact preserved initial infrastructure failure required')
 idle=c.get('heavy_idle',{});roles=c.get('settled_prerequisites',{})
 if set(roles)!={'SDK044f','image53_measurement','image54_build'}:raise Refusal('Fresh exact prerequisite settlement set required')
 if idle.get('all_Docker_empty')is not True or idle.get('all_other_heavy_idle')is not True:raise Refusal('Fresh Root heavy-idle clearance required')
 stamps=[c.get('issued_epoch'),c.get('valid_until_epoch'),idle.get('checked_epoch')]
 if not all(bounded_number(v)for v in stamps):raise Refusal('Explicit finite clearance clocks required')
 issued,expires,checked=stamps
 if not checked<=issued<=now<=expires:raise Refusal('Fresh causal Root clearance required')
 for role,s in roles.items():
  if s.get('settled')is not True or s.get('independent_cleanup_verified')is not True:raise Refusal('Prerequisite settlement/cleanup required '+role)
  if type(s.get('actual_controller_exit'))is not int:raise Refusal('Actual settled prerequisite exit required; business outcome is not condition')
  if not all(bounded_number(s.get(k))for k in ['settled_epoch','cleanup_verified_epoch'])or max(s['settled_epoch'],s['cleanup_verified_epoch'])>issued:raise Refusal('Prerequisite receipt chronology required '+role)
  for name in ['settlement_receipt','cleanup_review']:
   r=s.get(name,{})
   if not r.get('path')or not r.get('sha256')or sha(r['path'])!=r['sha256']:raise Refusal('Exact fresh prerequisite receipts required '+role)

 return True
def fresh_state(plan):return {'schema':1,'started':False,'halted':False,'nodes':{n['name']:{'status':'pending'}for n in plan['groups']},'lanes':{str(i):{'node':None,'reserved':None}for i in range(3)}}
def derived_blocked(plan,state,name):
 node=next(n for n in plan['groups']if n['name']==name)
 return any(state['nodes'][d]['status']in ['failed-clean','blocked']for d in node['requires'])
def allowed(plan,state,name,lane,now):
 nodes={n['name']:n for n in plan['groups']}
 if name not in nodes or str(lane)not in state['lanes']:raise Refusal('Exact28 names/three lanes only')
 if state.get('halted'):raise Refusal('Cleanup uncertainty or explicit halt blocks dispatch')
 n=nodes[name];current=state['nodes'][name];slot=state['lanes'][str(lane)]
 if current['status']!='pending':raise Refusal('No automatic repeat of any attempted node')
 if slot['node']is not None:raise Refusal('Lane retained until outcome/cleanup review')
 if slot.get('reserved')and slot['reserved']!=name:raise Refusal('Source/Native same-lane reservation')
 if any(state['nodes'][d]['status']!='passed-clean'for d in n['requires']):raise Refusal('Exact successful dependencies required')
 if not state['started']:
  if name!=GUIDE:raise Refusal('Guide must be first ordinary launch')
  minute=datetime.datetime.fromtimestamp(now,datetime.timezone.utc).minute
  if not 0<=minute<=35:raise Refusal('Guide UTC00-35 guard before output/fixture')
 if name==GUIDE:
  minute=datetime.datetime.fromtimestamp(now,datetime.timezone.utc).minute
  if not 0<=minute<=35:raise Refusal('Guide clock guard unchanged')
 active=[v['node']for v in state['lanes'].values()if v['node']]
 if name in HEAVY:
  if active:raise Refusal('Heavy must be solo including awaiting-review jobs')
  ordinary=[x for x in nodes if x not in HEAVY]
  if any(state['nodes'][x]['status']not in ['passed-clean','failed-clean','blocked']and not derived_blocked(plan,state,x)for x in ordinary):raise Refusal('Ordinary cohort must settle before heavy')
  if any(state['nodes'][x]['status']!='passed-clean'for x in HEAVY[:HEAVY.index(name)]):raise Refusal('Fixed shared/retention order')
 elif any(x in HEAVY for x in active):raise Refusal('No ordinary launch during heavy')
 if len(active)>=3:raise Refusal('Maximum three ordinary lanes')
 if name==NATIVE:
  prior=state['nodes'][SOURCE]
  if prior.get('lane')!=lane or slot.get('reserved')!=NATIVE:raise Refusal('Fresh refresh same-lane chain required')
  clock=prior.get('reference_clock_epoch')
  if not bounded_number(clock)or not 0<=now-clock<=900:raise Refusal('Actual Source reference clock maximum900 seconds')
 return True
def next_names(plan,state,lane,now):
 names=[n['name']for n in plan['groups']];reserved=state['lanes'][str(lane)].get('reserved');order=[reserved]if reserved else([GUIDE]+[n for n in names if n!=GUIDE])
 result=[]
 for name in order:
  if not name:continue
  try:allowed(plan,state,name,lane,now);result.append(name)
  except Refusal:pass
 return result
