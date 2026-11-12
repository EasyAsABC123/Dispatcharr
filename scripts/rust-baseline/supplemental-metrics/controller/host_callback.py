"""Four-point injection seam; uses caller's existing bounded physical supervisor.
No Docker/process invocation exists here; constructor receives a strict run port.
No periodic loop or workload modification; final Source attachment still blocked.
"""
import json,re,time
from metrics import aggregate
LABELS=('before-reader','after-warm-before-window','after-physical-close','retention')
ROLES={'backend','worker','infrastructure','origin'}
class HostCallbacks:
 def __init__(self,selected,inspect,run,redis_session,python_by_role):
  if set(selected)!=ROLES or set(python_by_role)!=ROLES:raise ValueError('ExactlyFourSelectedRoles')
  for role,value in selected.items():
   if not re.fullmatch('[a-f0-9]{64}',value['Id'])or not value['StartedAt']:raise ValueError('FixedContainerIdentity')
   if python_by_role[role]not in {'/dispatcharrpy/bin/python','python3'}:raise ValueError('HelperInterpreterAuthority')
  self.selected=selected;self.inspect=inspect;self.run=run;self.redis=redis_session;self.python=python_by_role;self.records=[]
 def sample(self,label):
  if len(self.records)>=4 or label!=LABELS[len(self.records)]:raise ValueError('ExactPointOrdering')
  began=time.monotonic();deadline=began+15;record={'label':label,'begin_monotonic':began,'roles':{},'errors':[],'resource_window_measurement':False,'resource_snapshots':True};self.records.append(record);retained_row_bytes=0
  def command(role,identity,mode,payload=None):
   remaining=deadline-time.monotonic()
   if remaining<=0:raise TimeoutError('SupplementalPointDeadline')
   argv=['docker','exec','-i','--user',str(identity[0])+':'+str(identity[1]),self.selected[role]['Id'],self.python[role],'/m0-supplemental/proc_helper.py','--mode',mode]
   before=time.monotonic();raw=self.run(argv,None if payload is None else json.dumps(payload).encode(),min(3,remaining))
   if len(raw)>1048576:raise ValueError('SupplementalHelperReplyCapacity')
   value=json.loads(raw);record['roles'][role]['commands'].append({'mode':mode,'identity':identity,'begin':before,'end':time.monotonic(),'reply_bytes':len(raw)})
   return value
  for role in sorted(ROLES):
   dest={'commands':[],'coverage_complete':False,'errors':[]};record['roles'][role]=dest
   try:
    remaining=deadline-time.monotonic()
    if remaining<=0:raise TimeoutError('SupplementalPointDeadline')
    actual=self.inspect(self.selected[role]['Id'],min(3,remaining));expected=self.selected[role]
    for field in ['Id','StartedAt','Memory','NanoCpus']:
     if actual[field]!=expected[field]:raise ValueError('ContainerIdentityOrLimitChanged')
    start=command(role,[0,0],'inventory');dest['inventory_start']=start;by_uid={}
    for process in start['processes']:by_uid.setdefault(tuple(process['identity']),[]).append(process['pid'])
    if len(by_uid)>16:raise ValueError('UIDInventoryCapacity')
    collected=[]
    for ident,ids in sorted(by_uid.items()):
     try:
      batch=command(role,list(ident),'rows',{'identity':list(ident),'processes':ids});size=len(json.dumps(batch,allow_nan=False).encode())+512
      if retained_row_bytes+size>1048576:raise ValueError('SupplementalPointRowsCapacity')
      retained_row_bytes+=size;collected.extend(batch)
     except Exception as error:dest['errors'].append({'identity':list(ident),'kind':type(error).__name__})
    end=command(role,[0,0],'inventory');dest['inventory_end']=end;dest['processes']=collected
    dest.update(aggregate(collected,[r['pid']for r in start['processes']],[r['pid']for r in end['processes']]))
    initial={r['pid']:(r['start_ticks'],r['identity'])for r in start['processes']};final={r['pid']:(r['start_ticks'],r['identity'])for r in end['processes']}
    stable=initial==final and all(initial.get(r['pid'],(None,None))[0]==r.get('start_ticks')for r in collected)
    dest['inventory_identity_stable']=stable;dest['coverage_complete']=dest['coverage_complete']and stable and start['coverage_complete']and end['coverage_complete']and not dest['errors'];dest['memory_sums_complete']=dest['coverage_complete']
   except Exception as error:dest['coverage_complete']=False;dest['memory_sums_complete']=False;dest['errors'].append({'kind':type(error).__name__})
  try:record['Redis']=self.redis.sample(label,deadline=deadline)
  except Exception as error:record['errors'].append({'service':'Redis','kind':type(error).__name__})
  record['end_monotonic']=time.monotonic();record['overhead_seconds']=record['end_monotonic']-began;record['coverage_complete']=not record['errors']and all(r['coverage_complete']for r in record['roles'].values());return record
