"""Explicit whole-window observer; import starts no Docker calls or threads.
Original Dispatcharr authors retain full application/algorithm credit and licenses.
"""
import importlib.util,math,threading,time
from pathlib import Path

def module(path,name):
 spec=importlib.util.spec_from_file_location(name,path);value=importlib.util.module_from_spec(spec);spec.loader.exec_module(value);return value

def arithmetic(raw,begin,end,minimum_samples=50):
 if not math.isfinite(begin+end)or end<=begin:raise ValueError('Invalid measurement window')
 selected=raw['selected'];before=raw['counters']['before'];after=raw['counters']['after']
 if set(selected)!= {'backend','infrastructure','origin'}or set(before)!=set(selected)or set(after)!=set(selected):raise ValueError('Incomplete service group')
 ids={item['id']for item in selected.values()}
 if len(ids)!=3:raise ValueError('Duplicate service ID')
 cpu=0
 for name,limits in selected.items():
  a=before[name];b=after[name]
  if any(a[k]!=b[k]for k in ['cpu_quota','cpu_period','memory_max']):raise ValueError('Changed limit')
  if a['memory_max']!=limits['limits']['memory_bytes']or a['cpu_quota']*10**9//a['cpu_period']!=limits['limits']['nano_cpus']:raise ValueError('Wrong limit')
  for row in [a,b]:
   if row['memory_events']['oom']or row['memory_events']['oom_kill']:raise ValueError('Absolute OOM')
  if any(type(v)is not int or v<0 for row in [a,b]for v in row['cpu'].values()):raise ValueError('CPU counter type')
  elapsed=b['sampled_monotonic']-a['sampled_monotonic']
  if not math.isfinite(elapsed)or elapsed<=0:raise ValueError('Invalid counter duration')
  if abs(elapsed-(end-begin))>1:raise ValueError('Counter window drift')
  if any(b['cpu'].get(k,-1)<v for k,v in a['cpu'].items()):raise ValueError('CPU reset')
  cpu+=(b['cpu']['usage_usec']-a['cpu']['usage_usec'])/(elapsed*10000)
 samples=[]
 for row in raw['samples']:
  if not begin<=row['started_monotonic']<=row['completed_monotonic']<=end:continue
  items=row['value']['containers']
  if len(items)!=3 or {v['id']for v in items}!=ids:raise ValueError('Incomplete sample')
  for item in items:
   usage=item['memory_usage_bytes'];inactive=item['inactive_file_bytes']
   if type(usage)is not int or type(inactive)is not int or min(usage,inactive)<0:raise ValueError('Memory counter')
   if item['memory_bytes']!=(usage-inactive if inactive<usage else usage):raise ValueError('Working memory arithmetic')
   expected=next(v['limits']['memory_bytes']for v in selected.values()if v['id']==item['id'])
   if item['memory_limit_bytes']!=expected:raise ValueError('Sample memory limit')
  total=sum(v['memory_bytes']for v in items)
  if total!=row['value']['memory_bytes']:raise ValueError('Service sum')
  samples.append(row)
 if raw['errors']or len(samples)<minimum_samples:raise ValueError('Missing/error samples')
 if any(not .9<=b['started_monotonic']-a['started_monotonic']<=2.5 for a,b in zip(samples,samples[1:])):raise ValueError('Sampler cadence')
 if samples[0]['started_monotonic']-begin>2.5 or end-samples[-1]['completed_monotonic']>2.5:raise ValueError('Missing window edge coverage')
 return {'cpu_percent_mean':cpu,'peak_working_memory_bytes':max(r['value']['memory_bytes']for r in samples),'samples':len(samples),'window_seconds':end-begin,'zero_oom':True}

class Window:
 def __init__(self,selected,tools,minimum_samples=50):
  if set(selected)!={'backend','infrastructure','origin'}:raise ValueError('Explicit complete service group required')
  self.minimum_samples=minimum_samples;self.selected=selected;self.cgroup=module(Path(tools)/'scripts/rust-guide-perf/cgroup.py','_live_cgroup');self.stats=module(Path(tools)/'scripts/rust-guide-perf/docker_stats.py','_live_stats').Stats(selected)
  self.stop=threading.Event();self.thread=None;self.samples=[];self.errors=[];self.used=False
 def counters(self):
  return {name:self.cgroup.read(item['id'],item['limits'])for name,item in self.selected.items()}
 def start(self):
  if self.thread is not None or self.used:raise ValueError('Window already used')
  self.stats.prime();self.before=self.counters()
  if any(v['memory_events']['oom']or v['memory_events']['oom_kill']for v in self.before.values()):raise ValueError('Absolute setup OOM')
  def sample():
   while not self.stop.is_set():
    started=time.monotonic()
    try:
     value=self.stats.sample();self.samples.append({'started_monotonic':started,'completed_monotonic':time.monotonic(),'value':value})
    except Exception as error:self.errors.append(type(error).__name__);self.stop.set()
    self.stop.wait(1)
  self.thread=threading.Thread(target=sample);self.thread.start();return self
 def close(self):
  self.stop.set()
  if self.thread is not None:
   self.thread.join(8)
   if self.thread.is_alive():raise RuntimeError('Observer did not retire')
 def finish(self,token,begin,end):
  if token is not self or self.thread is None or self.used:raise ValueError('Wrong or reused window')
  self.used=True
  try:after=self.counters()
  finally:self.close()
  raw={'window':{'begin':begin,'end':end},'selected':self.selected,'counters':{'before':self.before,'after':after},'samples':self.samples,'errors':self.errors}
  return {'raw':raw,'metrics':arithmetic(raw,begin,end,self.minimum_samples)}
