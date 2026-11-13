"""Read-only bounded cgroup-v2 counters; no sampler or measurement starts on import."""
import json,subprocess,time

def counters(text):
 if len(text.encode())>65536:raise ValueError('Cgroup reply exceeds64KiB')
 value=json.loads(text)
 def fields(raw):
  result={}
  for line in raw.splitlines():
   key,number=line.split()
   if key in result or not number.isdigit():raise ValueError('Invalid cgroup counter')
   result[key]=int(number)
  return result
 cpu=fields(value['cpu']);events=fields(value['events']);maximum=value['cpu_max'].split()
 if len(maximum)!=2 or not all(part.isdigit()for part in maximum)or int(maximum[1])<=0:raise ValueError('Finite CPU cgroup limit required')
 if not value['memory_max'].strip().isdigit()or not value['memory_current'].strip().isdigit():raise ValueError('Finite memory cgroup limit required')
 for key in ['usage_usec','nr_periods','nr_throttled','throttled_usec']:
  if key not in cpu:raise ValueError('Missing CPU counter')
 for key in ['oom','oom_kill']:
  if key not in events:raise ValueError('Missing memory event counter')
 return {'cpu':cpu,'memory_events':events,'cpu_quota':int(maximum[0]),'cpu_period':int(maximum[1]),'memory_max':int(value['memory_max']),'memory_current':int(value['memory_current'])}

def read(container,expected):
 # Container IDs come from inspected owned Compose services, never caller URLs.
 if not 12<=len(container)<=64 or any(v not in '0123456789abcdef'for v in container):raise ValueError('Owned full Docker ID required')
 command=['docker','exec',container,'sh','-c',"printf '{\"cpu\":\"'; tr '\\n' ';' </sys/fs/cgroup/cpu.stat; printf '\",\"events\":\"'; tr '\\n' ';' </sys/fs/cgroup/memory.events; printf '\",\"cpu_max\":\"'; tr -d '\\n' </sys/fs/cgroup/cpu.max; printf '\",\"memory_max\":\"'; tr -d '\\n' </sys/fs/cgroup/memory.max; printf '\",\"memory_current\":\"'; tr -d '\\n' </sys/fs/cgroup/memory.current; printf '\"}'"]
 began=time.monotonic();raw=subprocess.check_output(command,text=True,stderr=subprocess.PIPE,timeout=5)
 parsed=json.loads(raw);parsed['cpu']=parsed['cpu'].replace(';','\n');parsed['events']=parsed['events'].replace(';','\n');result=counters(json.dumps(parsed));result.update(sampled_monotonic=time.monotonic(),observer_seconds=time.monotonic()-began)
 if result['memory_max']!=expected['memory_bytes']or result['cpu_quota']*10**9//result['cpu_period']!=expected['nano_cpus']:raise ValueError('Observed cgroup limits differ from frozen policy')
 return result

def delta(before,after):
 if any(before[key]!=after[key]for key in ['cpu_quota','cpu_period','memory_max']):raise ValueError('Cgroup limits changed')
 elapsed=after['sampled_monotonic']-before['sampled_monotonic']
 if not elapsed>0:raise ValueError('Invalid counter window')
 for section in ['cpu','memory_events']:
  if any(after[section].get(key,-1)<value for key,value in before[section].items()):raise ValueError('Cgroup counter reset')
 if any(after['memory_events'][key]!=before['memory_events'][key]for key in ['oom','oom_kill']):raise ValueError('OOM during phase')
 usage=after['cpu']['usage_usec']-before['cpu']['usage_usec'];periods=after['cpu']['nr_periods']-before['cpu']['nr_periods'];throttled=after['cpu']['nr_throttled']-before['cpu']['nr_throttled']
 return {'cpu_percent_mean':usage/(elapsed*10000),'cpu_usage_usec':usage,'elapsed_seconds':elapsed,'throttled_period_fraction':throttled/periods if periods else 0,'throttled_usec':after['cpu']['throttled_usec']-before['cpu']['throttled_usec']}
