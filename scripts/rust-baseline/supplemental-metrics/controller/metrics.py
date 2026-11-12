"""Read-only, denied-aware per-PID memory/identity and Redis counter primitives.
No sampling on import. CLI remains blocked pending Root review/fresh quiet cue.
"""
import hashlib,os,time
from pathlib import Path
EXECUTION_BLOCKED=True
MEDIA={'ffmpeg','ffprobe','vlc','cvlc','streamlink'}
def bounded(path,limit=65536):
 with Path(path).open('rb')as f:data=f.read(limit+1)
 if len(data)>limit:raise ValueError('SupplementalReadCapacity')
 return data.decode('utf-8',errors='strict')
def stat(text):
 fields=text[text.rfind(')')+2:].split()
 if len(fields)<22 or text.find('(')<0:raise ValueError('MalformedPIDStat')
 return {'state':fields[0],'ppid':int(fields[1]),'start_ticks':int(fields[19]),'rss_pages':int(fields[21])}
def memory(text):
 values={}
 for line in text.splitlines():
  if ':'not in line:continue
  name,value=line.split(':',1)
  if name not in{'Rss','Pss'}:continue
  fields=value.split()
  if len(fields)!=2 or fields[1]!='kB'or not fields[0].isdigit()or name in values:raise ValueError('MalformedSmapsMemory')
  values[name.lower()+'_bytes']=int(fields[0])*1024
 if set(values)!={'rss_bytes','pss_bytes'}:raise ValueError('MissingSmapsMemory')
 return values

def collect_pid(pid,expected_identity,proc=Path('/proc'),read=bounded,readlink=os.readlink):
 if type(pid)is not int or not 0<pid<=2**31-1:raise ValueError('PIDBound')
 if list(expected_identity)!=[os.geteuid(),os.getegid()]:raise ValueError('CollectorIdentityMismatch')
 root=Path(proc)/str(pid);value={'pid':pid,'stable':False,'coverage_complete':False,'rss_bytes':None,'pss_bytes':None,'executable_basename':None,'identified_media':False,'denied_fields':[]}
 try:
  before=stat(read(root/'stat',16384));value.update(before)
  def identity():
   fields=dict(line.split(':',1)for line in read(root/'status').splitlines()if ':'in line)
   return[int(fields[k].split()[1])for k in['Uid','Gid']]
  if identity()!=list(expected_identity):raise ValueError('WrongCollectorIdentity')
  if before['state']=='Z':value['memory_status']='not-applicable-zombie';value['identity_status']='zombie';value['coverage_complete']=True
  else:
   for field in ['memory','executable']:
    try:
     if field=='memory':value.update(memory(read(root/'smaps_rollup')))
     else:
      name=Path(readlink(root/'exe').removesuffix(' (deleted)')).name
      if not name or len(name.encode())>256:raise ValueError('ExecutableNameCapacity')
      value['executable_basename']=name;value['identified_media']=name in MEDIA
    except PermissionError:value['denied_fields'].append(field)
   value['coverage_complete']=not value['denied_fields']
  after=stat(read(root/'stat',16384));value['stable']=before['start_ticks']==after['start_ticks']and (before['state']=='Z')==(after['state']=='Z')and identity()==list(expected_identity)
  if not value['stable']:value['coverage_complete']=False;value['unstable_identity']=True
 except FileNotFoundError:value['raced_process']=True
 except PermissionError:value['denied_fields'].append('process')
 return value

def redis_info(text):
 if len(text.encode())>65536:raise ValueError('RedisInfoCapacity')
 fields={}
 for line in text.splitlines():
  if ':'not in line or line.startswith('#'):continue
  key,value=line.split(':',1)
  if key in fields:raise ValueError('DuplicateRedisInfo')
  fields[key]=value
 result={k:int(fields[k])for k in['total_commands_processed','total_net_input_bytes','total_net_output_bytes']}
 if any(v<0 for v in result.values())or len(fields['run_id'])!=40 or any(c not in'0123456789abcdef'for c in fields['run_id']):raise ValueError('MalformedRedisCounters')
 result['run_id']=fields['run_id'];return result

def redis_delta(before,after):
 if before['run_id']!=after['run_id']:raise ValueError('RedisRestartInvalidatesDelta')
 result={k:after[k]-before[k]for k in before if k!='run_id'}
 if any(v<0 for v in result.values()):raise ValueError('RedisCounterReset')
 return {'server_protocol_counters_including_observer':result,'application_only_attribution':False,'observer_subtraction_performed':False}

class Continuity:
 """Bounded13-bit PID state. CC observations are NOT a lost-packet estimate."""
 def __init__(self):self.last={};self.partial=b'';self.counts=dict(packets=0,sync_errors=0,transport_errors=0,discontinuity_indicators=0,CC_mismatches=0,duplicate_payload_packets=0,malformed_adaptation=0)
 def feed(self,data):
  if len(data)>65536:raise ValueError('ChunkBound')
  raw=self.partial+data;size=len(raw)//188*188;self.partial=raw[size:]
  for n in range(0,size,188):
   p=raw[n:n+188];c=self.counts;c['packets']+=1
   if p[0]!=0x47:c['sync_errors']+=1;continue
   pid=((p[1]&31)<<8)|p[2];cc=p[3]&15;control=(p[3]>>4)&3;offset=4;di=False
   c['transport_errors']+=bool(p[1]&128)
   if control in[2,3]:
    length=p[4];offset=5+length
    if offset>188:c['malformed_adaptation']+=1;continue
    di=length>0 and bool(p[5]&128);c['discontinuity_indicators']+=di
   if control==0:c['malformed_adaptation']+=1;continue
   if pid==8191:continue
   payload=control in[1,3]and offset<188
   previous=self.last.get(pid)
   if payload:
    digest=hashlib.sha256(p[offset:]).digest()
    if previous and not di:
     if cc==previous[0]and digest==previous[1]:c['duplicate_payload_packets']+=1
     elif cc!=(previous[0]+1)%16:c['CC_mismatches']+=1
    self.last[pid]=(cc,digest)
   elif di:self.last.pop(pid,None)
 def snapshot(self):return {**self.counts,'partial_tail_bytes':len(self.partial),'tracked_PIDs':len(self.last),'dropped_packets_inferred':None,'scope':'observed188-byte packet fields, no whole-stream zero-loss inference'}
if __name__=='__main__':raise RuntimeError('SupplementalMetricExecutionBlockedPendingRootReviewAndQuiet')

def aggregate(rows,inventory_start,inventory_end):
 import json
 if len(rows)>4096 or len(json.dumps(rows,allow_nan=False).encode())>1048576:raise ValueError('SupplementalRowsCapacity')
 ids=[r['pid']for r in rows]
 complete=len(ids)==len(set(ids))and set(ids)==set(inventory_start)==set(inventory_end)and all(r['coverage_complete']and r['stable']for r in rows)
 return {'coverage_complete':complete,'processes_observed':len(rows),'live_processes':sum(r.get('state')is not None and r['state']!='Z'for r in rows),'unknown_process_state':sum(r.get('state')is None for r in rows),'zombies':sum(r.get('state')=='Z'for r in rows),'known_media_processes':[{'pid':r['pid'],'start_ticks':r.get('start_ticks'),'executable_basename':r['executable_basename']}for r in rows if r['identified_media']],'rss_bytes_sum_observed':sum(r['rss_bytes']for r in rows if r['rss_bytes']is not None)if any(r['rss_bytes']is not None for r in rows)else None,'pss_bytes_sum_observed':sum(r['pss_bytes']for r in rows if r['pss_bytes']is not None)if any(r['pss_bytes']is not None for r in rows)else None,'memory_sums_complete':complete,'denied_processes':[r['pid']for r in rows if r['denied_fields']],'unknown_media_wrappers_not_inferred':True}
