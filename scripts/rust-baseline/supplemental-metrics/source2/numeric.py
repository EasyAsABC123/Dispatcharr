"""Pure retained-data arithmetic, no kernel/network/subprocess access."""
import math,hashlib
MEDIA={'ffmpeg','ffprobe','vlc','cvlc','streamlink'}
def integer(value,minimum=0):
 if type(value)is not int or value<minimum:raise ValueError('NonnegativeIntegerRequired')
 return value
def finite(value):
 if type(value)not in[int,float]or not math.isfinite(value):raise ValueError('FiniteNumberRequired')
 return value
def close(a,b,tolerance=.000001):return abs(finite(a)-finite(b))<=tolerance

def role(data):
 if data['errors']or not data['coverage_complete']or not data['inventory_identity_stable']:raise ValueError('IncompleteRole')
 identities=[]
 for which in ['inventory_start','inventory_end']:
  inv=data[which]
  if not inv['coverage_complete']or inv['errors']:raise ValueError('IncompleteInventory')
  mapping={}
  for row in inv['processes']:
   pid=integer(row['pid'],1);integer(row['start_ticks'],1)
   if pid in mapping or len(row['identity'])!=2:raise ValueError('DuplicateOrMalformedIdentity')
   for ident in row['identity']:integer(ident)
   mapping[pid]=(row['start_ticks'],tuple(row['identity']))
  identities.append(mapping)
 if identities[0]!=identities[1]:raise ValueError('PIDIdentityChanged')
 commands=data['commands'];collector_UIDs={tuple(c['identity'])for c in commands if c['mode']=='rows'}
 if collector_UIDs!={value[1]for value in identities[0].values()}:raise ValueError('MissingUIDCollector')
 rows=data['processes'];seen=set();rss=pss=live=zombies=0;media=[]
 for row in rows:
  pid=integer(row['pid'],1)
  if pid in seen or pid not in identities[0]or row['start_ticks']!=identities[0][pid][0]:raise ValueError('RowPIDReuseOrDuplicate')
  seen.add(pid)
  if row['stable']is not True or row['coverage_complete']is not True or row['denied_fields']or row.get('raced_process')or row.get('unstable_identity'):raise ValueError('DeniedRacedOrUnstableRow')
  if row['state']=='Z':
   zombies+=1
   if row['rss_bytes']is not None or row['pss_bytes']is not None:raise ValueError('ZombieMemoryNotApplicable')
  else:
   live+=1;rss+=integer(row['rss_bytes']);pss+=integer(row['pss_bytes'])
   if not isinstance(row['executable_basename'],str)or not row['executable_basename']or '/'in row['executable_basename']:raise ValueError('ExecutableBasenameRequired')
  if row['identified_media']is not(row['executable_basename']in MEDIA):raise ValueError('MediaClassificationMismatch')
  if row['identified_media']:media.append({'pid':pid,'start_ticks':row['start_ticks'],'executable_basename':row['executable_basename']})
 if seen!=set(identities[0]):raise ValueError('IncompletePIDRows')
 expected={'processes_observed':len(rows),'live_processes':live,'zombies':zombies,'unknown_process_state':0,'rss_bytes_sum_observed':rss,'pss_bytes_sum_observed':pss,'known_media_processes':media,'denied_processes':[]}
 for key,value in expected.items():
  if type(value)is int:integer(data[key])
  if data[key]!=value:raise ValueError('AggregateMismatch:'+key)
 if not data['memory_sums_complete']or not data['unknown_media_wrappers_not_inferred']:raise ValueError('MemoryOrAttributionScope')
 return expected

def redis_delta(before,after):
 if before['run_id']!=after['run_id']or len(before['run_id'])!=40:raise ValueError('RedisGenerationChanged')
 names=['total_commands_processed','total_net_input_bytes','total_net_output_bytes'];delta={key:integer(after[key])-integer(before[key])for key in names}
 if any(v<0 for v in delta.values()):raise ValueError('RedisReset')
 return {'server_protocol_counters_including_observer':delta,'application_only_attribution':False,'observer_subtraction_performed':False}

def packet_fields(projection):
 raw=bytes.fromhex(projection['first_six_bytes_per_complete_packet_hex']);tail=bytes.fromhex(projection['tail_hex']);size=integer(projection['captured_bytes']);count=size//188
 if len(raw)!=count*6 or len(tail)!=size%188:raise ValueError('PacketProjectionLength')
 values=dict(packets=count,sync_errors=0,transport_errors=0,discontinuity_indicators=0,CC_mismatches=0,duplicate_payload_packets=0,malformed_adaptation=0);previous={};needed=set();digests=projection['payload_sha256_for_same_CC_candidates']
 for index in range(count):
  p=raw[index*6:(index+1)*6]
  if p[0]!=0x47:values['sync_errors']+=1;continue
  pid=((p[1]&31)<<8)|p[2];cc=p[3]&15;control=(p[3]>>4)&3;offset=4;di=False;values['transport_errors']+=bool(p[1]&128)
  if control in[2,3]:
   length=p[4];offset=5+length
   if offset>188:values['malformed_adaptation']+=1;continue
   di=length>0 and bool(p[5]&128);values['discontinuity_indicators']+=di
  if control==0:values['malformed_adaptation']+=1;continue
  if pid==8191:continue
  payload=control in[1,3]and offset<188;old=previous.get(pid)
  if payload:
   if old and not di:
    duplicate=False
    if cc==old[0]:
     needed.update([str(index),str(old[1])]);duplicate=digests[str(index)]==digests[str(old[1])]
    if duplicate:values['duplicate_payload_packets']+=1
    elif cc!=(old[0]+1)%16:values['CC_mismatches']+=1
   previous[pid]=(cc,index)
  elif di:previous.pop(pid,None)
 if set(digests)!=needed:raise ValueError('CandidateDigestProjectionMismatch')
 return {**values,'partial_tail_bytes':len(tail),'tracked_PIDs':len(previous),'dropped_packets_inferred':None,'scope':'observed188-byte packet fields, no whole-stream zero-loss inference'}
