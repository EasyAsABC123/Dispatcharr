"""Exact reused physical gate/terminal predicates; no app or sockets."""
import struct
def require(ok,label):
 if not ok:raise ValueError(label)
def raw_stage(value,want):
 held=value['proxy']['held'];require(held['stage']==value['stage']and type(held['backend_pid'])is int and held['backend_pid']>0,'ActualStageIdentity');require(held['sql']==want['sql']and type(held['rows'])is int and held['rows']==want['rows']and held['commands']==([]if want['command']is None else[want['command']])and held['errors']==want['errors']and held['tx']==want['tx'],'ActualCommandReadyCounts')
 events=value['proxy']['events'];start=max(i for i,e in enumerate(events)if e['kind']=='execute'and e['connection']==held['connection']and e['sql']==held['sql']);frames=[e for e in events[start:]if e['kind']=='actual-target-backend-frame'and e['connection']==held['connection']and e['sql']==held['sql']];rows=0;commands=[];errors=[];last=None
 for frame in frames:
  kind=bytes.fromhex(frame['kind_hex']);header=bytes.fromhex(frame['header_hex']);body=bytes.fromhex(frame['body_hex']);require(len(header)==4 and struct.unpack('!I',header)[0]==len(body)+4,'RawFrameLength')
  if kind==b'D':rows+=1
  elif kind==b'C':require(body.endswith(b'\0'),'LiteralCommand');commands.append(body[:-1].decode('ascii'))
  elif kind==b'E':errors.extend(f[1:].decode('ascii')for f in body.rstrip(b'\0').split(b'\0')if f[:1]==b'C')
  elif kind==b'Z':last=body.decode('ascii')
 require((rows,commands,errors,last)==(want['rows'],[]if want['command']is None else[want['command']],want['errors'],want['tx']),'RawFramesBindActualHeldStage')
 sockets=value['actual_SQLx_client_sockets'];require(len(sockets)==1 and sockets[0]['link']=='socket:['+sockets[0]['inode']+']','ActualSQLxFDIdentity')
 activity=value['backend_activity'];require(len(activity)==1 and activity[0]['pid']==held['backend_pid'],'DirectBackendPIDObservation')
 require(any(e['kind']=='ready-held'and e['ordinal']==held['ordinal']and e['backend_pid']==held['backend_pid']and e['stage']==held['stage']for e in events),'ActualHeldReceipt')
 return held
def post_close_terminal(row):
 require(type(row['metadata_after']['size'])is int,'LiteralPoolSize')
 sockets=row['old_SQLx_socket_identity'];links=row['actual_self_FD_links'];backends=row['backends'];connections=row['relay']['connections']
 return(row['metadata_after']['size']==0 and row['current_SQLx_sockets']==[]and bool(sockets)and all(s['link']not in links for s in sockets)and bool(backends)and all(type(b['pid'])is int and b['state']==[]for b in backends)and bool(connections)and all(c['closed']is True and c['up_joined']is True for c in connections)and 0<=row['elapsed_millis']<=10000)
