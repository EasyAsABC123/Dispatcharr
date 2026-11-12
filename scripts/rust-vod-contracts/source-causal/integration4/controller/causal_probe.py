"""Bounded interpreter observation, not application/method replacement.
Original Dispatcharr authors retain application/algorithm credit.
No trace, network, process or application activity occurs on import.
"""
import contextlib,dis,hashlib,json,math,os,sys,threading,time,types,weakref
from pathlib import Path

class Journal:
 def __init__(self,path,max_rows=4096,max_bytes=8*1024*1024,row_bytes=2048):
  if any(type(value)is not int or not 1<=value<=maximum for value,maximum in[(max_rows,4096),(max_bytes,8*1024*1024),(row_bytes,2048)]):raise ValueError('TelemetryLimitSchema')
  self.path=Path(path);self.max_rows=max_rows;self.max_bytes=max_bytes;self.row_bytes=row_bytes;self.rows=0;self.used=0;self.invalid=[];self.lock=threading.RLock()
 def fail(self,reason):
  with self.lock:
   if reason not in self.invalid and len(self.invalid)<16:self.invalid.append(reason)
 def measure(self,value,depth=0):
  if depth>3:raise ValueError('TelemetryNesting')
  if value is None:return 4
  if type(value)is bool:return 4 if value else 5
  if type(value)is int:
   if not -9223372036854775808<=value<=9223372036854775807:raise ValueError('TelemetryInteger')
   return len(str(value))
  if type(value)is float:
   if not math.isfinite(value):raise ValueError('TelemetryFloat')
   return len(json.dumps(value))
  if type(value)is str:
   if len(value)>128:raise ValueError('TelemetryString')
   return len(json.dumps(value).encode())
  if type(value)is list:
   if len(value)>4:raise ValueError('TelemetryList')
   size=2+max(0,len(value)-1)
   for item in value:
    size+=self.measure(item,depth+1)
    if size>self.row_bytes:raise ValueError('TelemetryRow')
   return size
  if type(value)is dict:
   if len(value)>20:raise ValueError('TelemetryFields')
   size=2+max(0,len(value)-1)
   for key,item in value.items():
    if type(key)is not str or len(key)>64:raise ValueError('TelemetryKey')
    size+=self.measure(key,depth+1)+1+self.measure(item,depth+1)
    if size>self.row_bytes:raise ValueError('TelemetryRow')
   return size
  raise ValueError('TelemetryValueType')
 def append(self,row):
  with self.lock:
   if self.invalid:return False
   try:
    try:estimated=self.measure(row)+1
    except ValueError:self.fail('TelemetrySchema');return False
    if estimated>self.row_bytes:self.fail('TelemetryCapacity');return False
    raw=(json.dumps(row,separators=(',',':'),allow_nan=False)+'\n').encode()
    if len(raw)>self.row_bytes or self.rows>=self.max_rows or self.used+len(raw)>self.max_bytes:self.fail('TelemetryCapacity');return False
    with self.path.open('ab')as stream:
     if stream.write(raw)!=len(raw):raise OSError('shortwrite')
    self.rows+=1;self.used+=len(raw);return True
   except Exception:self.fail('TelemetryWrite');return False
 def status(self):
  with self.lock:return{'rows':self.rows,'bytes':self.used,'invalid':list(self.invalid),'max_rows':self.max_rows,'max_bytes':self.max_bytes}

class Identities:
 """Only weakrefs/ordinals/scalars; never frames or strong application objects."""
 def __init__(self,journal,capacity=128):self.j=journal;self.capacity=capacity;self.rows={};self.next=0;self.lock=threading.RLock()
 def bind(self,obj,kind,metadata=None):
  pointer=id(obj)
  with self.lock:
   current=self.rows.get(pointer)
   if current is not None:
    if current['weak']()is obj:return current['ordinal']
    self.j.fail('IdentityReuseAmbiguous');return None
   if len(self.rows)>=self.capacity:self.j.fail('IdentityCapacity');return None
   self.next+=1;ordinal=self.next
   def gone(_):
    with self.lock:
     row=self.rows.get(pointer)
     if row is not None and row['ordinal']==ordinal:self.rows.pop(pointer)
   try:reference=weakref.ref(obj,gone)
   except TypeError:self.j.fail('IdentityNotWeakrefable');return None
   safe=dict(metadata or{})
   if any(k not in['role','sid_sha256','connection','generator','creator_generator','profile','action']or type(v)not in[str,int,bool,type(None)]or(type(v)is str and len(v)>128)or(type(v)is int and not -9223372036854775808<=v<=9223372036854775807)for k,v in safe.items()):self.j.fail('IdentityMetadataNotScalar');return None
   self.rows[pointer]={'weak':reference,'ordinal':ordinal,'kind':kind,'metadata':safe};return ordinal
 def get(self,obj):
  with self.lock:
   row=self.rows.get(id(obj));return row if row is not None and row['weak']()is obj else None
 def count(self):
  with self.lock:return len(self.rows)

class Probe:
 def __init__(self,journal,owned_sids,owned_profiles,context=None,max_frames=128,max_codes=48,wire_ports=(6383,)):
  if not 1<=len(owned_sids)<=16 or any(type(k)is not str or len(k)>128 for k in owned_sids)or len(owned_profiles)>16 or any(type(v)is not int or not 1<=v<=9223372036854775807 for v in owned_profiles):raise ValueError('OwnedDeclarationBound')
  self.j=journal;self.sids=dict(owned_sids);self.profiles=set(owned_profiles);self.identities=Identities(journal);self.local=context or threading.local();self.max_frames=max_frames;self.max_codes=max_codes;self.rules={};self.frames={};self.generators={};self.callbacks={};self.next_frame=0;self.lock=threading.RLock();self.started=False;self.wire_ports=set(wire_ports)
 def add(self,code,kind='function',markers=None):
  if not isinstance(code,types.CodeType)or len(self.rules)>=self.max_codes:self.j.fail('CodeRegistryCapacity');return
  instructions=list(dis.get_instructions(code));previous={instruction.offset:(instructions[index-1].opname if index else None)for index,instruction in enumerate(instructions)}
  self.rules[code]={'previous_ops':previous,'kind':kind,'code':hashlib.sha256(code.co_code).hexdigest(),'firstline':code.co_firstlineno,'ops':{i.offset:i.opname for i in dis.get_instructions(code)},'markers':dict(markers or{})}
 @contextlib.contextmanager
 def request(self,role,sid):
  if sid not in self.sids or role not in['initial','target','follower','other']:raise ValueError('OwnedRequestContext')
  previous=getattr(self.local,'current',None);self.local.current={'role':role,'sid_sha256':hashlib.sha256(sid.encode()).hexdigest()}
  try:yield
  finally:self.local.current=previous
 def emit(self,kind,row,**extra):
  value={k:v for k,v in row.items()if k in['frame','role','sid_sha256','generator','connection','callback','creator_generator','code','firstline','profile','writer_frame','parent_frame','rule_kind','action']};value.update(kind=kind,at=time.monotonic(),logical_thread_id=threading.get_ident(),native_thread_id=threading.get_native_id(),**extra);self.j.append(value)
 def bind_generator(self,generator,ctx=None):
  if not self.started:raise ValueError('TraceStartupBeforeGeneratorBindingRequired')
  if type(generator)is not types.GeneratorType:self.j.fail('GeneratorTypeRequired');return None
  frame=generator.gi_frame
  if frame is None:self.j.fail('GeneratorAlreadyFinished');return None
  context={k:v for k,v in dict(ctx or getattr(self.local,'current',None)or{}).items()if k in['role','sid_sha256','connection','generator','creator_generator','profile','action']};ordinal=self.identities.bind(generator,'generator',context)
  if ordinal is None:return None
  # weakref does not keep generator/frame alive. Purge stale frame IDs on loss.
  frame_id=id(frame)
  def gone(_):
   with self.lock:
    known=self.generators.get(frame_id)
    if known is not None and known['ordinal']==ordinal:self.generators.pop(frame_id,None);self.frames.pop(frame_id,None)
  if len(self.generators)>=self.max_frames:self.j.fail('GeneratorCapacity');return None
  reference=weakref.ref(generator,gone);self.generators[frame_id]={'weak':reference,'ordinal':ordinal,**context};self.emit('generator_bound',{'generator':ordinal,**context});return ordinal
 def bind_callback(self,function,row,connection=None):
  if type(function)is not types.FunctionType:self.j.fail('CallbackFunctionType');return
  metadata={k:v for k,v in row.items()if k in['role','sid_sha256','generator','connection','action']};metadata['creator_generator']=metadata.pop('generator',None)
  if connection is not None:metadata['connection']=self.identities.bind(connection,'connection')
  ordinal=self.identities.bind(function,'callback',metadata)
  if ordinal is None:return
  key=(function.__code__,metadata.get('connection'))
  previous=self.callbacks.get(key)
  if previous is not None and previous()is not None and previous()is not function:self.j.fail('CallbackIdentityAmbiguous');return
  if key not in self.callbacks and len(self.callbacks)>=self.max_frames:self.j.fail('CallbackCapacity');return
  def gone(reference):
   with self.lock:
    if self.callbacks.get(key)is reference:self.callbacks.pop(key,None)
  self.callbacks[key]=weakref.ref(function,gone);self.emit('callback_bound',{'callback':ordinal,**metadata})
 def connection(self,frame):
  for name in['redis_connection','conn','self']:
   obj=frame.f_locals.get(name)
   if obj is None:continue
   try:fields=object.__getattribute__(obj,'__dict__')
   except (AttributeError,TypeError):continue
   sid=fields.get('session_id')
   if type(sid)is str and sid in self.sids:
    ordinal=self.identities.bind(obj,'connection');return obj,ordinal,sid
  return None,None,None
 def context(self,frame):
  current=dict(getattr(self.local,'current',None)or{});obj,connection,sid=self.connection(frame)
  if connection is not None:current.update(connection=connection,sid_sha256=hashlib.sha256(sid.encode()).hexdigest())
  parent=frame.f_back
  for _ in range(32):
   if parent is None:break
   known=self.frames.get(id(parent))
   if known is not None:
    current['parent_frame']=known['frame']
    for key in['role','sid_sha256','generator','connection','callback','creator_generator','profile','writer_frame','action']:
     if key not in current and key in known:current[key]=known[key]
    break
   parent=parent.f_back
  if 'role'not in current and sid is not None:current.update(role='owned_object',action='known_owned_object')
  for name in['session_id','client_id']:
   value=frame.f_locals.get(name)
   if type(value)is str and value in self.sids:
    current.setdefault('sid_sha256',hashlib.sha256(value.encode()).hexdigest())
    if 'role'not in current:current.update(role='owned_object',action='known_owned_argument')
    break
  return current,obj
 def wire_link(self,frame,row):
  # Read only the already-borrowed connection/socket. Never request a pool
  # connection, read command arguments, SETNAME or issue Redis commands.
  connection=frame.f_locals.get('conn',frame.f_locals.get('connection'))
  if connection is None:return
  try:fields=object.__getattribute__(connection,'__dict__');sock=fields.get('_sock')
  except (AttributeError,TypeError):return
  if sock is None:return
  try:
   local=sock.getsockname();remote=sock.getpeername();descriptor=sock.fileno();inode=os.fstat(descriptor).st_ino
  except OSError:return
  if not isinstance(local,tuple)or not isinstance(remote,tuple)or len(local)<2 or len(remote)<2 or remote[1]not in self.wire_ports:return
  pair=(local[1],remote[1],inode)
  if row.get('wire_pair')==pair:return
  row['wire_pair']=pair
  self.emit('wire_link',row,local_port=local[1],remote_port=remote[1],socket_inode=inode)
 def _trace(self,frame,event,arg):
  rule=self.rules.get(frame.f_code)
  if rule is None:return None
  if self.j.invalid:return None
  pointer=id(frame)
  with self.lock:
   row=self.frames.get(pointer)
   if event=='call'and row is None:
    if len(self.frames)>=self.max_frames:self.j.fail('FrameCapacity');return None
    ctx,connection=self.context(frame)
    if rule['kind']=='connection_ctor':
     sid=frame.f_locals.get('session_id');obj=frame.f_locals.get('self')
     if type(sid)is str and sid in self.sids and obj is not None:ctx.update(connection=self.identities.bind(obj,'connection'),sid_sha256=hashlib.sha256(sid.encode()).hexdigest())
    for name in['profile_id','m3u_profile_id']:
     profile=frame.f_locals.get(name)
     owned_profile=profile if type(profile)is int and profile in self.profiles else next((value for value in self.profiles if type(profile)is str and profile==str(value)),None)
     if owned_profile is not None:
      ctx['profile']=owned_profile
      if 'role'not in ctx:ctx.update(role='owned_profile',action='known_owned_profile')
    registered=self.generators.get(pointer)
    if registered is not None:
     if registered['weak']()is None:self.j.fail('DeadGeneratorIdentity');return None
     ctx.update({k:v for k,v in registered.items()if k!='weak'and k!='ordinal'});ctx['generator']=registered['ordinal']
    if rule['kind']=='callback':
     reference=self.callbacks.get((frame.f_code,ctx.get('connection')));function=reference()if reference else None;identity=self.identities.get(function)if function is not None else None
     if identity is None:self.j.fail('CallbackCreatorCoverage');return None
     ctx.update(identity['metadata']);ctx['callback']=identity['ordinal']
    if 'role'not in ctx:return None
    if any(type(v)not in[str,int,bool,type(None)]for v in ctx.values()):self.j.fail('FrameContextNotScalar');return None
    if frame.f_code.co_flags&0x20 and 'generator'not in ctx:self.j.fail('GeneratorIdentityCoverage');return None
    self.next_frame+=1;row={**ctx,'frame':self.next_frame,'code':rule['code'],'firstline':rule['firstline'],'finally_entered':False,'rule_kind':rule['kind']}
    if rule['kind']=='counter':row['writer_frame']=row['frame']
    self.frames[pointer]=row;self.emit('call',row)
    if rule['kind']=='generator_ctor':self.bind_generator(frame.f_locals.get('generator'),ctx)
   if row is None:return self.trace
   if rule['kind']=='wire_call'and event in['call','line']:self.wire_link(frame,row)
   if event=='line'and frame.f_lineno in rule['markers']:
    marker=rule['markers'][frame.f_lineno]
    if marker=='finally_enter':row['finally_entered']=True
    if marker=='callback_create':self.bind_callback(frame.f_locals.get('delayed_cleanup'),row,self.connection(frame)[0])
    flags={}
    for name in['stream_decremented','profile_decremented','has_remaining','active_streams_reserved','profile_connections_incremented','session_is_new']:
     value=frame.f_locals.get(name)
     if type(value)is bool:flags[name]=value
    self.emit(marker,row,line=frame.f_lineno,flags=flags)
   elif event=='exception':
    row['exception_pending']=True;name=arg[0].__name__;self.emit('exception',row,exception=name[:64])
   elif event=='return':
    op=rule['ops'].get(frame.f_lasti,'UNKNOWN')
    resume_after_yield=(sys.implementation.name=='cpython'and sys.version_info[:2]==(3,13)and op=='RESUME'and rule['previous_ops'].get(frame.f_lasti)=='YIELD_VALUE')
    if op in['YIELD_VALUE','YIELD_FROM']or resume_after_yield:
     self.emit('yield',row,op=op,cpython313_resume_after_yield=resume_after_yield,yield_bytes=len(arg)if type(arg)is bytes else None,finally_entered=row['finally_entered']);return self.trace
    normal=op in['RETURN_VALUE','RETURN_CONST']
    if frame.f_code.co_flags&0x20 and not normal and op not in['RERAISE','RAISE_VARARGS']and not row.get('exception_pending'):self.j.fail('GeneratorReturnOpcodeUnsupported');return None
    detail={}
    if rule['kind']=='state_read'and normal:
     if arg is None:detail['state_marker']={'present':False}
     else:
      values=object.__getattribute__(arg,'__dict__');created=values.get('created_at');content=values.get('content_uuid');profile=values.get('m3u_profile_id')
      if type(created)not in[int,float]or not math.isfinite(created)or type(content)is not str or len(content)>128 or type(profile)not in[int,str,type(None)]or(type(profile)is str and len(profile)>19)or(type(profile)is int and not 1<=profile<=9223372036854775807):self.j.fail('GenerationMarkerSchema');return None
      detail['state_marker']={'present':True,'created_at':created,'content_uuid_sha256':hashlib.sha256(content.encode()).hexdigest(),'profile':profile}
    if rule['kind']in['counter','active_decr']:
     if type(arg)in[int,bool,type(None)]:detail['result']=arg
     elif type(arg)is tuple and len(arg)==2 and all(type(v)is bool for v in arg):detail['result']=list(arg)
     else:detail['result_type']=type(arg).__name__[:64]
    self.emit('normal_exit'if normal else'unwind',row,op=op,finally_entered=row['finally_entered'],is_generator=bool(frame.f_code.co_flags&0x20),**detail)
    self.frames.pop(pointer,None)
    if frame.f_code.co_flags&0x20:self.generators.pop(pointer,None)
   return self.trace
 def trace(self,frame,event,arg):
  try:return self._trace(frame,event,arg)
  except Exception:self.j.fail('TraceCallbackFailure');return None
 def start(self):
  if self.started or sys.gettrace()is not None:raise ValueError('ExistingTraceMustNotBeReplaced')
  self.started=True;self.previous_thread_trace=threading.gettrace();
  if self.previous_thread_trace is not None:self.started=False;raise ValueError('ExistingThreadTraceMustNotBeReplaced')
  threading.settrace(self.trace);sys.settrace(self.trace)
 def stop(self):
  if sys.gettrace()!=self.trace or threading.gettrace()!=self.trace:self.j.fail('TraceOwnershipLost');return
  sys.settrace(None);threading.settrace(self.previous_thread_trace);self.started=False

class RedisEffects:
 """Typed passive reply analyzer. QUEUED/no-op/EXEC-abort never imply a write.
No Redis method/command execution. Caller must supply exactly-owned key roles.
"""
 def __init__(self,journal,max_connections=64,max_pending=128):self.j=journal;self.max_connections=max_connections;self.max_pending=max_pending;self.rows={};self.sequence=0
 def command(self,connection,family,key_role=None):
  if type(connection)is not int or not 1<=connection<=9223372036854775807 or key_role not in[None,'profile_counter','metadata']or family not in['MULTI','EXEC','DISCARD','WATCH','UNWATCH','GET','INCR','DECR','DEL','OTHER']:self.j.fail('WireCommandSchema');return
  if connection not in self.rows:
   if len(self.rows)>=self.max_connections:self.j.fail('WireConnectionCapacity');return
   self.rows[connection]={'pending':[],'transaction':None}
  row=self.rows[connection]
  if len(row['pending'])>=self.max_pending:self.j.fail('WirePendingCapacity');return
  self.sequence+=1;row['pending'].append({'family':family,'key_role':key_role,'sequence':self.sequence,'command_at':time.monotonic()})
 def effect(self,connection,command,value,via):
  if command['key_role']!='profile_counter'or command['family']not in['INCR','DECR']:return
  if type(value)is not int or not -9223372036854775808<=value<=9223372036854775807:self.j.fail('CounterReplySchema');return
  self.j.append({'kind':'actual_counter_commit','at':time.monotonic(),'connection':connection,'sequence':command['sequence'],'command_at':command['command_at'],'family':command['family'],'value':value,'via':via})
 def reply(self,connection,value):
  row=self.rows.get(connection)
  if row is None or not row['pending']:self.j.fail('WireReplyWithoutCommand');return
  command=row['pending'].pop(0);family=command['family']
  status=value
  if family=='MULTI'or(row['transaction']is not None and family not in['EXEC','DISCARD']):
   try:status=value.decode('ascii')if type(value)is bytes else value
   except UnicodeDecodeError:self.j.fail('WireStatusEncoding');return
  if family=='MULTI':
   if status!='OK'or row['transaction']is not None:self.j.fail('WireMULTISchema');return
   row['transaction']=[]
  elif family=='EXEC':
   queued=row['transaction'];row['transaction']=None
   if queued is None:self.j.fail('WireEXECWithoutMULTI');return
   if value is None:self.j.append({'kind':'transaction_aborted','at':time.monotonic(),'connection':connection});return
   if type(value)is not list or len(value)!=len(queued):self.j.fail('WireEXECReplySchema');return
   for item,result in zip(queued,value):self.effect(connection,item,result,'EXEC')
  elif family=='DISCARD':row['transaction']=None
  elif row['transaction']is not None:
   if status!='QUEUED'or len(row['transaction'])>=self.max_pending:self.j.fail('WireQueuedSchema');return
   row['transaction'].append(command)
  else:self.effect(connection,command,value,'direct')
 def close(self,connection):
  row=self.rows.pop(connection,None)
  if row is None or row['pending']or row['transaction']is not None:self.j.fail('WireConnectionIncomplete')

class Liveness:
 """Generator progress is independent of consumer-buffer/origin-write evidence."""
 def __init__(self,max_identities=128,max_progress=128):self.zero_at=None;self.closed=set();self.finalizing=set();self.qualified=[];self.max_identities=max_identities;self.max_progress=max_progress;self.invalid=False;self.bound=set()
 def observe(self,event):
  kind=event['kind'];generator=event.get('generator')
  if self.invalid:return
  if len(self.bound|self.closed|self.finalizing)>=self.max_identities or len(self.qualified)>=self.max_progress:self.invalid=True;return
  at=event.get('at')
  if type(at)not in[int,float]or not math.isfinite(at):self.invalid=True;return
  if kind=='generator_bound':
   if type(generator)is not int or generator<1:self.invalid=True;return
   self.bound.add(generator)
  elif kind=='actual_counter_commit':
   if type(event.get('value'))is not int:self.invalid=True;return
   if event['value']==0:self.zero_at=at
  elif kind=='finally_enter':self.finalizing.add(generator)
  elif kind in['normal_exit','unwind']and event.get('is_generator')is True:self.closed.add(generator)
  elif kind=='yield'and event.get('role')=='other'and self.zero_at is not None and event['at']>self.zero_at and type(generator)is int and generator in self.bound and generator not in self.closed and generator not in self.finalizing and type(event.get('yield_bytes'))is int and event['yield_bytes']>0:self.qualified.append({'generator':generator,'at':event['at'],'counter_zero_at':self.zero_at})
