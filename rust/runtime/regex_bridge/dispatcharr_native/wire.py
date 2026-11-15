"""Private framed full-duplex callback channel, not a web/broker/SQL server.
The Rust owner provides stdin/stdout pipes and owns their actual EOF/child wait.
"""
import contextvars,io,json,os,queue,struct,threading,time
MAX_FRAME=8*1024*1024
operation=contextvars.ContextVar('native_operation',default=None)
class CompatibilityUnavailable(RuntimeError):pass
class Channel:
 def __init__(self,reader,writer):
  self.reader=reader;self.writer=writer;self.lock=threading.Lock();self.state=threading.Lock();self.pending={};self.serial=0;self.calls=queue.Queue(16);self.closed=False;self.failure=None
  self.thread=threading.Thread(target=self._read,name='native-plugin-rpc',daemon=False);self.thread.start()
 def _exact(self,size):
  result=bytearray()
  while len(result)<size:
   chunk=self.reader.read(size-len(result))
   if not chunk:raise EOFError('Actual native plugin pipe EOF')
   result.extend(chunk)
  return bytes(result)
 def send(self,value):
  raw=json.dumps(value,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf8')
  if len(raw)>MAX_FRAME:raise CompatibilityUnavailable('Native plugin frame capacity')
  with self.lock:
   self.writer.write(struct.pack('!I',len(raw)));self.writer.write(raw);self.writer.flush()
 def _read(self):
  try:
   while True:
    size=struct.unpack('!I',self._exact(4))[0]
    if size>MAX_FRAME:raise CompatibilityUnavailable('Native plugin frame capacity')
    value=json.loads(self._exact(size));kind=value.get('kind')
    if kind=='callback_result':
     with self.state:target=self.pending.get(value.get('id'))
     if target is None:raise CompatibilityUnavailable('Unowned callback reply')
     target.put_nowait(value)
    elif kind=='call':self.calls.put_nowait(value)
    else:raise CompatibilityUnavailable('Unknown native plugin frame')
  except BaseException as error:
   self.failure=type(error).__name__;self.closed=True
   with self.state:targets=list(self.pending.values())
   for target in targets:
    try:target.put_nowait({'transport_failure':self.failure})
    except queue.Full:pass
   try:self.calls.put_nowait(None)
   except queue.Full:pass
 def call(self,port,method,params):
  if self.closed:raise CompatibilityUnavailable('Native plugin callback channel closed')
  with self.state:
   if len(self.pending)>=64:raise CompatibilityUnavailable('Native plugin callback capacity')
   self.serial+=1;ident=self.serial;reply=queue.Queue(1);self.pending[ident]=reply
  try:
   self.send({'kind':'callback','id':ident,'operation':operation.get(),'port':port,'method':method,'params':params})
   # No observer timeout can free the Rust owner while the physical callback
   # remains outstanding. Child drain/pipe closure wakes all pending callers.
   value=reply.get()
   if value.get('transport_failure'):raise CompatibilityUnavailable(value['transport_failure'])
   if value.get('error')is not None:raise CompatibilityUnavailable(str(value['error']))
   return value['result']
  finally:
   with self.state:self.pending.pop(ident,None)
 def close_writer(self):
  with self.lock:self.writer.close()
_current=None
def install(channel):
 global _current
 if _current is not None:raise CompatibilityUnavailable('Native plugin channel already installed')
 _current=channel
def current():
 if _current is None:raise CompatibilityUnavailable('Native plugin channel is not installed')
 return _current
