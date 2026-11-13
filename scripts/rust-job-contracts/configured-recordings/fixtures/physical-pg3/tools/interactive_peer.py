"""Bounded direct-child interactive QA driver; no process starts on import.
Uses the qualified collector's actual wait/pipe ownership. Physical controller
must still supply an exact-owned stop callback and retain all raw diagnostics.
"""
import json,queue,time
from bounded_process import Collected,CollectionFailure
PREFIX=b'NATIVE_CONFIGURED_RECORDINGS_JSON='
class Peer(Collected):
 def __init__(self,argv,environment,stop_owned,limit=1048576):
  self.commands=queue.Queue(maxsize=16);self.input_bytes=0;self.cursor=0;self.pending=bytearray();self.writer_error=None
  super().__init__(argv,environment,limit,stop_owned,body=b'')
 def _write(self,unused):
  try:
   while True:
    raw=self.commands.get()
    if raw is None:break
    remaining=memoryview(raw)
    while remaining:
     written=self.process.stdin.write(remaining)
     if written is None or written<=0:raise OSError('OwnedPeerShortWrite')
     remaining=remaining[written:]
    self.process.stdin.flush()
  except BaseException as error:self.writer_error=type(error).__name__+':'+str(error)[:256]
  finally:
   try:self.process.stdin.close()
   except OSError:pass
 def send(self,value):
  raw=(json.dumps(value)+'\n').encode();self.input_bytes+=len(raw)
  if len(raw)>8192 or self.input_bytes>65536:raise ValueError('PeerInputCapacity')
  self.commands.put_nowait(raw)
 def poll_response(self,seconds=0):
  deadline=time.monotonic()+seconds
  while True:
   with self.lock:
    raw=bytes(self.output['stdout'][self.cursor:]);self.cursor+=len(raw);invalid=self.invalid
   self.pending.extend(raw)
   while b'\n'in self.pending:
    line,_,tail=self.pending.partition(b'\n');self.pending=bytearray(tail)
    if PREFIX in line:return json.loads(line.split(PREFIX,1)[1])
   if invalid:raise CollectionFailure(invalid)
   if self.writer_error:raise CollectionFailure('PeerWriter:'+self.writer_error)
   if self.process.poll()is not None:raise CollectionFailure('ActualPeerEOFBeforeResponse')
   if time.monotonic()>=deadline:return None
   time.sleep(.01)
 def receive(self,seconds=35):
  value=self.poll_response(seconds)
  if value is None:
   self.abort('PeerResponseDeadline');raise CollectionFailure('PeerResponseDeadline')
  return value
 def finish(self,seconds=35):
  self.commands.put_nowait(None)
  result=self.wait(seconds)
  if self.writer_error:
   self.invalid=self.invalid or('PeerWriter:'+self.writer_error);raise CollectionFailure(self.invalid)
  return result
