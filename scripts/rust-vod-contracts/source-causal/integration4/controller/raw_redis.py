"""Bounded local Redis protocol control; no activity on import/application hooks."""
import socket
class RedisFailure:pass
class Client:
 def __init__(self,port):self.socket=socket.create_connection(('127.0.0.1',port),timeout=3);self.socket.settimeout(3);self.bytes=0
 def send(self,*args):
  data=[str(x).encode()for x in args]
  if len(data)>128 or sum(map(len,data))>8192:raise ValueError('OwnCommandCapacity')
  self.socket.sendall(b'*'+str(len(data)).encode()+b'\r\n'+b''.join(b'$'+str(len(x)).encode()+b'\r\n'+x+b'\r\n'for x in data));self.bytes=0;return self.read()
 def take(self,count):
  if self.bytes+count>1048576:raise ValueError('OwnReplyCapacity')
  data=bytearray()
  while len(data)<count:
   part=self.socket.recv(count-len(data))
   if not part:raise EOFError('OwnReplyEOF')
   data.extend(part)
  self.bytes+=count;return bytes(data)
 def line(self):
  data=bytearray()
  while not data.endswith(b'\r\n'):
   if len(data)>=8192:raise ValueError('OwnReplyLineCapacity')
   data.extend(self.take(1))
  return bytes(data[:-2])
 def read(self,depth=0):
  if depth>16:raise ValueError('OwnReplyDepth')
  tag=self.take(1);line=self.line()
  if tag==b'+':return line
  if tag==b'-':return RedisFailure()
  if tag==b':':return int(line)
  if tag==b'$':
   count=int(line)
   if count==-1:return None
   if not 0<=count<=1048576:raise ValueError('OwnBulkCapacity')
   value=self.take(count)
   if self.take(2)!=b'\r\n':raise ValueError('OwnBulkTerminator')
   return value
  if tag==b'*':
   count=int(line)
   if count==-1:return None
   if not 0<=count<=128:raise ValueError('OwnArrayCapacity')
   return[self.read(depth+1)for _ in range(count)]
  raise ValueError('OwnReplyType')
 def close(self):self.socket.close()
