"""Fixture-only abortable raw TCP consumer; never cancels a server handler.
No network operation occurs on import. No authentication headers are retained.
"""
import http.client,os,socket,threading,time
from urllib.parse import urlsplit

class Response:
 def __init__(self,connection,response,sock):
  self.connection=connection;self.raw=response;self.status_code=response.status;self.headers=dict(response.getheaders());self._fixture_socket=sock
 def close(self):
  try:self.raw.close()
  finally:self.connection.close()

class Target:
 def __init__(self,base):
  url=urlsplit(base)
  if url.scheme!='http'or url.hostname not in['127.0.0.1','candidate']or url.port not in[5657,9192]:raise ValueError('OwnedTargetRequired')
  self.connection=http.client.HTTPConnection(url.hostname,url.port,timeout=8);self.lock=threading.Lock();self.sock=None;self.receipt=None
 def request(self,path,headers):
  try:
   self.connection.request('GET',path,headers=headers)
   with self.lock:
    self.sock=self.connection.sock
    if self.sock is None:raise ValueError('TargetSocketNotOwned')
   response=self.connection.getresponse()
   return Response(self.connection,response,self.sock)
  except BaseException:
   self.connection.close();raise
 def abort(self):
  with self.lock:
   if self.sock is None or self.receipt is not None:raise ValueError('TargetAbortSocketMissingOrRepeated')
   fd=self.sock.fileno()
   if fd<0:raise ValueError('TargetSocketAlreadyClosedBeforeAbort')
   self.receipt={'role':'target','close_begin':time.monotonic(),'socket_observed':True,'fd_before':fd,'inode_before':os.fstat(fd).st_ino,'server_cancellation_inferred':False}
   self.sock.shutdown(socket.SHUT_RDWR);self.receipt['local_shutdown']=True;self.connection.close();self.sock.close();self.receipt['close_complete']=time.monotonic();self.receipt['fd_immediately_after_shutdown']=self.sock.fileno();self.receipt['physical_close_pending_until_reader_join']=True
   return dict(self.receipt)
 def joined(self):
  with self.lock:
   if self.receipt is None:raise ValueError('TargetAbortReceiptMissing')
   self.connection.close();self.sock.close();self.receipt['fd_after']=self.sock.fileno()
   if self.receipt['fd_after']!=-1:raise ValueError('TargetAbortDescriptorNotClosedAfterJoin')
   try:old_inode_still_open=os.fstat(self.receipt['fd_before']).st_ino==self.receipt['inode_before']
   except OSError:old_inode_still_open=False
   self.receipt.update(reader_joined_monotonic=time.monotonic(),reader_joined=True,original_descriptor_identity_absent=not old_inode_still_open)
   if old_inode_still_open:raise ValueError('TargetOriginalDescriptorStillOpen')
   return dict(self.receipt)
