"""Owned internal Native-only origin: fixed replies and explicit held-wire modes.
No Source imports, public network, dynamic reply fitting or handler substitution.
"""
import argparse,hashlib,json,os,re,signal,socket,time
from pathlib import Path
IP='11.253.250.2';SOURCE='11.253.250.3';PORT=18080
need=lambda ok,label:None if ok else (_ for _ in()).throw(ValueError(label))
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def fixed(plan,path,occurrence):
 rows=plan['origin_definition']['fixtures'].get(path);need(type(rows)is list and rows and(len(rows)==1 or occurrence<=len(rows)),'PredeclaredFiniteOriginPathOccurrence');row=rows[min(occurrence-1,len(rows)-1)];body=row['body'].encode();need(len(body)<=4096 and type(row['status'])is int and 200<=row['status']<=599,'FixedBoundedReply');return row,body

def main():
 p=argparse.ArgumentParser();p.add_argument('--mode',choices=['normal','headers-abort','body-deadline'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--plan',type=Path,required=True);a=p.parse_args();out=a.output;plan=json.loads(a.plan.read_bytes());need(out.is_dir()and not(out/'origin-ready.json').exists(),'FreshOrigin');server=socket.socket();server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);server.bind((IP,PORT));server.listen(4);server.settimeout(.1);running=True;rows=[];counts={};began=time.monotonic()
 def stop(*_):
  nonlocal running
  running=False
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop);save(out/'origin-ready.json',{'PID':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'PID_namespace':os.readlink('/proc/self/ns/pid'),'address':[IP,PORT],'mode':a.mode,'Source_application':False})
 try:
  while running and time.monotonic()-began<900 and len(rows)<32:
   try:client,peer=server.accept()
   except socket.timeout:continue
   client.settimeout(5);row={'ordinal':len(rows)+1,'peer':list(peer),'accepted_epoch':time.time(),'actual_socket_inode':os.fstat(client.fileno()).st_ino,'valid':False};wire=b''
   try:
    need(peer[0]==SOURCE,'OnlyDeclaredOwnedNativePeer')
    while b'\r\n\r\n'not in wire:
     need(len(wire)<8192,'RequestHeaders8KiB');part=client.recv(min(8192-len(wire),2048));need(part,'CompleteGETHeaders');wire+=part
    first=wire.split(b'\r\n',1)[0].decode('ascii');method,path,version=first.split(' ');need(method=='GET'and version=='HTTP/1.1','ActualNativeGET11');row.update(method=method,path=path,request_header_hex=wire.hex(),request_header_sha256=hashlib.sha256(wire).hexdigest());counts[path]=counts.get(path,0)+1;row['path_occurrence']=counts[path]
    if a.mode=='normal':reply,body=fixed(plan,path,counts[path])
    else:
     need(path=='/physical/'+a.mode+'.json'and len(rows)==0,'OneExactPhysicalRoute');reply={'status':200,'content_type':'application/json'};body=b'{"manifest":{"registry_name":"Owned Physical","plugins":[]}}'
    headers=[f'HTTP/1.0 {reply["status"]} Owned',f'Content-Type: {reply["content_type"]}',f'Content-Length: {len(body)}','Connection: close']
    if 'location'in reply:headers.append('Location: '+reply['location'])
    rawhead=('\r\n'.join(headers)+'\r\n\r\n').encode();row.update(status=reply['status'],content_type=reply['content_type'],body_hex=body.hex(),body_sha256=hashlib.sha256(body).hexdigest(),header_hex=rawhead.hex())
    if a.mode!='normal':
     if a.mode=='body-deadline':client.sendall(rawhead+body[:1])
     held=time.monotonic();save(out/'origin-wire-held.json',{'mode':a.mode,'actual_peer':list(peer),'actual_socket_inode':row['actual_socket_inode'],'held_monotonic':held,'header_bytes_sent':0 if a.mode=='headers-abort'else len(rawhead),'body_bytes_sent':0 if a.mode=='headers-abort'else 1,'remaining_wire_bytes':len(rawhead)+len(body)if a.mode=='headers-abort'else len(body)-1,'actual_request_header_sha256':row['request_header_sha256']})
     while not(out/'origin-wire-release').is_file():need(running and time.monotonic()-held<5,'FixedHeldWire5Seconds');time.sleep(.01)
     released=time.monotonic();need(.25<=released-held<5,'ActualFiniteSustainedHold');row.update(held_monotonic=held,released_monotonic=released,actual_hold_seconds=released-held);client.sendall(rawhead+body if a.mode=='headers-abort'else body[1:])
     # A real peer read EOF/reset is a separate observed terminal, never inferred
     # from the Native handle's logical completion or from JSON success alone.
     try:extra=client.recv(1);need(extra==b'','ActualNoExtraRequestAndClientEOF');fate='actual_read_EOF'
     except ConnectionResetError:fate='actual_connection_reset'
     save(out/'origin-client-eof.json',{'mode':a.mode,'actual_socket_inode':row['actual_socket_inode'],'fate':fate,'observed_monotonic':time.monotonic(),'source_or_mock_client':False})
    else:client.sendall(rawhead+body)
    row.update(valid=True,wire_write_complete=True)
   except BaseException as error:row['error']={'type':type(error).__name__,'message':str(error)}
   finally:client.close();row['closed_epoch']=time.time();rows.append(row);(out/'origin-requests.jsonl').open('a').write(json.dumps(row,allow_nan=False)+'\n')
 finally:server.close();save(out/'origin-terminal.json',{'actual_connections':len(rows),'valid_all':all(row['valid']for row in rows),'counts':counts,'server_closed':True,'stopped_by_signal':not running,'elapsed_seconds':time.monotonic()-began,'no_Source_app':True})
if __name__=='__main__':main()
