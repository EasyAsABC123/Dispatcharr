"""Finite Native-only origin for four NEW physical cases; never Source/app import."""
import argparse,hashlib,json,os,signal,socket,time
from pathlib import Path
IP='11.253.250.2';PEER='11.253.250.3';PORT=18080
need=lambda ok,label:None if ok else (_ for _ in()).throw(ValueError(label))
def save(p,v):p.write_text(json.dumps(v,indent=2,allow_nan=False)+'\n')
def reply(case,path,ordinal,refresh):
 allowed={1:['/physical/frontend-fd-loss.json'],2:['/refresh.json'],3:['/refresh.json'],4:['/physical/frame-1.json','/physical/frame-2.json','/physical/frame-reuse.json']}
 need(type(case)is int and case in allowed and type(ordinal)is int and 1<=ordinal<=len(allowed[case])and path==allowed[case][ordinal-1],'ExactFiniteCaseOriginOrderNoFifthContact')
 body=refresh if case in(2,3)else b'{"manifest":{"registry_name":"Owned Physical","plugins":[]}}'
 need(0<len(body)<=4096,'BoundedFixedOriginBody');return body

def main():
 p=argparse.ArgumentParser();p.add_argument('--case',type=int,choices=[1,2,3,4],required=True);p.add_argument('--refresh-body',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();refresh=a.refresh_body.read_bytes();need(len(refresh)<=4096,'ActualRefreshBody4KiB');out=a.output;need(out.is_dir()and not(out/'origin-ready.json').exists(),'FreshOriginNamespace');server=socket.socket();server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);server.bind((IP,PORT));server.listen(4);server.settimeout(.1);running=True;rows=[];began=time.monotonic()
 def stop(*_):
  nonlocal running
  running=False
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop);save(out/'origin-ready.json',{'PID':os.getpid(),'starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'PID_namespace':os.readlink('/proc/self/ns/pid'),'address':[IP,PORT],'case':a.case,'Source_application':False})
 try:
  while running and time.monotonic()-began<900 and len(rows)<8:
   try:client,peer=server.accept()
   except socket.timeout:continue
   client.settimeout(5);row={'ordinal':len(rows)+1,'peer':list(peer),'accepted_epoch':time.time(),'actual_socket_inode':os.fstat(client.fileno()).st_ino,'valid':False};wire=b''
   try:
    need(peer[0]==PEER,'OnlyOwnedNativePeer')
    while b'\r\n\r\n'not in wire:
     need(len(wire)<8192,'RequestHeaders8KiB');part=client.recv(min(2048,8192-len(wire)));need(part,'ActualCompleteGETHeaders');wire+=part
    head,tail=wire.split(b'\r\n\r\n',1);need(not tail,'NoPipelinedOrBodyBytes');method,path,version=head.split(b'\r\n',1)[0].decode('ascii').split(' ');need(method=='GET'and version=='HTTP/1.1','ActualGET11');body=reply(a.case,path,len(rows)+1,refresh);rawhead=f'HTTP/1.0 200 Owned\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\nConnection: close\r\n\r\n'.encode();row.update(method=method,path=path,status=200,request_header_hex=wire.hex(),request_header_sha256=hashlib.sha256(wire).hexdigest(),body_hex=body.hex(),body_sha256=hashlib.sha256(body).hexdigest(),body_bytes=len(body),header_hex=rawhead.hex())
    if a.case==1:
     held=time.monotonic();save(out/'origin-wire-held.json',{'case':1,'actual_peer':list(peer),'actual_socket_inode':row['actual_socket_inode'],'held_monotonic':held,'header_bytes_sent':0,'body_bytes_sent':0,'remaining_wire_bytes':len(rawhead)+len(body)})
     while not(out/'origin-wire-release').is_file():need(running and time.monotonic()-held<5,'ExistingFixedHeaderHold5Seconds');time.sleep(.01)
     released=time.monotonic();need(.25<=released-held<5,'ActualHeldWireSustained250ms');row.update(held_monotonic=held,released_monotonic=released,actual_hold_seconds=released-held)
    client.sendall(rawhead+body);row['wire_write_complete']=True
    try:extra=client.recv(1);need(extra==b'','NoExtraUpstreamRequest');fate='actual_read_EOF'
    except ConnectionResetError:fate='actual_connection_reset'
    row.update(actual_peer_terminal_fate=fate,actual_peer_terminal_monotonic=time.monotonic(),valid=True)
   except BaseException as error:row['error']={'type':type(error).__name__,'message':str(error)}
   finally:client.close();row['closed_epoch']=time.time();rows.append(row);(out/'origin-requests.jsonl').open('a').write(json.dumps(row,allow_nan=False)+'\n')
 finally:
  server.close();save(out/'origin-terminal.json',{'actual_connections':len(rows),'valid_all':all(r['valid']for r in rows),'server_closed':True,'stopped_by_signal':not running,'elapsed_seconds':time.monotonic()-began,'Source_programs':0})
if __name__=='__main__':main()
