"""Bounded infrastructure-charged observation transport; original author credit retained."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import select
import signal
import socket
import subprocess
import sys
import time
import uuid

# Byte-exact historical phase.py Lua, kept as two distinct EVAL operations.
META="local p='live:channel:'..ARGV[1];return {redis.call('GET',p..':owner') or '',redis.call('GET','profile_connections:'..ARGV[2]) or '0',redis.call('SCARD',p..':clients'),redis.call('HLEN',p..':metadata'),redis.call('GET',p..':input:buffer:index') or '',redis.call('ZCARD',p..':input:buffer:chunk_timestamps'),redis.call('HGET',p..':metadata','state') or ''}"
IDENTITY="local keys=redis.call('KEYS',ARGV[1]);local n=0;for _,k in ipairs(keys)do if redis.call('TYPE',k).ok=='hash'then if redis.call('HGET',k,'user_id')~=ARGV[2]then return {-1,#keys}end;n=n+1 end end;return {n,#keys}"
REQUEST_LIMIT=4096
MESSAGE_LIMIT=65536
TIMEOUT=15

def generation(pid):
    try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
    except (FileNotFoundError,ProcessLookupError):return None

def cgroup(pid):
    group=next(x[3:]for x in Path('/proc',str(pid),'cgroup').read_text().splitlines()if x.startswith('0::'))
    return group

def encode(arguments):
    parts=[b'*'+str(len(arguments)).encode()+b'\r\n']
    for value in arguments:
        raw=value if isinstance(value,bytes)else str(value).encode('utf-8')
        parts.extend([b'$'+str(len(raw)).encode()+b'\r\n',raw,b'\r\n'])
    data=b''.join(parts)
    if len(data)>MESSAGE_LIMIT:raise ValueError('RESPRequestBound')
    return data

class RESP:
    def __init__(self,stream):self.stream=stream;self.buffer=bytearray();self.used=0;self.deadline=None
    def take(self,n):
        while len(self.buffer)<n:
            self.stream.settimeout(max(.001,self.deadline-time.monotonic()))
            if time.monotonic()>=self.deadline:raise TimeoutError('RedisReplyDeadline')
            block=self.stream.recv(min(4096,MESSAGE_LIMIT+1-self.used))
            if not block:raise EOFError('RedisReplyEOF')
            self.used+=len(block)
            if self.used>MESSAGE_LIMIT:raise ValueError('RESPReplyBound')
            self.buffer.extend(block)
        value=bytes(self.buffer[:n]);del self.buffer[:n];return value
    def line(self):
        data=bytearray()
        while True:
            data.extend(self.take(1))
            if len(data)>16384:raise ValueError('RESPLineBound')
            if data.endswith(b'\r\n'):return bytes(data[:-2])
    def value(self,depth=0):
        if depth>4:raise ValueError('RESPDepthBound')
        kind=self.take(1);line=self.line()
        if kind==b'-':raise RuntimeError('RedisErrorReply') # never emit server/credential text
        if kind==b'+':return line.decode('utf-8',errors='strict')
        if kind==b':':return int(line)
        if kind==b'$':
            n=int(line)
            if not 0<=n<=16384:raise ValueError('RESPBulkBoundOrNull')
            data=self.take(n)
            if self.take(2)!=b'\r\n':raise ValueError('RESPBulkDelimiter')
            return data.decode('utf-8',errors='strict')
        if kind==b'*':
            n=int(line)
            if not 0<=n<=64:raise ValueError('RESPArrayBoundOrNull')
            return [self.value(depth+1)for _ in range(n)]
        raise ValueError('RESPUnsupportedType')
    def request(self,arguments):
        if self.buffer:raise ValueError('RedisUnexpectedQueuedBytes')
        self.used=0;self.deadline=time.monotonic()+TIMEOUT
        self.stream.settimeout(TIMEOUT);self.stream.sendall(encode(arguments))
        result=self.value()
        if self.buffer:raise ValueError('RedisTrailingReply')
        return result

def queries(channel,profile,user):
    return (['EVAL',META,'0',str(channel),str(profile)],['EVAL',IDENTITY,'0','live:channel:'+str(channel)+':clients:*',str(user)])

def validate(replies):
    a,b=replies
    if not isinstance(a,list)or len(a)!=7 or not isinstance(b,list)or len(b)!=2:raise ValueError('RedisReplyShape')
    if any(type(a[i])is not str for i in (0,1,4,6))or any(type(a[i])is not int for i in (2,3,5))or any(type(v)is not int for v in b):raise ValueError('RedisReplyTypes')
    return replies

def guest(marker):
    auth=os.environ.get('REDISCLI_AUTH')
    if not auth:raise RuntimeError('InheritedRedisCLIAuthRequired')
    # redis-cli's historical argv has no -n/-h/-p/-a. Inherited REDISCLI_AUTH
    # performs AUTH; localhost:6379 and DB0 are its defaults. Never log auth.
    with socket.create_connection(('127.0.0.1',6379),TIMEOUT)as stream:
        redis=RESP(stream)
        if redis.request(['AUTH',auth])!='OK':raise RuntimeError('RedisAuthNotOK')
        def emit(value):
            raw=json.dumps(value,separators=(',',':'),ensure_ascii=False).encode('utf-8')+b'\n'
            if len(raw)>MESSAGE_LIMIT:raise ValueError('ObserverReplyBound')
            sys.stdout.buffer.write(raw);sys.stdout.buffer.flush()
        emit({'ready':True,'marker':marker,'guestPID':os.getpid(),'starttime':generation(os.getpid()),'cgroup':cgroup(os.getpid()),'database':0,'auth_source':'inherited_REDISCLI_AUTH','script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()})
        sequence=0
        while True:
            raw=sys.stdin.buffer.readline(REQUEST_LIMIT+1)
            if not raw:return
            if len(raw)>REQUEST_LIMIT or not raw.endswith(b'\n'):raise ValueError('ObserverRequestBoundOrEOF')
            request=json.loads(raw)
            if request=={'stop':True}:
                emit({'stopped':True,'completed_requests':sequence});return
            if request.get('sequence')!=sequence or set(request)!={'sequence','channel','profile','user'}:raise ValueError('ObserverSequenceOrFields')
            values=[request[k]for k in ('channel','profile','user')]
            if any(type(v)is not str or not v or len(v.encode('utf-8'))>512 for v in values):raise ValueError('ObserverArgumentBound')
            # No reconnect, retry, pipeline or cached answer after either EVAL.
            replies=validate([redis.request(q)for q in queries(*values)])
            emit({'sequence':sequence,'replies':replies});sequence+=1

class Observer:
    def __init__(self,command,compose,docker,output):
        self.command=command;self.compose=compose;self.docker=docker;self.output=Path(output)
        self.process=None;self.identity=None;self.closed=False;self.sequence=0;self.failure=None;self.buffer=bytearray();self.stderr=bytearray();self.receipt={};self.stderr_eof=False
    def _exchange(self,value=None,timeout=32):
        deadline=time.monotonic()+timeout
        if value is not None:
            raw=json.dumps(value,separators=(',',':')).encode()+b'\n'
            if len(raw)>REQUEST_LIMIT:raise ValueError('ObserverHostRequestBound')
            while raw:
                remaining=deadline-time.monotonic()
                if remaining<=0:raise TimeoutError('ObserverWriteDeadline')
                _,ready,_=select.select([], [self.process.stdin],[],remaining)
                if not ready:raise TimeoutError('ObserverWriteDeadline')
                count=os.write(self.process.stdin.fileno(),raw);raw=raw[count:]
        while b'\n'not in self.buffer:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('ObserverReadDeadline')
            ready,_,_=select.select([self.process.stdout]+([]if self.stderr_eof else[self.process.stderr]),[],[],remaining)
            if not ready:raise TimeoutError('ObserverReadDeadline')
            for stream in ready:
                data=os.read(stream.fileno(),4096)
                if stream is self.process.stderr:
                    if not data:self.stderr_eof=True
                    self.stderr.extend(data);self.stderr=self.stderr[-8192:]
                elif not data:raise EOFError('ObserverHostEOF')
                else:self.buffer.extend(data)
                if len(self.buffer)>MESSAGE_LIMIT:raise ValueError('ObserverHostReplyBound')
        line,_,rest=self.buffer.partition(b'\n');self.buffer=bytearray(rest)
        if rest:raise ValueError('ObserverUnexpectedExtraReply')
        return json.loads(line)
    def start(self):
        self.marker='dispatcharr-metadata-observer-'+uuid.uuid4().hex
        try:
            self.cid=self.command(self.compose+['ps','--all','-q','baseline']).strip()
            if len(self.cid)!=64 or any(c not in '0123456789abcdef'for c in self.cid):raise ValueError('ObserverOwnedCID')
            state=json.loads(self.command([self.docker,'inspect','--format','{{json .State}}',self.cid]))
            if not state['Running']or state['Pid']<=0:raise ValueError('ObserverInfrastructureNotRunning')
            infrastructure=cgroup(state['Pid'])
            self.process=subprocess.Popen(self.compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-u','/resource-tools/redis_observer.py','--guest',self.marker],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
            self.cli_identity={'pid':self.process.pid,'starttime':generation(self.process.pid)}
            for stream in (self.process.stdin,self.process.stdout,self.process.stderr):os.set_blocking(stream.fileno(),False)
            ready=self._exchange(timeout=20)
            if not ready.get('ready')or ready.get('marker')!=self.marker or ready.get('script_sha256')!=hashlib.sha256(Path(__file__).read_bytes()).hexdigest():raise ValueError('ObserverReadyIdentity')
            text=self.command([self.docker,'top',self.cid,'-eo','pid,args'])
            matches=[int(line.split()[0])for line in text.splitlines()[1:]if self.marker in line and '/resource-tools/redis_observer.py' in line]
            if len(matches)!=1:raise ValueError('ObserverUniqueHostPID')
            pid=matches[0];start=generation(pid)
            status=Path('/proc',str(pid),'status').read_text().splitlines();namespace=next(line.split()[1:]for line in status if line.startswith('NSpid:'))
            if int(namespace[-1])!=ready['guestPID']or start!=ready['starttime']or cgroup(pid)!=infrastructure:raise ValueError('ObserverNamespaceGenerationCgroup')
            self.identity={'hostPID':pid,'starttime':start,'guestPID':ready['guestPID'],'NSpid':namespace,'cgroup':infrastructure,'CID':self.cid,'marker':self.marker,'ready':ready}
            return self
        except BaseException as error:
            self.failure=type(error).__name__;self.close();raise
    def snapshot(self,channel,profile,user):
        if self.failure or self.closed:raise RuntimeError('ObserverNotReusableAfterFailure')
        try:
            reply=self._exchange({'sequence':self.sequence,'channel':str(channel),'profile':str(profile),'user':str(user)})
            if reply.get('sequence')!=self.sequence or set(reply)!={'sequence','replies'}:raise ValueError('ObserverReplySequence')
            values=validate(reply['replies']);self.sequence+=1;return values
        except BaseException as error:self.failure=type(error).__name__;raise
    def close(self):
        if self.closed:return self.receipt
        self.closed=True;normal=False;forced=[];eofs={'stdout':False,'stderr':False};error=None
        try:
            if self.process is not None and not self.failure:
                response=self._exchange({'stop':True},timeout=5)
                normal=response=={'stopped':True,'completed_requests':self.sequence}
                if not normal:raise ValueError('ObserverStopReceipt')
        except BaseException as exc:error=type(exc).__name__
        if self.process is not None:
            try:self.process.stdin.close()
            except OSError:pass
            def alive():
                self.process.poll() # reap an exited CLI; never mistake its zombie for live work
                return self.identity and generation(self.identity['hostPID'])==self.identity['starttime']
            deadline=time.monotonic()+2
            while alive()and time.monotonic()<deadline:time.sleep(.02)
            for sig in (signal.SIGTERM,signal.SIGKILL):
                if not alive():break
                forced.append(sig.name)
                try:os.kill(self.identity['hostPID'],sig)
                except ProcessLookupError:pass
                except PermissionError:error='ObserverGuestSignalPermission'
                deadline=time.monotonic()+2
                while alive()and time.monotonic()<deadline:time.sleep(.02)
            # If mapping failed or original guest persists, kill only exact owned
            # infrastructure CID; phase fails regardless of successful teardown.
            if self.identity is None or alive():
                forced.append('owned_exact_CID_kill')
                try:self.command([self.docker,'kill',self.cid],timeout=15)
                except BaseException as exc:error=type(exc).__name__
            try:self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                forced.append('CLI_group_KILL')
                try:os.killpg(self.process.pid,signal.SIGKILL)
                except ProcessLookupError:pass # raced natural exit; still wait/reap
                self.process.wait(timeout=3)
            deadline=time.monotonic()+3
            streams=[self.process.stdout,self.process.stderr]
            while streams and time.monotonic()<deadline:
                ready,_,_=select.select(streams,[],[],max(0,deadline-time.monotonic()))
                for stream in ready:
                    data=os.read(stream.fileno(),4096)
                    if not data:eofs['stdout'if stream is self.process.stdout else'stderr']=True;streams.remove(stream)
                    elif stream is self.process.stderr:self.stderr.extend(data);self.stderr=self.stderr[-8192:]
                    else:error='ObserverUnexpectedTrailingStdout'
            for stream in (self.process.stdout,self.process.stderr):stream.close()
        absent=self.identity is not None and generation(self.identity['hostPID'])!=self.identity['starttime']
        self.receipt={'identity':self.identity,'CLI_identity':getattr(self,'cli_identity',None),'completed_requests':self.sequence,'normal_STOP':normal,'forced':forced,'failure':self.failure,'close_error':error,'EOF':eofs,'actual_exit':self.process.returncode if self.process else None,'original_guest_generation_absent':absent,'cleanup_passed':normal and not forced and not self.failure and error is None and absent and all(eofs.values())and self.process.returncode==0}
        (self.output/'metadata-observer-receipt.json').write_text(json.dumps(self.receipt,indent=2)+'\n')
        return self.receipt

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--guest',required=True);args=parser.parse_args()
    try:guest(args.guest)
    except BaseException as error:
        print(type(error).__name__,file=sys.stderr);raise SystemExit(1)
