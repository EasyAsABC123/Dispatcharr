"""Small bounded RFC6455 fixture client; no third-party host dependencies.

Original Dispatcharr application authors retain full credit. This helper never
logs handshake URLs or credentials. It handles uncompressed server messages.
"""
import base64
import hashlib
import json
import os
import socket
import struct


class Client:
    def __init__(self,host,port,path,origin='https://fixture.invalid',headers=None,max_message_bytes=2*1024*1024):
        self.socket=socket.create_connection((host,port),timeout=5)
        self.buffer=b'';self.status=None;self.headers={};self.closed=False
        if not 1024<=max_message_bytes<=32*1024*1024:raise ValueError('fixture message bound invalid')
        self.max_message_bytes=max_message_bytes
        self.fragments=bytearray();self.kind=None
        key=base64.b64encode(os.urandom(16)).decode()
        request=['GET '+path+' HTTP/1.1','Host: fixture.invalid','Upgrade: websocket','Connection: Upgrade',
                 'Sec-WebSocket-Key: '+key,'Sec-WebSocket-Version: 13']
        if origin is not None:request.append('Origin: '+origin)
        request.extend(name+': '+value for name,value in (headers or {}).items())
        self.socket.sendall(('\r\n'.join(request)+'\r\n\r\n').encode())
        while b'\r\n\r\n' not in self.buffer:
            data=self.socket.recv(4096)
            if not data:raise EOFError('upgrade response ended early')
            self.buffer+=data
            if len(self.buffer)>65536:raise ValueError('upgrade response exceeds64KiB')
        header,self.buffer=self.buffer.split(b'\r\n\r\n',1)
        lines=header.decode('latin1').split('\r\n');self.status=int(lines[0].split()[1])
        self.headers=dict(line.split(': ',1) for line in lines[1:] if ': ' in line)
        if self.status==101:
            expected=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
            assert next(value for name,value in self.headers.items() if name.lower()=='sec-websocket-accept')==expected

    def frame(self):
        # Commit only a complete frame. A short optional receive may time out
        # midway through a payload; the next call must retain its header too.
        def need(size):
            while len(self.buffer)<size:
                data=self.socket.recv(min(65536,size-len(self.buffer)))
                if not data:raise EOFError('websocket transport ended')
                self.buffer+=data
        need(2);head=self.buffer[:2]
        final=bool(head[0]&0x80);opcode=head[0]&15
        if head[0]&0x70 or head[1]&0x80:raise ValueError('unexpected compressed/masked server frame')
        length=head[1]&127;offset=2
        if length==126:need(4);length=struct.unpack('!H',self.buffer[2:4])[0];offset=4
        elif length==127:need(10);length=struct.unpack('!Q',self.buffer[2:10])[0];offset=10
        if opcode<8 and length>self.max_message_bytes:raise ValueError('server frame exceeds fixture bound')
        if opcode>=8 and (not final or length>125):raise ValueError('invalid control frame')
        need(offset+length);data=self.buffer[offset:offset+length];self.buffer=self.buffer[offset+length:]
        return final,opcode,data

    def send(self,opcode,data,final=True):
        if len(data)>2*1024*1024:raise ValueError('fixture frame exceeds2MiB')
        mask=os.urandom(4);head=bytes([(0x80 if final else 0)|opcode])
        if len(data)<126:head+=bytes([0x80|len(data)])
        elif len(data)<65536:head+=bytes([0x80|126])+struct.pack('!H',len(data))
        else:head+=bytes([0x80|127])+struct.pack('!Q',len(data))
        self.socket.sendall(head+mask+bytes(value^mask[index%4] for index,value in enumerate(data)))

    def json(self,data):self.send(1,json.dumps(data).encode())

    def receive(self,timeout=3):
        self.socket.settimeout(timeout)
        while True:
            try:final,opcode,data=self.frame()
            except EOFError:return {'closed':1006}
            if opcode<8 and len(self.fragments)+len(data)>self.max_message_bytes:raise ValueError('server message exceeds fixture bound')
            if opcode==8:return {'closed':struct.unpack('!H',data[:2])[0] if len(data)>=2 else 1005}
            if opcode==9:self.send(10,data);continue
            if opcode==10:return {'pong':data.hex()}
            if opcode in (1,2):
                if self.kind is not None:raise ValueError('overlapping fragmented messages')
                self.kind=opcode
            elif opcode!=0 or self.kind is None:raise ValueError('invalid continuation')
            self.fragments.extend(data)
            if final:
                fragments,self.fragments=self.fragments,bytearray();kind,self.kind=self.kind,None
                if kind==1:return {'message':json.loads(fragments.decode())}
                return {'binary_hex':fragments.hex()}

    def optional(self,timeout=.15):
        try:return self.receive(timeout)
        except socket.timeout:return None

    def close(self):
        if self.closed:return None
        self.closed=True;result=None
        try:
            if self.status==101:
                self.send(8,struct.pack('!H',1000));result=self.optional(.5)
        except (OSError,EOFError):pass
        finally:self.socket.close()
        return result
