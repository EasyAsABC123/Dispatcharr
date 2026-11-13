"""Small bounded RESP2 adapter for the disposable guide cache only.
Avoids launching Django/CPython inside a measured service for each cold wave.
Original Dispatcharr authors retain application credit.
"""
import socket

BASE = 'epg_content:all:anonymous:d=1:p=0:logos=True:tvgid=channel_number:origin=http://guide-perf.invalid:8088:g='
STATE = """local n=redis.call('LLEN',KEYS[1]); if n>100000 then return redis.error_reply('guide chunks bound') end; local total=0; local smallest=nil; local largest=0; for i=0,n-1,256 do local batch=redis.call('LRANGE',KEYS[1],i,math.min(i+255,n-1)); for _,v in ipairs(batch)do local size=string.len(v); total=total+size; if smallest==nil or size<smallest then smallest=size end; largest=math.max(largest,size); if total>33554432 then return redis.error_reply('guide bytes bound') end end end; local types={}; local ttls={}; for i,key in ipairs(KEYS)do types[i]=redis.call('TYPE',key)['ok']; ttls[i]=redis.call('TTL',key) end; return {n,total,redis.call('GET',KEYS[2])or '',redis.call('GET',KEYS[3])or '',redis.call('EXISTS',KEYS[4]),redis.call('TTL',KEYS[2]),smallest or 0,largest,types,ttls}"""


def reply(stream, depth=0):
    if depth > 2:
        raise ValueError('Redis reply nesting bound')
    raw = stream.readline(1025)
    if not raw.endswith(b'\r\n') or len(raw) > 1024:
        raise ValueError('Redis reply line bound')
    kind, value = raw[:1], raw[1:-2]
    if kind == b'+':
        return value
    if kind == b'-':
        raise ValueError('Disposable cache command rejected')
    if kind == b':':
        return int(value)
    if kind == b'$':
        length = int(value)
        if length == -1:
            return None
        if not 0 <= length <= 1024:
            raise ValueError('Redis bulk reply bound')
        body = stream.read(length + 2)
        if len(body) != length + 2 or not body.endswith(b'\r\n'):
            raise ValueError('Truncated Redis bulk reply')
        return body[:-2]
    if kind == b'*':
        count = int(value)
        if not 0 <= count <= 16:
            raise ValueError('Redis array reply bound')
        return [reply(stream, depth + 1) for _ in range(count)]
    raise ValueError('Unexpected Redis reply type')


def encoded(parts):
    bodies = [str(v).encode() if not isinstance(v, bytes) else v for v in parts]
    if not 1 <= len(bodies) <= 16 or sum(map(len, bodies)) > 16384:
        raise ValueError('Cache command bound')
    return b'*' + str(len(bodies)).encode() + b'\r\n' + b''.join(
        b'$' + str(len(body)).encode() + b'\r\n' + body + b'\r\n' for body in bodies)


class Cache:
    def __init__(self, port):
        if type(port) is not int or not 61000 <= port <= 65000:
            raise ValueError('Disposable high loopback Redis port required')
        self.port = port

    def command(self, *parts):
        with socket.create_connection(('127.0.0.1', self.port), timeout=5) as sock:
            with sock.makefile('rb') as stream:
                sock.sendall(encoded(['AUTH', 'playback-fixture-only']))
                if reply(stream) != b'OK':
                    raise ValueError('Disposable cache authentication failed')
                sock.sendall(encoded(parts))
                return reply(stream)

    def keys(self):
        generation = self.command('GET', 'epg_content_generation') or b'0'
        if len(generation) > 32 or not generation.isdigit():
            raise ValueError('Unexpected owned cache generation')
        generation = generation.decode('ascii')
        return generation, [BASE + generation + ':' + suffix
                            for suffix in ['chunks', 'ready', 'status', 'lock']]

    def state(self):
        generation, keys = self.keys()
        values = self.command('EVAL', STATE, 4, *keys)
        if not isinstance(values, list) or len(values) != 10:
            raise ValueError('Invalid cache-state reply')
        suffixes = ['chunks', 'ready', 'status', 'lock']
        if (not isinstance(values[8], list) or len(values[8]) != 4
                or not isinstance(values[9], list) or len(values[9]) != 4):
            raise ValueError('Invalid cache type/TTL reply')
        return dict(generation=generation, chunks=values[0], bytes=values[1],
                    ready=values[2].decode(), status=values[3].decode(),
                    locked=bool(values[4]), ready_ttl_seconds=values[5],
                    smallest_chunk_bytes=values[6], largest_chunk_bytes=values[7],
                    key_types={name: value.decode() for name,value in zip(suffixes,values[8])},
                    key_ttl_seconds=dict(zip(suffixes,values[9])))

    def clear(self):
        generation, keys = self.keys()
        removed = self.command('DEL', *keys)
        return dict(generation=generation, owned_xml_keys_removed=removed)
