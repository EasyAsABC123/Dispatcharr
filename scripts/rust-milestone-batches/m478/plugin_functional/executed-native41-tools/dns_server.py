"""Finite owned authoritative DNS fixture. Never forwards a packet or opens outbound sockets."""
import argparse
import base64
import ipaddress
import json
import selectors
import signal
import socket
import struct
import time
from pathlib import Path
from contract import answer, question


def addresses(name, kind, counts, owned_ip):
    if name not in {'trusted.plugin-fixture.test', 'untrusted.plugin-fixture.test', 'mismatch.plugin-fixture.test', 'public.plugin-fixture.test', 'loopback.plugin-fixture.test', 'mixed.plugin-fixture.test', 'rebind.plugin-fixture.test'}:
        raise ValueError('undeclared hostname')
    if kind == 28:
        return []
    if name == 'loopback.plugin-fixture.test':
        return ['127.0.0.1']
    if name == 'mixed.plugin-fixture.test':
        return [owned_ip, '127.0.0.1']
    if name == 'rebind.plugin-fixture.test':
        return [owned_ip] if counts.get(name, 0) == 0 else ['127.0.0.1']
    if name in {'trusted.plugin-fixture.test', 'untrusted.plugin-fixture.test', 'mismatch.plugin-fixture.test', 'public.plugin-fixture.test'}:
        return [owned_ip]
    raise ValueError('undeclared hostname')


def run(bind_ip, owned_ip, output):
    for value in (bind_ip, owned_ip):
        if str(ipaddress.IPv4Address(value)) != value:
            raise ValueError('literal IPv4 required')
    output = Path(output)
    output.mkdir(exist_ok=False)
    selector = selectors.DefaultSelector()
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    clients = {}
    counts = {}
    records = 0
    written = 0
    accepted = 0
    stopping = False
    began = time.monotonic()
    errors = []

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    events = 0

    def record(stream, row):
        nonlocal written, events
        encoded = (json.dumps(row, separators=(',', ':'), allow_nan=False) + '\n').encode()
        if events >= 1024 or written + len(encoded) > 512 * 1024:
            raise ValueError('DNS finite raw journal capacity; later bytes not captured')
        stream.write(encoded)
        stream.flush()
        written += len(encoded)
        events += 1

    def respond(data, peer, transport, stream):
        nonlocal records
        record(stream, {'kind': 'ingress', 'epoch': time.time(), 'monotonic': time.monotonic(), 'transport': transport, 'peer': peer, 'request_b64': base64.b64encode(data).decode()})
        ident, flags, name, kind, raw = question(data)
        ips = addresses(name, kind, counts, owned_ip)
        response = answer(data, ips)
        if records >= 256:
            raise ValueError('DNS finite response count')
        if kind == 1:
            counts[name] = counts.get(name, 0) + 1
        record(stream, {'kind': 'response-planned', 'query_index': records, 'epoch': time.time(), 'monotonic': time.monotonic(), 'transport': transport, 'peer': peer, 'name': name, 'qtype': kind, 'addresses': ips, 'request_b64': base64.b64encode(data).decode(), 'reply_b64': base64.b64encode(response).decode()})
        records += 1
        return response

    try:
        udp.bind((bind_ip, 53))
        tcp.bind((bind_ip, 53))
        tcp.listen(8)
        for listener, kind in ((udp, 'udp'), (tcp, 'listener')):
            listener.setblocking(False)
            selector.register(listener, selectors.EVENT_READ, kind)
        (output / 'ready.json').write_text(json.dumps({'bind': bind_ip, 'port': 53, 'protocols': ['udp', 'tcp'], 'epoch': time.time(), 'monotonic': began}) + '\n')
        with (output / 'queries.jsonl').open('xb') as stream:
            while not stopping:
                if time.monotonic() - began > 1290:
                    raise TimeoutError('DNS lifetime1290')
                for key, events in selector.select(.2):
                    if key.data == 'udp':
                        data, peer = udp.recvfrom(513)
                        udp.sendto(respond(data, peer, 'udp', stream), peer)
                    elif key.data == 'listener':
                        conn, peer = tcp.accept()
                        if accepted >= 32 or len(clients) >= 8:
                            conn.close()
                            raise ValueError('DNS TCP connection capacity')
                        accepted += 1
                        conn.setblocking(False)
                        clients[conn] = {'peer': peer, 'input': b'', 'output': None, 'began': time.monotonic()}
                        selector.register(conn, selectors.EVENT_READ, 'client')
                    else:
                        conn = key.fileobj
                        state = clients[conn]
                        if events & selectors.EVENT_READ:
                            data = conn.recv(515)
                            if not data:
                                raise ValueError('DNS TCP incomplete request')
                            record(stream, {'kind': 'tcp-ingress-chunk', 'epoch': time.time(), 'monotonic': time.monotonic(), 'peer': state['peer'], 'raw_b64': base64.b64encode(data).decode()})
                            state['input'] += data
                            wire = state['input']
                            if len(wire) > 514:
                                raise ValueError('DNS TCP request capacity')
                            if len(wire) >= 2:
                                length = struct.unpack('!H', wire[:2])[0]
                                if not 17 <= length <= 512 or len(wire) > length + 2:
                                    raise ValueError('DNS TCP framing')
                                if len(wire) == length + 2:
                                    response = respond(wire[2:], state['peer'], 'tcp', stream)
                                    state['output'] = struct.pack('!H', len(response)) + response
                                    selector.modify(conn, selectors.EVENT_WRITE, 'client')
                        if events & selectors.EVENT_WRITE:
                            remaining = state['output']
                            sent = conn.send(remaining)
                            if sent <= 0:
                                raise ValueError('DNS TCP zero send')
                            state['output'] = remaining[sent:]
                            if not state['output']:
                                selector.unregister(conn)
                                conn.close()
                                del clients[conn]
                if any(time.monotonic() - state['began'] > 2 for state in clients.values()):
                    raise TimeoutError('DNS TCP lifetime2')
    except BaseException as exc:
        errors.append({'type': type(exc).__name__, 'message': str(exc)})
    finally:
        incomplete = len(clients)
        for conn in list(clients):
            conn.close()
        udp.close()
        tcp.close()
        selector.close()
        (output / 'terminal.json').write_text(json.dumps({'complete': stopping and not errors and incomplete == 0, 'stopped_by_signal': stopping, 'started_monotonic': began, 'terminal_epoch': time.time(), 'terminal_monotonic': time.monotonic(), 'queries': records, 'events': events, 'captured_bytes': written, 'tcp_connections': accepted, 'unfinished_tcp_clients': incomplete, 'errors': errors}, indent=2) + '\n')
    return 0 if stopping and not errors and incomplete == 0 else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bind-ip', required=True)
    parser.add_argument('--origin-ip', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.bind_ip, args.origin_ip, args.output))
