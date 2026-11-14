"""Unexecuted bounded owned HTTP/TLS manifest origin. No outbound sockets."""
import argparse
import base64
import hashlib
import ipaddress
import json
import os
import selectors
import signal
import socket
import ssl
import time
from pathlib import Path
from urllib.parse import urlsplit


class Journal:
    def __init__(self, path):
        self.path = path
        self.count = 0
        self.bytes = 0

    def append(self, row):
        encoded = (json.dumps(row, separators=(',', ':'), allow_nan=False) + '\n').encode()
        if self.count >= 256 or self.bytes + len(encoded) > 1024 * 1024:
            raise ValueError('origin finite journal capacity')
        with self.path.open('ab') as stream:
            stream.write(encoded)
            stream.flush()
        self.count += 1
        self.bytes += len(encoded)


def request_target(raw):
    if type(raw) is not bytes or not raw.endswith(b'\r\n\r\n') or len(raw) > 8192:
        raise ValueError('bounded complete HTTP header required')
    lines = raw[:-4].split(b'\r\n')
    parts = lines[0].split(b' ')
    if len(parts) != 3 or parts[0] != b'GET' or parts[2] != b'HTTP/1.1':
        raise ValueError('finite GET HTTP/1.1 only')
    try:
        target = parts[1].decode('ascii')
    except UnicodeDecodeError:
        raise ValueError('ASCII target required')
    if not target.startswith('/') or target.startswith('//') or '#' in target:
        raise ValueError('origin form target required')
    headers = {}
    for line in lines[1:]:
        if b':' not in line:
            raise ValueError('HTTP header colon')
        key, value = line.split(b':', 1)
        key = key.lower()
        if not key or any(b not in b'abcdefghijklmnopqrstuvwxyz0123456789-' for b in key) or key in headers:
            raise ValueError('HTTP unique named headers')
        headers[key] = value.strip()
    if b'host' not in headers or b'transfer-encoding' in headers or headers.get(b'content-length', b'0') != b'0':
        raise ValueError('host required and GET no body')
    return target, headers


def wire_response(status, body, extra=()):
    if status not in (200, 302) or type(body) is not bytes or len(body) > 65536:
        raise ValueError('bounded fixed origin response')
    label = b'OK' if status == 200 else b'Found'
    rows = [b'HTTP/1.1 ' + str(status).encode() + b' ' + label, b'Content-Type: application/json', b'Content-Length: ' + str(len(body)).encode(), b'Connection: close']
    for key, value in extra:
        if b'\r' in key + value or b'\n' in key + value:
            raise ValueError('fixed header safety')
        rows.append(key + b': ' + value)
    return b'\r\n'.join(rows) + b'\r\n\r\n' + body


def run(bind_ip, source_ip, assets, output):
    for value in (bind_ip, source_ip):
        if str(ipaddress.IPv4Address(value)) != value:
            raise ValueError('literal owned IPv4 required')
    assets = Path(assets)
    output = Path(output)
    output.mkdir(exist_ok=False)
    journal = Journal(output / 'events.jsonl')
    began = time.monotonic()
    stopping = False
    errors = []
    contexts = {}
    choices = []
    accepted = 0
    requests = 0
    listeners = []
    selector = selectors.DefaultSelector()
    bodies = {}
    active = None

    def stamp(kind, **fields):
        journal.append({'kind': kind, 'epoch': time.time(), 'monotonic': time.monotonic(), **fields})

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for route in ('unsigned', 'signed-ascii', 'signed-unicode', 'bad-signature', 'flat-signed'):
            raw = (assets / ('body-' + route + '.json')).read_bytes()
            if len(raw) > 65536:
                raise ValueError('manifest asset capacity')
            json.loads(raw)
            bodies['/' + route] = raw
        certs = {}
        for label in ('trusted', 'untrusted', 'mismatch'):
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.minimum_version = ssl.TLSVersion.TLSv1_2
            context.load_cert_chain(str(assets / (label + '.pem')), str(assets / (label + '.key')))
            contexts[label] = context
            certs[label] = hashlib.sha256(ssl.PEM_cert_to_DER_cert((assets / (label + '.pem')).read_text())).hexdigest()

        def select_certificate(sock, name, initial):
            label = {'trusted.plugin-fixture.test': 'trusted', 'untrusted.plugin-fixture.test': 'untrusted', 'mismatch.plugin-fixture.test': 'mismatch'}.get(name)
            choices.append({'name': name, 'certificate': label})
            stamp('sni', requested_name=name, selected_certificate=label, certificate_der_sha256=certs.get(label))
            if label is None:
                raise ssl.SSLError('unowned SNI')
            sock.context = contexts[label]

        contexts['trusted'].set_servername_callback(select_certificate)
        for port in (8080, 8443):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind((bind_ip, port))
            listener.listen(8)
            listener.setblocking(False)
            listeners.append(listener)
            selector.register(listener, selectors.EVENT_READ, port)
        (output / 'ready.json').write_text(json.dumps({'PID': os.getpid(), 'starttime': Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19], 'PID_namespace': os.readlink('/proc/self/ns/pid'), 'bind': bind_ip, 'source': source_ip, 'ports': [8080, 8443], 'started_epoch': time.time(), 'started_monotonic': began, 'certificate_der_sha256': certs}, indent=2) + '\n')
        while not stopping:
            if time.monotonic() - began > 1290:
                raise TimeoutError('origin lifetime1290')
            for key, _ in selector.select(.2):
                active, peer = key.fileobj.accept()
                active.settimeout(2)
                accepted += 1
                fd = active.fileno()
                inode = os.fstat(fd).st_ino
                stamp('accepted', connection=accepted, peer=peer, port=key.data, fd=fd, socket_inode=inode)
                if accepted > 64 or peer[0] != source_ip:
                    raise ValueError('owned source and finite connection count required')
                try:
                    if key.data == 8443:
                        first_choice = len(choices)
                        try:
                            active = contexts['trusted'].wrap_socket(active, server_side=True)
                            stamp('tls-handshake-complete', connection=accepted, tls_version=active.version(), cipher=active.cipher())
                        except ssl.SSLError as exc:
                            stamp('tls-handshake-error', connection=accepted, error_type=type(exc).__name__, error=str(exc), actual_sni=choices[first_choice:])
                            if len(choices) != first_choice + 1 or choices[-1]['certificate'] not in ('untrusted', 'mismatch'):
                                raise
                            continue
                    raw = bytearray()
                    while not raw.endswith(b'\r\n\r\n'):
                        part = active.recv(min(1024, 8193 - len(raw)))
                        if not part:
                            raise ValueError('origin EOF before complete request')
                        raw.extend(part)
                        stamp('ingress-chunk', connection=accepted, bytes=len(part), raw_b64=base64.b64encode(part).decode())
                        if len(raw) > 8192:
                            raise ValueError('origin request header capacity')
                    target, headers = request_target(bytes(raw))
                    expected_host = ({8080: ('public', 'rebind'), 8443: ('trusted', 'untrusted', 'mismatch')})[key.data]
                    allowed_hosts = {(prefix + '.plugin-fixture.test:' + str(key.data)).encode() for prefix in expected_host}
                    if headers[b'host'].lower() not in allowed_hosts:
                        raise ValueError('actual Host outside owned fixed ports')
                    path = urlsplit(target).path
                    if path == '/redirect' and key.data == 8443:
                        response = wire_response(302, b'', [(b'Location', b'https://trusted.plugin-fixture.test:8443/unsigned')])
                    elif path in bodies:
                        response = wire_response(200, bodies[path])
                    else:
                        raise ValueError('undeclared origin route')
                    requests += 1
                    if requests > 32:
                        raise ValueError('origin request capacity32')
                    stamp('request', connection=accepted, request_index=requests, target=target, raw_b64=base64.b64encode(bytes(raw)).decode(), response_b64=base64.b64encode(response).decode(), response_sha256=hashlib.sha256(response).hexdigest())
                    active.sendall(response)
                    stamp('response-sendall-complete', connection=accepted, bytes=len(response))
                finally:
                    if active is not None:
                        try:
                            active.shutdown(socket.SHUT_RDWR)
                        except OSError as exc:
                            stamp('shutdown-error', connection=accepted, error_type=type(exc).__name__, error=str(exc))
                        active.close()
                        active = None
                        stamp('owned-socket-closed', connection=accepted, original_fd=fd, socket_inode=inode)
    except BaseException as exc:
        errors.append({'type': type(exc).__name__, 'message': str(exc)})
    finally:
        if active is not None:
            active.close()
        for listener in listeners:
            listener.close()
        selector.close()
        (output / 'terminal.json').write_text(json.dumps({'complete': stopping and not errors, 'stopped_by_signal': stopping, 'started_monotonic': began, 'terminal_epoch': time.time(), 'terminal_monotonic': time.monotonic(), 'accepted_connections': accepted, 'HTTP_requests': requests, 'journal_records': journal.count, 'journal_bytes': journal.bytes, 'errors': errors}, indent=2) + '\n')
    return 0 if stopping and not errors else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--bind-ip', required=True)
    parser.add_argument('--source-ip', required=True)
    parser.add_argument('--assets', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.bind_ip, args.source_ip, args.assets, args.output))
