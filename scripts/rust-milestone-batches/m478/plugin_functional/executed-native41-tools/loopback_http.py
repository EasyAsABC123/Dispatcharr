"""Bounded actual Source-namespace loopback sentinel, no outbound sockets."""
import argparse
import base64
import hashlib
import json
import os
import selectors
import signal
import socket
import time
from pathlib import Path
from tls_origin import Journal, request_target, wire_response


def run(body_path, phase_cue, output):
    body = Path(body_path).read_bytes()
    if len(body) > 65536:
        raise ValueError('loopback fixed manifest size')
    json.loads(body)
    response = wire_response(200, body)
    output = Path(output)
    output.mkdir(exist_ok=False)
    journal = Journal(output / 'contacts.jsonl')
    stopping = False
    errors = []
    accepted = 0
    phase_seen = False
    phase_sha = None
    began = time.monotonic()
    selector = selectors.DefaultSelector()
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    active = None

    def cue():
        nonlocal phase_seen, phase_sha
        path = Path(phase_cue)
        if path.exists():
            if path.is_symlink() or not path.is_file() or path.stat().st_size > 8192:
                raise ValueError('finite actual Source phase cue')
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if phase_seen and digest != phase_sha:
                raise ValueError('Source phase cue mutated')
            phase_seen, phase_sha = True, digest

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        listener.bind(('127.0.0.1', 8080))
        listener.listen(4)
        listener.setblocking(False)
        selector.register(listener, selectors.EVENT_READ)
        (output / 'ready.json').write_text(json.dumps({'pid': os.getpid(), 'starttime': Path('/proc/self/stat').read_text().rsplit(')', 1)[1].split()[19], 'PID_namespace': os.readlink('/proc/self/ns/pid'), 'bind': '127.0.0.1', 'port': 8080, 'epoch': time.time(), 'monotonic': began, 'response_body_sha256': hashlib.sha256(body).hexdigest()}) + '\n')
        while not stopping:
            cue()
            if time.monotonic() - began > 1290:
                raise TimeoutError('loopback lifetime1290')
            for key, _ in selector.select(.2):
                active, peer = listener.accept()
                active.settimeout(2)
                accepted += 1
                if accepted > 4 or peer[0] != '127.0.0.1':
                    raise ValueError('bounded loopback contacts only')
                raw = bytearray()
                try:
                    while not raw.endswith(b'\r\n\r\n'):
                        part = active.recv(min(1024, 8193 - len(raw)))
                        if not part:
                            raise ValueError('loopback EOF before complete headers')
                        raw.extend(part)
                        journal.append({'kind': 'ingress-chunk', 'epoch': time.time(), 'monotonic': time.monotonic(), 'contact': accepted, 'peer': peer, 'raw_b64': base64.b64encode(part).decode()})
                        if len(raw) > 8192:
                            raise ValueError('loopback header capacity')
                    target, headers = request_target(bytes(raw))
                    if target != '/unsigned' or headers[b'host'].lower() not in {b'rebind.plugin-fixture.test:8080', b'loopback.plugin-fixture.test:8080', b'mixed.plugin-fixture.test:8080'}:
                        raise ValueError('actual owned loopback request binding')
                    journal.append({'kind': 'request', 'epoch': time.time(), 'monotonic': time.monotonic(), 'contact': accepted, 'peer': peer, 'raw_b64': base64.b64encode(bytes(raw)).decode(), 'response_b64': base64.b64encode(response).decode()})
                    active.sendall(response)
                    journal.append({'kind': 'response-sendall-complete', 'epoch': time.time(), 'monotonic': time.monotonic(), 'contact': accepted, 'bytes': len(response)})
                finally:
                    try:
                        active.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                    active.close()
                    active = None
    except BaseException as exc:
        errors.append({'type': type(exc).__name__, 'message': str(exc)})
    finally:
        if active is not None:
            active.close()
        listener.close()
        selector.close()
        (output / 'terminal.json').write_text(json.dumps({'complete': stopping and not errors, 'stopped_by_signal': stopping, 'phase_cue_seen': phase_seen, 'phase_cue_sha256': phase_sha, 'contacts': accepted, 'started_monotonic': began, 'terminal_epoch': time.time(), 'terminal_monotonic': time.monotonic(), 'errors': errors}, indent=2) + '\n')
    return 0 if stopping and not errors else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--body', required=True)
    parser.add_argument('--phase-cue', required=True)
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.body, args.phase_cue, args.output))
