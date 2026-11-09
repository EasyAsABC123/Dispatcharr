"""Bounded no-media origin for isolated cold TS wire characterization.

Original proxied application credit belongs to Dispatcharr's authors.
"""
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import select
import socket
import subprocess
from pathlib import Path
import sys
import threading
import time

LOCK = threading.Lock()
MODES = {}
STATS = {'connections': 0, 'active': 0, 'maximum_active': 0, 'paths': {}}


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def json(self, value, status=200):
        data = json.dumps(value).encode()
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        if self.path != '/control':
            return self.json({'error': 'not found'}, 404)
        size = int(self.headers.get('Content-Length', '0'))
        if not 0 < size <= 4096:
            return self.json({'error': 'invalid body size'}, 400)
        data = json.loads(self.rfile.read(size))
        if data.get('mode') not in ('fail', 'stall', 'empty', 'warm') or not str(data.get('path', '')).startswith('/live/'):
            return self.json({'error': 'invalid mode/path'}, 400)
        with LOCK:
            MODES[data['path']] = data['mode']
        self.json({'applied': True})

    def do_GET(self):
        if self.path == '/health':
            return self.json({'ready': True})
        if self.path == '/stats':
            with LOCK:
                value = json.loads(json.dumps(STATS))
            return self.json(value)
        with LOCK:
            mode = MODES.get(self.path, 'fail')
            STATS['connections'] += 1
            STATS['active'] += 1
            STATS['maximum_active'] = max(STATS['maximum_active'], STATS['active'])
            row = STATS['paths'].setdefault(self.path, {'connections': 0, 'active': 0})
            row['connections'] += 1
            row['active'] += 1
        try:
            if mode == 'fail':
                return self.json({'error': 'isolated fixture unavailable'}, 503)
            self.send_response(200)
            self.send_header('Content-Type', 'video/mp2t')
            self.send_header('Content-Length', '0') if mode == 'empty' else self.send_header('Transfer-Encoding', 'chunked')
            self.end_headers()
            if mode == 'empty':
                return
            if mode == 'warm':
                with LOCK:
                    fixture=Path('/tmp/cold-warm.ts')
                    if not fixture.exists():
                        sys.path.insert(0,'/verification')
                        from origin import fixture_command
                        subprocess.run(fixture_command(fixture,12,'h264'),check=True,timeout=30,capture_output=True)
                    STATS['paths'][self.path]['phase']='media'
                with fixture.open('rb') as stream:
                    while data:=stream.read(188*7):
                        self.wfile.write(format(len(data),'x').encode()+b'\r\n'+data+b'\r\n')
                        time.sleep(len(data)/250000)
                with LOCK:
                    STATS['paths'][self.path]['phase']='stalled'
            # Send headers but no body. Observe real peer shutdown without
            # manufacturing payload bytes or waiting out a disconnected socket.
            deadline = time.monotonic() + 120
            while time.monotonic() < deadline:
                readable, _, _ = select.select([self.connection], [], [], .1)
                if readable:
                    try:
                        if not self.connection.recv(1, socket.MSG_PEEK):
                            return
                    except OSError:
                        return
            self.wfile.write(b'0\r\n\r\n')
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with LOCK:
                STATS['active'] -= 1
                STATS['paths'][self.path]['active'] -= 1


if __name__ == '__main__':
    ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
