"""Deterministic valid TS file served at fixed rate with upstream connection counters."""
import hashlib
import json
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

CLIP = Path('/tmp/fixture.ts')
subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
                '-i', 'testsrc2=size=320x180:rate=25', '-f', 'lavfi', '-i', 'sine=frequency=440:sample_rate=48000',
                '-t', '180', '-c:v', 'mpeg2video', '-threads', '1', '-b:v', '1000k',
                '-c:a', 'mp2', '-b:a', '128k', '-flags:v', '+bitexact', '-flags:a', '+bitexact',
                '-fflags', '+bitexact', '-muxrate', '2000000', '-f', 'mpegts', str(CLIP)], check=True)
stats = {'active': 0, 'maximum_active': 0, 'connections': 0, 'bytes': 0,
         'fixture_sha256': hashlib.sha256(CLIP.read_bytes()).hexdigest(),
         'fixture_bytes': CLIP.stat().st_size, 'bytes_per_second': 250000,
         'duration_seconds': 180, 'video': 'mpeg2video 320x180 25fps', 'audio': 'mp2 48kHz'}
lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def do_GET(self):
        if self.path in ('/health', '/stats'):
            with lock:
                payload = json.dumps(stats).encode()
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.path != '/live.ts':
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header('Content-Type', 'video/mp2t')
        self.send_header('Connection', 'close')
        self.end_headers()
        with lock:
            stats['active'] += 1
            stats['connections'] += 1
            stats['maximum_active'] = max(stats['maximum_active'], stats['active'])
        try:
            with CLIP.open('rb') as stream:
                began = time.monotonic()
                sent = 0
                while True:
                    chunk = stream.read(188 * 7)
                    if not chunk:
                        # Timestamp reset at 180s: initial short characterization
                        # must finish first; longer soak needs a continuous muxer.
                        stream.seek(0)
                        continue
                    self.wfile.write(chunk)
                    sent += len(chunk)
                    with lock:
                        stats['bytes'] += len(chunk)
                    delay = began + sent / 250000 - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            with lock:
                stats['active'] -= 1


ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()
