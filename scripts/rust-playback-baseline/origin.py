"""Deterministic, packet-aligned TS origin with test-only fault controls.

Full credit for the proxied application belongs to Dispatcharr's original authors.
"""
import hashlib
import json
import os
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

from range_http import serve_file

RATE = 250000
PACKET_CHUNK = 188 * 7


def fixture_command(path, duration, codec='mpeg2'):
    video = (['-c:v', 'mpeg2video', '-threads', '1', '-b:v', '1000k'] if codec == 'mpeg2' else
             ['-c:v', 'libx264', '-preset', 'ultrafast', '-threads', '1', '-pix_fmt', 'yuv420p',
              '-g', '50', '-keyint_min', '50', '-sc_threshold', '0', '-b:v', '1000k'])
    return ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
            '-i', 'testsrc2=size=320x180:rate=25', '-f', 'lavfi',
            '-i', 'sine=frequency=440:sample_rate=48000', '-t', str(duration),
            *video, '-c:a', 'mp2' if codec == 'mpeg2' else 'aac', '-b:a', '128k',
            '-flags:v', '+bitexact', '-flags:a', '+bitexact',
            '-fflags', '+bitexact', '-muxrate', '2000000', '-f', 'mpegts', str(path)]


def mp4_command(path):
    return ['ffmpeg', '-hide_banner', '-loglevel', 'error', '-f', 'lavfi',
            '-i', 'testsrc2=size=320x180:rate=25', '-f', 'lavfi',
            '-i', 'sine=frequency=440:sample_rate=48000', '-t', '30',
            '-c:v', 'libx264', '-preset', 'ultrafast', '-threads', '1', '-pix_fmt', 'yuv420p',
            '-g', '50', '-keyint_min', '50', '-sc_threshold', '0', '-b:v', '1000k',
            '-c:a', 'aac', '-b:a', '128k', '-flags:v', '+bitexact', '-flags:a', '+bitexact',
            '-fflags', '+bitexact', '-movflags', '+faststart', '-f', 'mp4', str(path)]


def file_sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


class FixtureState:
    def __init__(self, clip, duration, codec='mpeg2', mp4=None):
        self.clip = clip
        self.mp4 = mp4
        self.lock = threading.Lock()
        self.controls = {}
        self.stats = {'active': 0, 'maximum_active': 0, 'connections': 0, 'bytes': 0,
                      'disconnections': 0, 'eof_connections': 0, 'fault_requests': 0,
                      'fixture_sha256': file_sha256(clip), 'fixture_bytes': clip.stat().st_size,
                      'bytes_per_second': RATE, 'duration_seconds': duration,
                      'codec': codec,
                      'video': ('mpeg2video' if codec == 'mpeg2' else 'h264') + ' 320x180 25fps',
                      'audio': ('mp2' if codec == 'mpeg2' else 'aac') + ' 48kHz', 'paths': {}}
        if mp4:
            self.stats['vod'] = {'fixture_sha256': file_sha256(mp4), 'fixture_bytes': mp4.stat().st_size,
                                 'duration_seconds': 30, 'video': 'h264', 'audio': 'aac',
                                 'etag': '"' + file_sha256(mp4) + '"', 'requests': 0, 'head_requests': 0, 'bytes': 0}

    def control(self, path, action, seconds=0):
        if not valid_stream_path(path) or action not in ('stall', 'drop', 'recover', 'fail'):
            raise ValueError('invalid fixture control')
        if not isinstance(seconds, (int, float)) or not 0 <= seconds <= 300:
            raise ValueError('invalid stall duration')
        with self.lock:
            control = self.controls.setdefault(path, {'generation': 0, 'fail': False, 'stall_until': 0})
            if action == 'stall':
                control['stall_until'] = time.monotonic() + seconds
            elif action == 'drop':
                control['generation'] += 1
            elif action == 'fail':
                control.update(fail=True, generation=control['generation'] + 1)
            else:
                control.update(fail=False, stall_until=0)
            self.stats['fault_requests'] += 1

    def snapshot(self):
        with self.lock:
            # JSON copy ensures handlers cannot observe concurrently mutated dicts.
            return json.loads(json.dumps(self.stats))


def valid_stream_path(path):
    return path == '/live.ts' or (path.startswith('/live/') and path.endswith('.ts')
                                  and path[6:-3].isdigit())


def handler_for(state):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            pass

        def json_response(self, payload, status=200):
            payload = json.dumps(payload).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):
            if self.path != '/control':
                self.send_error(404)
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 < length <= 4096:
                    raise ValueError('invalid body')
                body = json.loads(self.rfile.read(length))
                state.control(body['path'], body['action'], body.get('seconds', 0))
                self.json_response({'applied': True})
            except (KeyError, ValueError, TypeError):
                self.json_response({'error': 'invalid fixture control'}, 400)

        def do_HEAD(self):
            if urlsplit(self.path).path != '/vod.mp4' or state.mp4 is None:
                self.send_error(404)
                return
            with state.lock:
                state.stats['vod']['head_requests'] += 1
            serve_file(self, state.mp4, state.stats['vod']['etag'], head=True)

        def do_GET(self):
            path = urlsplit(self.path).path
            if path == '/vod.mp4' and state.mp4 is not None:
                with state.lock:
                    state.stats['vod']['requests'] += 1
                _, written = serve_file(self, state.mp4, state.stats['vod']['etag'])
                with state.lock:
                    state.stats['vod']['bytes'] += written
                return
            if path in ('/health', '/stats'):
                self.json_response(state.snapshot())
                return
            if not valid_stream_path(path):
                self.send_error(404)
                return
            with state.lock:
                control = state.controls.setdefault(path, {'generation': 0, 'fail': False, 'stall_until': 0})
                if control['fail']:
                    self.send_error(503)
                    return
                generation = control['generation']
                state.stats['active'] += 1
                state.stats['connections'] += 1
                state.stats['maximum_active'] = max(state.stats['maximum_active'], state.stats['active'])
                counters = state.stats['paths'].setdefault(path, {'active': 0, 'connections': 0, 'bytes': 0})
                counters['active'] += 1
                counters['connections'] += 1
            self.send_response(200)
            self.send_header('Content-Type', 'video/mp2t')
            self.send_header('Connection', 'close')
            self.end_headers()
            self.close_connection = True
            try:
                with state.clip.open('rb') as stream:
                    began, sent = time.monotonic(), 0
                    while True:
                        with state.lock:
                            control = state.controls[path]
                            if generation != control['generation']:
                                break
                            stall = max(0, control['stall_until'] - time.monotonic())
                        if stall:
                            time.sleep(min(stall, .1))
                            # Stalls must not cause a catchup burst after recovery.
                            began = time.monotonic() - sent / RATE
                            continue
                        chunk = stream.read(PACKET_CHUNK)
                        if not chunk:
                            # Never wrap timestamps silently; insufficient fixture duration fails evidence.
                            with state.lock:
                                state.stats['eof_connections'] += 1
                            break
                        self.wfile.write(chunk)
                        sent += len(chunk)
                        with state.lock:
                            state.stats['bytes'] += len(chunk)
                            counters['bytes'] += len(chunk)
                        delay = began + sent / RATE - time.monotonic()
                        if delay > 0:
                            time.sleep(delay)
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                with state.lock:
                    state.stats['active'] -= 1
                    counters['active'] -= 1
                    state.stats['disconnections'] += 1
    return Handler


def main():
    duration = int(os.environ.get('FIXTURE_SECONDS', '1800'))
    if not 60 <= duration <= 3600:
        raise ValueError('FIXTURE_SECONDS must be 60..3600')
    codec = os.environ.get('FIXTURE_CODEC', 'mpeg2')
    if codec not in ('mpeg2', 'h264'):
        raise ValueError('FIXTURE_CODEC must be mpeg2 or h264')
    clip = Path('/tmp/fixture.ts')
    subprocess.run(fixture_command(clip, duration, codec), check=True, timeout=180)
    mp4 = Path('/tmp/fixture.mp4') if os.environ.get('ENABLE_VOD_FIXTURE') == '1' else None
    if mp4:
        subprocess.run(mp4_command(mp4), check=True, timeout=60)
    ThreadingHTTPServer(('0.0.0.0', 8080), handler_for(FixtureState(clip, duration, codec, mp4))).serve_forever()


if __name__ == '__main__':
    main()
