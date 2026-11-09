"""One-time long deterministic fixture generation, then paced immutable file I/O.
Original application credit: Dispatcharr authors and contributors.
"""
import subprocess
import sys
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.parse import urlsplit

from plan import PLAN
sys.path.insert(0, '/timeshift-verification')
sys.path.insert(0, '/verification')
from archive_origin import ArchiveMixin
from origin import FixtureState, fixture_command, mp4_command, handler_for


class PacedWriter:
    def __init__(self, raw, rate):
        import time
        self.raw, self.rate, self.sent, self.began = raw, rate, 0, time.monotonic()
    def __getattr__(self, name):
        return getattr(self.raw, name)
    def write(self, data):
        import time
        delay = self.began + (self.sent + len(data)) / self.rate - time.monotonic()
        if delay > 0:
            time.sleep(delay)
        value = self.raw.write(data)
        self.sent += len(data)
        return value


def main():
    seconds = PLAN['fixture']['duration_seconds']
    clip, mp4 = Path('/tmp/fixture.ts'), Path('/tmp/fixture.mp4')
    commands = [fixture_command(clip, seconds, 'h264'), mp4_command(mp4)]
    commands[1][commands[1].index('-t') + 1] = str(seconds)
    for command in commands:
        subprocess.run(command, check=True, timeout=600)
    state = FixtureState(clip, seconds, 'h264', mp4)
    state.stats['generator_commands'] = commands
    state.stats['fixture_profile'] = 'm4-sustained900-h264-aac-vod262144-archive500000'
    state.stats['vod']['duration_seconds'] = seconds
    state.stats['vod']['bytes_per_second'] = PLAN['fixture']['vod_bytes_per_second']
    parent = handler_for(state)
    class Handler(ArchiveMixin, parent):
        archive_path = clip
        archive_hash = state.stats['fixture_sha256']
        archive_rate = PLAN['fixture']['timeshift_bytes_per_second']
        def do_GET(self):
            if isinstance(self.wfile, PacedWriter):
                self.wfile = self.wfile.raw
            if urlsplit(self.path).path in ('/movie/fixture/fixture/fixture.mp4', '/series/fixture/fixture/fixture.mp4'):
                self.path = '/vod.mp4'
            self.pace = urlsplit(self.path).path == '/vod.mp4'
            super().do_GET()
        def do_HEAD(self):
            if isinstance(self.wfile, PacedWriter):
                self.wfile = self.wfile.raw
            self.pace = False
            super().do_HEAD()
        def end_headers(self):
            super().end_headers()
            if getattr(self, 'pace', False):
                self.wfile = PacedWriter(self.wfile, PLAN['fixture']['vod_bytes_per_second'])
    ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()


if __name__ == '__main__':
    main()
