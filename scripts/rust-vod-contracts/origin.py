#!/usr/bin/env python3
"""Native XC URL alias for the deterministic seekable MP4 fixture."""
import sys
import subprocess
import os
import time
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, '/verification')
sys.path.insert(0, '/timeshift-verification')
from archive_origin import ArchiveMixin
from origin import FixtureState, fixture_command, mp4_command, handler_for


class PacedWriter:
    """Optional functional fixture pacing; headers and file bytes stay exact."""
    def __init__(self,raw,rate):self.raw=raw;self.rate=rate;self.sent=0;self.began=time.monotonic()
    def __getattr__(self,name):return getattr(self.raw,name)
    def write(self,data):
        target=self.began+(self.sent+len(data))/self.rate
        delay=target-time.monotonic()
        if delay>0:time.sleep(delay)
        value=self.raw.write(data);self.sent+=len(data);return value


def main():
    clip = Path('/tmp/fixture.ts')
    mp4 = Path('/tmp/fixture.mp4')
    subprocess.run(fixture_command(clip, 60, 'h264'), check=True, timeout=180)
    subprocess.run(mp4_command(mp4), check=True, timeout=60)
    state = FixtureState(clip, 60, 'h264', mp4)
    rate=int(os.environ.get('VOD_FIXTURE_BYTES_PER_SECOND','0'))
    if rate and not 8192<=rate<=10*1024*1024:raise ValueError('Invalid fixture VOD pace')
    if rate:state.stats['vod']['bytes_per_second']=rate
    parent = handler_for(state)
    class Handler(ArchiveMixin,parent):
        archive_path = clip
        archive_hash = state.stats['fixture_sha256']
        def alias(self):
            if urlsplit(self.path).path in ('/movie/fixture/fixture/fixture.mp4', '/series/fixture/fixture/fixture.mp4'):
                self.path = '/vod.mp4'
        def do_GET(self):
            if isinstance(self.wfile,PacedWriter):self.wfile=self.wfile.raw
            self.alias()
            self._vod_pace=bool(rate and urlsplit(self.path).path=='/vod.mp4')
            super().do_GET()
        def do_HEAD(self):
            if isinstance(self.wfile,PacedWriter):self.wfile=self.wfile.raw
            self._vod_pace=False
            self.alias()
            super().do_HEAD()
        def end_headers(self):
            super().end_headers()
            if getattr(self,'_vod_pace',False):self.wfile=PacedWriter(self.wfile,rate)
    ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()

if __name__ == '__main__':
    main()
