#!/usr/bin/env python3
"""Native XC URL alias for the deterministic seekable MP4 fixture."""
import sys
import subprocess
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, '/verification')
sys.path.insert(0, '/timeshift-verification')
from archive_origin import ArchiveMixin
from origin import FixtureState, fixture_command, mp4_command, handler_for


def main():
    clip = Path('/tmp/fixture.ts')
    mp4 = Path('/tmp/fixture.mp4')
    subprocess.run(fixture_command(clip, 60, 'h264'), check=True, timeout=180)
    subprocess.run(mp4_command(mp4), check=True, timeout=60)
    state = FixtureState(clip, 60, 'h264', mp4)
    parent = handler_for(state)
    class Handler(ArchiveMixin,parent):
        archive_path = clip
        archive_hash = state.stats['fixture_sha256']
        def alias(self):
            if urlsplit(self.path).path in ('/movie/fixture/fixture/fixture.mp4', '/series/fixture/fixture/fixture.mp4'):
                self.path = '/vod.mp4'
        def do_GET(self):
            self.alias()
            super().do_GET()
        def do_HEAD(self):
            self.alias()
            super().do_HEAD()
    ThreadingHTTPServer(('0.0.0.0', 8080), Handler).serve_forever()

if __name__ == '__main__':
    main()
