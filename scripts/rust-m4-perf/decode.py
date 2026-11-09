"""Strict actual HTTP decoder probe. Capability-bearing URLs only arrive via stdin.
Original application credit remains with Dispatcharr authors/contributors.
"""
import hashlib
import json
import os
import subprocess
import sys
from urllib.parse import urlsplit
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
request = json.loads(sys.stdin.buffer.read(65537))
url = request['url']
parts = urlsplit(url)
if parts.scheme != 'http' or parts.hostname not in ('source', 'candidate') or len(url) > 8192:
    raise ValueError('Only isolated backend media endpoints may be decoded')
completed = subprocess.run(['ffmpeg', '-hide_banner', '-loglevel', 'error', '-xerror',
    '-progress', 'pipe:1', '-nostats', '-i', url, '-map', '0:v:0', '-map', '0:a:0',
    '-t', '8', '-f', 'null', '-'], capture_output=True, timeout=45)
frames = max((int(line.split(b'=')[1]) for line in completed.stdout.splitlines() if line.startswith(b'frame=')), default=0)
# Never print raw stderr: FFmpeg can repeat the credential-bearing input URL.
print(json.dumps({'frames': frames, 'audio_and_video_mapped': True,
                  'return_code': completed.returncode,
                  'stderr_sha256': hashlib.sha256(completed.stderr).hexdigest(),
                  'strict_decoder_passed': completed.returncode == 0 and frames >= 50}))
