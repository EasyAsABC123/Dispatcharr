"""Paced seekable MPEG-TS archive fixture, separate from the live benchmark.
All application credit belongs to original Dispatcharr authors/contributors.
"""
import datetime
from email.utils import format_datetime
import json
import re
import time
from urllib.parse import urlsplit, parse_qs
from range_http import parse_range, if_range_matches, UnsatisfiableRange


class ArchiveMixin:
    archive_mode = 'all'
    archive_requests = []
    archive_rate = 500000

    def archive_target(self):
        parsed = urlsplit(self.path)
        if parsed.path.startswith('/timeshift/fixture/fixture/') and parsed.path.endswith('/fixture.ts'):
            stamp = parsed.path.split('/')[-2]
            shape = 'underscore' if '_' in stamp else 'colon-seconds' if re.search(r':\d\d:\d\d:\d\d$',stamp) else 'colon-dash'
            return shape
        if parsed.path == '/streaming/timeshift.php':
            query = parse_qs(parsed.query)
            if all(query.get(key) == [value] for key,value in [('username','fixture'),('password','fixture'),('stream','fixture')]):
                return 'query'
        return None

    def do_POST(self):
        if urlsplit(self.path).path != '/archive-control':
            return super().do_POST()
        try:
            length = int(self.headers.get('Content-Length','0'))
            if not 0 < length <= 4096:
                raise ValueError('bounded control required')
            mode = json.loads(self.rfile.read(length))['mode']
            if mode not in ('all','query-only','html-first','forbidden','not-found','server-error'):
                raise ValueError('invalid archive mode')
        except (ValueError,KeyError,TypeError):
            self.send_error(400)
            return
        self.__class__.archive_mode = mode
        self.__class__.archive_requests = []
        payload = json.dumps({'mode':mode}).encode()
        self.send_response(200)
        self.send_header('Content-Length',str(len(payload)))
        self.send_header('Content-Type','application/json')
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if urlsplit(self.path).path == '/archive-stats':
            payload = json.dumps({'mode':self.archive_mode,'requests':self.archive_requests,
                'fixture_sha256':self.archive_hash,'fixture_bytes':self.archive_path.stat().st_size,
                'bytes_per_second':self.archive_rate}).encode()
            self.send_response(200)
            self.send_header('Content-Length',str(len(payload)))
            self.send_header('Content-Type','application/json')
            self.end_headers()
            self.wfile.write(payload)
            return
        shape = self.archive_target()
        if shape is None:
            return super().do_GET()
        self.serve_archive(shape)

    def do_HEAD(self):
        shape = self.archive_target()
        if shape is None:
            return super().do_HEAD()
        self.serve_archive(shape,head=True)

    def serve_archive(self,shape,head=False):
        mode = self.archive_mode
        error = 403 if mode == 'forbidden' else 404 if mode == 'not-found' else 500 if mode == 'server-error' else 404 if mode == 'query-only' and shape != 'query' else 400 if mode == 'html-first' and shape == 'query' else None
        if error:
            self.archive_requests.append({'shape':shape,'status':error})
            self.send_error(error)
            return
        if mode == 'html-first' and shape == 'colon-dash':
            payload = b'<html>fixture provider error; no MPEG-TS data</html>'
            self.archive_requests.append({'shape':shape,'status':200,'media':False})
            self.send_response(200)
            self.send_header('Content-Type','text/html')
            self.send_header('Content-Length',str(len(payload)))
            self.end_headers()
            if not head:
                self.wfile.write(payload)
            return
        size = self.archive_path.stat().st_size
        etag = '"'+self.archive_hash+'"'
        selected = None
        status = 200
        try:
            if not head and if_range_matches(self.headers.get('If-Range'),etag,int(self.archive_path.stat().st_mtime)):
                selected = parse_range(self.headers.get('Range'),size)
                if selected is not None:
                    status = 206
        except UnsatisfiableRange:
            status = 416
        self.archive_requests.append({'shape':shape,'status':status,'range':self.headers.get('Range')})
        self.send_response(status)
        self.send_header('Content-Type','video/mp2t')
        self.send_header('Accept-Ranges','bytes')
        self.send_header('ETag',etag)
        self.send_header('Last-Modified',format_datetime(datetime.datetime.fromtimestamp(int(self.archive_path.stat().st_mtime),datetime.timezone.utc),usegmt=True))
        if status == 416:
            self.send_header('Content-Range',f'bytes */{size}')
            self.send_header('Content-Length','0')
            self.end_headers()
            return
        start,end = selected if selected is not None else (0,size-1)
        length = end-start+1
        self.send_header('Content-Length',str(length))
        if selected is not None:
            self.send_header('Content-Range',f'bytes {start}-{end}/{size}')
        self.end_headers()
        if head:
            return
        began = time.monotonic()
        sent = 0
        try:
            with self.archive_path.open('rb') as stream:
                stream.seek(start)
                while sent < length:
                    chunk = stream.read(min(65536,length-sent))
                    if not chunk:
                        raise RuntimeError('immutable archive changed')
                    self.wfile.write(chunk)
                    sent += len(chunk)
                    delay = began+sent/self.archive_rate-time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
        except (BrokenPipeError,ConnectionResetError):
            pass
