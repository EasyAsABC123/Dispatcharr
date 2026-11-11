"""Source-only nginx static-location oracle, never application acceptance.
Original Dispatcharr, nginx, DRF and bundled asset authors retain all credit.
"""
import hashlib
import http.client
import json
from pathlib import Path
import subprocess
import time

import importlib.metadata
import sys
import shutil
if __import__('os').environ.get('STATIC_SOURCE_DISPOSABLE_FIXTURE') != 'owned-static-only':
    raise RuntimeError('Owned static-only container required')
sys.path.insert(0, '/runtime')
from collect_static import collect
import rest_framework
package = Path(rest_framework.__file__).parent
manifest = collect(package / 'static/rest_framework', '/out/rest_framework')
manifest['DRF_version'] = importlib.metadata.version('djangorestframework')
manifest['image'] = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
distribution = importlib.metadata.distribution('djangorestframework')
licenses = []
for file in distribution.files:
    if 'license' in str(file).lower() and Path(distribution.locate_file(file)).is_file():
        target = Path('/out/licenses') / str(file)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(distribution.locate_file(file), target)
        licenses.append(str(file))
manifest['licenses'] = licenses
Path('/out/manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
config = '''worker_processes 1;
daemon off;
error_log /tmp/static-nginx-error.log;
pid /tmp/static-nginx.pid;
events { worker_connections 64; }
http {
 include /etc/nginx/mime.types;
 default_type application/octet-stream;
 access_log off;
 server {
  listen 127.0.0.1:8080;
  location /static/ { root /app; }
 }
}
'''
Path('/tmp/static-nginx.conf').write_text(config)
process = subprocess.Popen(['/usr/sbin/nginx', '-c', '/tmp/static-nginx.conf'])
records = []


def observe(method, path, headers=None):
    connection = http.client.HTTPConnection('127.0.0.1', 8080, timeout=5)
    try:
        connection.request(method, path, headers={'Accept-Encoding': 'identity', **(headers or {})})
        response = connection.getresponse()
        body = response.read(2 * 1024 * 1024 + 1)
        assert len(body) <= 2 * 1024 * 1024
        record = {'method': method, 'path': path, 'request_headers': headers or {},
                  'status': response.status, 'headers': response.getheaders(),
                  'body_bytes': len(body), 'body_sha256': hashlib.sha256(body).hexdigest()}
        if response.status >= 400:
            record['body_text'] = body.decode('utf-8')
        return record, body
    finally:
        connection.close()


try:
    deadline = time.monotonic() + 5
    while True:
        try:
            observe('HEAD', '/static/')
            break
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(.02)
    for file in manifest['files']:
        path = '/static/rest_framework/' + file['path']
        get, body = observe('GET', path)
        assert get['status'] == 200 and get['body_sha256'] == file['sha256'], (path, get, Path('/tmp/static-nginx-error.log').read_text()[-2000:])
        assert body == (Path('/app/static/rest_framework') / file['path']).read_bytes()
        head, empty = observe('HEAD', path)
        assert head['status'] == 200 and empty == b''
        assert dict((k.lower(), v) for k, v in head['headers'])['content-length'] == str(file['bytes'])
        records += [get, head]
    path = '/static/rest_framework/css/bootstrap.min.css'
    first, full = observe('GET', path)
    headers = {k.lower(): v for k, v in first['headers']}
    for request in [{'If-None-Match': headers['etag']}, {'If-Modified-Since': headers['last-modified']},
                    {'Range': 'bytes=100-227'}, {'Range': 'bytes=-128'},
                    {'Range': 'bytes=999999999-'},
                    {'Range': 'bytes=100-227', 'If-Range': headers['etag']},
                    {'Range': 'bytes=100-227', 'If-Range': '"wrong"'}]:
        record, body = observe('GET', path, request)
        records.append(record)
        if record['status'] == 206:
            assert body == (full[-128:] if request['Range'] == 'bytes=-128' else full[100:228])
    for path in ['/static/rest_framework/missing.js', '/static/rest_framework/../missing.js',
                 '/static/rest_framework/%2e%2e/missing.js', '/static/rest_framework/%00.js']:
        records.append(observe('GET', path)[0])
    for method, path in [('POST', '/static/rest_framework/css/bootstrap.min.css'), ('GET', '/static/rest_framework/'), ('HEAD', '/static/rest_framework/missing.js')]:
        records.append(observe(method, path)[0])
    report = {'passed': True, 'scope': 'Pinned image nginx with exact Source static location and mime table; identity encoding only; Source-only, not full production proxy/collectstatic or native acceptance',
              'image': manifest['image'], 'DRF_version': manifest['DRF_version'],
              'nginx_version': subprocess.check_output(['/usr/sbin/nginx', '-v'], stderr=subprocess.STDOUT, text=True).strip(),
              'assets': len(manifest['files']), 'records': records,
              'source_static_location': 'location /static/ { root /app; }',
              'native_verified': False, 'performance_verified': False}
    Path('/out/source-nginx-static.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': True, 'assets': len(manifest['files']), 'observations': len(records)}))
finally:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)
