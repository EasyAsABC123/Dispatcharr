"""Read-only task observer plus explicit owned HTTP-route fixture control.
Original Dispatcharr/Celery authors retain credit/licenses. No handler replaced.
The version task's external HTTP dependency is redirected to an owned503server;
this does not establish real upstream version availability/update behavior.
"""
import json
import os
from pathlib import Path
from urllib.parse import urlsplit
from celery.signals import worker_process_init
from periodic_worker_observer import app


@worker_process_init.connect
def owned_http_dependency(**kwargs):
    import requests.sessions
    origin = os.environ['DEFAULT_TASK_HTTP_ORIGIN']
    parsed = urlsplit(origin)
    assert parsed.scheme == 'http' and parsed.hostname == '127.0.0.1' and parsed.port
    original = requests.sessions.Session.request

    def request(self, method, url, *args, **options):
        target = urlsplit(url)
        if target.scheme != 'https' or target.hostname not in ['hub.docker.com', 'api.github.com']:
            raise RuntimeError('External HTTP blocked by owned default-task fixture')
        value = {'method': method, 'original_host': target.hostname, 'original_path': target.path,
                 'fixture_origin': origin, 'scope': 'Explicit version HTTP route dependency control'}
        path = Path('/evidence/default-version-http-route-private.jsonl')
        with path.open('a') as output:
            output.write(json.dumps(value) + '\n')
        return original(self, method, origin + '/owned-version503', *args, **options)

    requests.sessions.Session.request = request
