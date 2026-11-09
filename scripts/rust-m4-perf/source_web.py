"""Benchmark-only isolated original web process group; no PG/Redis in this cgroup.
All original application credit belongs to Dispatcharr authors/contributors.
"""
import os
import signal
import subprocess
import threading

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture only')
stop = threading.Event()
for event in (signal.SIGINT, signal.SIGTERM):
    signal.signal(event, lambda *_: stop.set())
children = []
try:
    for command in (['/dispatcharrpy/bin/uwsgi', '--ini', '/verification/uwsgi.ini'],
                    ['/dispatcharrpy/bin/daphne', '-b', '0.0.0.0', '-p', '9191', 'dispatcharr.asgi:application']):
        # Daphne access logs can contain JWT query strings. No process logs persist.
        children.append(subprocess.Popen(command, cwd='/app', stdin=subprocess.DEVNULL,
                                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                         start_new_session=True))
    while not stop.wait(.2):
        if any(child.poll() is not None for child in children):
            raise RuntimeError('Original web child exited')
finally:
    for child in children:
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    for child in children:
        try:
            child.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(child.pid, signal.SIGKILL)
            child.wait(timeout=2)
