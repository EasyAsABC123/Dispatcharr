"""Benchmark-only web lifecycle; leave PG/Redis alive for fair paired trials.

The upstream application is unchanged. Only the uniquely owned web process group
is stopped, and control is reachable through a container-local Unix socket.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import socketserver
import subprocess
import sys

SOCKET = '/tmp/dispatcharr-benchmark-web.sock'


class WebProcess:
    def __init__(self, command):
        self.command = command
        self.process = None

    def status(self):
        return {'running': self.process is not None and self.process.poll() is None,
                'pid': self.process.pid if self.process else None,
                'exit_code': self.process.poll() if self.process else None}

    def start(self):
        if not self.status()['running']:
            self.process = subprocess.Popen(self.command, start_new_session=True)
        return self.status()

    def stop(self):
        if self.process is not None:
            try:
                # The master may have exited before its workers. Keep the
                # process group scoped to this supervisor's original child.
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.process.pid, signal.SIGKILL)
                self.process.wait(timeout=3)
        return self.status()


def control(action):
    with socket.socket(socket.AF_UNIX) as client:
        client.settimeout(15)
        client.connect(SOCKET)
        client.sendall(action.encode() + b'\n')
        response = b''
        while not response.endswith(b'\n'):
            chunk = client.recv(4096)
            if not chunk:
                raise RuntimeError('web supervisor returned no complete response')
            response += chunk
        output = json.loads(response)
    print(json.dumps(output))
    return 0 if output.get('ok') else 1


def serve():
    web = WebProcess(['uwsgi', '--ini', '/verification/uwsgi.ini'])

    class Handler(socketserver.StreamRequestHandler):
        def handle(self):
            self.connection.settimeout(15)
            action = self.rfile.readline(32).decode().strip()
            if action in ('start', 'stop', 'status'):
                result = getattr(web, action)()
                result['ok'] = True
            else:
                result = {'ok': False, 'error': 'unknown web control'}
            self.wfile.write(json.dumps(result).encode() + b'\n')

    def terminate(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, terminate)
    signal.signal(signal.SIGINT, terminate)
    Path(SOCKET).unlink(missing_ok=True)
    try:
        with socketserver.UnixStreamServer(SOCKET, Handler) as server:
            os.chmod(SOCKET, 0o600)
            web.start()
            server.serve_forever(poll_interval=.2)
    except KeyboardInterrupt:
        pass
    finally:
        web.stop()
        for command in (['redis-cli', 'shutdown', 'nosave'],
                        ['su', '-', os.environ['POSTGRES_USER'], '-c',
                         os.environ['PG_BINDIR'] + '/pg_ctl -D ' + os.environ['POSTGRES_DIR'] + ' stop -m fast']):
            try:
                subprocess.run(command, timeout=8, check=False)
            except (OSError, subprocess.TimeoutExpired):
                pass
        Path(SOCKET).unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['serve', 'control'])
    parser.add_argument('action', nargs='?', choices=['start', 'stop', 'status'])
    args = parser.parse_args()
    if args.mode == 'control':
        if not args.action:
            parser.error('control needs an action')
        return control(args.action)
    serve()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
