"""One owned origin namespace, two real child servers; no Source imports."""
import argparse
import json
import os
import signal
import resource
import subprocess
import time
from pathlib import Path


def identity(pid):
    value = Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19]
    if not value.isdecimal() or int(value) <= 0:
        raise ValueError('actual positive server child identity')
    return value


def run(output, assets):
    resource.setrlimit(resource.RLIMIT_FSIZE,(1048576,1048576))
    output = Path(output)
    if not output.is_dir() or (output / 'origin-ready.json').exists():
        raise ValueError('fresh owned origin output')
    stopping = False
    began = time.monotonic()
    children = []
    logs = []
    errors = []

    def stop(signum, frame):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for role, arguments in [('http', ['tls_origin.py', '--bind-ip', '11.253.250.2', '--source-ip', '11.253.250.3', '--assets', assets, '--output', str(output / 'http')]), ('dns', ['dns_server.py', '--bind-ip', '11.253.250.2', '--origin-ip', '11.253.250.2', '--output', str(output / 'dns')])]:
            stdout = (output / (role + '.stdout')).open('xb')
            stderr = (output / (role + '.stderr')).open('xb')
            logs.extend([stdout, stderr])
            process = subprocess.Popen(['/dispatcharrpy/bin/python', '-B', '/plugin-tools/' + arguments[0], *arguments[1:]], stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr)
            children.append({'role': role, 'process': process, 'PID': process.pid, 'starttime': identity(process.pid)})
        deadline = time.monotonic() + 10
        while not all((output / c['role'] / 'ready.json').exists() for c in children):
            if time.monotonic() >= deadline or any(c['process'].poll() is not None for c in children):
                raise ValueError('real TLS/DNS children ready within10s')
            time.sleep(.02)
        (output / 'origin-ready.json').write_text(json.dumps({'PID': os.getpid(), 'starttime': identity(os.getpid()), 'PID_namespace': os.readlink('/proc/self/ns/pid'), 'network_namespace': os.readlink('/proc/self/ns/net'), 'time_namespace': os.readlink('/proc/self/ns/time'), 'started_epoch': time.time(), 'children': [{k: v for k, v in c.items() if k != 'process'} for c in children]}, indent=2) + '\n')
        while not stopping:
            if time.monotonic() - began > 1290 or any(c['process'].poll() is not None for c in children):
                raise ValueError('owned TLS/DNS lifetime or unexpected child exit')
            if sum(f.stat().st_size for f in output.glob('*.std*')) > 1048576:
                raise ValueError('owned server log capacity1MiB')
            time.sleep(.05)
    except BaseException as exc:
        errors.append({'type': type(exc).__name__, 'message': str(exc)})
    finally:
        for child in children:
            process = child['process']
            try:
                if process.poll() is None:
                    if identity(process.pid) != child['starttime']:
                        raise ValueError('server child generation mismatch before TERM')
                    process.terminate()
            except BaseException as exc:
                errors.append({'type': type(exc).__name__, 'message': str(exc)})
        rows = []
        deadline = time.monotonic() + 5
        for child in children:
            process = child['process']
            forced = False
            try:
                actual = process.wait(timeout=max(.01, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                forced = True
                process.kill()
                actual = process.wait(timeout=2)
            rows.append({k: v for k, v in child.items() if k != 'process'} | {'actual_wait_exit': actual, 'actual_poll_exit': process.poll(), 'forced_kill': forced})
        for stream in logs:
            stream.close()
        complete = stopping and not errors and len(rows) == 2 and all(row['actual_wait_exit'] == row['actual_poll_exit'] == 0 and row['forced_kill'] is False for row in rows)
        (output / 'origin-terminal.json').write_text(json.dumps({'complete': complete, 'children': rows, 'errors': errors, 'stopped_by_signal': stopping, 'terminal_epoch': time.time(), 'terminal_monotonic': time.monotonic(), 'elapsed_seconds': time.monotonic() - began}, indent=2) + '\n')
    return 0 if complete else 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    parser.add_argument('--assets', required=True)
    args = parser.parse_args()
    raise SystemExit(run(args.output, args.assets))
