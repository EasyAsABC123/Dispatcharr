"""Fixture-only in-memory command bridge; never persist synthetic passwords.
Original application/Django authors retain all behavior credit.
"""
import json
import subprocess
import sys

def run(argv, *, env, stdin, capture_output, timeout):
    assert argv[:2] == ['/evidence/native-operator', 'createsuperuser']
    assert stdin == subprocess.DEVNULL and capture_output and timeout == 30
    request = {'operator_rpc': True, 'arguments': argv[1:],
               'environment': {k: v for k, v in env.items() if k.startswith('DJANGO_SUPERUSER_')}}
    encoded = json.dumps(request)
    assert len(encoded.encode()) <= 65536
    print(encoded, flush=True)
    raw = sys.stdin.buffer.readline(65537)
    if not raw.endswith(b'\n') or len(raw) > 65536:
        raise RuntimeError('BoundedOperatorReply')
    reply = json.loads(raw)
    assert type(reply['returncode']) is int
    return subprocess.CompletedProcess(argv, reply['returncode'],
        bytes.fromhex(reply['stdout_hex']), bytes.fromhex(reply['stderr_hex']))
