"""Read-only known Source process witness; never a command-line/env census."""
import json
import os
from pathlib import Path
import socket
from identity import kernel


def main():
    if os.environ.get('PENDING_MAIN_FIXTURE') != 'playback-fixture-only': raise ValueError('OwnedHeaderFixture')
    record = json.loads(Path('/evidence/owned-source-web.json').read_text())
    if record.get('role') != 'source-web' or record.get('nonce') != os.environ.get('PENDING_MAIN_NONCE'): raise ValueError('KnownSourceWebRoleNonce')
    pid = record.get('pid')
    if type(pid) is not int or not 1 < pid < 2**31: raise ValueError('KnownSourcePID')
    root = Path('/proc', str(pid))
    value = kernel((socket.gethostname() + '\n').encode(), (root / 'stat').read_bytes(), (root / 'status').read_bytes(), (root / 'cgroup').read_bytes(), pid, record['starttime'])
    value['application_PID_from_owned_start_record'] = True
    print(json.dumps(value))


if __name__ == '__main__': main()
