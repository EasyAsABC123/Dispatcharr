"""Bounded metadata-only control, detecting observer death before RPC timeout.
No app/Redis/Source import or process creation. Used by the existing guest.
"""
import json
import time
from pathlib import Path


class ObserverUnavailable(RuntimeError):
    observer_infrastructure_failure = True


def request(root, name, body, pid, generation, deadline, proc=Path('/proc')):
    root = Path(root)
    path = root / (name + '.json')
    pending = path.with_suffix('.pending')
    pending.write_text(json.dumps(body))
    pending.replace(path)
    receipt = root / (name + '-receipt.json')
    while True:
        if receipt.exists():
            value = json.loads(receipt.read_text())
            if value.get('actual') is not True:
                return {'observer_failure': {'kind': 'nonactual_control_receipt', 'receipt': value}}
            return value
        final = root / 'observer-final.json'
        if final.exists():
            return {'observer_failure': {'kind': 'observer_terminated', 'final': json.loads(final.read_text())}}
        try:
            stat = (proc / str(pid) / 'stat').read_text().rsplit(')', 1)[1].split()
            observed_generation, state = stat[19], stat[0]
        except (FileNotFoundError, ProcessLookupError):
            observed_generation, state = None, None
        if observed_generation != generation or state == 'Z':
            return {'observer_failure': {'kind': 'observer_generation_absent_or_zombie',
                                         'PID': pid, 'expected_generation': generation,
                                         'observed_generation': observed_generation, 'state': state}}
        if time.monotonic() >= deadline:
            return {'observer_failure': {'kind': 'live_observer_control_deadline', 'PID': pid}}
        time.sleep(min(.01, max(0, deadline - time.monotonic())))
