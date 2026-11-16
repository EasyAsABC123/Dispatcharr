#!/usr/bin/env python3
"""Exercise actual script CLI with host stdlib; never starts Django/Main.

Original Dispatcharr and CPython authors retain all application/framework credit.
An empty exit0 fails: importing definitions is not a functioning entrypoint.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--script', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    script = args.script.resolve(strict=True)
    if args.output.exists():
        parser.error('Fresh receipt required')
    before = hashlib.sha256(script.read_bytes()).hexdigest()
    # --help exits from main's initial ArgumentParser before native adapters,
    # Django, Celery, SQL, broker, processes or application setup can be reached.
    argv = [sys.executable, '-I', '-B', str(script), '--help']
    result = subprocess.run(argv, stdin=subprocess.DEVNULL, capture_output=True,
                            timeout=5, check=False)
    required = [b'usage:', b'--mode', b'--role', b'--source', b'--plugins']
    help_valid = (result.returncode == 0 and not result.stderr and
                  0 < len(result.stdout) <= 16384 and
                  all(value in result.stdout for value in required))
    unchanged = before == hashlib.sha256(script.read_bytes()).hexdigest()
    receipt = {
        'passed': help_valid and unchanged,
        'scope': 'Actual script command-line entrypoint only; not application readiness',
        'argv': argv,
        'script_sha256': before,
        'source_unchanged': unchanged,
        'actual_exit': result.returncode,
        'stdout_bytes': len(result.stdout),
        'stderr_bytes': len(result.stderr),
        'stdout_sha256': hashlib.sha256(result.stdout).hexdigest(),
        'stderr_sha256': hashlib.sha256(result.stderr).hexdigest(),
        'usage_and_all_declared_options_present': help_valid,
        'application_acceptance': False,
        'Django_or_native_service_started': False,
        'performance_verified': False,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
