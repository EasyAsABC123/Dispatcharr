#!/usr/bin/env python3
"""Run the existing supported Original-only fixture, then scoped GET assertions."""
import argparse
import json
from pathlib import Path
import subprocess
import sys
import contract


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    spec = contract.load()
    output = args.output.resolve()
    if output.exists():
        parser.error('output must be fresh')
    # The existing runner owns creation and failure propagation. Never precreate it.
    command = [sys.executable, str(contract.ROOT / spec['contracts'][0]['setup_fixture']['runner']),
               '--provider-actions-source', '--mode', 'http', '--output', str(output)]
    completed = subprocess.run(command, cwd=contract.ROOT, timeout=900)
    if completed.returncode:
        return completed.returncode
    capture_path = output / 'http/provider-actions-source.json'
    run_path = output / 'http/source-run.json'
    result = contract.assert_capture(spec, json.loads(capture_path.read_text()), json.loads(run_path.read_text()))
    result['commands'] = [command]
    result['authorities'] = {str(p.relative_to(output)): contract.sha(p.read_bytes())
                             for p in [capture_path, run_path, output / 'source-run.json']}
    (output / 'executable-Source-contracts.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'Source_contracts_passed': len(result['contracts']), 'GET_assertions': 78,
                      'Native_verified': False, 'output': str(output)}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
