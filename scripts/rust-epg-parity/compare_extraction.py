#!/usr/bin/env python3
"""Replay actual original owned-file XMLTV cases against a compiled native driver.
Original Dispatcharr authors and contributors retain full application credit.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--fixture', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    cases = json.loads(args.fixture.read_text())['extraction_cases']
    if not cases:
        raise ValueError('Empty original extraction corpus')
    args.output.mkdir(parents=True, exist_ok=False)
    result = subprocess.run([str(args.binary.resolve())], input=''.join(
        json.dumps(case)+'\n' for case in cases), text=True,
        capture_output=True, timeout=120, check=True)
    (args.output/'native.jsonl').write_text(result.stdout)
    (args.output/'native-stderr.log').write_text(result.stderr)
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    differences = [{'index': i, 'name': case['name'],
                    'expected': case['expected'], 'actual': row}
                   for i, (case, row) in enumerate(zip(cases, actual))
                   if case['expected'] != row]
    report = {
        'scope': 'Low-level explicit owned-output extraction; no fetch/job acceptance',
        'passed': len(cases) == len(actual) and not differences,
        'source_cases': len(cases), 'native_cases': len(actual),
        'differences': differences,
        'source_fixture_sha256': hashlib.sha256(args.fixture.read_bytes()).hexdigest(),
        'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
        'performance_verified': False,
    }
    (args.output/'comparison.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({key: value for key, value in report.items()
                      if key != 'differences'}))
    if not report['passed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
