"""Compare native programme-row projection to the unchanged original loop."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--goldens', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    original = json.loads(args.goldens.read_bytes())
    cases = original['cases']
    result = subprocess.run([str(args.binary.resolve())], input=''.join(json.dumps(case['input'])+'\n' for case in cases),
                            capture_output=True, text=True, check=True, timeout=60)
    actual = [json.loads(line) for line in result.stdout.split('\n') if line]
    failures = [{'index': index, **case, 'actual': value} for index, (case, value) in enumerate(zip(cases, actual))
                if case['expected'] != value]
    report = {'scope': original['scope'], 'passed': len(actual) == len(cases) and not failures,
              'cases': len(cases), 'responses': len(actual), 'failures': failures,
              'goldens_sha256': hashlib.sha256(args.goldens.read_bytes()).hexdigest(),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'source_sha256': original['source_sha256'], 'full_refresh_job_verified': False,
              'application_performance_verified': False}
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'cases': len(cases), 'failures': len(failures)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
