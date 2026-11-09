"""Compare a native XMLTV parser driver with actual original lxml goldens."""
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
    source = json.loads(args.goldens.read_bytes())
    cases = source['cases']
    result = subprocess.run([str(args.binary.resolve())],
                            input=''.join(json.dumps({k: v for k, v in case.items() if k != 'expected'})+'\n'
                                          for case in cases), text=True, capture_output=True, timeout=60)
    args.output.with_suffix('.stderr.log').write_text(result.stderr)
    values = [json.loads(line) for line in result.stdout.split('\n') if line]
    failures = [{'index': index, **case, 'actual': value}
                for index, (case, value) in enumerate(zip(cases, values)) if case['expected'] != value]
    report = {'scope': source['scope'], 'passed': result.returncode == 0 and len(values) == len(cases) and not failures,
              'cases': len(cases), 'responses': len(values), 'exit_code': result.returncode,
              'failures': failures, 'goldens_sha256': hashlib.sha256(args.goldens.read_bytes()).hexdigest(),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'original_libxml': source['libxml_version'],
              'full_refresh_job_verified': False, 'application_performance_verified': False}
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({key: report[key] for key in ('passed', 'cases', 'responses', 'exit_code')})
          +f' differences={len(failures)}')
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
