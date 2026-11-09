#!/usr/bin/env python3
"""Compare Rust XMLTV preparation against separately captured original runtime."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--goldens', type=Path, required=True)
    parser.add_argument('--binary', type=Path, default=ROOT/'rust/target/debug/examples/xmltv_parity')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    original = json.loads(args.goldens.read_text())
    cases = [({'kind': 'time', 'value': case['input']}, case['expected']) for case in original['times']]
    cases.extend(({'kind': 'complete', 'input_base64': case['input_base64']}, case['expected'])
                 for case in original['completeness'])
    cases.append(({'kind': 'missing'}, original['missing_file_complete']))
    cases.extend(({'kind': 'prefix', 'input_base64': case['input_base64'], 'chunk': case['chunk']},
                  case['expected_sha256']) for case in original['prefixes'])
    result = subprocess.run([str(args.binary)], input=''.join(json.dumps(case)+'\n' for case, _ in cases),
                            text=True, capture_output=True, check=True)
    responses = [json.loads(line) for line in result.stdout.split('\n') if line]
    if len(responses) != len(cases):
        raise RuntimeError('XMLTV fixture response count differs')
    failures = [{'case': case, 'expected': expected, 'actual': actual}
                for (case, expected), actual in zip(cases, responses) if expected != actual]
    source_files = [ROOT/'rust/crates/ingest-core/Cargo.toml', ROOT/'rust/Cargo.lock',
                    *sorted((ROOT/'rust/crates/ingest-core/src').glob('*.rs')),
                    ROOT/'rust/crates/ingest-core/examples/xmltv_parity.rs',
                    ROOT/'rust/crates/ingest-core/datasets/html4-doctype.xml', Path(__file__)]
    report = {'scope': original['scope'], 'passed': not failures, 'cases': len(cases),
              'failures': failures, 'source_baseline': '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430',
              'original_python': original['python'], 'original_unicode': original['unicode_version'],
              'original_lxml': original['lxml_version'], 'original_libxml': original['libxml_version'],
              'original_epg_source_sha256': original['source_sha256'],
              'original_capture_tool_sha256': original['tool_sha256'],
              'goldens_sha256': hashlib.sha256(args.goldens.read_bytes()).hexdigest(),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in source_files},
              'full_guide_jobs_migrated': False, 'application_performance_verified': False}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'cases': len(cases), 'failures': len(failures)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
