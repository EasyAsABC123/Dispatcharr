"""Replay immutable original XMLTV metadata goldens against a Rust driver."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--goldens', type=Path, required=True)
    parser.add_argument('--binary', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    source = json.loads(args.goldens.read_bytes())
    cases = source['cases']
    inputs = ''.join(json.dumps(case['input'])+'\n' for case in cases)
    result = subprocess.run([str(args.binary)], input=inputs, text=True, capture_output=True,
                            check=True, timeout=60)
    values = [json.loads(line) for line in result.stdout.split('\n') if line]
    if len(values) != len(cases):
        raise RuntimeError('response count differs')
    failures = [{'index': index, **case, 'actual': value}
                for index, (case, value) in enumerate(zip(cases, values)) if case['expected'] != value]
    files = [ROOT/'rust/crates/ingest-core/Cargo.toml', ROOT/'rust/Cargo.lock',
             *sorted((ROOT/'rust/crates/ingest-core/src').glob('*.rs')),
             ROOT/'rust/crates/ingest-core/examples/xmltv_metadata_parity.rs', Path(__file__)]
    report = {'scope': source['scope'], 'passed': not failures, 'cases': len(cases),
              'failures': failures, 'source_sha256': source['source_sha256'],
              'original_python': source['python'], 'original_lxml': source['lxml_version'],
              'original_libxml': source['libxml_version'],
              'goldens_sha256': hashlib.sha256(args.goldens.read_bytes()).hexdigest(),
              'binary_sha256': hashlib.sha256(args.binary.read_bytes()).hexdigest(),
              'rust_source_sha256': {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                                     for p in files},
              'full_xml_parser_verified': False, 'application_performance_verified': False}
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'cases': len(cases), 'failures': len(failures)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
