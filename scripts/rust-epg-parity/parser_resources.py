"""Check isolated native-parser shim sanitizers and retained-memory bounds.

This exercises a native probe, not full ingestion-job resource acceptance.
"""
import argparse
import ast
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--goldens', type=Path, required=True)
    parser.add_argument('--asan-driver', type=Path, required=True)
    parser.add_argument('--resource-driver', type=Path, required=True)
    parser.add_argument('--resource-kind', choices=['c-shim', 'rust-library'], default='c-shim')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    goldens = json.loads(args.goldens.read_bytes())
    original = (ROOT/'apps/epg/tasks.py').read_bytes()
    assert hashlib.sha256(original).hexdigest() == goldens['source_sha256']
    scope = {'os': os, '_HTML_ENTITY_DOCTYPE': (ROOT/'rust/crates/ingest-core/datasets/html4-doctype.xml').read_bytes()}
    names = {'_PrependStream', '_open_xmltv_file'}
    nodes = [node for node in ast.parse(original).body
             if isinstance(node, (ast.ClassDef, ast.FunctionDef)) and node.name in names]
    exec(compile(ast.Module(body=nodes, type_ignores=[]), '<original prefix>', 'exec'), scope)
    reports, failures, resources = [], [], []
    environment = dict(os.environ, ASAN_OPTIONS='detect_leaks=1:abort_on_error=1')
    with tempfile.TemporaryDirectory() as directory:
        raw, prepared = Path(directory)/'raw.xml', Path(directory)/'prepared.xml'

        def prepare():
            with scope['_open_xmltv_file'](str(raw)) as source, prepared.open('wb') as dest:
                while block := source.read(32768):
                    dest.write(block)

        for index, case in enumerate(goldens['cases']):
            raw.write_bytes(base64.b64decode(case['input_base64']))
            prepare()
            result = subprocess.run([str(args.asan_driver.resolve()), str(prepared), str(case['chunk'])],
                                    capture_output=True, text=True, timeout=5, env=environment)
            try:
                actual = json.loads(result.stdout)
            except ValueError:
                actual = None
            passed = (result.returncode == 0 and not result.stderr and actual is not None
                      and not actual['capacity'] and actual['events'] == len(case['expected']['events']))
            reports.append({'case': index, 'passed': passed})
            if not passed:
                failures.append({'case': index, 'returncode': result.returncode,
                                 'stderr': result.stderr, 'stdout': result.stdout})
        for count in [50000, 100000]:
            with raw.open('wb') as dest:
                dest.write(b'<tv>')
                record = b'<programme channel="one"><title>'+b'x'*1024+b'</title></programme>'
                for _ in range(count):
                    dest.write(record)
                dest.write(b'</tv>')
            prepare()
            result = subprocess.run([str(args.resource_driver.resolve()), str(prepared)], capture_output=True,
                                    text=True, timeout=30, check=True)
            actual = json.loads(result.stdout)
            passed = actual['events'] == count and not actual['capacity'] and actual['peak_rss_kib'] < 48*1024
            resources.append({'records': count, 'input_bytes': raw.stat().st_size,
                              'passed': passed, **actual})
        for name, body in [('oversized-record', b'<tv><programme><title>'+b'x'*(5*1024*1024)+b'</title></programme></tv>'),
                           ('comment-node-budget', b'<tv><programme>'+b'<!--x-->'*10000+b'</programme></tv>'),
                           ('oversized-prologue', b' '*(2*1024*1024)+b'<tv/>')]:
            raw.write_bytes(body)
            prepare()
            result = subprocess.run([str(args.resource_driver.resolve()), str(prepared)], capture_output=True,
                                    text=True, timeout=5, check=True)
            actual = json.loads(result.stdout)
            resources.append({'case': name, 'passed': actual['capacity'] and actual['events'] == 0, **actual})
    report = {'scope': 'isolated-native-XML-parser-shim-sanitizer-and-memory-checks',
              'passed': not failures and all(case['passed'] for case in resources),
              'sanitizer_cases': len(reports), 'sanitizer_failures': failures, 'resources': resources,
              'resource_driver_kind': args.resource_kind,
              'goldens_sha256': hashlib.sha256(args.goldens.read_bytes()).hexdigest(),
              'driver_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                                for p in [args.asan_driver, args.resource_driver]},
              'limits': ['ASAN-instrumented shim/driver; libxml2 static library is not sanitizer-instrumented.',
                         'Peak RSS comes from /proc/self/status VmHWM after exec; getrusage inherited parent high water is excluded.',
                         'RSS is the selected standalone resource driver only; no application, SQL or job resource claim.',
                         'Source-equivalence tree contents have a separate native Rust driver comparison.'],
              'application_performance_verified': False, 'full_refresh_job_verified': False}
    args.output.write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'sanitizer_cases': len(reports), 'failures': len(failures),
                      'resource_checks': len(resources)}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
