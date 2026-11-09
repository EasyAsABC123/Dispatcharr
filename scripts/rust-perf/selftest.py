#!/usr/bin/env python3
"""Exercise Docker sampling, concurrent clients, counters, and comparison gates."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import urllib.request

HERE = Path(__file__).resolve().parent


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def metrics(url):
    with urllib.request.urlopen(url + '/metrics', timeout=3) as response:
        return json.load(response)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--project', default='dispatcharr-perf-selftest')
    parser.add_argument('--keep-running', action='store_true')
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    compose = ['docker', 'compose', '-p', args.project, '-f', str(HERE / 'compose.selftest.yml')]
    url = 'http://127.0.0.1:' + os.environ.get('PERF_FIXTURE_PORT', '19292')
    try:
        run(compose + ['up', '--build', '-d', '--wait'])
        inspection = json.loads(subprocess.check_output(
            ['docker', 'inspect', subprocess.check_output(compose + ['ps', '-q', 'fixture'],
                                                        text=True).strip()], text=True))[0]
        assert inspection['Config']['Labels']['dispatcharr.test.role'] == 'benchmark-fixture'
        with tempfile.TemporaryDirectory(prefix='dispatcharr-perf-') as directory:
            urls = Path(directory) / 'urls.txt'
            urls.write_text(url + '/stream\n')
            for name in ('baseline', 'candidate'):
                run([sys.executable, str(HERE / 'perf.py'), 'run', '--compose-file',
                     str(HERE / 'compose.selftest.yml'), '--project', args.project,
                     '--services', 'fixture', '--urls-file', str(urls), '--scenario',
                     'same-channel', '--viewers', '2', '--warmup', '1', '--duration', '10',
                     '--revision', inspection['Image'], '--fixture-id', 'synthetic-bytes-v1-8192-per-10ms',
                     '--output', str(args.output_dir / (name + '.json'))])
        time.sleep(.2)
        counters = metrics(url)
        assert counters['requests'] >= 8 and counters['bytes'] > 0
        assert counters['active'] == 0 and counters['disconnects'] == counters['requests']
        (args.output_dir / 'fixture-counters.json').write_text(json.dumps(counters, indent=2) + '\n')
        # Independent real runs have scheduling/noise variance. Test comparison
        # mechanics using the same captured evidence for identity, then inject
        # a load error to ensure failure. This asserts no performance improvement.
        baseline = args.output_dir / 'baseline.json'
        run([sys.executable, str(HERE / 'perf.py'), 'compare', str(baseline), str(baseline)])
        bad = json.loads(baseline.read_text())
        bad['measurement']['summary']['errors'] = 1
        bad_path = args.output_dir / 'injected-error.json'
        bad_path.write_text(json.dumps(bad))
        rejected = subprocess.run([sys.executable, str(HERE / 'perf.py'), 'compare',
                                   str(baseline), str(bad_path)], check=False)
        assert rejected.returncode == 1, 'Injected failure must reject comparison'
        print(json.dumps({'selftest_passed': True, 'fixture_counters': counters,
                          'note': 'Harness validation only; no backend performance claim'}, indent=2))
    finally:
        if not args.keep_running:
            run(compose + ['down', '--remove-orphans'])


if __name__ == '__main__':
    main()
