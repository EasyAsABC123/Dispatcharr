#!/usr/bin/env python3
"""Run core verification in an isolated Compose project and preserve evidence."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def capture(args):
    return subprocess.check_output(args, cwd=ROOT, text=True, timeout=30).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project', default=f'dispatcharr-rust-check-{os.getpid()}')
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--keep', action='store_true', help='Retain this test project for inspection')
    parser.add_argument('--timeout', type=int, default=1800)
    args = parser.parse_args()
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    destination = (args.evidence_dir or ROOT / 'artifacts/rust' / stamp).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        parser.error('Evidence directory must be empty; use a new directory for each run')
    os.environ['RUST_EVIDENCE_DIR'] = str(destination)
    compose = ['docker', 'compose', '-p', args.project, '-f', 'docker/compose.rust.yml']
    report = {'scope': 'core-verification-not-application-parity', 'passed': False,
              'project': args.project, 'started_utc': stamp, 'commands': []}
    status = 1
    try:
        report['revision'] = capture(['git', 'rev-parse', 'HEAD'])
        report['worktree_status'] = capture(['git', 'status', '--short'])
        report['docker_version'] = capture(['docker', 'version', '--format', '{{.Server.Version}}'])
        report['compose_version'] = capture(['docker', 'compose', 'version', '--short'])
        # New migration files are initially untracked, so a Git diff alone is insufficient.
        paths = capture(['git', 'ls-files', '--cached', '--others', '--exclude-standard']).splitlines()
        report['source_sha256'] = {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in paths if (ROOT / path).is_file() and
            path.startswith(('rust/', 'scripts/rust-', 'scripts/verify-rust-',
                             'docker/compose.rust', 'docs/rust-',
                             'apps/proxy/live_proxy/input/ts_discontinuity.py'))
        }
        with (destination / 'compose.log').open('w') as log:
            for step in (compose + ['config', '--quiet'],
                         compose + ['up', '--build', '--abort-on-container-exit', '--exit-code-from', 'verify']):
                began = time.monotonic()
                result = subprocess.run(step, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
                                        timeout=args.timeout)
                report['commands'].append({'argv': step, 'exit_code': result.returncode,
                                           'elapsed_seconds': time.monotonic() - began})
                if result.returncode:
                    raise RuntimeError('Compose verification failed; inspect compose.log')
        report['image_ids'] = capture(compose + ['images', '-q']).splitlines()
        for artifact in ('rustc.txt', 'stream-core-fanout.jsonl', 'ts-helper-comparison.json'):
            if not (destination / artifact).is_file() or not (destination / artifact).stat().st_size:
                raise RuntimeError('Missing verification artifact: ' + artifact)
        component = json.loads((destination / 'ts-helper-comparison.json').read_text())
        if component.get('scope') != 'ts-discontinuity-helper-only' or not component.get('samples'):
            raise RuntimeError('Invalid component benchmark evidence')
        for path, digest in report['source_sha256'].items():
            if hashlib.sha256((ROOT / path).read_bytes()).hexdigest() != digest:
                raise RuntimeError('Source changed during verification: ' + path)
        report['passed'] = True
        status = 0
    except (OSError, subprocess.SubprocessError, RuntimeError, ValueError) as exc:
        report['error'] = str(exc)
        print(str(exc), file=sys.stderr)
    except KeyboardInterrupt:
        report['error'] = 'Verification interrupted'
        status = 130
    finally:
        if not args.keep:
            try:
                cleanup = subprocess.run(compose + ['down', '--volumes', '--remove-orphans'],
                                         cwd=ROOT, capture_output=True, text=True, timeout=60)
                report['cleanup_exit_code'] = cleanup.returncode
                if cleanup.returncode:
                    report['passed'] = False
                    status = 1
            except (OSError, subprocess.SubprocessError) as exc:
                report['cleanup_error'] = str(exc)
                report['passed'] = False
                status = 1
        report['resources_retained'] = args.keep
        (destination / 'verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(f'Evidence: {destination}')
    return status


if __name__ == '__main__':
    raise SystemExit(main())
