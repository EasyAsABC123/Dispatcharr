#!/usr/bin/env python3
"""Archive immutable Python baseline source and run its existing CI tests in Compose."""
import argparse
import datetime
import hashlib
import json
import math
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--timeout', type=float, default=1800,
                        help='Maximum Compose run seconds; failures retain evidence')
    parser.add_argument('labels', nargs='*', help='Override the focused default test labels')
    args = parser.parse_args()
    if not math.isfinite(args.timeout) or args.timeout <= 0:
        parser.error('--timeout must be finite and positive')
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error('Output directory must be empty to avoid overwriting earlier evidence')
    unique = f'dispatcharr-rust-baseline-{os.getpid()}-{uuid.uuid4().hex[:8]}'
    evidence = {'source_commit': BASELINE, 'image': IMAGE, 'platform': 'linux/amd64',
                'labels': args.labels or [
                    'apps.proxy.live_proxy.tests.test_ts_discontinuity',
                    'apps.proxy.live_proxy.tests.test_buffer_discontinuity',
                    'apps.proxy.live_proxy.tests.test_hls_segmenter'],
                'timeout_seconds': args.timeout, 'container': unique,
                'exit_code': 1, 'tools_sha256': {}}
    for path in sorted(Path(__file__).parent.glob('*')):
        if path.is_file() and path.suffix in ('.py', '.sh'):
            evidence['tools_sha256'][str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
    compose_file = ROOT / 'docker/compose.rust-baseline.yml'
    evidence['tools_sha256'][str(compose_file.relative_to(ROOT))] = hashlib.sha256(compose_file.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix='dispatcharr-baseline-') as temporary:
        with (output / 'tests.log').open('w') as log:
            attempted_container = False
            try:
                # Archive bypasses all working-tree edits; report their paths only.
                evidence['working_tree_changes_excluded'] = subprocess.check_output(
                    ['git', 'diff', '--name-only', BASELINE, '--'], cwd=ROOT, text=True,
                    timeout=30).splitlines()
                source = Path(temporary) / 'source'
                source.mkdir()
                archive = Path(temporary) / 'source.tar'
                with archive.open('wb') as stream:
                    subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, stdout=stream,
                                   check=True, timeout=30)
                evidence['git_archive_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
                subprocess.run(['tar', '-xf', str(archive), '-C', str(source), '--no-same-owner'],
                               check=True, timeout=30)
                (source / 'media').mkdir(exist_ok=True)
                environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source),
                                   BASELINE_EVIDENCE_DIR=str(output))
                command = [args.docker, 'compose', '-p', unique, '-f', str(compose_file),
                           'run', '--rm', '--name', unique, 'baseline-tests', *args.labels]
                evidence['command'] = command
                attempted_container = True
                process = subprocess.run(command, env=environment, stdout=log,
                                         stderr=subprocess.STDOUT, timeout=args.timeout)
                evidence['exit_code'] = process.returncode
            except subprocess.TimeoutExpired:
                evidence.update(exit_code=124, failure='timeout')
            except KeyboardInterrupt:
                evidence.update(exit_code=130, failure='interrupted')
            except (OSError, subprocess.SubprocessError) as error:
                evidence.update(exit_code=1, failure=type(error).__name__)
            finally:
                if attempted_container:
                    try:
                        # Only this invocation's unguessable name is targeted.
                        cleanup = subprocess.run([args.docker, 'rm', '-f', unique],
                                                 stdout=log, stderr=subprocess.STDOUT, timeout=30)
                        evidence['cleanup_exit_code'] = cleanup.returncode
                    except (OSError, subprocess.SubprocessError, KeyboardInterrupt) as error:
                        evidence['cleanup_error'] = type(error).__name__
                        if evidence['exit_code'] == 0:
                            evidence['exit_code'] = 1
                evidence['timestamp_utc'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                log.flush()
                (output / 'run.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps(evidence, indent=2))
    raise SystemExit(evidence['exit_code'])


if __name__ == '__main__':
    main()
