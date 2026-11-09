#!/usr/bin/env python3
"""Capture immutable-source helper vectors in a digest-pinned Docker container."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--docker', default='docker')
    parser.add_argument('--busy-controls', action='store_true', help='Capture pure source busy archive Range controls')
    parser.add_argument('--wire-ranges', action='store_true', help='Capture native archive pure range/length-header contracts')
    args = parser.parse_args()
    if args.output.exists():
        parser.error('Output must be new')
    with tempfile.TemporaryDirectory(prefix='dispatcharr-timeshift-helpers-') as temporary:
        source = Path(temporary)/'source'
        source.mkdir()
        archive = subprocess.run(['git', 'archive', BASELINE], cwd=ROOT, check=True, capture_output=True).stdout
        subprocess.run(['tar', '-x', '--same-permissions', '-C', str(source)], input=archive, check=True)
        command = [args.docker, 'run', '--rm', '--network', 'none', '--read-only',
                   '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges:true',
                   '-e', 'PYTHONDONTWRITEBYTECODE=1', '-v', str(source)+':/app:ro',
                   '-v', str(Path(__file__).resolve().parent)+':/verification:ro',
                   '--entrypoint', '/dispatcharrpy/bin/python', IMAGE, '/verification/'+('capture_busy_controls.py' if args.busy_controls else 'capture_ranges.py' if args.wire_ranges else 'capture_helpers.py')]
        result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=60)
        report = json.loads(result.stdout)
        report.update(source_commit=BASELINE, source_image=IMAGE,
                      source_archive_sha256=hashlib.sha256(archive).hexdigest(),
                      network_disabled=True, source_read_only=True)
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps({'capture_complete': True, 'vectors': report['vector_count'], 'rust_parity_verified': False}))

if __name__ == '__main__':
    main()
