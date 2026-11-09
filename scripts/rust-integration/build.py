#!/usr/bin/env python3
"""Build from an immutable source tar and bind its exact content to an image ID."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tag', default='dispatcharr-rust-candidate:milestone2')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--target',choices=['frontend-tests'],help='Validate unchanged React tests without building the app runtime')
    args = parser.parse_args()
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        parser.error('output must be empty')
    paths = [ROOT / 'LICENSE', ROOT / 'ATTRIBUTION.md']
    # Preserve the original React source and lockfile; exclude untracked local
    # environment files and build/dependency artifacts from the image.
    tracked_frontend = subprocess.check_output(['git','ls-files','-z','--','frontend'],cwd=ROOT).split(b'\0')
    paths.extend(ROOT / os.fsdecode(path) for path in tracked_frontend if path)
    paths.extend(path for path in (ROOT / 'rust').rglob('*') if path.is_file()
                 and not set(path.relative_to(ROOT / 'rust').parts) & {'target', '.git', '__pycache__'})
    # Hash the same in-memory bytes that enter the tar; later source edits cannot
    # change the snapshot compiled by Docker.
    inputs = {str(path.relative_to(ROOT)): path.read_bytes() for path in sorted(paths)}
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in inputs.items()}
    source_sha = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    report = {'format_version': 1, 'passed': False, 'tag': args.tag,
              'source_tree_sha256': source_sha, 'source_sha256': hashes,
              'git_revision': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'worktree_status': subprocess.check_output(['git', 'status', '--short'], cwd=ROOT, text=True),
              'source_scope': 'immutable Rust tree, tracked unchanged React frontend, LICENSE and ATTRIBUTION.md; excludes local environment/dependency/build artifacts'}
    status = 1
    with tempfile.TemporaryFile() as archive:
        with tarfile.open(fileobj=archive, mode='w') as tar:
            for name, content in inputs.items():
                info = tarfile.TarInfo(name)
                info.size, info.mode, info.mtime = len(content), 0o644, 0
                tar.addfile(info, io.BytesIO(content))
        archive.seek(0)
        # Retain the exact source bytes as well as their hashes. A source-bound
        # image may be built between commits while parallel work continues; its
        # archive must remain reconstructible independently of later edits.
        with (destination/'source.tar').open('wb') as retained:
            shutil.copyfileobj(archive,retained)
        archive_digest=hashlib.sha256()
        with (destination/'source.tar').open('rb') as retained:
            for block in iter(lambda:retained.read(1024*1024),b''):
                archive_digest.update(block)
        report['source_archive_sha256']=archive_digest.hexdigest()
        report['source_archive']='source.tar'
        archive.seek(0)
        command = ['docker', 'build', '-f', 'rust/Dockerfile.app', '-t', args.tag,
                   '--label', 'org.opencontainers.image.source-tree-sha256=' + source_sha, '-']
        if args.target:command[-1:-1]=['--target',args.target]
        report['command'] = command
        began = time.monotonic()
        try:
            with (destination / 'build.log').open('w') as log:
                result = subprocess.run(command, cwd=ROOT, stdin=archive, stdout=log,
                                        stderr=subprocess.STDOUT, timeout=args.timeout)
            report['exit_code'] = result.returncode
            if result.returncode:
                raise RuntimeError('Docker build failed; inspect build.log')
            inspected = json.loads(subprocess.check_output(['docker', 'image', 'inspect', args.tag], text=True, timeout=30))[0]
            if inspected['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') != source_sha:
                raise RuntimeError('Image source label mismatch')
            if args.target=='frontend-tests':
                frontend_tests=subprocess.check_output(['docker','run','--rm','--network','none','--entrypoint','cat',args.tag,'/verification-results/frontend-tests.json'],text=True,timeout=30)
                (destination/'frontend-tests.json').write_text(frontend_tests)
                frontend=json.loads(frontend_tests)
                report['frontend_tests']={key:frontend.get(key) for key in ('numTotalTests','numPassedTests','numFailedTests','numPendingTests','numTotalTestSuites','numPassedTestSuites','success')}
                if frontend.get('success') is not True:raise RuntimeError('Original frontend tests did not pass')
            else:
                runtime = ['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges']
                report['ffmpeg_version'] = subprocess.check_output(runtime+['--entrypoint','ffmpeg',args.tag,'-version'],text=True,timeout=30).splitlines()[0]
                packages = subprocess.check_output(runtime+['--entrypoint','cat',args.tag,'/usr/share/doc/dispatcharr/runtime-packages.txt'],text=True,timeout=30)
                (destination/'runtime-packages.txt').write_text(packages)
                report['runtime_packages_sha256'] = hashlib.sha256(packages.encode()).hexdigest()
                report['runtime_packages_frozen'] = True
                report['media_runtime_scope'] = 'Exact pinned original FFmpeg/ffprobe ELF closure in isolated prefix; no Python runtime copied'
            report.update(passed=True, image_id=inspected['Id'], platform=inspected['Os'] + '/' + inspected['Architecture'])
            status = 0
        except (OSError, ValueError, subprocess.SubprocessError, RuntimeError) as error:
            report['failure'] = str(error)
        except KeyboardInterrupt:
            report['failure'], status = 'interrupted', 130
        report['elapsed_seconds'] = time.monotonic() - began
        (destination / 'build.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'source_tree_sha256': source_sha, 'evidence': str(destination)}))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
