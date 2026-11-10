#!/usr/bin/env python3
"""Build from an immutable source tar and bind its exact content to an image ID."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import re
from pathlib import PurePosixPath
import subprocess
import tarfile
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]


MAX_ARCHIVE_BYTES = 2 * 1024**3
MAX_MEMBER_BYTES = 512 * 1024**2
MAX_MEMBERS = 100000

def _digest_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()

def _source_name(name):
    path = PurePosixPath(name)
    return (isinstance(name, str) and '\\' not in name and '\0' not in name
            and not path.is_absolute() and str(path) == name
            and all(part not in ('', '.', '..') for part in path.parts)
            and (name in ('LICENSE', 'ATTRIBUTION.md') or (len(path.parts) >= 2 and path.parts[0] in ('rust', 'frontend')))
            and not set(path.parts) & {'target', '.git', '__pycache__', 'node_modules', 'dist'})

def copy_replay_archive(source, destination):
    # Copy before validating: Docker consumes retained bytes, never a later
    # read of the operator's potentially changing input or mutable workspace.
    total = 0
    with Path(source).open('rb') as incoming, Path(destination).open('xb') as output:
        for block in iter(lambda: incoming.read(1024 * 1024), b''):
            total += len(block)
            if total > MAX_ARCHIVE_BYTES:
                raise ValueError('Source archive exceeds bounded intake')
            output.write(block)

def validate_replay_archive(archive, manifest_raw):
    if len(manifest_raw) > 32 * 1024**2:
        raise ValueError('Source manifest exceeds bounded intake')
    def unique_pairs(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('Duplicate source manifest key')
            result[key] = value
        return result
    manifest = json.loads(manifest_raw, object_pairs_hook=unique_pairs)
    if not isinstance(manifest, dict) or type(manifest.get('format_version')) is not int or manifest.get('format_version') != 1 or manifest.get('passed') is not True:
        raise ValueError('Successful source build manifest required')
    hashes = manifest.get('source_sha256')
    if not isinstance(hashes, dict) or not 1 <= len(hashes) <= MAX_MEMBERS:
        raise ValueError('Invalid source hash map')
    if any(not _source_name(name) or not isinstance(value, str)
           or re.fullmatch('[0-9a-f]{64}', value) is None for name, value in hashes.items()):
        raise ValueError('Unsafe source member or invalid digest')
    required = {'LICENSE', 'ATTRIBUTION.md', 'rust/Dockerfile.app', 'rust/Cargo.toml',
                'frontend/package.json', 'frontend/package-lock.json'}
    if not required <= hashes.keys():
        raise ValueError('Required application source inputs missing')
    tree = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if manifest.get('source_tree_sha256') != tree:
        raise ValueError('Source manifest tree digest mismatch')
    if Path(archive).stat().st_size > MAX_ARCHIVE_BYTES:
        raise ValueError('Source archive exceeds bounded intake')
    if _digest_file(archive) != manifest.get('source_archive_sha256'):
        raise ValueError('Source archive byte digest mismatch')
    seen, expanded = set(), 0
    try:
        with tarfile.open(archive, mode='r:') as source:
            for member in source:
                if len(seen) >= MAX_MEMBERS or member.type not in (tarfile.REGTYPE, tarfile.AREGTYPE) or not _source_name(member.name):
                    raise ValueError('Unsafe or excessive archive members')
                if member.name in seen or member.name not in hashes:
                    raise ValueError('Duplicate or unexpected archive member')
                if member.size < 0 or member.size > MAX_MEMBER_BYTES:
                    raise ValueError('Source member exceeds bounded intake')
                expanded += member.size
                if expanded > MAX_ARCHIVE_BYTES:
                    raise ValueError('Expanded archive exceeds bounded intake')
                seen.add(member.name)
                digest = hashlib.sha256()
                handle = source.extractfile(member)
                if handle is None:
                    raise ValueError('Missing regular file data')
                count = 0
                for block in iter(lambda: handle.read(1024 * 1024), b''):
                    count += len(block)
                    digest.update(block)
                if count != member.size or digest.hexdigest() != hashes[member.name]:
                    raise ValueError('Archive member digest mismatch')
    except tarfile.TarError as error:
        raise ValueError('Invalid source tar archive') from error
    if seen != hashes.keys():
        raise ValueError('Archive members differ from source manifest')
    return manifest

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--tag', default='dispatcharr-rust-candidate:milestone2')
    parser.add_argument('--timeout', type=int, default=600)
    parser.add_argument('--target',choices=['frontend-tests'],help='Validate unchanged React tests without building the app runtime')
    parser.add_argument('--source-archive', type=Path, help='Replay exact retained source.tar; requires source-manifest')
    parser.add_argument('--source-manifest', type=Path, help='Passing original build.json binding the retained archive')
    args = parser.parse_args()
    if (args.source_archive is None) != (args.source_manifest is None):
        parser.error('source-archive and source-manifest must be supplied together')
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    if any(destination.iterdir()):
        parser.error('output must be empty')
    replay = args.source_archive is not None
    source_manifest = None
    if replay:
        try:
            with args.source_manifest.open('rb') as manifest_input:
                raw_manifest = manifest_input.read(32 * 1024**2 + 1)
            if len(raw_manifest) > 32 * 1024**2:
                raise ValueError('Source manifest exceeds bounded intake')
            copy_replay_archive(args.source_archive, destination/'source.tar')
            source_manifest = validate_replay_archive(destination/'source.tar', raw_manifest)
            (destination/'source-manifest.json').write_bytes(raw_manifest)
        except (OSError, ValueError) as error:
            parser.error('Invalid replay inputs: '+str(error))
        inputs = None
        hashes = source_manifest['source_sha256']
        source_sha = source_manifest['source_tree_sha256']
    else:
        paths = [ROOT / 'LICENSE', ROOT / 'ATTRIBUTION.md']
        # Preserve the original React source and lockfile; exclude untracked local
        # environment files and build/dependency artifacts from the image.
        tracked_frontend = subprocess.check_output(['git','ls-files','-z','--','frontend'],cwd=ROOT).split(b'\0')
        paths.extend(ROOT / os.fsdecode(path) for path in tracked_frontend if path)
        excluded = {'target', '.git', '__pycache__'}
        for directory, subdirectories, filenames in os.walk(ROOT / 'rust'):
            # Prune build trees before traversing them; their contents are never
            # source inputs and can dwarf the repository during parallel checks.
            subdirectories[:] = [name for name in subdirectories if name not in excluded]
            paths.extend(Path(directory) / name for name in filenames if name not in excluded)
        # Hash the same in-memory bytes that enter the tar; later source edits cannot
        # change the snapshot compiled by Docker.
        inputs = {str(path.relative_to(ROOT)): path.read_bytes() for path in sorted(paths)}
        hashes = {name: hashlib.sha256(content).hexdigest() for name, content in inputs.items()}
        source_sha = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    report = {'format_version': 1, 'passed': False, 'tag': args.tag,
              'source_tree_sha256': source_sha, 'source_sha256': hashes,
              'git_revision': source_manifest.get('git_revision') if replay else subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
              'worktree_status': source_manifest.get('worktree_status') if replay else subprocess.check_output(['git', 'status', '--short'], cwd=ROOT, text=True),
              'source_scope': 'immutable Rust tree, tracked unchanged React frontend, LICENSE and ATTRIBUTION.md; excludes local environment/dependency/build artifacts'}
    report['source_input_mode'] = 'validated-archive-replay' if replay else 'workspace-snapshot'
    report['candidate_overlay_scope'] = 'current workspace deployment template; not archived application source'
    if replay:
        report['replayed_source_manifest_sha256'] = hashlib.sha256(raw_manifest).hexdigest()
        report['replayed_source_image_id'] = source_manifest.get('image_id')
        report['mutable_application_source_read'] = False
    overlay_template=(ROOT/'docker/compose.rust-candidate.yml').read_text()
    if overlay_template.count('image: dispatcharr-rust-candidate:milestone2')!=1:parser.error('Candidate Compose image slot changed')
    status = 1
    with tempfile.TemporaryFile() as archive:
        if replay:
            with (destination/'source.tar').open('rb') as incoming:
                shutil.copyfileobj(incoming, archive)
        else:
            with tarfile.open(fileobj=archive, mode='w') as tar:
                for name, content in inputs.items():
                    info = tarfile.TarInfo(name)
                    info.size, info.mode, info.mtime = len(content), 0o644, 0
                    tar.addfile(info, io.BytesIO(content))
        archive.seek(0)
        # Retain the exact source bytes as well as their hashes. A source-bound
        # image may be built between commits while parallel work continues; its
        # archive must remain reconstructible independently of later edits.
        if not replay:
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
                overlay=overlay_template.replace('image: dispatcharr-rust-candidate:milestone2','image: '+args.tag,1)
                (destination/'candidate-overlay.yml').write_text(overlay)
                (destination/'candidate-overlay-template.yml').write_text(overlay_template)
                report['candidate_overlay_sha256']=hashlib.sha256(overlay.encode()).hexdigest()
                report['candidate_overlay_template_sha256']=hashlib.sha256(overlay_template.encode()).hexdigest()
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
