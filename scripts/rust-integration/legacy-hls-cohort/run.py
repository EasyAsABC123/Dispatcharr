"""Export the legacy HLS fixture from Git and optionally run the immutable pair."""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import time

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
COORDINATOR = 'scripts/rust-milestone-batches/coordinator.py'
ORIGINAL_REVISION = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ORIGINAL_IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ORIGINAL_ARCHIVE = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
CANDIDATE_FILE = 'rust/crates/hls-http/src/shared.rs'
CANDIDATE_FILE_SHA = '57fe46fec2ab7d097699123e1c23d0949687913ab1739527839b5a0e7ad3b055'
BUDGET = 600
CLEANUP = 90


def digest(stream):
    return hashlib.file_digest(stream, 'sha256').hexdigest()


def sha(path):
    with path.open('rb') as stream:
        return digest(stream)


def load(path):
    return json.loads(path.read_bytes())


def save(path, value):
    path.write_text(json.dumps(value, indent=2)+'\n')


def need(condition, message):
    if not condition:
        raise ValueError(message)


def verify_build(build_path):
    build = load(build_path)
    need(build['format_version'] == 1 and build['passed'] is True and build['exit_code'] == 0,
         'Successful immutable build receipt required')
    need(re.fullmatch('[0-9a-f]{40}', build['git_revision']), 'Build revision')
    need(re.fullmatch(r'sha256:[0-9a-f]{64}', build['image_id']), 'Build image ID')
    source_map = build['source_sha256']
    encoded = json.dumps(source_map, sort_keys=True, separators=(',', ':')).encode()
    need(hashlib.sha256(encoded).hexdigest() == build['source_tree_sha256'], 'Build source map mismatch')
    archive = build_path.parent/build['source_archive']
    need(sha(archive) == build['source_archive_sha256'], 'Build archive mismatch')
    with tarfile.open(archive) as bundle:
        member = bundle.getmember(CANDIDATE_FILE)
        need(member.isfile() and member.size <= 1048576, 'Archived HLS renderer type or size')
        renderer_hash = digest(bundle.extractfile(member))
    need(renderer_hash == source_map[CANDIDATE_FILE] == CANDIDATE_FILE_SHA, 'Expected paired HLS renderer')
    return build, archive


def export(output):
    output.mkdir(mode=0o700)
    manifest = load(HERE/'support.json')
    support = output/'support'
    for name, row in manifest['files'].items():
        path = support/name
        path.parent.mkdir(parents=True, exist_ok=True)
        data = subprocess.check_output(['git', 'show', f"{manifest['revision']}:{row['source']}"], cwd=ROOT)
        need(hashlib.sha256(data).hexdigest() == row['sha256'], f'Pinned support input: {name}')
        path.write_bytes(data)
        path.chmod(row['mode'])
    archive = output/'original.tar'
    git_root = subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], cwd=ROOT, text=True).strip()
    with archive.open('xb') as stream:
        subprocess.run(['git', 'archive', '--format=tar', ORIGINAL_REVISION], cwd=git_root, stdout=stream, check=True)
    return verify_export(output)


def verify_export(output):
    manifest = load(HERE/'support.json')
    support = output/'support'
    for name, row in manifest['files'].items():
        item = support/name
        need(item.is_file() and not item.is_symlink() and sha(item) == row['sha256']
             and item.stat().st_mode & 0o777 == row['mode'], f'Exact support input: {name}')
    need({path.relative_to(support).as_posix() for path in support.rglob('*') if path.is_file()}
         == set(manifest['files']), 'Unexpected exported support file')
    archive = output/'original.tar'
    need(archive.is_file() and not archive.is_symlink() and sha(archive) == ORIGINAL_ARCHIVE, 'Pinned Original archive')
    with tarfile.open(archive) as original:
        files = {row.name: digest(original.extractfile(row)) for row in original.getmembers() if row.isfile()}
    return {
        'Original_archive': str(archive), 'Original_source_root': None, 'Original_source_sha256': files,
        'support_root': str(support), 'functional_pids_limit': 1600,
    }


def pinned_coordinator(support):
    spec = importlib.util.spec_from_file_location('owned_coordinator', support/COORDINATOR)
    coordinator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(coordinator)
    return coordinator


def run_bound(output):
    binding = verify_export(output)
    declared = load(output/'binding.json')
    docker = require_local_docker(declared['docker'])
    build, _ = verify_build(Path(declared['candidate_build']))
    need(declared['candidate_image_id'] == build['image_id']
         and declared['candidate_source_tree_sha256'] == build['source_tree_sha256'], 'Run build binding drift')
    binding.update(docker=str(docker), candidate_image_id=build['image_id'],
                   fixture_output=str(output/'fixtures'), performance=False)
    context = load(output/'dispatch/context.json')
    need(context['heavy_exclusive'] and context['active_functional_fixtures'] == 1
         and context['claim_persisted'] and not context['performance']
         and context['holder_pid'] == os.getppid()
         and 0 < context['deadline_monotonic']-time.monotonic() <= BUDGET, 'Owned dispatcher context required')
    support = Path(binding['support_root'])
    sys.path.insert(0, str(support/'cohort'))
    import fixture as adapter
    import hls_case
    adapter.ROOT = support
    results = []
    report = {'status': 'running', 'scope': 'one valid Original and Native HLS descriptor reader',
              'full_M3_passed': False, 'performance_verified': False, 'backends': results}
    summary = output/'report.json'
    cleanup_keys = ('exact_owned_absence', 'all_original_process_generations_absent', 'host_CLI_groups_settled')
    for backend in ('Original', 'Native'):
        row = {'backend': backend}
        results.append(row)
        save(summary, report)
        fixture = adapter.Fixture(binding, backend, context)
        try:
            [base] = json.loads(fixture.command([fixture.docker, 'image', 'inspect', ORIGINAL_IMAGE], secret=True))
            need(ORIGINAL_IMAGE in base['RepoDigests'], 'Exact local Original image')
            if backend == 'Native':
                [candidate] = json.loads(fixture.command([fixture.docker, 'image', 'inspect', build['image_id']], secret=True))
                need(candidate['Id'] == build['image_id'], 'Exact local Native image')
            fixture.bootstrap()
            with fixture.independent_context('output-profile'):
                cid = fixture.native if backend == 'Native' else fixture.command(fixture.compose+['ps', '-q', 'baseline']).strip()
                [image] = json.loads(fixture.command([fixture.docker, 'inspect', cid], secret=True))
                need(image['State']['Running'], 'Running immutable application')
                row['image_id'] = image['Image']
                if backend == 'Native':
                    actual_tree = image['Config']['Labels']['org.opencontainers.image.source-tree-sha256']
                    need(image['Image'] == build['image_id'] and image['Path'] == '/usr/local/bin/dispatcharr-rs'
                         and actual_tree == build['source_tree_sha256'], 'Native runtime image identity')
                    row['source_tree_sha256'] = actual_tree
                else:
                    need(image['Config']['Image'] == ORIGINAL_IMAGE and image['Image'] == base['Id'],
                         'Original runtime image identity')
                    row['original_image_reference'] = ORIGINAL_IMAGE
                row['application'] = hls_case.run(fixture)
        except Exception as error:
            fixture.failure = type(error).__name__
            row['failure_type'] = type(error).__name__
        finally:
            try:
                fixture.close()
                row['cleanup'] = fixture.cleanup_proof()
            except Exception as error:
                row['cleanup_failure_type'] = type(error).__name__
            save(summary, report)
        if not all(row.get('cleanup', {}).get(key) is True for key in cleanup_keys):
            break
    passed = len(results) == 2 and all(
        row.get('application', {}).get('status') == 'passed' and 'failure_type' not in row
        and all(row.get('cleanup', {}).get(key) is True for key in cleanup_keys) for row in results
    )
    report['status'] = 'passed' if passed else 'failed'
    save(summary, report)
    return 0 if passed else 1


def require_local_docker(executable):
    host = os.environ.get('DOCKER_HOST', '')
    need(host.startswith('unix:///') and Path(host.removeprefix('unix://')).is_socket(),
         'Set DOCKER_HOST to the already-owned local Unix socket')
    docker = shutil.which(executable)
    need(docker is not None, 'Docker CLI required')
    return Path(docker).resolve()


def launch(output, build_path, executable):
    docker = require_local_docker(executable)
    build, _ = verify_build(build_path)
    with (ROOT/COORDINATOR).with_name('.dispatcher.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        binding = export(output)
        dispatch = output/'dispatch'
        dispatch.mkdir(mode=0o700)
        save(output/'binding.json', {'candidate_build': str(build_path), 'candidate_image_id': build['image_id'],
                                    'candidate_source_tree_sha256': build['source_tree_sha256'], 'docker': str(docker)})
        save(dispatch/'context.json', {'heavy_exclusive': True, 'active_functional_fixtures': 1,
                                       'attempt': 1, 'claim_persisted': True, 'deadline_monotonic': time.monotonic()+BUDGET,
                                       'cleanup_seconds': CLEANUP, 'holder_pid': os.getpid(), 'performance': False})
        argv = [sys.executable, '-B', str(Path(__file__).resolve()), '--bound-run', str(output)]
        save(dispatch/'command.json', {'argv': argv, 'seconds': BUDGET+2*CLEANUP})
        coordinator = pinned_coordinator(Path(binding['support_root']))
        receipt = coordinator.command(argv, {'cwd': str(ROOT), 'output_bytes': 1048576},
                                      dispatch, 'execution', BUDGET+2*CLEANUP)
        save(dispatch/'execution.json', receipt)
        return 0 if receipt['actual_exit'] == 0 and receipt['failure'] is None and coordinator.settled(receipt) else 1


def main():
    need(__debug__, 'Fixture requires enabled runtime assertions')
    parser = argparse.ArgumentParser(description=__doc__)
    function = parser.add_mutually_exclusive_group()
    function.add_argument('--prepare-only', action='store_true')
    function.add_argument('--verify-export', action='store_true')
    function.add_argument('--bound-run', action='store_true', help=argparse.SUPPRESS)
    parser.add_argument('--output', type=lambda path: Path(path).resolve(), required=True)
    parser.add_argument('--build', type=lambda path: Path(path).resolve())
    parser.add_argument('--docker', default='docker', help='Docker CLI for the owned local daemon')
    args = parser.parse_args()
    if args.bound_run:
        return run_bound(args.output)
    if args.prepare_only or args.verify_export:
        image = verify_build(args.build)[0]['image_id'] if args.build else None
        binding = export(args.output) if args.prepare_only else verify_export(args.output)
        print(json.dumps({'passed': True, 'support_inputs': len(load(HERE/'support.json')['files']),
                          'original_git_files': len(binding['Original_source_sha256']), 'candidate_image': image,
                          'created_Docker_resources': 0, 'application_calls': 0}, indent=2))
        return 0
    if not args.build:
        parser.error('execution requires --build')
    return launch(args.output, args.build, args.docker)


if __name__ == '__main__':
    raise SystemExit(main())
