#!/usr/bin/env python3
"""Capture the representative paired Connect method and role matrix."""
import argparse
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import uuid

from foundation_receipt import foundation_receipt

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
ARTIFACTS = HERE.parent
TREE = HERE / 'copied'
TEMPLATE = HERE / 'freeze.json'
DRIVER = ROOT / 'scripts/rust-milestone-batches/coordinator.py'
SOURCE_REVISION = '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
SOURCE_TAR_SHA = 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
REGISTER_BASE = 'd0a4296a2a3bb72e11500bf2bc3925c60835773a'
PAIRED_SCOPES = ('source-bare','source-json','native-bare','native-json')


def sha(path):
    with Path(path).open('rb') as file:
        return hashlib.file_digest(file, 'sha256').hexdigest()


def save(path, value):
    with path.open('x') as file:
        json.dump(value, file, indent=2, sort_keys=True)
        file.write('\n')


def load(path):
    return json.loads(Path(path).read_text())


def load_coordinator():
    spec = importlib.util.spec_from_file_location('connect_coordinator', TREE / 'scripts/rust-milestone-batches/coordinator.py')
    coordinator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(coordinator)
    return coordinator


def verify_inputs():
    freeze = load(TEMPLATE)
    if (freeze['original_revision'] != SOURCE_REVISION
            or freeze['copied_inputs']['original-source.tar'] != SOURCE_TAR_SHA):
        raise ValueError('Connect fixture identity changed')
    for inputs, root in (('driver_inputs', HERE), ('copied_inputs', TREE)):
        for name, expected in freeze[inputs].items():
            if sha(root / name) != expected:
                raise ValueError(f'Frozen Connect fixture drift: {name}')
    return freeze


def verify_build(path, revision):
    manifest = load(path)
    if (not re.fullmatch('[a-f0-9]{40}', revision) or manifest['git_revision'] != revision
            or not re.fullmatch('sha256:[a-f0-9]{64}', manifest['image_id'])
            or not manifest['passed'] or manifest['format_version'] != 1 or manifest['exit_code'] != 0):
        raise ValueError('Passing immutable version 1 candidate and exact revision required')
    archive = path.parent / manifest['source_archive']
    if sha(archive) != manifest['source_archive_sha256']:
        raise ValueError('Candidate source archive identity changed')
    source_map = json.dumps(manifest['source_sha256'], sort_keys=True, separators=(',', ':')).encode()
    if hashlib.sha256(source_map).hexdigest() != manifest['source_tree_sha256']:
        raise ValueError('Candidate source map identity changed')
    required = {
        'connect_reads.rs': (b'"/api/connect/integrations/"', b'"/api/connect/logs/"', b'pub(crate) fn router'),
    }
    with tarfile.open(archive) as source:
        for name, markers in required.items():
            member = 'rust/crates/store/src/' + name
            data = source.extractfile(member).read()
            if (hashlib.sha256(data).hexdigest() != manifest['source_sha256'].get(member)
                    or not all(marker in data for marker in markers)):
                raise ValueError(f'Exact candidate does not bind the existing normal Connect router: {name}')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}', manifest['tag']):
        raise ValueError('Invalid sealed image reference')
    return manifest, archive


def independent_cleanup(project, evidence):
    label = 'label=com.docker.compose.project=' + project

    def docker(*args):
        return subprocess.check_output(['docker', *args], text=True, timeout=10).splitlines()

    owned = load(evidence / 'owned-before-compose-down.json')
    inventories = {
        'container': docker('container', 'ls', '-aq', '--no-trunc', '--filter', label),
        'network': docker('network', 'ls', '-q', '--no-trunc', '--filter', label),
        'volume': docker('volume', 'ls', '-q', '--filter', label),
    }
    for row in owned:
        row['container_absent'] = not docker('container', 'ls', '-aq', '--no-trunc', '--filter', 'id=' + row['id'])
        pid = row['pid']
        row['host_generation_absent'] = True
        if pid:
            try:
                actual = Path(f'/proc/{pid}/stat').read_text().rsplit(')', 1)[1].split()[19]
                row['host_generation_absent'] = actual != row['generation']
            except FileNotFoundError:
                pass
    exact_roles = sorted(row['service'] for row in owned) == ['baseline', 'candidate', 'data-init', 'origin']
    passed = exact_roles and not any(inventories.values()) and all(
        row['container_absent'] and row['host_generation_absent'] for row in owned
    )
    return {'project': project, 'inventories': inventories, 'owned_container_generations': owned, 'passed': passed}


def qualification(report, result, cleanup, manifest, project, settled, freeze):
    checks = report['checks']
    rows = {row['name']:row for row in checks}
    expected = {'connect-matrix:'+name for name in PAIRED_SCOPES+('paired',)}
    if not expected<=rows.keys() or len(rows)!=len(checks):
        return {'complete':False,'accepted':False}
    phases={scope:rows['connect-matrix:'+scope]['details'] for scope in PAIRED_SCOPES}
    summary=rows['connect-matrix:paired']['details']
    phase_shape=all(len(row['cases'])==row['completed_cell_count']==60 and row['preserved_unrelated_SQL_rows']
                    and row['zero_owned_delivery'] and all(type(case['actual_status']) is int
                    and case['transport_error'] is None for case in row['cases'].values()) for row in phases.values())
    counts=(summary['physical_http_cells_attempted']==240 and set(summary['pairs'])=={'bare','json'}
            and all(len(cells)==60 for cells in summary['pairs'].values()))
    foundation=lifecycle_foundation(checks)
    image=report.get('candidate_image_binding',{})
    identity=(report.get('project')==project and report.get('source_baseline')==SOURCE_REVISION
              and report.get('baseline_archive_sha256')==SOURCE_TAR_SHA and
              image=={'image_id':manifest['image_id'],'source_tree_sha256':manifest['source_tree_sha256'],
                      'manifest_verified':True})
    snapshot={name:checksum for name,checksum in freeze['copied_inputs'].items()
              if name!='original-source.tar' and not name.startswith('scripts/rust-milestone-batches/')}
    complete=bool(identity and phase_shape and counts and report.get('tool_source_sha256')==snapshot
                  and report['contract_suites']==['connect_matrix'] and foundation['passed']
                  and result['actual_exit'] in (0,1) and settled(result) and result['failure'] is None
                  and report.get('failure') is None and report.get('owned_capture_error') is None
                  and report.get('cleanup')=='project removed' and not report.get('resources_retained')
                  and cleanup['passed'])
    passed=complete and report['passed'] is True and result['actual_exit']==0 and all(rows[name]['passed'] for name in expected)
    return {'complete':complete,'accepted':passed,'paired_matching':summary['paired_cells_matching'],
            'paired_representative_role_groups':summary['paired_representative_role_groups'],
            'per_phase':{key:{'attempted':row['completed_cell_count'],'matching':row['source_or_native_scope_matches'],
                              'exact_SQL_transition':row['complete_selected_table_transition'],
                              'sequence_transition':row['expected_sequence_transition'],
                              'transport_failures':sum(case['transport_error'] is not None for case in row['cases'].values())}
                         for key,row in phases.items()}}


def lifecycle_foundation(checks):
    return foundation_receipt([row for row in checks if not row['name'].startswith('connect-matrix')])


def launch(build, revision, output):
    if os.environ.get('DOCKER_HOST') != 'unix:///tmp/dispatcharr-docker.sock' or (
        Path(os.environ.get('DOCKER_CONFIG', '')).resolve() != ARTIFACTS / 'tooling/docker-config'
    ):
        raise ValueError('Select the owned validation Docker engine')
    if output.exists():
        raise ValueError('Fresh Connect evidence directory required')
    freeze = verify_inputs()
    manifest, archive = verify_build(build, revision)
    coordinator = load_coordinator()
    with DRIVER.with_name('.dispatcher.lock').open('a+') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        output.mkdir(mode=0o755)
        project = 'dispatcharr-connect-matrix-' + uuid.uuid4().hex[:12]
        evidence = output / 'evidence'
        save(output / 'binding.json', {'project': project, 'build_sha256': sha(build),
             'source_archive_sha256': sha(archive), 'candidate_revision': revision,
             'image_id': manifest['image_id'], 'frozen_inputs_sha256': sha(TEMPLATE),
             'scope': 'representative_paired_connect_method_role_matrix', 'accept': 'application/json'})
        argv = runner_argv(build, manifest['tag'], evidence)
        result = coordinator.command(argv, {'cwd': str(ROOT), 'output_bytes': 8388608,
                                      'environment': {'DISPATCHARR_LIFECYCLE_PROJECT': project}},
                                     output, 'execution', 990)
        save(output / 'execution.json', result)
        try:
            cleanup = independent_cleanup(project, evidence)
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            cleanup = {'project': project, 'passed': False, 'error': type(error).__name__}
        save(output / 'independent-cleanup.json', cleanup)
        if (evidence / 'integration.json').is_file():
            report = load(evidence / 'integration.json')
            qualified = qualification(report, result, cleanup, manifest, project, coordinator.settled, freeze)
        else:
            report = {}
            qualified = {'complete':False,'accepted':False}
        foundation = lifecycle_foundation(report.get('checks', []))
        full_passed = qualified['accepted']
        save(output / 'receipt.json', {'project': project, 'source_revision': SOURCE_REVISION,
             'candidate_revision': revision, 'image_id': manifest['image_id'], 'passed': full_passed, 'capture_completed':qualified['complete'],'selected_matrix':qualified,
             'tracked_runner_actual_exit': result['actual_exit'], 'foundation': foundation,
             'scope': 'representative_paired_connect_method_role_matrix', 'performance_verified':False,'full_milestone_accepted':False,'fully_verified_register_operations':0})
        print(json.dumps({'passed': full_passed, 'capture_completed':qualified['complete'],'selected_matrix':qualified,
                          'tracked_runner_actual_exit': result['actual_exit'], 'project': project, 'scope': 'representative_paired_connect_method_role_matrix'}))
        return 0 if full_passed else 1


def runner_argv(build, image, output):
    return [sys.executable, '-B', str(TREE / 'scripts/rust-integration/run.py'), '--output', str(output),
            '--candidate-image', image, '--candidate-build-manifest', str(build), '--contract-suites', 'connect_matrix']


def main():
    os.umask(0o022)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build', type=lambda value: Path(value).resolve())
    parser.add_argument('--revision')
    parser.add_argument('--output', type=lambda value: Path(value).resolve())
    parser.add_argument('--preflight', action='store_true', help='Verify local inputs without acquiring the Compose lease')
    args = parser.parse_args()
    if args.preflight:
        freeze = verify_inputs()
        load_coordinator()
        if bool(args.build) != bool(args.revision):
            parser.error('--build and --revision must be provided together')
        if args.build:
            verify_build(args.build, args.revision)
        print(json.dumps({'frozen_inputs': len(freeze['copied_inputs']), 'build_verified': bool(args.build),
                          'frozen_inputs_sha256': sha(TEMPLATE)}, sort_keys=True))
        return 0
    if not (args.build and args.revision and args.output):
        parser.error('--build, --revision and --output are required to run')
    return launch(args.build, args.revision, args.output)


if __name__ == '__main__':
    raise SystemExit(main())
