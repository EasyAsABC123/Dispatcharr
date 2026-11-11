#!/usr/bin/env python3
"""Capture original operator commands on an owned copied database.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
OWN = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT/'scripts/rust-integration'))
from run import BASELINE, fixture_source
from fixture_ports import write_port_overlay
from tool_snapshot import snapshot_sources


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--native-binary',type=Path,required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    tools = output/'tool-source'
    hashes = snapshot_sources(ROOT, tools)
    for name in ['run_sql_boundaries.py', 'sql_fixture.py']:
        relative = 'scripts/rust-operator-contracts/'+name
        data = (OWN/name).read_bytes()
        target = tools/relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        hashes[relative] = hashlib.sha256(data).hexdigest()
    binary=output/'operator-resource-tests'
    binary.write_bytes(args.native_binary.read_bytes());binary.chmod(0o755)
    nonce = uuid.uuid4().hex[:10]
    database = 'rust_probe_backup_operator_'+nonce
    project = 'dispatcharr-operator-cli-source-'+nonce
    report = {'passed': False, 'scope': 'Original/native actual SQL sequence and locked full-save boundary controls',
              'original_revision': BASELINE, 'project': project, 'copied_database': database,
              'native_verified': False, 'performance_verified': False, 'tool_source_sha256': hashes}
    status = 1
    with fixture_source(report, False, output) as directory:
        source = Path(directory)/'source'
        source.mkdir(mode=0o755)
        original_archive = subprocess.check_output(['git', 'archive', BASELINE], cwd=ROOT)
        report['original_archive_sha256'] = hashlib.sha256(original_archive).hexdigest()
        assert report['original_archive_sha256'] == 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
        subprocess.run(['tar', '-x', '--same-permissions', '--no-same-owner', '-C', str(source)],
                       input=original_archive, check=True)
        (source/'media').mkdir(exist_ok=True)
        environment = dict(os.environ, BASELINE_SOURCE_DIR=str(source),
                           BASELINE_EVIDENCE_DIR=str(output), DISPATCHARR_ENV='aio')
        overlay = output/'operator-fixture.yml'
        overlay.write_text(json.dumps({'services': {'baseline': {'volumes': [
            str(tools/'scripts/rust-operator-contracts')+':/operator-tools:ro',
            str(tools/'scripts/rust-integration')+':/backup-tools:ro']}},
            'networks': {'default': {'internal': True}}}))
        ports = output/'fixture-ports.yml'
        report['ports'] = write_port_overlay(ports, {'baseline': [5656], 'origin': [8080]})
        compose = ['docker', 'compose', '-p', project, '-f', str(tools/'docker/compose.rust-playback.yml'),
                   '-f', str(ports), '-f', str(overlay)]

        def command(arguments, body=None, timeout=180):
            return subprocess.check_output(arguments, env=environment, input=body,
                                           text=True, stderr=subprocess.PIPE, timeout=timeout)

        def database_helper(name, action):
            raw = command(compose+['exec', '-T', '-e', 'POSTGRES_DB='+name,
                '-e', 'BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only', 'baseline',
                '/dispatcharrpy/bin/python', '/backup-tools/backup_fixture.py'], json.dumps({'action': action}))
            return json.loads(raw.splitlines()[-1])

        before = None
        try:
            command(compose+['up', '-d', 'baseline'])
            deadline = time.monotonic()+180
            while True:
                try:
                    command(compose+['exec', '-T', 'baseline', '/dispatcharrpy/bin/python', '-c',
                        "import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"], timeout=5)
                    break
                except subprocess.SubprocessError:
                    if time.monotonic() >= deadline:
                        raise RuntimeError('Original readiness deadline')
                    time.sleep(.2)
            before = database_helper('dispatcharr', 'baseline_manifest')
            command(compose+['exec', '-T', '-e', 'PGPASSWORD=playback-fixture-only', 'baseline',
                'psql', '-h', '127.0.0.1', '-U', 'dispatch', '-d', 'postgres',
                '-v', 'ON_ERROR_STOP=1', '-c', 'CREATE DATABASE '+database])
            seed = database_helper(database, 'seed')
            report['copied_nonempty_tables'] = sum(row['count'] > 0 for row in seed['manifest']['tables'].values())
            command(compose+['exec', '-T', '-e', 'POSTGRES_DB='+database,
                '-e', 'OPERATOR_DISPOSABLE_FIXTURE=playback-fixture-only',
                '-e', 'CELERY_BROKER_URL=redis://:playback-fixture-only@localhost:6379/8',
                '-e', 'CELERY_RESULT_BACKEND=redis://:playback-fixture-only@localhost:6379/9',
                'baseline', '/dispatcharrpy/bin/python', '/operator-tools/sql_fixture.py'], timeout=180)
            evidence = json.loads((output/'operator-cli-source.json').read_text())
            report.update(passed=evidence['passed'], cases=len(evidence['cases']),
                          source_report_sha256=hashlib.sha256((output/'operator-cli-source.json').read_bytes()).hexdigest(),
                          default_database_unchanged=before == database_helper('dispatcharr', 'baseline_manifest'))
            assert report['default_database_unchanged']
            status = 0
        except (OSError, ValueError, RuntimeError, AssertionError, subprocess.SubprocessError) as error:
            report['passed'] = False
            report['error_type'] = type(error).__name__
            if isinstance(error, subprocess.CalledProcessError):
                (output/'failure-private.log').write_text(str(error.stderr or '')[-65536:])
        finally:
            if before is not None:
                try:
                    report['default_database_unchanged'] = before == database_helper('dispatcharr', 'baseline_manifest')
                    if not report['default_database_unchanged']:
                        report['passed'] = False
                        status = 1
                except subprocess.SubprocessError:
                    report['default_database_check_failed'] = True
                    report['passed'] = False
                    status = 1
            try:
                command(compose+['down', '--volumes', '--remove-orphans'])
                report['cleanup'] = 'project removed'
            except subprocess.SubprocessError:
                report.update(passed=False, cleanup_error=True)
                status = 1
    print(json.dumps({'passed': report['passed'], 'evidence': str(output)}))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
