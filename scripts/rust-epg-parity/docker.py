"""Verify the native XML library in Compose using a retained immutable app tar.

This is a parser component gate; no guide refresh/job acceptance is inferred.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-manifest', type=Path, required=True)
    parser.add_argument('--goldens', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    out = args.output.resolve()
    out.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.build_manifest.read_bytes())
    if not manifest['passed']:
        raise RuntimeError('requires a passing app build manifest')
    hashes = manifest['source_sha256']
    tree = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if tree != manifest['source_tree_sha256']:
        raise RuntimeError('source tree binding differs')
    archive = args.build_manifest.parent/'source.tar'
    if digest(archive) != manifest['source_archive_sha256']:
        raise RuntimeError('retained source archive differs')
    original = json.loads(args.goldens.read_bytes())
    cases = original['cases']
    project = 'xmltv-'+hashlib.sha256(str(out).encode()).hexdigest()[:12]
    tag = 'dispatcharr-rust-xmltv-tests:'+project
    report = {'scope': 'immutable-native-XML-parser-Compose-component', 'passed': False,
              'source_tree_sha256': tree, 'source_archive_sha256': digest(archive),
              'app_manifest_sha256': digest(args.build_manifest),
              'goldens_sha256': digest(args.goldens), 'cases': len(cases),
              'tool_sha256': digest(Path(__file__)), 'application_performance_verified': False,
              'full_refresh_job_verified': False}
    compose = out/'compose.json'
    cleanup = None
    try:
        command = ['docker', 'build', '-f', 'rust/Dockerfile.app', '--target', 'xmltv-parser-tests',
                   '--label', 'org.opencontainers.image.source-tree-sha256='+tree, '-t', tag, '-']
        began = time.monotonic()
        with archive.open('rb') as source, (out/'build.log').open('wb') as log:
            subprocess.run(command, stdin=source, stdout=log, stderr=subprocess.STDOUT, timeout=900, check=True)
        report['build_seconds'] = time.monotonic()-began
        inspected = json.loads(subprocess.check_output(['docker', 'image', 'inspect', tag], timeout=30))[0]
        if inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256'] != tree:
            raise RuntimeError('helper image label differs')
        report['image_id'] = inspected['Id']
        config = {'services': {'xmltv': {'image': inspected['Id'], 'network_mode': 'none',
                    'read_only': True, 'tmpfs': ['/tmp:rw,nosuid,nodev,size=256m,mode=1777'],
                    'mem_limit': '256m', 'pids_limit': 64, 'cpus': 2,
                    'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
                    'stdin_open': True}}}
        compose.write_text(json.dumps(config, indent=2)+'\n')
        base = ['docker', 'compose', '-p', project, '-f', str(compose)]
        inputs = ''.join(json.dumps({k: v for k, v in case.items() if k != 'expected'})+'\n' for case in cases)
        with (out/'stdout.jsonl').open('wb') as stdout, (out/'stderr.log').open('wb') as stderr:
            result = subprocess.run(base+['run', '--rm', '--no-deps', '-T', 'xmltv'],
                                    input=inputs.encode(), stdout=stdout, stderr=stderr, timeout=120)
        report['exit_code'] = result.returncode
        values = [json.loads(line) for line in (out/'stdout.jsonl').read_text().split('\n') if line]
        differences = [{'case': index, 'expected': case['expected'], 'actual': value}
                       for index, (case, value) in enumerate(zip(cases, values)) if case['expected'] != value]
        report.update(responses=len(values), differences=differences)
        report['passed'] = result.returncode == 0 and len(values) == len(cases) and not differences
        report['limits'] = ['Immutable Rust/cc library test target; no Python backend or mounted application source.',
                            'Container has no network and runs read-only/nonroot with256MiB and2CPU limits.',
                            'Full ingestion scheduling, SQL guide swap and job/playback resource acceptance remain open.']
    finally:
        if compose.exists():
            cleanup = subprocess.run(['docker', 'compose', '-p', project, '-f', str(compose),
                                      'down', '--remove-orphans'], capture_output=True, timeout=30)
            remaining = subprocess.check_output(['docker', 'ps', '-aq', '--filter',
                                                 'label=com.docker.compose.project='+project], timeout=30).strip()
            report['cleanup_passed'] = cleanup.returncode == 0 and not remaining
        else:
            report['cleanup_passed'] = True
        report['passed'] = report['passed'] and report['cleanup_passed']
        report['logs_sha256'] = {p.name: digest(p) for p in out.iterdir() if p.is_file()}
        (out/'verification.json').write_text(json.dumps(report, indent=2)+'\n')
    print(json.dumps({'passed': report['passed'], 'cases': len(cases), 'cleanup_passed': report['cleanup_passed']}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
