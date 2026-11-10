#!/usr/bin/env python3
"""Build component test image from an existing immutable canonical Rust archive.
All original Dispatcharr authors retain application and algorithm credit.
"""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--build-manifest', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = json.loads(args.build_manifest.read_bytes())
    sources = manifest['source_sha256']
    tree = hashlib.sha256(json.dumps(sources, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    archive = args.build_manifest.parent / 'source.tar'
    if not manifest['passed'] or tree != manifest['source_tree_sha256'] or sha(archive) != manifest['source_archive_sha256']:
        raise ValueError('Canonical immutable source binding failed')
    for name in ('rust/Dockerfile.ingest-epg-tests', 'rust/runtime/ingest_epg_test.sh',
                 'rust/crates/ingest-epg/tests/source_sql.rs', 'rust/crates/ingest-epg/tests/policy_cache_sql.rs'):
        if name not in sources:
            raise ValueError('Canonical archive predates component Docker verification')
    tag = 'dispatcharr-rust-epg-tests:' + tree[:20]
    report = {'format_version': 1, 'scope': 'Immutable EPG component test image only',
              'passed': False, 'tag': tag, 'source_tree_sha256': tree, 'source_sha256': sources,
              'source_archive_sha256': sha(archive), 'canonical_manifest_sha256': sha(args.build_manifest),
              'tool_sha256': sha(Path(__file__)), 'application_image_verified': False,
              'full_refresh_job_verified': False, 'performance_verified': False}
    try:
        began = time.monotonic()
        with archive.open('rb') as source, (output / 'build.log').open('wb') as log:
            subprocess.run(['docker', 'build', '-f', 'rust/Dockerfile.ingest-epg-tests',
                '--label', 'org.opencontainers.image.source-tree-sha256=' + tree, '-t', tag, '-'],
                stdin=source, stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1800)
        report['build_seconds'] = time.monotonic() - began
        actual = json.loads(subprocess.check_output(['docker', 'image', 'inspect', tag], timeout=30))[0]
        if actual['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') != tree:
            raise ValueError('Built component image source label differs')
        report['image_id'] = actual['Id']
        report['passed'] = True
    finally:
        (output / 'build.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'passed': report['passed'], 'image_id': report.get('image_id')}))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
