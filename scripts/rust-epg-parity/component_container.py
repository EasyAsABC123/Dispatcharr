"""Execute immutable EPG test binaries as a bounded nonroot Compose service.
Original application and algorithm credit belongs to Dispatcharr authors.
"""
import hashlib
import json
from pathlib import Path
import subprocess


def execute(compose, environment, output, manifest_path, suite, fixture_name):
    if suite not in ('source_sql', 'policy_cache_sql', 'reporter_sql', 'channels_sql', 'file_job_sql', 'auto_logos_sql'):
        raise ValueError('Unknown EPG component suite')
    output = Path(output)
    manifest = json.loads(Path(manifest_path).read_bytes())
    canonical = hashlib.sha256(json.dumps(manifest['source_sha256'], sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    if not manifest.get('passed') or canonical != manifest['source_tree_sha256']:
        raise ValueError('Immutable EPG test build manifest differs')
    actual = json.loads(subprocess.check_output(['docker', 'image', 'inspect', manifest['image_id']], timeout=30))[0]
    if actual['Id'] != manifest['image_id'] or actual['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256') != canonical or actual['Config']['User'] != '65532:65532' or actual['Config'].get('Entrypoint') != ['/test-bin/run']:
        raise ValueError('Runtime EPG test image provenance/user differs')
    inputs = output / 'container-input'
    inputs.mkdir()
    inputs.chmod(0o755)
    public_fixture = inputs / fixture_name
    public_fixture.write_bytes((output / fixture_name).read_bytes())
    public_fixture.chmod(0o644)
    artifacts = output / 'container-output'
    artifacts.mkdir()
    artifacts.chmod(0o777)  # Only uniquely owned disposable evidence, for mapped nonroot UID.
    test_environment = {'DATABASE_TEST_URL': 'postgresql://dispatch:playback-fixture-only@baseline:5432/dispatcharr',
        'REDIS_TEST_URL': 'redis://:playback-fixture-only@baseline:6379/0', 'EPG_TEST_SUITE': suite,
        'EPG_IMPORT_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_POLICY_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_REPORTER_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_LOGO_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_LOGO_FIXTURE': '/evidence/' + fixture_name,
        'EPG_FILE_JOB_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_FILE_JOB_FIXTURE': '/evidence/' + fixture_name,
        'EPG_CHANNEL_DISPOSABLE_FIXTURE': 'playback-fixture-only',
        'EPG_CHANNEL_FIXTURE': '/evidence/' + fixture_name,
        'EPG_REPORTER_FIXTURE': '/evidence/' + fixture_name,
        'EPG_IMPORT_FIXTURE': '/evidence/' + fixture_name,
        'EPG_POLICY_FIXTURE': '/evidence/' + fixture_name,
        'EPG_POLICY_NATIVE_CACHE': '/test-output/epg-policy-native-cache.json'}
    config = {'services': {'epg-native-tests': {'image': manifest['image_id'],
        'environment': test_environment, 'read_only': True,
        'volumes': [str(inputs) + ':/evidence:ro', str(artifacts) + ':/test-output'],
        'tmpfs': ['/tmp:mode=1777,size=268435456'], 'mem_limit': '512m', 'cpus': 2,
        'pids_limit': 128, 'cap_drop': ['ALL'], 'security_opt': ['no-new-privileges:true'],
        'stop_grace_period': '15s'}}}
    overlay = output / 'native-component-compose.json'
    overlay.write_text(json.dumps(config, indent=2) + '\n')
    with (output / 'native-container-tests.log').open('wb') as log:
        result = subprocess.run(compose + ['-f', str(overlay), 'run', '--rm', '--no-deps', '-T', 'epg-native-tests'],
            env=environment, stdout=log, stderr=subprocess.STDOUT, timeout=600)
    cache = artifacts / 'epg-policy-native-cache.json'
    if cache.exists():
        (output / cache.name).write_bytes(cache.read_bytes())
    return result.returncode, {'image_id': manifest['image_id'], 'source_tree_sha256': canonical,
        'source_archive_sha256': manifest['source_archive_sha256'],
        'source_sha256': manifest['source_sha256'], 'manifest_sha256': hashlib.sha256(Path(manifest_path).read_bytes()).hexdigest(),
        'compose_sha256': hashlib.sha256(overlay.read_bytes()).hexdigest(),
        'runtime_user': actual['Config']['User'], 'limits': {'memory_bytes': 512 * 1024**2, 'cpus': 2, 'pids': 128},
        'scope': 'Native parser/SQL/cache component execution; application refresh routes remain unwired'}
