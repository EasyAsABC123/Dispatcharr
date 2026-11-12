"""Unexecuted, whitelisted metadata draft. Never dumps environment/argv/JWTs."""
import hashlib
import json
import platform
import sys
from pathlib import Path


def capture(command, selected, policy, source, controller, output, budget=None):
    def read(arguments):
        raw = command(arguments, timeout=15)
        if len(raw) > 1024*1024:
            raise ValueError('MetadataCapacity')
        return raw.decode(errors='replace')

    def file(path):
        data = Path(path).read_bytes()
        if len(data) > 1024*1024:
            raise ValueError('HostMetadataCapacity')
        return data.decode(errors='replace')

    services = {}
    for name, selected_service in selected.items():
        record = json.loads(read(['docker', 'inspect', selected_service['id']]))[0]
        services[name] = {'id': record['Id'], 'image_id': record['Image'],
                          'image_reference': record['Config']['Image'],
                          'started_at': record['State']['StartedAt'],
                          'limits': {k: record['HostConfig'][k] for k in
                                     ['Memory', 'NanoCpus', 'PidsLimit', 'MemorySwap']},
                          'expected_limits': selected_service['limits']}
    package_script = ("import importlib.metadata,json;rows=sorted((d.metadata['Name'],d.version)"
                      "for d in importlib.metadata.distributions());"
                      "assert len(rows)<=2048;print(json.dumps(rows))")
    package_inventory = json.loads(read(['docker', 'exec', selected['source']['id'],
                                         '/dispatcharrpy/bin/python', '-c', package_script]))
    value = {'scope': 'Source-only, four-service group; controller unaccepted',
             'python': sys.version, 'platform': platform.platform(),
             'boot_id': file('/proc/sys/kernel/random/boot_id').strip(),
             'uptime_at_metadata': file('/proc/uptime').strip(),
             'meminfo': file('/proc/meminfo'), 'cpu_inventory': read(['lscpu', '--json']),
             'docker_version': read(['docker', 'version', '--format', '{{json .}}']),
             'compose_version': read(['docker', 'compose', 'version', '--short']),
             'original_dependency_versions': package_inventory,
             'services': services,
             'policy_sha256': hashlib.sha256((controller/'policy.json').read_bytes()).hexdigest(),
             'controller_sha256': {str(p.relative_to(controller)): hashlib.sha256(p.read_bytes()).hexdigest()
                                   for p in sorted(controller.iterdir()) if p.is_file()},
             'source_revision': '1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430',
             'source_archive_sha256': 'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff',
             'source_files': {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in sorted(source.rglob('*')) if p.is_file()}}
    # Private metadata: service configuration was inspected only to select the
    # above fixed fields. No environment values, argv or full inspection persisted.
    encoded=(json.dumps(value,indent=2)+'\n').encode()
    if len(encoded)>8388608:raise ValueError('MetadataOutputCapacity')
    if budget is not None:budget.charge(len(encoded))
    (output/'resource-metadata-private.json').write_bytes(encoded)
    return value
