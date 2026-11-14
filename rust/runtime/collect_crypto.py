"""Copy the pinned Original GPG ELF/trust closure; authors retain full credit.

Runs only inside the existing pinned media image build stage. No key generation,
GPG home, package install, network connection or application operation.
"""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
root = Path('/media-runtime')
prefix = root / 'opt/dispatcharr-crypto'
(prefix / 'bin').mkdir(parents=True)
(prefix / 'lib').mkdir()
(prefix / 'trust').mkdir()
docs = root / 'usr/share/doc/dispatcharr'
crypto_docs = docs / 'crypto-copyright'
crypto_docs.mkdir()
license_cache = {}


def retain_debian_licenses(path):
    """Bind each ELF to its owning package's retained, byte-identical notice."""
    paths = list(dict.fromkeys((str(path.resolve()), str(path))))
    owners = set()
    for query in paths:
        result = subprocess.run(['dpkg-query', '--search', query], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=10, check=False)
        if result.returncode not in (0, 1):
            raise ValueError('cannot query ELF package owner: ' + query)
        for line in result.stdout.splitlines():
            owner, separator, filename = line.rpartition(': ')
            if separator and filename == query:
                owners.update(owner.split(', '))
        if owners:
            break
    if not owners:
        raise ValueError('ELF has no Debian package license owner: ' + str(path))
    retained = []
    for owner in sorted(owners):
        if not re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?', owner):
            raise ValueError('invalid Debian package owner: ' + owner)
        package = owner.partition(':')[0]
        if package not in license_cache:
            notice = Path('/usr/share/doc') / package / 'copyright'
            if not notice.is_file():
                raise ValueError('missing owning-package copyright: ' + package)
            # collect_media already retains every available Debian copyright.
            # Reuse it only after checking actual bytes, otherwise copy this one.
            target = docs / 'media-copyright' / package / 'copyright'
            if not target.is_file() or sha(target) != sha(notice):
                target = crypto_docs / package / 'copyright'
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(notice, target)
            license_cache[package] = {'package': package,
                                      'source': str(notice),
                                      'retained_path': '/' + str(target.relative_to(root)),
                                      'sha256': sha(target)}
        retained.append({'owner': owner, **license_cache[package]})
    return retained


source = Path('/usr/bin/gpg')
if source.is_symlink() or not source.is_file():
    raise ValueError('exact pinned GPG binary required')
sha = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
shutil.copy2(source, prefix / 'bin/gpg')
manifest = {'source_image': IMAGE, 'binary': {'source': str(source), 'sha256': sha(source), 'licenses': retain_debian_licenses(source)}, 'libraries': {}, 'no_agent_autostart': True}
loader = None
output = subprocess.check_output(['ldd', str(source)], text=True, timeout=10)
for line in output.splitlines():
    for token in line.split():
        path = Path(token)
        if not token.startswith('/') or not path.is_file():
            continue
        digest = sha(path)
        target = prefix / 'lib' / path.name
        if target.exists() and sha(target) != digest:
            raise ValueError('conflicting crypto dependency: ' + path.name)
        shutil.copy2(path, target)
        manifest['libraries'][path.name] = {'source': str(path), 'sha256': digest, 'licenses': retain_debian_licenses(path)}
        if path.name.startswith('ld-linux-'):
            loader = path.name
if loader is None:
    raise ValueError('GPG ELF interpreter required')
wrapper = root / 'usr/local/bin/gpg'
wrapper.write_text('#!/bin/sh\nexec /opt/dispatcharr-crypto/lib/' + loader + ' --library-path /opt/dispatcharr-crypto/lib /opt/dispatcharr-crypto/bin/gpg "$@"\n')
wrapper.chmod(0o755)
# Actual pinned certifi installation previously captured in Source metadata9.
trust = Path('/dispatcharrpy/lib/python3.13/site-packages/certifi/cacert.pem')
if trust.is_symlink() or not trust.is_file():
    raise ValueError('pinned certifi trust data required')
shutil.copy2(trust, prefix / 'trust/cacert.pem')
manifest['trust'] = {'source': str(trust), 'sha256': sha(trust)}
for name, path in [('GPG', Path('/usr/share/doc/gpg/copyright')), ('certifi', Path('/dispatcharrpy/lib/python3.13/site-packages/certifi-2026.10.23.dist-info/licenses/LICENSE'))]:
    if not path.is_file():
        raise ValueError('required installed license: ' + name)
    shutil.copy2(path, crypto_docs / (name + '-LICENSE'))
    manifest.setdefault('licenses', {})[name] = {'source': str(path), 'sha256': sha(path)}
manifest['version'] = subprocess.check_output(['/usr/bin/gpg', '--version'], text=True, timeout=10).splitlines()[0]
(docs / 'crypto-runtime.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
