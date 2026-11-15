"""Collect actual PG17 ELF clients from pinned Original, never a runtime image.

Original Dispatcharr, PostgreSQL and dependency authors retain all credit and
licenses. Build-stage only: no package install, network, keys, credentials,
database contents, Python or Perl interpreter are copied.
"""
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
root = Path('/media-runtime')
prefix = root / 'opt/dispatcharr-postgres'
if not root.is_dir() or prefix.exists():
    raise ValueError('existing media stage and fresh PG prefix required')
(prefix / 'bin').mkdir(parents=True)
(prefix / 'lib').mkdir()
docs = root / 'usr/share/doc/dispatcharr'
pg_docs = docs / 'postgres-copyright'
pg_docs.mkdir()
manifest = {'source_image': IMAGE, 'platform': 'linux/amd64', 'binaries': {},
            'libraries': {}, 'auxiliary': {}, 'wrappers': {},
            'omitted': ['Python', 'Perl', 'database data', 'credentials',
                        'TLS private keys', 'Kerberos keytabs']}
licenses = {}
visited = set()
loader = None


def sha(path):
    result = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda: handle.read(1 << 20), b''):
            result.update(block)
    return result.hexdigest()


def retain_licenses(path):
    owners = set()
    queries = list(dict.fromkeys((str(path.resolve()), str(path))))
    # Debian usrmerge ownership may retain the pre-/usr spelling.
    for value in list(queries):
        if value.startswith('/usr/lib/'):
            queries.append(value[4:])
        elif value.startswith('/lib/'):
            queries.append('/usr' + value)
    for query in queries:
        result = subprocess.run(['dpkg-query', '--search', query], text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=10, check=False)
        if result.returncode not in (0, 1):
            raise ValueError('cannot query PG file owner: ' + query)
        for line in result.stdout.splitlines():
            owner, separator, filename = line.rpartition(': ')
            if separator and filename == query:
                owners.update(owner.split(', '))
        if owners:
            break
    if not owners:
        raise ValueError('PG file has no Debian license owner: ' + str(path))
    retained = []
    for owner in sorted(owners):
        if not re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?', owner):
            raise ValueError('invalid Debian owner')
        package = owner.partition(':')[0]
        if package not in licenses:
            notice = Path('/usr/share/doc') / package / 'copyright'
            if not notice.is_file():
                raise ValueError('missing PG owning-package notice: ' + package)
            target = None
            for category in ('media-copyright', 'crypto-copyright'):
                existing = docs / category / package / 'copyright'
                if existing.is_file() and sha(existing) == sha(notice):
                    target = existing
                    break
            if target is None:
                target = pg_docs / package / 'copyright'
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(notice, target)
            licenses[package] = {'package': package, 'source': str(notice),
                                 'retained_path': '/' + str(target.relative_to(root)),
                                 'sha256': sha(target)}
        retained.append({'owner': owner, **licenses[package]})
    return retained


def dependencies(path):
    identity = str(path.resolve())
    if identity in visited:
        return
    visited.add(identity)
    output = subprocess.check_output(['ldd', str(path)], text=True, timeout=10)
    if 'not found' in output:
        raise ValueError('unresolved PG ELF dependency')
    for line in output.splitlines():
        for token in line.split():
            if token.startswith('/') and Path(token).is_file():
                library(Path(token))


def library(path):
    global loader
    target = prefix / 'lib' / path.name
    digest = sha(path)
    if target.exists() and sha(target) != digest:
        raise ValueError('conflicting PG library basename: ' + path.name)
    shutil.copy2(path, target)
    manifest['libraries'][path.name] = {'source': str(path), 'sha256': digest,
                                       'licenses': retain_licenses(path)}
    if path.name.startswith('ld-linux-'):
        if loader not in (None, path.name):
            raise ValueError('conflicting PG ELF interpreters')
        loader = path.name
    dependencies(path)


for name in ('pg_dump', 'pg_restore', 'psql'):
    source = Path('/usr/lib/postgresql/17/bin') / name
    with source.open('rb') as handle:
        if handle.read(4) != b'\x7fELF':
            raise ValueError('actual PG17 ELF required, never pg_wrapper')
    version = subprocess.check_output([str(source), '--version'], text=True,
                                      timeout=10).strip()
    if '(PostgreSQL) 17.' not in version:
        raise ValueError('PG17 client required')
    shutil.copy2(source, prefix / 'bin' / name)
    manifest['binaries'][name] = {'source': str(source), 'sha256': sha(source),
                                 'version': version, 'licenses': retain_licenses(source)}
    dependencies(source)
if loader is None:
    raise ValueError('PG ELF interpreter required')

# NSS/OpenSSL may dlopen these objects; collect each actual object's own ldd
# closure and notice too, rather than treating provider directories as data.
for source in sorted(Path('/lib/x86_64-linux-gnu').glob('libnss_*.so*')):
    if source.is_file():
        library(source)
providers = Path('/usr/lib/x86_64-linux-gnu/ossl-modules')
if providers.is_dir():
    for source in sorted(providers.iterdir()):
        if not source.is_file():
            raise ValueError('unexpected OpenSSL module entry')
        target = prefix / 'ossl-modules' / source.name
        target.parent.mkdir(exist_ok=True)
        shutil.copy2(source, target)
        manifest['auxiliary']['/' + str(target.relative_to(root))] = {
            'source': str(source), 'sha256': sha(source), 'licenses': retain_licenses(source)}
        dependencies(source)

for name in ('pg_dump', 'pg_restore', 'psql'):
    wrapper = root / 'usr/local/bin' / name
    if wrapper.exists():
        raise ValueError('refuse conflicting PG wrapper')
    wrapper.parent.mkdir(parents=True, exist_ok=True)
    wrapper.write_text('#!/bin/sh\nexport OPENSSL_MODULES=/opt/dispatcharr-postgres/ossl-modules\n'
                       'exec /opt/dispatcharr-postgres/lib/' + loader +
                       ' --library-path /opt/dispatcharr-postgres/lib '
                       '/opt/dispatcharr-postgres/bin/' + name + ' "$@"\n')
    wrapper.chmod(0o755)
    manifest['wrappers'][name] = {'path': '/' + str(wrapper.relative_to(root)),
                                  'sha256': sha(wrapper)}

# Public client encoding/timezone/catalog support data only, with explicit owner
# attribution; no /etc, home directories, lib/postgresql data or private keys.
shared = Path('/usr/share/postgresql/17')
if shared.is_dir():
    target = root / 'usr/share/postgresql/17'
    if target.exists():
        raise ValueError('unexpected existing PG share directory')
    shutil.copytree(shared, target, symlinks=False)
    for source in sorted(shared.rglob('*')):
        if source.is_file():
            manifest['auxiliary'][str(source)] = {
                'source': str(source), 'sha256': sha(source), 'licenses': retain_licenses(source)}
common = Path('/usr/share/common-licenses')
if not (docs / 'common-licenses').is_dir() and common.is_dir():
    shutil.copytree(common, docs / 'postgres-common-licenses')
(docs / 'postgres-runtime.json').write_text(json.dumps(manifest, indent=2, sort_keys=True) + '\n')
