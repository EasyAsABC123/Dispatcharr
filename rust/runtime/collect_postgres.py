"""Bounded component build-stage collector: collect pinned original PG17 client ELF closure and notices.

Build-stage only. No Python/Perl runtime is copied. Original Dispatcharr authors
retain all application credit. Never copy database files, service secrets, client
private keys, credential files or inherited runtime environment into this tree.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

if os.environ.get('COLLECT_PINNED_PG_RUNTIME') != 'draft-build-stage-only':
    raise RuntimeError('Explicit isolated build-stage opt-in required')
IMAGE = 'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
root = Path('/pg-client-runtime')
if root.exists():
    raise RuntimeError('Collector output must not already exist')
prefix = root/'opt/dispatcharr-postgres'
(prefix/'bin').mkdir(parents=True)
(prefix/'lib').mkdir()
(root/'usr/local/bin').mkdir(parents=True)
docs = root/'usr/share/doc/dispatcharr-postgres'
docs.mkdir(parents=True)
manifest = {'component_only':True, 'source_image':IMAGE, 'platform':'linux/amd64',
            'binaries':{}, 'libraries':{}, 'auxiliary':{}, 'omitted':['CPython','Perl','database data','JWT secret','password files','TLS private keys','Kerberos keytabs']}
loader = None

def digest(path):
    result = hashlib.sha256()
    with path.open('rb') as handle:
        for block in iter(lambda:handle.read(1<<20),b''):
            result.update(block)
    return result.hexdigest()

def library(path):
    global loader
    target = prefix/'lib'/path.name
    value = digest(path)
    if target.exists() and digest(target) != value:
        raise RuntimeError('Conflicting PG client library basename')
    shutil.copy2(path,target)
    manifest['libraries'][path.name] = {'source':str(path),'sha256':value}
    if path.name.startswith('ld-linux-'):
        if loader not in [None,path.name]:
            raise RuntimeError('Conflicting ELF loaders')
        loader = path.name

for name in ['pg_dump','pg_restore','psql']:
    # Debian /usr/bin/pg_* can be Perl pg_wrapper scripts. Select the actual
    # pinned major's ELF instead, so the native image needs neither interpreter.
    path = Path('/usr/lib/postgresql/17/bin')/name
    with path.open('rb') as handle:
        if handle.read(4) != b'\x7fELF':
            raise RuntimeError('Expected actual PG17 ELF')
    version = subprocess.check_output([str(path),'--version'],text=True).strip()
    if '(PostgreSQL) 17.' not in version:
        raise RuntimeError('Expected PostgreSQL17 clients')
    shutil.copy2(path,prefix/'bin'/name)
    manifest['binaries'][name] = {'source':str(path),'sha256':digest(path),'version':version}
    output = subprocess.check_output(['ldd',str(path)],text=True)
    if 'not found' in output:
        raise RuntimeError('Unresolved PG client dependency')
    for line in output.splitlines():
        for token in line.split():
            if token.startswith('/') and Path(token).is_file():
                library(Path(token))
if loader is None:
    raise RuntimeError('Expected ELF interpreter')

# NSS and OpenSSL providers may be dlopened and absent from ldd output. Their
# actual use still requires DNS/TLS integration tests in the packaged image.
for path in sorted(Path('/lib/x86_64-linux-gnu').glob('libnss_*.so*')):
    if path.is_file():
        library(path)
providers = Path('/usr/lib/x86_64-linux-gnu/ossl-modules')
if providers.is_dir():
    target = prefix/'ossl-modules'
    shutil.copytree(providers,target,symlinks=False)
    for path in sorted(target.rglob('*')):
        if path.is_file():
            manifest['auxiliary'][str(path.relative_to(root))] = digest(path)

for name in ['pg_dump','pg_restore','psql']:
    wrapper = root/'usr/local/bin'/name
    wrapper.write_text('#!/bin/sh\nexport OPENSSL_MODULES=/opt/dispatcharr-postgres/ossl-modules\nexec /opt/dispatcharr-postgres/lib/'+loader+
        ' --library-path /opt/dispatcharr-postgres/lib /opt/dispatcharr-postgres/bin/'+name+' "$@"\n')
    wrapper.chmod(0o755)

# Preserve all available dependency notices rather than guessing which license
# owns a transitive/dynamically loaded library. No operational /etc is copied.
for path in sorted(Path('/usr/share/doc').glob('*/copyright')):
    if path.is_file():
        target = docs/'copyright'/path.parent.name
        target.mkdir(parents=True,exist_ok=True)
        shutil.copy2(path,target/'copyright')
common = Path('/usr/share/common-licenses')
if common.is_dir():
    shutil.copytree(common,docs/'common-licenses')
# PG client encoding, timezone and system catalog support data is public code /
# data, not database contents. Capture exactly and review file/size manifests.
shared = Path('/usr/share/postgresql/17')
if shared.is_dir():
    shutil.copytree(shared,root/'usr/share/postgresql/17')
    for path in sorted(shared.rglob('*')):
        if path.is_file():
            manifest['auxiliary'][str(path)] = digest(path)
(docs/'runtime-packages.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
