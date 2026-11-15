"""Package pinned Original VLC for normal input profiles; full authorship retained.

Build-stage filesystem/ELF inspection only. No network, package installation,
VLC execution, user configuration, credentials or private files are included.
"""
import hashlib,json,re,shlex,shutil,subprocess
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
root=Path('/media-runtime');prefix=root/'opt/dispatcharr-vlc'
if not root.is_dir()or prefix.exists():raise ValueError('existing media stage and fresh VLC prefix required')
(prefix/'bin').mkdir(parents=True);(prefix/'lib').mkdir()
docs=root/'usr/share/doc/dispatcharr';pg_docs=docs/'vlc-copyright';pg_docs.mkdir()
manifest={'source_image':IMAGE,'platform':'linux/amd64','binaries':{},'libraries':{},'plugins':{},'data':{},'wrappers':{},'omitted':['VLC generated plugin cache','user configuration','credentials','private files']}
licenses={};visited=set();loader=None

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
            raise ValueError('cannot query VLC file owner: ' + query)
        for line in result.stdout.splitlines():
            owner, separator, filename = line.rpartition(': ')
            if separator and filename == query:
                owners.update(owner.split(', '))
        if owners:
            break
    if not owners:
        raise ValueError('VLC file has no Debian license owner: ' + str(path))
    retained = []
    for owner in sorted(owners):
        if not re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?', owner):
            raise ValueError('invalid Debian owner')
        package = owner.partition(':')[0]
        if package not in licenses:
            notice = Path('/usr/share/doc') / package / 'copyright'
            if not notice.is_file():
                raise ValueError('missing VLC owning-package notice: ' + package)
            target = None
            for category in ('media-copyright', 'crypto-copyright', 'postgres-copyright'):
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
        raise ValueError('unresolved VLC ELF dependency')
    for line in output.splitlines():
        for token in line.split():
            if token.startswith('/') and Path(token).is_file():
                library(Path(token))


def library(path):
    global loader
    target = prefix / 'lib' / path.name
    digest = sha(path)
    if target.exists() and sha(target) != digest:
        raise ValueError('conflicting VLC library basename: ' + path.name)
    shutil.copy2(path, target)
    manifest['libraries'][path.name] = {'source': str(path), 'sha256': digest,
                                       'licenses': retain_licenses(path)}
    if path.name.startswith('ld-linux-'):
        if loader not in (None, path.name):
            raise ValueError('conflicting VLC ELF interpreters')
        loader = path.name
    dependencies(path)



source=Path('/usr/bin/vlc')
with source.open('rb')as stream:
    if stream.read(4)!=b'\x7fELF':raise ValueError('actual pinned VLC ELF required')
shutil.copy2(source,prefix/'bin/vlc')
manifest['binaries']['vlc']={'source':str(source),'sha256':sha(source),'licenses':retain_licenses(source)}
dependencies(source)
if loader is None:raise ValueError('VLC ELF interpreter required')

# Keep the actual compiled plugin directory, avoiding a relocated/stale cache.
# Every installed plugin gets its own recursive ELF closure and owning notice.
plugins=Path('/usr/lib/x86_64-linux-gnu/vlc/plugins')
if not plugins.is_dir():raise ValueError('actual pinned VLC plugin directory required')
for source in sorted(plugins.rglob('*')):
    if not source.is_file():continue
    if source.name=='plugins.dat':
        manifest['omitted_plugin_cache']={'source':str(source),'sha256':sha(source)}
        continue
    target=root/source.relative_to('/')
    target.parent.mkdir(parents=True,exist_ok=True)
    if target.exists():raise ValueError('conflicting VLC plugin path')
    shutil.copy2(source,target)
    manifest['plugins']['/'+str(target.relative_to(root))]={'source':str(source),'sha256':sha(source),'licenses':retain_licenses(source)}
    with source.open('rb')as stream:elf=stream.read(4)==b'\x7fELF'
    if elf:dependencies(source)

# Public package data only: no /etc or home/configuration directories.
shared=Path('/usr/share/vlc')
if shared.is_dir():
    for source in sorted(shared.rglob('*')):
        if not source.is_file():continue
        target=root/source.relative_to('/');target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source,target)
        manifest['data']['/'+str(target.relative_to(root))]={'source':str(source),'sha256':sha(source),'licenses':retain_licenses(source)}
for source in sorted(Path('/lib/x86_64-linux-gnu').glob('libnss_*.so*')):
    if source.is_file():library(source)

# Preserve the pinned cvlc shell wrapper's literal prefix arguments. Never run
# or copy an arbitrary shell program, and never silently guess its semantics.
cvlc=Path('/usr/bin/cvlc');original=cvlc.read_text()
lines=[line.strip()for line in original.splitlines()if line.strip()and not line.lstrip().startswith('#')]
if len(lines)!=1:raise ValueError('unknown pinned cvlc wrapper program')
words=shlex.split(lines[0])
if len(words)<3 or words[:2]!=['exec','/usr/bin/vlc']or words[-1]!='$@':raise ValueError('unknown pinned cvlc exec wrapper')
arguments=words[2:-1]
if any(any(char in value for char in '$`;|&<>\n\r')for value in arguments):raise ValueError('nonliteral cvlc prefix arguments')
manifest['binaries']['cvlc_source_wrapper']={'source':str(cvlc),'sha256':sha(cvlc),'literal_prefix_arguments':arguments,'licenses':retain_licenses(cvlc)}
launcher='/opt/dispatcharr-vlc/lib/'+loader+' --library-path /opt/dispatcharr-vlc/lib /opt/dispatcharr-vlc/bin/vlc'
for name,args in [('vlc',[]),('cvlc',arguments)]:
    wrapper=root/'usr/local/bin'/name
    if wrapper.exists():raise ValueError('conflicting VLC wrapper')
    wrapper.parent.mkdir(parents=True,exist_ok=True)
    wrapper.write_text('#!/bin/sh\nexec '+launcher+''.join(' '+shlex.quote(value)for value in args)+' "$@"\n')
    wrapper.chmod(0o755)
    manifest['wrappers'][name]={'path':'/'+str(wrapper.relative_to(root)),'sha256':sha(wrapper)}
(docs/'vlc-runtime.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
