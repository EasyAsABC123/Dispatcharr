"""Build-only pinned lazy CPython/regex-only dependency closure.
Original Dispatcharr, CPython, Debian and distribution authors retain credit.
No django/app import, install, subprocess host, DB/HTTP/cache operation or keys.
NOT wired into Docker until the actual host/ORM/Jobs/Channels lifecycle is ready.
"""
import hashlib,importlib.metadata as metadata,json,os,re,shutil,subprocess,sys,sysconfig
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
OUT=Path('/media-runtime');PREFIX=OUT/'opt/dispatcharr-python';DOCS=OUT/'usr/share/doc/dispatcharr/regex-host';DOCS.mkdir(parents=True,exist_ok=False)
if sys.version_info[:2]!=(3,13)or Path(sys.prefix)!=Path('/dispatcharrpy')or Path(sys.base_prefix)!=Path('/usr'):raise RuntimeError('Pinned Original CPython3.13 venv required')
PREFIX.mkdir(parents=True,exist_ok=False)
def sha(path):
 h=hashlib.sha256()
 with path.open('rb')as stream:
  for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def copy_file(source,target):
 source=Path(source);target=Path(target)
 if not source.is_file():raise RuntimeError('Missing regular pinned runtime file: '+str(source))
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target,follow_symlinks=True)
def tree(source,target):
 source=Path(source)
 if not source.is_dir():raise RuntimeError('Missing pinned runtime directory: '+str(source))
 for child in sorted(source.rglob('*')):
  if '__pycache__'in child.parts or child.suffix in('.pyc','.pyo')or 'test'in child.relative_to(source).parts or 'tests'in child.relative_to(source).parts:continue
  dest=target/child.relative_to(source)
  if child.is_symlink()and child.is_dir():raise RuntimeError('Directory symlink requires resolved runtime closure: '+str(child))
  if child.is_dir():dest.mkdir(parents=True,exist_ok=True)
  elif child.is_file():copy_file(child,dest)
  else:raise RuntimeError('Unsupported runtime special/symlink object: '+str(child))
stdlib=Path(sysconfig.get_path('stdlib'));site=Path(sysconfig.get_path('purelib'));exe=Path(sys.executable).resolve()
copy_file(exe,PREFIX/'bin/python3.13');tree(stdlib,PREFIX/'lib/python3.13');(PREFIX/'lib/python3.13/site-packages').mkdir(parents=True)
# Dependency-only mode copies no Source Python/application assets. The final
# Docker stage retains the repository LICENSE and ATTRIBUTION.md explicitly.
licenses={};wheel_owners={}
for distribution in [metadata.distribution('regex')]:
 for item in distribution.files or[]:
  source=Path(distribution.locate_file(item))
  if not source.resolve().is_relative_to(site.resolve()):raise RuntimeError('Regex distribution escapes pinned site-packages')
  if '__pycache__'in source.parts or source.suffix in('.pyc','.pyo')or source.name.startswith('test_')or 'tests'in source.parts:continue
  copy_file(source,PREFIX/'lib/python3.13/site-packages'/source.relative_to(site))
 name=distribution.metadata['Name'];version=distribution.version
 if not name or not re.fullmatch(r'[a-zA-Z0-9_.+-]+',name):raise RuntimeError('Invalid installed distribution name')
 info=Path(distribution._path)
 notices=[p for p in info.rglob('*')if p.is_file()and any(word in p.name.upper()for word in('LICENSE','COPYING','COPYRIGHT','NOTICE'))]
 if not notices:raise RuntimeError('Installed distribution lacks retained license text: '+name)
 retained=[]
 for notice in sorted(notices):
  dest=DOCS/'distributions'/name/notice.relative_to(info);copy_file(notice,dest);retained.append({'source':str(notice),'retained':'/'+str(dest.relative_to(OUT)),'sha256':sha(notice)})
 licenses[name]={'version':version,'notices':retained}
 for item in distribution.files or[]:
  source=Path(distribution.locate_file(item))
  if source.is_file():wheel_owners.setdefault(str(source.resolve()),set()).add(name)
package_cache={}
def owners(path):
 # Wheels own their vendored ELF and associated license texts. All other ELF
 # must have an actual Debian package owner and byte-identical copyright.
 if str(path.resolve())in wheel_owners:return[{'distribution':n,**licenses[n]}for n in sorted(wheel_owners[str(path.resolve())])]
 found=set()
 for query in dict.fromkeys([str(path.resolve()),str(path)]):
  result=subprocess.run(['dpkg-query','--search',query],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=10,check=False)
  if result.returncode not in(0,1):raise RuntimeError('ELF owning-package query failed')
  for line in result.stdout.splitlines():
   owner,sep,filename=line.rpartition(': ')
   if sep and filename==query:found.update(owner.split(', '))
  if found:break
 if not found:raise RuntimeError('ELF missing actual Debian/distribution license owner: '+str(path))
 result=[]
 for owner in sorted(found):
  if not re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?',owner):raise RuntimeError('Invalid Debian package owner')
  package=owner.partition(':')[0]
  if package not in package_cache:
   notice=Path('/usr/share/doc')/package/'copyright';dest=DOCS/'debian'/package/'copyright';copy_file(notice,dest);package_cache[package]={'source':str(notice),'retained':'/'+str(dest.relative_to(OUT)),'sha256':sha(notice)}
  result.append({'package':owner,**package_cache[package]})
 return result
manifest={'source_image':IMAGE,'python':sys.version,'base_prefix':sys.base_prefix,'distributions':licenses,'ELF':[],'files':[],'application_started':False,'external_key_homes_or_runtime_data_copied':False,'distribution_fixture_key_audit_required':True,'source_application_assets_copied':False,'mode':'regex-only'}
loader=None;lib=PREFIX/'elf';lib.mkdir()
# Inspect only real retained ELF, preserving site-package-relative RPATH assets.
# ldd is bounded and runs only against binaries from the pinned image.
for retained in sorted(PREFIX.rglob('*')):
 if not retained.is_file():continue
 with retained.open('rb')as stream:is_elf=stream.read(4)==b'\x7fELF'
 if not is_elf:continue
 if retained==PREFIX/'bin/python3.13':source=exe
 elif retained.is_relative_to(PREFIX/'lib/python3.13/site-packages'):source=site/retained.relative_to(PREFIX/'lib/python3.13/site-packages')
 else:source=stdlib/retained.relative_to(PREFIX/'lib/python3.13')
 record={'source':str(source),'retained':'/'+str(retained.relative_to(OUT)),'sha256':sha(retained),'licenses':owners(source),'dependencies':[]}
 dependency=subprocess.run(['ldd',str(source)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=15,check=False)
 if dependency.returncode:raise RuntimeError('Pinned ELF dependency enumeration failed: '+str(source))
 if 'not found'in dependency.stdout:raise RuntimeError('Missing pinned ELF dependency: '+str(source))
 for token in dependency.stdout.split():
  path=Path(token)
  if not token.startswith('/')or not path.is_file():continue
  dest=lib/path.name
  if dest.exists()and sha(dest)!=sha(path):raise RuntimeError('Conflicting Python ELF dependency basename: '+path.name)
  if not dest.exists():copy_file(path,dest)
  record['dependencies'].append({'source':str(path),'retained':'/'+str(dest.relative_to(OUT)),'sha256':sha(path),'licenses':owners(path)})
  if path.name.startswith('ld-linux-'):loader=path.name
 manifest['ELF'].append(record)
if not loader:raise RuntimeError('Pinned Python dynamic ELF loader missing')
wrapper=OUT/'usr/local/bin/dispatcharr-regex-python';wrapper.parent.mkdir(parents=True,exist_ok=True)
wrapper.write_text('#!/bin/sh\nunset PYTHONHOME PYTHONPATH\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 -I /opt/dispatcharr-python/bridge/regex_bootstrap.py "$@"\n');wrapper.chmod(0o755)
cli=OUT/'usr/local/bin/dispatcharr-python';cli.write_text('#!/bin/sh\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 "$@"\n');cli.chmod(0o755)
# Bootstrap/custom backend are supplied separately from reviewed owned source;
# this collector never copies a fixture, shell environment, upload or key home.
for path in sorted(PREFIX.rglob('*')):
 if path.is_file():manifest['files'].append({'path':'/'+str(path.relative_to(OUT)),'bytes':path.stat().st_size,'sha256':sha(path)})
(DOCS/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
