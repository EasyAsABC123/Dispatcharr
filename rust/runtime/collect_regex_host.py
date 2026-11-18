"""Build-only pinned lazy CPython/regex/packaging dependency closure.
Original Dispatcharr, CPython, Debian and distribution authors retain credit.
No Django/application import, install, DB/HTTP/cache operation or runtime keys.
The copied dependency interpreter runs only a bounded module-origin audit.
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
# Actual pinned Debian non-venv policy recognizes this dist-packages path.
# The source venv's site-packages path is NOT discovered after relocation.
retained_site=PREFIX/'lib/python3.13/dist-packages'
copy_file(exe,PREFIX/'bin/python3.13');tree(stdlib,PREFIX/'lib/python3.13');retained_site.mkdir(parents=True)
# Dependency-only mode copies no Source Python/application assets. The final
# Docker stage retains the repository LICENSE and ATTRIBUTION.md explicitly.
licenses={};wheel_owners={}
for distribution in [metadata.distribution('regex'),metadata.distribution('packaging')]:
 for item in distribution.files or[]:
  source=Path(distribution.locate_file(item))
  if not source.resolve().is_relative_to(site.resolve()):raise RuntimeError('Regex distribution escapes pinned site-packages')
  if '__pycache__'in source.parts or source.suffix in('.pyc','.pyo')or source.name.startswith('test_')or 'tests'in source.parts:continue
  copy_file(source,retained_site/source.relative_to(site))
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
 elif retained.is_relative_to(retained_site):source=site/retained.relative_to(retained_site)
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
# Probe the COPIED interpreter/stdlib/regex closure, not ambient Source sys.path.
# -I suppresses PYTHONPATH/HOME/user-site. Fail before any successful manifest if
# the dependency or loaded extension resolves outside this exact retained tree.
audit_code=r"""
import hashlib,importlib.util,json,sys
from pathlib import Path
import regex,packaging.version,unicodedata
root=Path(sys.argv[1]).resolve()
assert Path(sys.prefix).resolve()==root
assert importlib.util.find_spec('django') is None
modules=[]
for name,module in list(sys.modules.items()):
 if name=='regex' or name.startswith('regex.') or name=='packaging' or name.startswith('packaging.') or name=='unicodedata':
  filename=getattr(module,'__file__',None)
  if filename is None:continue
  path=Path(filename).resolve();assert path.is_relative_to(root)
  modules.append({'name':name,'relative_origin':str(path.relative_to(root)),'sha256':hashlib.sha256(path.read_bytes()).hexdigest()})
assert modules and any(m['relative_origin'].endswith('.so')for m in modules)
assert any(m['name']=='packaging.version'for m in modules)
assert any(m['name']=='unicodedata'for m in modules)
assert str.lower.__objclass__ is str and str.isalnum.__objclass__ is str
assert packaging.version.parse('1!0.1')>packaging.version.parse('9.0')
assert all(Path(path).is_relative_to(root)for path in sys.path if path)
print(json.dumps({'python':sys.version,'regex':regex.__version__,'packaging':packaging.__version__,'prefix':str(root),'modules':modules,'Django_available':False,'Unicode_version':unicodedata.unidata_version,'string_primitives':'pinned CPython str.lower/str.isalnum'}))
"""
audit=subprocess.run([str(lib/loader),'--library-path',str(lib),str(PREFIX/'bin/python3.13'),'-B','-I','-c',audit_code,str(PREFIX)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=15,check=False)
if audit.returncode or audit.stderr or len(audit.stdout)>65536:raise RuntimeError('Copied isolated dependency module-origin audit failed: '+audit.stderr[:4096])
manifest['actual_relocated_module_audit']=json.loads(audit.stdout)
assert manifest['actual_relocated_module_audit']['packaging']==licenses['packaging']['version']
manifest['retained_distribution_directory']='/'+str(retained_site.relative_to(OUT))
wrapper=OUT/'usr/local/bin/dispatcharr-regex-python';wrapper.parent.mkdir(parents=True,exist_ok=True)
wrapper.write_text('#!/bin/sh\nunset PYTHONHOME PYTHONPATH\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 -I /opt/dispatcharr-python/bridge/regex_bootstrap.py "$@"\n');wrapper.chmod(0o755)
cli=OUT/'usr/local/bin/dispatcharr-python';cli.write_text('#!/bin/sh\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 "$@"\n');cli.chmod(0o755)
# Bootstrap/custom backend are supplied separately from reviewed owned source;
# this collector never copies a fixture, shell environment, upload or key home.
for path in sorted(PREFIX.rglob('*')):
 if path.is_file():manifest['files'].append({'path':'/'+str(path.relative_to(OUT)),'bytes':path.stat().st_size,'sha256':sha(path)})
(DOCS/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
