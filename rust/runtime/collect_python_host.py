"""Build-only pinned lazy CPython/plugin compatibility closure.
Original Dispatcharr, CPython, Debian and distribution authors retain credit.
No django/app import, install, subprocess host, DB/HTTP/cache operation or keys.
Loaded lazily only by the owned native host; no application process starts here.
"""
import ast,hashlib,importlib.metadata as metadata,json,os,re,shutil,subprocess,sys,sysconfig
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
OUT=Path('/media-runtime');PREFIX=OUT/'opt/dispatcharr-python';DOCS=OUT/'usr/share/doc/dispatcharr/python-host';DOCS.mkdir(parents=True,exist_ok=False)
if sys.version_info[:2]!=(3,13)or Path(sys.prefix)!=Path('/dispatcharrpy')or Path(sys.base_prefix)!=Path('/usr'):raise RuntimeError('Pinned Original CPython3.13 venv required')
PREFIX.mkdir(parents=True,exist_ok=True)
def sha(path):
 h=hashlib.sha256()
 with path.open('rb')as stream:
  for chunk in iter(lambda:stream.read(1024*1024),b''):h.update(chunk)
 return h.hexdigest()
def copy_file(source,target):
 source=Path(source);target=Path(target)
 if not source.is_file():raise RuntimeError('Missing regular pinned runtime file: '+str(source))
 target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target,follow_symlinks=True)
excluded_source_assets=[]
def source_http_module(path):
 if path.suffix!='.py':return False
 module=ast.parse(path.read_text(),filename=str(path))
 def name(node):
  return node.id if isinstance(node,ast.Name)else node.attr if isinstance(node,ast.Attribute)else name(node.func)if isinstance(node,ast.Call)else''
 for node in ast.walk(module):
  if isinstance(node,ast.ImportFrom)and (node.module or '').startswith(('rest_framework.views','rest_framework.viewsets','rest_framework.generics')):return True
  if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef))and any(name(d)in('api_view','action','require_GET','require_POST','require_http_methods','require_safe','csrf_exempt')for d in node.decorator_list):return True
 return False
app=PREFIX/'application'
def tree(source,target):
 source=Path(source)
 if not source.is_dir():raise RuntimeError('Missing pinned runtime directory: '+str(source))
 for child in sorted(source.rglob('*')):
  if '__pycache__'in child.parts or child.suffix in('.pyc','.pyo')or 'tests'in child.relative_to(source).parts or 'test'in child.relative_to(source).parts:continue
  relative=child.relative_to(source)
  # Source HTTP/admin/management entrypoints are neither needed for the ORM
  # nor shipped as a hidden fallback backend. Unsupported plugin imports fail.
  if target==app or target.is_relative_to(app):
   if 'management'in relative.parts or 'views'in relative.parts or child.name.endswith('_views.py')or child.name in('api_views.py','views.py','api_urls.py','urls.py','wsgi.py','asgi.py','admin.py','routing.py','server.py','stream_manager.py')or child.is_file()and source_http_module(child):
    if child.is_file():excluded_source_assets.append({'source':str(child),'sha256':sha(child)})
    continue
  dest=target/relative
  if child.is_symlink()and child.is_dir():raise RuntimeError('Directory symlink requires resolved runtime closure: '+str(child))
  if child.is_dir():dest.mkdir(parents=True,exist_ok=True)
  elif child.is_file():copy_file(child,dest)
  else:raise RuntimeError('Unsupported runtime special/symlink object: '+str(child))
stdlib=Path(sysconfig.get_path('stdlib'));site=Path(sysconfig.get_path('purelib'));exe=Path(sys.executable).resolve()
# Reuse the certified dependency-only interpreter/stdlib. A changed byte is an
# error, never an implicit replacement of the already licensed regex closure.
if not(PREFIX/'bin/python3.13').is_file()or sha(PREFIX/'bin/python3.13')!=sha(exe):raise RuntimeError('Certified pinned interpreter preimage missing or changed')
retained_site=PREFIX/'lib/python3.13/dist-packages'
retained_site.mkdir(parents=True,exist_ok=True)
tree(site,retained_site)
# Preserve actual Source application modules, never a second always-on backend.
# These assets are loaded only by the explicit lazy owned host bootstrap.
app=PREFIX/'application'
# Immutable reviewed Original library subset is supplied by the build context.
# The pinned dependency image deliberately has no /app application checkout.
import tarfile
source_input=Path('/tmp/plugin_compat_source')
source_manifest=json.loads((source_input/'manifest.json').read_text())
if source_manifest.get('schema')!=1:raise RuntimeError('Unsupported compatibility source manifest')
source_archive=source_input/'source.tar.gz'
if sha(source_archive)!=source_manifest['archive_sha256']:raise RuntimeError('Compatibility source archive hash mismatch')
expected={item['path']:item for item in source_manifest['retained']}
if len(expected)!=len(source_manifest['retained'])or len(expected)>1211:raise RuntimeError('Invalid compatibility source inventory')
validated=[];seen=set()
with tarfile.open(source_archive,mode='r:gz')as archive:
 for member in archive:
  name=member.name;parts=Path(name).parts
  if not member.isfile()or name not in expected or name in seen or name.startswith('/')or '..'in parts:raise RuntimeError('Unexpected compatibility source member')
  item=expected[name]
  if member.size!=item['bytes']or member.size>16*1024*1024:raise RuntimeError('Compatibility source size mismatch')
  data=archive.extractfile(member).read(member.size+1)
  if len(data)!=member.size or hashlib.sha256(data).hexdigest()!=item['sha256']:raise RuntimeError('Compatibility source byte mismatch')
  if name not in('LICENSE','version.py')and parts[0]not in('apps','core','dispatcharr'):raise RuntimeError('Compatibility source outside declared libraries')
  seen.add(name);validated.append((name,data))
if seen!=set(expected):raise RuntimeError('Missing compatibility source members')
# Validate the entire closure before writing any application library file.
for name,data in validated:
 target=DOCS/'Dispatcharr-LICENSE'if name=='LICENSE'else app/name
 target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);target.chmod(0o444)
copy_file(source_input/'manifest.json',DOCS/'source-manifest.json')
excluded_source_assets.extend(source_manifest['excluded'])
licenses={};wheel_owners={}
for distribution in metadata.distributions(path=[str(site)]):
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
manifest={'source_library_original_archive_sha256':source_manifest['original_archive_sha256'],'source_library_archive_sha256':source_manifest['archive_sha256'],'excluded_source_assets':excluded_source_assets,'source_application_assets_copied':True,'original_http_admin_management_entrypoints_excluded':True,'actual_execution_audit_required':True,'source_backend_started':False,'source_image':IMAGE,'python':sys.version,'base_prefix':sys.base_prefix,'distributions':licenses,'ELF':[],'files':[],'application_started':False,'external_key_homes_or_runtime_data_copied':False,'distribution_fixture_key_audit_required':True}
loader=None;lib=PREFIX/'elf';lib.mkdir(exist_ok=True)
# Inspect only real retained ELF, preserving site-package-relative RPATH assets.
# ldd is bounded and runs only against binaries from the pinned image.
for retained in sorted(PREFIX.rglob('*')):
 if not retained.is_file():continue
 with retained.open('rb')as stream:is_elf=stream.read(4)==b'\x7fELF'
 if not is_elf:continue
 if retained==PREFIX/'bin/python3.13':source=exe
 elif retained.is_relative_to(retained_site):source=site/retained.relative_to(retained_site)
 elif retained.is_relative_to(app):continue
 elif retained.is_relative_to(lib):continue
 elif retained.is_relative_to(PREFIX/'bridge'):continue
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
# Audit only dependency module origins in the COPIED prefix. Importing these
# modules does not configure Django, Celery or the Source application.
audit_code=r'''import importlib,sys,json
from pathlib import Path
root=Path(sys.argv[1]).resolve();result={}
for name in ('django','psycopg','celery','kombu','redis','django_redis','channels','channels_redis','rest_framework','regex'):
 module=importlib.import_module(name);origin=Path(module.__file__).resolve();assert origin.is_relative_to(root);result[name]=str(origin.relative_to(root))
assert all(Path(p).is_relative_to(root)for p in sys.path if p)
print(json.dumps(result))'''
audit=subprocess.run([str(lib/loader),'--library-path',str(lib),str(PREFIX/'bin/python3.13'),'-B','-I','-c',audit_code,str(PREFIX)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=30,check=False)
if audit.returncode or audit.stderr or len(audit.stdout)>65536:raise RuntimeError('Copied plugin dependency module origin audit failed: '+audit.stderr[:4096])
manifest['actual_relocated_dependency_module_audit']=json.loads(audit.stdout)
manifest['application_root']='/opt/dispatcharr-python/application'
manifest['retained_distribution_directory']='/opt/dispatcharr-python/lib/python3.13/dist-packages'
wrapper=OUT/'usr/local/bin/dispatcharr-plugin-python';wrapper.parent.mkdir(parents=True,exist_ok=True)
wrapper.write_text('#!/bin/sh\nunset PYTHONHOME PYTHONPATH\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 -I /opt/dispatcharr-python/plugin-bridge/dispatcharr_native/bootstrap.py "$@"\n');wrapper.chmod(0o755)
cli=OUT/'usr/local/bin/dispatcharr-python';cli.write_text('#!/bin/sh\nexec /opt/dispatcharr-python/elf/'+loader+' --library-path /opt/dispatcharr-python/elf /opt/dispatcharr-python/bin/python3.13 "$@"\n');cli.chmod(0o755)
# Bootstrap/custom backend are supplied separately from reviewed owned source;
# this collector never copies a fixture, shell environment, upload or key home.
for path in sorted(PREFIX.rglob('*')):
 if path.is_file():manifest['files'].append({'path':'/'+str(path.relative_to(OUT)),'bytes':path.stat().st_size,'sha256':sha(path)})
(DOCS/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
