"""Pinned Original Comskip ELF closure; no download, fallback executable or backend.
Original Dispatcharr, Comskip and dependency authors retain all credit.
"""
import hashlib,json,shutil,subprocess,re
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ROOT=Path('/media-runtime');PREFIX=ROOT/'opt/dispatcharr-comskip'
(PREFIX/'bin').mkdir(parents=True)
(PREFIX/'lib').mkdir()
EXPECTED='1452696c945a64f5351977143ecad3d384e7036479ba5d85ed9c928d6e20d460'
INI='7614488f8addcd44850f524f5a7d104527aa6d80c25dbdf8d60219d4e658b896'
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
source=Path(shutil.which('comskip')or'/missing-pinned-comskip')
assert source.is_file()and digest(source)==EXPECTED,'Pinned Original comskip binary unavailable/changed'
shutil.copy2(source,PREFIX/'bin/comskip')
manifest={'source_image':IMAGE,'binary':{'path':str(source),'sha256':digest(source)},'libraries':{},'credit':'Original Dispatcharr, Comskip and dependency authors retain all credit. Binary copied unchanged from pinned Original.','Source_default_ini_from_pinned_repository':{'revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','sha256':INI}}
p=subprocess.run(['ldd',str(source)],check=True,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
loader=None
for line in p.stdout.splitlines():
 for token in line.split():
  path=Path(token)
  if not token.startswith('/')or not path.is_file():continue
  target=PREFIX/'lib'/path.name;actual=digest(path)
  if target.exists():assert digest(target)==actual,'Conflicting exact pinned Comskip dependency'
  else:shutil.copy2(path,target)
  manifest['libraries'][path.name]={'source':str(path),'sha256':actual}
  if path.name.startswith('ld-linux-'):loader=path.name
assert loader
wrapper=ROOT/'usr/local/bin/comskip';wrapper.write_text('#!/bin/sh\nexec /opt/dispatcharr-comskip/lib/'+loader+' --library-path /opt/dispatcharr-comskip/lib /opt/dispatcharr-comskip/bin/comskip "$@"\n');wrapper.chmod(0o755)
ini=Path('/tmp/comskip-source.ini');assert digest(ini)==INI
target=ROOT/'app/docker/comskip.ini';target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ini,target)
# collect_media already retained every available pinned Source package copyright,
# not only the media-linked packages. Reuse that byte-exact legal closure and
# record actual package ownership for Comskip's separate dependency namespace.
paths=sorted({value['source'] for value in manifest['libraries'].values()}|{str(Path(value['source']).resolve())for value in manifest['libraries'].values()})
query=subprocess.run(['dpkg-query','-S',*paths],text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
assert query.returncode in(0,1),'Actual pinned package ownership query failed'
owners={}
for line in query.stdout.splitlines():
    package,sep,path=line.rpartition(': ')
    if sep and path in paths and re.fullmatch(r'[a-z0-9][a-z0-9+.-]*(?::[a-z0-9]+)?',package):owners.setdefault(path,set()).add(package)
missing=[]
for name,value in manifest['libraries'].items():
    source_path=Path(value['source']);packages=sorted(owners.get(str(source_path),set())|owners.get(str(source_path.resolve()),set()));notices=[]
    for package in packages:
        original=Path('/usr/share/doc')/package.split(':',1)[0]/'copyright'
        retained=ROOT/'usr/share/doc/dispatcharr/media-copyright'/package.split(':',1)[0]/'copyright'
        if original.is_file():
            assert retained.is_file()and digest(retained)==digest(original),'Existing pinned media legal closure changed'
            notices.append({'package':package,'source':str(original),'path':'/'+str(retained.relative_to(ROOT)),'sha256':digest(retained)})
    value.update(actual_binary_packages=packages,copyright_notices=notices)
    if not packages or not notices:missing.append(name)
manifest['dependency_notice_scope']={'existing_exact_Source_package_copyright_closure':'/usr/share/doc/dispatcharr/media-copyright','actual_package_query_exit':query.returncode,'libraries_without_verified_owner_or_copyright':missing,'full_corresponding_source_or_release_closure_verified':False}
(ROOT/'usr/share/doc/dispatcharr/comskip-runtime.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')

for name in ('comskip-LICENSE','comskip-NOTICE','comskip-Original-DispatcharrBase'):
    shutil.copy2(Path('/tmp')/name,ROOT/'usr/share/doc/dispatcharr'/name)
