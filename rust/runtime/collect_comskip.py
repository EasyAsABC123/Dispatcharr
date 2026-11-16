"""Pinned Original Comskip ELF closure; no download, fallback executable or backend.
Original Dispatcharr, Comskip and dependency authors retain all credit.
"""
import hashlib,json,shutil,subprocess
from pathlib import Path
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
ROOT=Path('/media-runtime');PREFIX=ROOT/'opt/dispatcharr-media'
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
  if target.exists():assert digest(target)==actual,'Conflicting exact pinned media dependency'
  else:shutil.copy2(path,target)
  manifest['libraries'][path.name]={'source':str(path),'sha256':actual}
  if path.name.startswith('ld-linux-'):loader=path.name
assert loader
wrapper=ROOT/'usr/local/bin/comskip';wrapper.write_text('#!/bin/sh\nexec /opt/dispatcharr-media/lib/'+loader+' --library-path /opt/dispatcharr-media/lib /opt/dispatcharr-media/bin/comskip "$@"\n');wrapper.chmod(0o755)
ini=Path('/tmp/comskip-source.ini');assert digest(ini)==INI
target=ROOT/'app/docker/comskip.ini';target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(ini,target)
(ROOT/'usr/share/doc/dispatcharr/comskip-runtime.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')

for name in ('comskip-LICENSE','comskip-NOTICE','comskip-Original-DispatcharrBase'):
    shutil.copy2(Path('/tmp')/name,ROOT/'usr/share/doc/dispatcharr'/name)
