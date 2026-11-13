#!/usr/bin/env python3
"""Reconstruct exact standalone component source from immutable Git+8 overlays.
No current-worktree/private-source fallback, builds, Docker or network calls.
"""
import argparse,hashlib,json,subprocess,tarfile,tempfile,shutil,os
from pathlib import Path
ROOT=Path(__file__).resolve().parent
sha=lambda p:hashlib.sha256(Path(p).read_bytes()).hexdigest()
def reconstruct(repo,output,root=ROOT):
 repo=Path(repo).resolve();output=Path(output).resolve()
 if output.exists():raise FileExistsError('Fresh output required')
 plan=json.loads((root/'source-overlays.json').read_text());mapping=json.loads((root/'source-inputs.json').read_text())
 if sha(root/'source-inputs.json')!=plan['full_source_map_sha256']:raise ValueError('Full source map seal')
 if len(mapping)!=814:raise ValueError('Exact standalone source814')
 for name,value in plan['overlays'].items():
  path=Path(name);relative=Path(value['historical_file'])
  if path.is_absolute()or '..'in path.parts or relative.is_absolute()or '..'in relative.parts:raise ValueError('Unsafe overlay path')
  data=(root/relative).read_bytes()
  if hashlib.sha256(data).hexdigest()!=value['sha256']or mapping.get(name)!=value['sha256']:raise ValueError('Overlay source hash')
  if hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest()!=value['git_blob']:raise ValueError('Overlay Git blob')
 output.parent.mkdir(parents=True,exist_ok=True)
 with tempfile.TemporaryDirectory(prefix='parented-reconstruct-',dir=output.parent)as temporary:
  temporary=Path(temporary);archive=temporary/'base.tar';source=temporary/'source';source.mkdir()
  coupled=[name for name in mapping if not name.startswith('rust/')]
  subprocess.run(['git','-C',str(repo),'archive','--format=tar','--output',str(archive),plan['base'],'--','rust',*coupled],check=True,stdout=subprocess.DEVNULL,env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')})
  with tarfile.open(archive)as stream:stream.extractall(source,filter='data')
  for name,value in plan['overlays'].items():
   dest=source/name;expected=value['preimage_sha256']
   if expected is None:
    if dest.exists():raise ValueError('Unexpected existing new overlay')
   elif not dest.is_file()or sha(dest)!=expected:raise ValueError('Immutable base preimage')
   dest.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/value['historical_file'],dest)
  actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()}
  if actual!=mapping:raise ValueError('Every reconstructed source input must match')
  shutil.move(str(source),output)
 return {'passed':True,'base':plan['base'],'inputs':len(mapping),'source_map_sha256':plan['full_source_map_sha256'],'current_worktree_used':False,'application_or_build_started':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output)))
