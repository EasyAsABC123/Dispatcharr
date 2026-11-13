#!/usr/bin/env python3
"""Exact historical source reconstruction from immutable Git blobs and selected overlays."""
import argparse,hashlib,json,os,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='3b97e4f5f6fe2b3d744b93ac8aa29979b5f926b6'
EXPECTED_MANIFEST_SHA='294fe99c5cf40a7e03f41441ac8dd8073cef2892dc4043e5c481c299f29d0193'
def need(ok,label):
 if not ok:raise ValueError(label)
def sha(b):return hashlib.sha256(b).hexdigest()
def safe(path):
 p=Path(path);need(not p.is_absolute()and '..'not in p.parts and str(p)==path,'SafeRelativeInput');return p
def git(repo,*args):
 p=subprocess.run(['git','--git-dir='+str(Path(repo).resolve()/'.git'),*args],stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
 need(p.returncode==0 and len(p.stdout)<=33554432,'ImmutableGitRead');return p.stdout
def load_inputs():
 raw=(HERE/'historical-source-inputs.json').read_bytes();need(sha(raw)==EXPECTED_MANIFEST_SHA,'PinnedHistoricalManifest');d=json.loads(raw);need(d['base_revision']==BASE and d['Rust_inputs']==841 and d['full_inputs']==846,'HistoricalInputScope')
 need(len(d['base_inputs'])==842 and len(d['after_inputs'])==846 and len(d['overlays'])==11,'ExactBaseAndOverlayCoverage')
 for field in ('base_inputs','after_inputs'):
  for path,digest in d[field].items():safe(path);need(type(digest)is str and len(digest)==64 and all(c in'0123456789abcdef'for c in digest),'TypedSourceHash')
 changed={p for p,h in d['after_inputs'].items()if d['base_inputs'].get(p)!=h};need(changed==set(d['overlays']),'ExactSelectedSourceChanges')
 need(set(d['base_inputs'])<=set(d['after_inputs']),'NoHistoricalSourceLoss')
 for path,row in d['overlays'].items():
  need(row=={'preimage_sha256':d['base_inputs'].get(path),'sha256':d['after_inputs'][path]},'ExactOverlayPreimage')
  p=HERE/'historical-source-overlay'/safe(path);need(p.is_file()and not p.is_symlink()and sha(p.read_bytes())==row['sha256'],'ExactSelectedOverlayBlob')
 actual={str(p.relative_to(HERE/'historical-source-overlay'))for p in(HERE/'historical-source-overlay').rglob('*')if p.is_file()}
 need(actual==set(d['overlays']),'NoExtraSelectedSource');return d
def reconstruct(repo,output):
 d=load_inputs();out=Path(output).resolve();need(not out.exists()and not out.is_symlink(),'FreshReconstructionOutput')
 raw=git(repo,'ls-tree','-r','-z',BASE,'rust','scripts/rust-timeshift-contracts');available={}
 for row in raw.split(b'\0'):
  if not row:continue
  meta,path=row.split(b'\t',1);mode,kind,oid=meta.split();path=path.decode()
  if path in d['base_inputs']:
   need(kind==b'blob'and mode in(b'100644',b'100755'),'GitBlobMode');available[path]=oid.decode()
 rust={p for p in git(repo,'ls-tree','-r','--name-only',BASE,'rust').decode().splitlines()};need(rust=={p for p in d['base_inputs']if p.startswith('rust/')}and set(available)==set(d['base_inputs']),'ExactImmutableBaseFileSet')
 blobs={}
 for path,oid in available.items():
  value=git(repo,'cat-file','blob',oid);need(sha(value)==d['base_inputs'][path],'ImmutableBaseContentHash');blobs[path]=value
 for path in d['overlays']:blobs[path]=(HERE/'historical-source-overlay'/path).read_bytes()
 need({p:sha(v)for p,v in blobs.items()}==d['after_inputs'],'Full846AfterInputDictionary')
 out.mkdir(parents=True,exist_ok=False)
 for path,raw in blobs.items():
  p=out/safe(path);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
 return {'reconstructed':True,'files':len(blobs),'Rust_inputs':841,'coupled_inputs':5,'base_revision':BASE,'application_executed':False,'Main_activated':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
