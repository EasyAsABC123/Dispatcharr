"""Strict Git/overlay-only reconstruction of the824+5 component. No private fallback.
Original Dispatcharr authors retain application/algorithm/license credit.
"""
import argparse,hashlib,json,os,re
from pathlib import Path
import bounded_process as B
HERE=Path(__file__).resolve().parent
INPUTS_SHA='c295e872042b3012837fa3af6c491759a35f60a7f589faee37e74ab903dc19c2'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def safe(path):
 p=Path(path)
 if p.is_absolute()or'..'in p.parts or not p.parts:raise ValueError('SafeSourcePath')
 return p

def load():
 raw=(HERE/'source-inputs.json').read_bytes()
 if sha(raw)!=INPUTS_SHA:raise ValueError('ExactSourceInputManifest')
 spec=json.loads(raw)
 if not re.fullmatch('[0-9a-f]{40}',spec['base_revision'])or len(spec['base_inputs'])!=811 or len(spec['inputs'])!=829 or sum(k.startswith('rust/')for k in spec['inputs'])!=824 or len(spec['overlay'])!=25:raise ValueError('ExactComponentClosure')
 changes={r['path']:r for r in spec['overlay']}
 if len(changes)!=25:raise ValueError('UniqueOverlayPaths')
 for path,h in spec['inputs'].items():
  safe(path)
  if not re.fullmatch('[0-9a-f]{64}',h):raise ValueError('TypedInputDigest')
  row=changes.get(path)
  if row is None:
   if spec['base_inputs'].get(path)!=h:raise ValueError('UnchangedBaseInput')
  elif row['sha256']!=h or row['preimage_sha256']!=spec['base_inputs'].get(path):raise ValueError('OverlayPreimage')
 if not set(spec['base_inputs'])<=set(spec['inputs'])or not set(changes)<=set(spec['inputs']):raise ValueError('NoDroppedOrExtraInputs')
 for row in changes.values():
  p=HERE/'overlay'/safe(row['path'])
  if not p.is_file()or p.is_symlink()or sha(p.read_bytes())!=row['sha256']:raise ValueError('ExactOverlayPayload')
 return spec

def git_blob(repo,revision,path):
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env['GIT_OPTIONAL_LOCKS']='0'
 child=B.Collected(['git','--no-replace-objects','--git-dir='+str(repo/'.git'),'cat-file','blob',revision+':'+path],env,4*1024*1024,lambda:None);child.wait(10)
 if not child.physically_settled or child.invalid is not None:raise ValueError('SettledBoundedGitBlob')
 return bytes(child.output['stdout'])

def reconstruct(repo,out):
 repo=Path(repo).resolve();out=Path(out).resolve();spec=load()
 if out.exists()or out.is_symlink():raise ValueError('FreshOutputRequired')
 if not(repo/'.git').exists():raise ValueError('ExplicitGitRepository')
 # Validate every exact immutable base blob before creating the output. Each
 # child and output is bounded; data is written only after all base preimages.
 for path,h in spec['base_inputs'].items():
  if sha(git_blob(repo,spec['base_revision'],path))!=h:raise ValueError('ImmutableBaseBlob:'+path)
 out.mkdir(parents=True)
 changes={r['path']:r for r in spec['overlay']}
 for path,h in spec['inputs'].items():
  if path in changes:raw=(HERE/'overlay'/safe(path)).read_bytes()
  else:raw=git_blob(repo,spec['base_revision'],path)
  if sha(raw)!=h:raise ValueError('ExactFinalBlob:'+path)
  target=out/safe(path);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
 actual={p:sha((out/p).read_bytes())for p in spec['inputs']}
 if actual!=spec['inputs']:raise ValueError('FullReconstructedClosure')
 report={'scope':'Exact824Rust+5coupled component source, not Main/app qualification','passed':True,'base_revision':spec['base_revision'],'source_manifest_sha256':INPUTS_SHA,'full_inputs':829,'Rust_inputs':824,'coupled_inputs':5,'private_fallback':False,'build_or_fixture_executed':False}
 (out/'reconstruction.json').write_text(json.dumps(report,indent=2)+'\n');return report
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output)))
