#!/usr/bin/env python3
"""Reconstruct only exact854 Rust plus5 coupled inputs from immutable Git46b91."""
import argparse,hashlib,io,json,os,tarfile
from pathlib import Path
from bounded_process import Collected
HERE=Path(__file__).resolve().parent
MANIFEST_SHA='ac22222a7b88352c10579452f6307e62d98c4a9ad1f7dca43c89afc3e9fd1aad'
REVISION='46b91d70f2282fdadd14a6fc8b7a2110825d1019'
def need(v,label):
 if not v:raise ValueError(label)
def manifest():
 path=HERE/'source-inputs.json';raw=path.read_bytes();need(len(raw)<=1048576 and hashlib.sha256(raw).hexdigest()==MANIFEST_SHA,'ExactImmutableSourceManifest');value=json.loads(raw)
 need(value['revision']==REVISION and value['Rust_inputs']==854 and value['full_inputs']==len(value['full_source_sha256'])==859,'Exact854Plus5')
 return value
def validate_archive(raw,expected):
 need(len(raw)<=33554432,'GitArchiveCapacity');found={}
 with tarfile.open(fileobj=io.BytesIO(raw),mode='r:')as archive:
  for member in archive:
   path=Path(member.name);need(not path.is_absolute()and'..'not in path.parts,'SafeArchivePath')
   if member.isdir():continue
   need(member.isfile()and not member.issym()and not member.islnk()and member.name in expected and member.name not in found,'ExactRegularUniqueGitInput')
   data=archive.extractfile(member).read();need(hashlib.sha256(data).hexdigest()==expected[member.name],'GitBlobHash:'+member.name);found[member.name]=data
 need(set(found)==set(expected),'Complete859GitInputs');return found
def reconstruct(repo,output):
 value=manifest();repo=Path(repo).resolve(strict=True);output=Path(output);need(not output.exists()and not output.is_symlink(),'FreshOutputOnly');gitdir=repo/'.git';need(gitdir.is_dir(),'ExplicitGitRepository')
 environment={k:v for k,v in os.environ.items()if not k.startswith('GIT_')}
 argv=['git','--git-dir='+str(gitdir),'archive',REVISION,'rust']+[p for p in value['full_source_sha256']if not p.startswith('rust/')]
 job=Collected(argv,environment,33554432,lambda:None);job.wait(30);need(job.physically_settled and all(not t.is_alive()for t in job.threads),'ActualGitChildAndPipesSettled')
 files=validate_archive(bytes(job.output['stdout']),value['full_source_sha256']);output.mkdir(parents=False,exist_ok=False)
 for name,data in files.items():
  target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 need(all(hashlib.sha256((output/name).read_bytes()).hexdigest()==want for name,want in value['full_source_sha256'].items()),'Written859Exact')
 return {'reconstructed_inputs':len(files),'Rust_inputs':854,'coupled_inputs':5,'immutable_revision':REVISION,'no_private_or_mutable_fallback':True,'application_or_Cargo_executed':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);a=parser.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
