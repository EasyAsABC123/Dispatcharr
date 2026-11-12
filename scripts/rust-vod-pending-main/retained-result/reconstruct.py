"""Reconstruct qualified763 only from immutable Git objects; no private/live fallback."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
BASE='6054f7b52073485cff1d2ca249f8e3bde8bd46b3'
INPUTS_SHA='0d3a7b62ec5c416eb7d98f1175c2358eb68f707f304df2ed216e6828b7c70432'
SELECTION_SHA='b36f52782ffab005c8a81194ba390fa2bfa52307efb8530ebb5c743e2cef5a56'
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();repo=a.repo.resolve();out=a.output.resolve()
 if out.exists()or out==repo or repo/'.git'==out or repo/'.git'in out.parents:raise ValueError('FreshNonGitOutputRequired')
 inputs_raw=(HERE/'inputs.json').read_bytes();selection_raw=(HERE/'selection.json').read_bytes()
 if hashlib.sha256(inputs_raw).hexdigest()!=INPUTS_SHA or hashlib.sha256(selection_raw).hexdigest()!=SELECTION_SHA:raise ValueError('QualifiedImmutableInputsBinding')
 inputs=json.loads(inputs_raw);selection=json.loads(selection_raw);assert selection['base_git_revision']==BASE
 raw=subprocess.check_output(['git','-C',str(repo),'archive',BASE,'rust','scripts']);out.mkdir(parents=True,exist_ok=False)
 with tarfile.open(fileobj=io.BytesIO(raw))as archive:
  for member in archive:
   name=Path(member.name)
   if name.is_absolute()or'..'in name.parts or not(member.isfile()or member.isdir()):raise ValueError('ImmutableArchiveMember')
   target=out/name
   if member.isdir():target.mkdir(parents=True,exist_ok=True)
   else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(member).read());target.chmod(member.mode&0o777)
 for row in selection['selected_files']:
  name=row['path'];assert name in inputs['files'];current=out/name
  if row['base_sha256']is None:
   if current.exists():raise ValueError('UnexpectedBasePath '+name)
  elif hashlib.sha256(current.read_bytes()).hexdigest()!=row['base_sha256']:raise ValueError('BasePathBinding '+name)
  raw=subprocess.check_output(['git','-C',str(repo),'cat-file','blob',row['git_blob_sha1']])
  if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('SelectedGitBlobBinding '+name)
  current.parent.mkdir(parents=True,exist_ok=True);current.write_bytes(raw)
 actual={str(p.relative_to(out)):hashlib.sha256(p.read_bytes()).hexdigest()for p in(out/'rust').rglob('*')if p.is_file()};expected={n:v['sha256']for n,v in inputs['files'].items()}
 if actual!=expected:raise ValueError('Full763SourceClosureMismatch')
 proof={'passed':True,'base_git_revision':BASE,'immutable_Git_only':True,'runtime_files':len(actual),'private_or_mutable_fallback':False,'inputs_sha256':hashlib.sha256((HERE/'inputs.json').read_bytes()).hexdigest(),'selection_sha256':hashlib.sha256((HERE/'selection.json').read_bytes()).hexdigest()};(out/'reconstruction.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof))
if __name__=='__main__':main()
