"""Provisional immutable Git+selected-source reconstructor. No private fallback."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path,PurePosixPath
HERE=Path(__file__).resolve().parent

def digest(raw):return hashlib.sha256(raw).hexdigest()
def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();repo=args.repo.resolve();out=args.output.resolve();selection=json.loads((HERE/'selection.json').read_text())
 if out.exists():raise ValueError('Fresh output required')
 revision=subprocess.check_output(['git','-C',str(repo),'rev-parse',selection['base_git_revision']+'^{commit}'],text=True).strip()
 if revision!=selection['base_git_revision']:raise ValueError('Base commit binding')
 raw=subprocess.check_output(['git','-C',str(repo),'archive',revision,'rust','scripts','docker'])
 out.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as archive:
  for item in archive:
   path=PurePosixPath(item.name)
   if path.is_absolute()or'..'in path.parts or not(item.isfile()or item.isdir()):raise ValueError('Unsafe Git archive member')
   target=out/str(path)
   if item.isdir():target.mkdir(parents=True,exist_ok=True)
   else:target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(archive.extractfile(item).read());target.chmod(item.mode&0o777)
 for item in selection['selected']:
  name=PurePosixPath(item['path'])
  if name.is_absolute()or'..'in name.parts:raise ValueError('Unsafe selected path')
  target=out/str(name);expected=item['base_sha256']
  if (digest(target.read_bytes())if target.exists()else None)!=expected:raise ValueError('Selected base overlap mismatch '+str(name))
  source=HERE/'selected'/str(name);data=source.read_bytes()
  if digest(data)!=item['sha256']:raise ValueError('Selected input hash mismatch '+str(name))
  target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
 proof={'scope':selection['status'],'base_git_revision':revision,'base_archive_sha256':digest(raw),'selection_sha256':digest((HERE/'selection.json').read_bytes()),'rust_inputs':{str(f.relative_to(out)):digest(f.read_bytes())for f in sorted((out/'rust').rglob('*'))if f.is_file()},'no_execution_or_acceptance':True}
 (out/'reconstruction.json').write_text(json.dumps(proof,indent=2)+'\n')
 print(json.dumps({'reconstructed':True,'rust_inputs':len(proof['rust_inputs']),'no_execution_or_acceptance':True}))
if __name__=='__main__':main()
