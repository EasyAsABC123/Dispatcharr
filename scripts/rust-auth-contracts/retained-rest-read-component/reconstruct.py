"""Byteexact historical source reconstruction from local immutable Git; no network."""
from pathlib import Path
import argparse,hashlib,io,json,subprocess,tarfile
ROOT=Path(__file__).resolve().parent
COUPLED=['python-busy-control-vectors.json','python-busy-seek-goldens.json','python-helper-goldens.json','python-range-goldens.json','python-session-goldens.json']
def reconstruct(repo,output):
 manifest=json.loads((ROOT/'manifest.json').read_text());expected=json.loads((ROOT/'source-inputs.json').read_text());output=Path(output);assert not output.exists();data=subprocess.check_output(['git','-C',str(repo),'archive',manifest['base_git'],'rust',*['scripts/rust-timeshift-contracts/'+n for n in COUPLED]]);assert len(data)<=128*1048576;output.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(data))as tar:
  for member in tar.getmembers():assert not Path(member.name).is_absolute()and'..'not in Path(member.name).parts and not member.issym()and not member.islnk()
  tar.extractall(output,filter='data')
 for rel in manifest['overlay']:
  p=output/rel;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes((ROOT/'source-overlay'/rel).read_bytes())
 actual={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted(output.rglob('*'))if p.is_file()};assert actual==expected and len(actual)==852
 return {'passed':True,'inputs':852,'rust':847,'coupled':5,'source_map_sha256':hashlib.sha256((ROOT/'source-inputs.json').read_bytes()).hexdigest(),'Git':manifest['base_git'],'apps_or_builds':0}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',required=True,type=Path);p.add_argument('--output',required=True,type=Path);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
