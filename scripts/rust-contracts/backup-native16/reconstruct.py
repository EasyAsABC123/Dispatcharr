"""Read-only Git+eight exact overlay reconstruction; no Cargo, imports of app, Docker or ELF run."""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path
P=Path(__file__).resolve().parent

def materialize(repo,destination):
 destination=destination.resolve();destination.mkdir(parents=True,exist_ok=False)
 base=json.loads((P/'qualified-source/BASE.json').read_text())['Git'];expected=json.loads((P/'qualified-source/full-source-map.json').read_text());selection=json.loads((P/'qualified-source/selected-source.json').read_text())['files'];base_map=json.loads((P/'qualified-source/base-source-map.json').read_text())
 archive=subprocess.run(['git','-C',str(repo),'archive','--format=tar',base],check=True,capture_output=True).stdout
 with tarfile.open(fileobj=io.BytesIO(archive),mode='r:')as t:
  for name,digest in base_map.items():
   member=t.getmember(name);assert member.isfile();body=t.extractfile(member).read();assert hashlib.sha256(body).hexdigest()==digest
   target=destination/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body)
 for row in selection:
  # Published repository layout places the reviewed overlay at its real Rust paths.
  overlay=P.parents[2]/row['path'];body=overlay.read_bytes();assert hashlib.sha256(body).hexdigest()==row['sha256'];target=destination/row['path'];old=hashlib.sha256(target.read_bytes()).hexdigest()if target.exists()else None;assert old==row['before_sha256'];target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(body)
 actual={str(f.relative_to(destination)):hashlib.sha256(f.read_bytes()).hexdigest()for f in destination.rglob('*')if f.is_file()};assert actual==expected and len(actual)==802
 return {'passed':True,'Git':base,'inputs':len(actual),'Rust_inputs':sum(k.startswith('rust/')for k in actual),'coupled_inputs':sum(not k.startswith('rust/')for k in actual),'selected_overlay_paths':len(selection),'source_dictionary_exact':True,'application_builds_or_runs':0}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--destination',type=Path,required=True);a=parser.parse_args();print(json.dumps(materialize(a.repo,a.destination),indent=2))
