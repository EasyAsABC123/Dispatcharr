"""Read-only exact public packet inventory; Git/Root selection authenticates index."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def validate():
 index=json.loads((ROOT/'packet-index.json').read_bytes());actual={str(p.relative_to(ROOT))for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-index.json'}
 if actual!=set(index['files']):raise ValueError('CompleteExactPublicPacketInventory')
 total=0
 for name,row in index['files'].items():
  p=ROOT/name
  if p.is_symlink()or p.stat().st_size!=row['bytes']or hashlib.sha256(p.read_bytes()).hexdigest()!=row['sha256']:raise ValueError('ExactPacketFile')
  total+=row['bytes']
 return {'passed':True,'files':len(actual),'bytes':total,'apps_Docker_builds':0}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
