"""Read-only finite packet file/hash inventory; Git/Root delivery authenticates index."""
import hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def validate():
 index=json.loads((ROOT/'packet-index.json').read_bytes());actual={str(p.relative_to(ROOT))for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-index.json'}
 if actual!=set(index['files']):raise ValueError('CompleteExactPublicPacketInventory')
 total=0
 for name,row in index['files'].items():
  p=ROOT/name
  if p.is_symlink()or p.stat().st_size!=row['bytes']:raise ValueError('SafeExactPacketFileSize')
  raw=p.read_bytes();total+=len(raw)
  if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('ExactPacketFileSHA')
 return {'passed':True,'files':len(actual),'bytes':total,'apps_builds_Docker':0}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
