"""Verify the exact existing local SQLx crate/source subset; no download/build/app."""
import argparse,hashlib,json,tarfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
EXPECTED='ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6'
def verify(path):
 data=path.read_bytes()
 if hashlib.sha256(data).hexdigest()!=EXPECTED:raise ValueError('ExactCargoLockCrateSHAOnly')
 with tarfile.open(path,'r:gz')as tar:
  names=[]
  for p in(ROOT/'upstream').rglob('*'):
   if p.is_file():
    name=str(p.relative_to(ROOT/'upstream'));raw=tar.extractfile('sqlx-core-0.8.6/'+name).read()
    if raw!=p.read_bytes():raise ValueError('ExactUpstreamSourceLicenseBytes')
    names.append(name)
 return {'passed':True,'crate_sha256':EXPECTED,'exact_source_license_members':len(names),'network_build_or_app_runs':0}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--crate',type=Path,required=True);args=parser.parse_args();print(json.dumps(verify(args.crate)))
