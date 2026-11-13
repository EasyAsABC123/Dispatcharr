#!/usr/bin/env python3
"""Restore exact PROJECTED proof files locally. Authentication bytes are redacted, NOT replay-qualified; no raw/private fallback."""
import argparse,gzip,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
MAX_PROJECTED=16*1024*1024

def validate_projected_tree(output):
 expected=json.loads((HERE/'projected-proof-map.json').read_text())
 actual={str(p.relative_to(output)):hashlib.sha256(p.read_bytes()).hexdigest()for p in output.rglob('*')if p.is_file()}
 if actual!=expected:raise ValueError('OnlyExactProjectedProofsAccepted_NoRawFallback')

def materialize(output):
 if output.exists():raise ValueError('FreshOutputRequired')
 manifest=json.loads((HERE/'proof-compression.json').read_text());compressed={v['compressed_path']for v in manifest.values()};output.mkdir(parents=True,exist_ok=False)
 for source in(HERE/'proofs').rglob('*'):
  if not source.is_file():continue
  name=str(source.relative_to(HERE/'proofs'))
  if name in compressed:continue
  dest=output/name;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,dest)
 for name,record in manifest.items():
  if Path(name).is_absolute()or'..'in Path(name).parts or not(0<=record['projected_bytes']<=MAX_PROJECTED):raise ValueError('BoundedRelativeProofRequired')
  source=HERE/'proofs'/record['compressed_path']
  if source.stat().st_size!=record['compressed_bytes']or hashlib.sha256(source.read_bytes()).hexdigest()!=record['compressed_sha256']:raise ValueError('CompressedProofChanged')
  dest=output/name;dest.parent.mkdir(parents=True,exist_ok=True);digest=hashlib.sha256();total=0
  with gzip.open(source,'rb')as stream,dest.open('xb')as target:
   while True:
    chunk=stream.read(min(65536,record['projected_bytes']-total+1))
    if not chunk:break
    total+=len(chunk)
    if total>record['projected_bytes']:raise ValueError('ExpandedProjectedProofTooLarge')
    target.write(chunk);digest.update(chunk)
  if total!=record['projected_bytes']or digest.hexdigest()!=record['projected_sha256']:raise ValueError('ProjectedProofChanged')
 validate_projected_tree(output)
 return {'passed':True,'restored_compressed_files':len(manifest),'projected_bytes_verified':sum(v['projected_bytes']for v in manifest.values()),'output':str(output),'scope':'Exact projected proof materialization only; PG authentication bytes are redacted, no lossless authentication replay or raw/private fallback','authentication_bytes_replay_qualified':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--record',type=Path,required=True);a=p.parse_args();result=materialize(a.output.resolve());a.record.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))
if __name__=='__main__':main()
