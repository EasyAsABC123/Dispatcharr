"""Offline strict projected evidence restore or Git-only current878 reconstruction."""
import argparse,gzip,hashlib,io,json,subprocess,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
H=lambda b:hashlib.sha256(b).hexdigest()
def payload(root,row):
 if type(row['projected_bytes'])is not int or not 0<=row['projected_bytes']<=16*1024*1024 or re.fullmatch(r'blobs/[a-f0-9]{64}\.gz',row['payload'])is None:raise ValueError('BoundedCanonicalPayload')
 blob=(root/row['payload']).read_bytes()
 if H(blob)!=row['gzip_sha256']:raise ValueError('CompressedHash')
 with gzip.GzipFile(fileobj=io.BytesIO(blob))as f:data=f.read(row['projected_bytes']+1)
 if len(data)!=row['projected_bytes']or H(data)!=row['projected_sha256']:raise ValueError('ProjectedOnlyHashAndSize')
 return data
def destination(root,relative):
 p=Path(relative)
 if p.is_absolute()or '..'in p.parts:raise ValueError('OwnedRelativePath')
 return root/p
def fresh(out):
 if out.exists()or out.is_symlink():raise ValueError('FreshOutputRequired')
 out.mkdir(parents=True)
def restore(root,out):
 manifest=json.loads((root/'evidence-manifest.json').read_text());fresh(out)
 for relative,row in manifest.items():
  data=payload(root,row);p=destination(out,relative);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 return {'restored':len(manifest),'projected_only':True,'app_execution':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(restore(ROOT,a.output)))
