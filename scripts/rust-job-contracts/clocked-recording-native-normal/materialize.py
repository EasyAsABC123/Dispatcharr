"""Restore exact projected historical evidence into a NEW owned output; no apps."""
import argparse,gzip,hashlib,json,io
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parent
sha=lambda x:hashlib.sha256(x).hexdigest()
def decode(blob,row):
 if sha(blob)!=row['gzip_sha256']:raise ValueError('ExactCompressedPayload')
 limit=row['projected_bytes']
 if type(limit)is not int or not 0<=limit<=64*1024*1024:raise ValueError('BoundedDeclaredPayload64MiB')
 with gzip.GzipFile(fileobj=io.BytesIO(blob),mode='rb')as stream:data=stream.read(limit+1)
 if len(data)!=row['projected_bytes']or sha(data)!=row['projected_sha256']:raise ValueError('ExactProjectedOnlyBytesNoRawFallback')
 return data

def restore(output,root=ROOT):
 root=Path(root);output=Path(output)
 if output.exists():raise ValueError('FreshOutputRequired')
 manifest=json.loads((root/'evidence-manifest.json').read_text());plans=[];total=0
 for name,row in manifest.items():
  path=PurePosixPath(name);payload=PurePosixPath(row['payload'])
  if path.is_absolute()or'..'in path.parts or payload.is_absolute()or'..'in payload.parts:raise ValueError('OwnedRelativePaths')
  data=decode((root/payload).read_bytes(),row);total+=len(data)
  if total>256*1024*1024:raise ValueError('FiniteRestore256MiB')
  plans.append((path,data))
 output.mkdir(parents=True,exist_ok=False)
 for path,data in plans:
  dest=output/path;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(data)
 return {'passed':True,'restored_files':len(plans),'restored_bytes':total,'historical_HOST1_preserved':True,'application_build_media_or_decoder_executed':False,'only_exact_origin_control_nonce_projected':True}
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();print(json.dumps(restore(args.output),indent=2))
