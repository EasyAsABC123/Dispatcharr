"""Offline strict projected evidence restore or Git-only current878 reconstruction."""
import argparse,gzip,hashlib,io,json,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
H=lambda b:hashlib.sha256(b).hexdigest()
def payload(root,row):
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
def source(root,repo,out):
 authority=root/'current-source-selection';base=json.loads((authority/'promotion.json').read_text())['canonical_base'];mapping=json.loads((root/'current-build/source-inputs.json').read_text());selected=json.loads((authority/'selection.json').read_text());fresh(out)
 for n,expected in mapping.items():
  if n in selected:data=(authority/'selected'/n).read_bytes()
  else:data=subprocess.check_output(['git','-C',str(repo),'show',base+':'+n],stderr=subprocess.PIPE)
  if H(data)!=expected:raise ValueError('ExactCurrentSourceHash:'+n)
  p=destination(out,n);p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
 return {'inputs':len(mapping),'base':base,'build_execution':False,'private_fallback':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--kind',choices=['evidence','source'],required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--repo',type=Path);a=p.parse_args();print(json.dumps(restore(ROOT,a.output)if a.kind=='evidence'else source(ROOT,a.repo,a.output)))
