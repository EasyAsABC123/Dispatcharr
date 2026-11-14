"""Reconstruct the exact historical 890-input component, using Git only.
No checkout/HEAD selection, build, network, private fallback or ELF copying.
"""
import argparse,hashlib,io,json,subprocess,tarfile
from pathlib import Path,PurePosixPath
HERE=Path(__file__).resolve().parent;BASE='bccc7ff313631152fb269dfbd1eda1437d4cc5e8'
sha=lambda b:hashlib.sha256(b).hexdigest()
def need(v,label):
 if not v:raise ValueError(label)
def maps():
 s=HERE/'source';return [json.loads((s/name).read_text())for name in ['base882.json','qualified890.json','selected16.json']]
def compose(files,base,final,selected):
 need(len(base)==882 and len(final)==890 and len(selected)==16,'FiniteMapCoverage')
 need(set(files)==set(base)and all(sha(files[p])==h for p,h in base.items()),'ExactBase882')
 result=dict(files)
 for path,row in selected.items():
  need((sha(result[path])if path in result else None)==row['before'],'SelectedPreimage:'+path)
  raw=(HERE/'source/selected'/path).read_bytes();need(sha(raw)==row['after'],'SelectedPostimage:'+path);result[path]=raw
 need(set(result)==set(final)and all(sha(result[p])==h for p,h in final.items()),'ExactQualified890')
 return result
def reconstruct(repo,out):
 base,final,selected=maps();need(not out.exists(),'FreshOutputRequired')
 names=list(base);need(all(not PurePosixPath(p).is_absolute()and '..'not in PurePosixPath(p).parts for p in names),'SafePaths')
 rust=subprocess.run(['git','-C',str(repo),'ls-tree','-r','--name-only',BASE,'rust'],check=True,capture_output=True,timeout=30).stdout.decode().splitlines()
 need(set(rust)=={p for p in base if p.startswith('rust/')},'ExactGitRustInputCoverage')
 proc=subprocess.run(['git','-C',str(repo),'archive','--format=tar',BASE,*names],check=True,capture_output=True,timeout=60)
 need(len(proc.stdout)<=32*1024*1024,'ArchiveCapacity');files={}
 with tarfile.open(fileobj=io.BytesIO(proc.stdout),mode='r:')as tf:
  for m in tf:
   if m.isdir():continue
   need(m.isfile()and m.name in base and m.name not in files,'ExactRegularGitInput');files[m.name]=tf.extractfile(m).read()
 result=compose(files,base,final,selected);out.mkdir(parents=True,exist_ok=False)
 for p,raw in result.items():
  dest=out/p;dest.parent.mkdir(parents=True,exist_ok=True);dest.write_bytes(raw)
 return {'reconstructed':True,'base':BASE,'inputs':len(result),'build_or_actual_fixture':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo.resolve(),a.output.resolve())))
