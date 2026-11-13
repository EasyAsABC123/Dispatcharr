"""Exact Git-only QA15 build-source reconstruction; not current runtime rollout."""
import argparse,hashlib,json,subprocess
from pathlib import Path,PurePosixPath
ROOT=Path(__file__).resolve().parent;BASE='7e62363e06a3119c45436db7d2b01887317d279c'
sha=lambda x:hashlib.sha256(x).hexdigest()
def reconstruct(repo,output):
 output=Path(output)
 if output.exists():raise ValueError('FreshSourceOutputRequired')
 mapping=json.loads((ROOT/'historical-QA15-source-inputs.json').read_text());overlays=json.loads((ROOT/'historical-QA15-selection.json').read_text())['new_or_changed'];plans=[]
 if len(mapping)!=866 or len(overlays)!=23:raise ValueError('ExactHistorical866And23')
 for name,digest in mapping.items():
  path=PurePosixPath(name)
  if path.is_absolute()or'..'in path.parts:raise ValueError('OwnedSourcePath')
  if name in overlays:data=(ROOT/'historical-QA15-overlays'/name).read_bytes()
  else:data=subprocess.run(['git','-C',str(repo),'show',BASE+':'+name],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout
  if sha(data)!=digest:raise ValueError('ExactHistoricalSource:'+name)
  plans.append((path,data))
 output.mkdir(parents=True,exist_ok=False)
 for name,data in plans:
  path=output/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
 return {'passed':True,'files':866,'Git_base':BASE,'private_fallback':False,'build_or_app_executed':False,'current_runtime_or_physical_qualification_transferred':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--repo',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(reconstruct(a.repo,a.output),indent=2))
