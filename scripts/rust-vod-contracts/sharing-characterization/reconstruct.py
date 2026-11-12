"""Restore the exact hash-bound historical87 inputs; never execute a fixture."""
import argparse,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
EXECUTED='b19c5fb9f1ab243e47e124dc10fe23816f68336a361a432d686a105c31eac31f'
QUALIFIED='696971279c9463e8c11c79f01c5dff6b0177e47c2ee842681031ee874d1557bf'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def reconstruct(output,mode='blocked',source=HERE):
 output=Path(output);source=Path(source)
 if output.exists()or output.is_symlink():raise ValueError('FreshOutputRequired')
 executed=source/'execution-selection.json';qualified=source/'qualified-selection.json'
 if digest(executed)!=EXECUTED or digest(qualified)!=QUALIFIED:raise ValueError('ImmutableSelection')
 rows=json.loads(executed.read_text())['files']
 if len(rows)!=87 or len({r['path']for r in rows})!=87:raise ValueError('Exact87')
 for row in rows:
  path=Path(row['path']);file=source/'execution-inputs'/path
  if path.is_absolute()or'..'in path.parts or file.is_symlink()or any(p.is_symlink()for p in file.parents if p!=source.parent)or digest(file)!=row['sha256']:raise ValueError('ExactHistoricalInput')
 if mode not in ['blocked','executed-historical']:raise ValueError('RestorationMode')
 output.mkdir(parents=True,exist_ok=False)
 for row in rows:
  dst=output/row['path'];dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source/'execution-inputs'/row['path'],dst)
 target_selection=qualified if mode=='blocked'else executed
 if mode=='blocked':
  run=output/'tools/run.py';text=run.read_text()
  if text.count('EXECUTION_BLOCKED=False')!=1:raise ValueError('ExactActivation')
  run.write_text(text.replace('EXECUTION_BLOCKED=False','EXECUTION_BLOCKED=True'))
 for row in json.loads(target_selection.read_text())['files']:
  if digest(output/row['path'])!=row['sha256']:raise ValueError('ExactRestored87')
 shutil.copyfile(target_selection,output/'selection.json')
 return {'restored_files':87,'mode':mode,'selection_sha256':digest(output/'selection.json'),'application_executed':False,'authorization_granted':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--mode',choices=['blocked','executed-historical'],default='blocked');args=p.parse_args();print(json.dumps(reconstruct(args.output,args.mode),indent=2))
