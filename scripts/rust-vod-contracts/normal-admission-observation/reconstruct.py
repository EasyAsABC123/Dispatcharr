"""Restore the exact hash-bound historical69 inputs; never execute a fixture."""
import argparse,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
EXECUTED='2e24bb01b681afb5551fddf9aa5e82617a18ef48c401258964692daf275b5310'
QUALIFIED='dd888e443cd348e60e32f71e714cebb1028f67dda19b7a77dd4f46533441143d'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def reconstruct(output,mode='blocked',source=HERE):
 output=Path(output);source=Path(source)
 if output.exists()or output.is_symlink():raise ValueError('FreshOutputRequired')
 executed=source/'execution-selection.json';qualified=source/'qualified-selection.json'
 if digest(executed)!=EXECUTED or digest(qualified)!=QUALIFIED:raise ValueError('ImmutableSelection')
 rows=json.loads(executed.read_text())['files']
 if len(rows)!=69 or len({r['path']for r in rows})!=69:raise ValueError('Exact69')
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
  if digest(output/row['path'])!=row['sha256']:raise ValueError('ExactRestored69')
 shutil.copyfile(target_selection,output/'selection.json')
 return {'restored_files':69,'mode':mode,'selection_sha256':digest(output/'selection.json'),'application_executed':False,'authorization_granted':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--mode',choices=['blocked','executed-historical'],default='blocked');args=p.parse_args();print(json.dumps(reconstruct(args.output,args.mode),indent=2))
