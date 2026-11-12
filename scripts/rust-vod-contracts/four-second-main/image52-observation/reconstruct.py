"""Restore exact blocked25 or historical flag-only25; never execute or provide authorization."""
import argparse,hashlib,json,shutil
from pathlib import Path
HERE=Path(__file__).resolve().parent
SELECTION='d76e662ffb665779192f87c401cc4a64c54cf0e04dbe3b5bc9992178c8b31574'
EXECUTED='922b2ae833441ee61a5a7ce93fb05e4774d8ce9c53ea4a1bbfd188d4ae9ae608'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def reconstruct(output,mode='blocked',source=HERE):
 output=Path(output);source=Path(source)
 if output.exists()or output.is_symlink():raise ValueError('FreshOutput')
 if mode not in ['blocked','executed-historical']:raise ValueError('Mode')
 if sha(source/'blocked-selection.json')!=SELECTION:raise ValueError('ExactBlockedSelection')
 rows=json.loads((source/'blocked-selection.json').read_text())['files']
 if len(rows)!=25 or len({r['path']for r in rows})!=25:raise ValueError('Exact25')
 for r in rows:
  relative=Path(r['path']);p=source/'blocked-inputs'/relative
  if relative.is_absolute()or'..'in relative.parts or any(parent.is_symlink()for parent in [p,*p.parents]if parent!=source.parent)or sha(p)!=r['sha256']:raise ValueError('ExactHistoricalInput')
 executed=source/'executed-launch_once.py'
 if executed.is_symlink()or sha(executed)!=EXECUTED:raise ValueError('ExactFlagActivation')
 blocked=source/'blocked-inputs/launch_once.py'
 if executed.read_bytes().replace(b'\nEXECUTION_BLOCKED=False\n',b'\nEXECUTION_BLOCKED=True\n')!=blocked.read_bytes():raise ValueError('OnlyFlagChanged')
 output.mkdir(parents=True,exist_ok=False)
 for r in rows:
  dst=output/r['path'];dst.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(source/'blocked-inputs'/r['path'],dst)
 shutil.copyfile(source/'blocked-selection.json',output/'selection.json')
 if mode=='executed-historical':shutil.copyfile(executed,output/'launch_once.py')
 for r in rows:
  expected=EXECUTED if mode=='executed-historical'and r['path']=='launch_once.py'else r['sha256']
  if sha(output/r['path'])!=expected:raise ValueError('ExactRestored25')
 return {'restored_files':25,'mode':mode,'application_executed':False,'authorization_granted':False,'historical_cue_not_restored_to_live_path':True}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);p.add_argument('--mode',choices=['blocked','executed-historical'],default='blocked');args=p.parse_args();print(json.dumps(reconstruct(args.output,args.mode),indent=2))
