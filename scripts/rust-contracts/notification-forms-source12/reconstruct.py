#!/usr/bin/env python3
"""Reconstruct exact executed Source5 inputs from pinned local Original Git; no app."""
import argparse,hashlib,json,shutil,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parent
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE_SHA='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def reconstruct(repository,output):
 output=Path(output).resolve();output.mkdir(parents=True,exist_ok=False)
 reference=ROOT/'executed-executor5';seal=json.loads((reference/'seal.json').read_text());count=0
 for row in seal['files']:
  path=Path(row['path'])
  if path.is_absolute()or'..'in path.parts:raise ValueError('FiniteRelativeInputPath')
  target=output/path;target.parent.mkdir(parents=True,exist_ok=True)
  if row['path']=='inputs/original-source.tar':
   proc=subprocess.run(['git','-C',str(Path(repository).resolve()),'archive','--format=tar',REVISION],capture_output=True,timeout=30,check=True)
   if len(proc.stdout)!=16609280 or sha(proc.stdout)!=ARCHIVE_SHA:raise ValueError('ExactPinnedOriginalArchiveBytes')
   target.write_bytes(proc.stdout)
  else:
   source=reference/path
   if source.is_symlink()or not source.is_file():raise ValueError('ExactRegularExecutedInput')
   shutil.copyfile(source,target)
  raw=target.read_bytes()
  if len(raw)!=row['bytes']or sha(raw)!=row['sha256']:raise ValueError('ExactExecutedInput:'+str(path))
  count+=1
 shutil.copyfile(reference/'seal.json',output/'seal.json')
 return {'passed':True,'exact_inputs':count,'executed_seal_sha256':sha((output/'seal.json').read_bytes()),'Original_Git':REVISION,'archive_sha256':ARCHIVE_SHA,'application_runs':0,'reconstruction_is_input_materialization_not_a_new_execution':True,'executed_controller_exit_unchanged':1}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--original-git',type=Path,required=True);p.add_argument('--output',type=Path,required=True);args=p.parse_args();print(json.dumps(reconstruct(args.original_git,args.output),indent=2))
if __name__=='__main__':main()
