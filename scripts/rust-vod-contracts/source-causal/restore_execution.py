"""Restore exact reviewed historical execution bytes; NEVER execute them."""
import argparse,json,shutil
from pathlib import Path
import validate
HERE=Path(__file__).resolve().parent

def restore(output):
 root=validate.verify_bundle();output=Path(output)
 if output.exists():raise ValueError('FreshRestorationOutputRequired')
 reference=(HERE/'reference-execution-selection.json').read_bytes();selected=json.loads(reference);output.mkdir(parents=True,exist_ok=False)
 try:
  seen=set()
  for row in selected['files']:
   name=Path(row['path'])
   if name.is_absolute()or'..'in name.parts or str(name)in seen:raise ValueError('SafeUniqueExecutionPath')
   seen.add(str(name));source=root/name;raw=source.read_bytes()
   if str(name)=='controller/run.py':
    if raw.count(b'EXECUTION_BLOCKED=True')!=1:raise ValueError('UniqueOriginalActivationFlag')
    raw=raw.replace(b'EXECUTION_BLOCKED=True',b'EXECUTION_BLOCKED=False')
   import hashlib
   if hashlib.sha256(raw).hexdigest()!=row['sha256']:raise ValueError('ExactExecutionInput '+str(name))
   target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw)
  if len(seen)!=86:raise ValueError('Exact86ExecutionPayloads')
  (output/'selection.json').write_bytes(reference)
  if validate.sha(output/'selection.json')!=validate.EXECUTION:raise ValueError('ExactExecutionSelectionHash')
 except BaseException:
  # Preserve partial restoration for diagnosis; never remove unknown user files.
  raise
 return{'restored':True,'files':len(seen),'selection_sha256':validate.sha(output/'selection.json'),'source_or_Docker_executed':False,'cue_or_claim_copied':False,'requires_new_Root_review_fresh_cue_for_any_future_execution':True}
if __name__=='__main__':
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();print(json.dumps(restore(args.output),indent=2))
