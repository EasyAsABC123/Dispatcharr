"""Restore BLOCKED NEW-credential catalogue inputs only. Never run apps or reuse an original cue."""
import argparse,hashlib,json,shutil
from pathlib import Path
import verify as V
import reconstruct as R
HERE=Path(__file__).resolve().parent
def restore(kind,output):
 R.selected();V.need(kind in ['catalogue26','backend-observer'],'FiniteNewBinding');out=R.fresh(output);root=HERE/'tools'/kind;meta=V.read(root/'original-inputs.json');original=V.read(HERE/('evidence/Main/'+kind+'/controller/selection.json'));rows=original['files'];V.need(len(rows)in [29,30],'OriginalFiniteDefinition');out.mkdir(parents=True)
 for row in rows:
  n=row['path'];dest=out/n;dest.parent.mkdir(parents=True,exist_ok=True)
  if n=='definition.py':src=root/'definition.new-input-binding.py'
  elif n in meta['new_credential_bindings']['files']:src=root/meta['new_credential_bindings']['files'][n]['new_path']
  elif n=='reference-inputs.json':src=root/'reference-inputs.new-input-binding.json'
  elif n.endswith('.py'):src=root/n
  else:src=HERE/'evidence/Main'/kind/'controller'/n
  V.need(src.is_file(),'DeclaredMountedPayload:'+n);shutil.copyfile(src,dest)
 V.need('\nEXECUTION_BLOCKED=True\n'in(out/'run.py').read_text(),'StillBlocked')
 new={'scope':'NEW unexecuted public credential binding; original results and original seal unchanged','files':[{'path':r['path'],'bytes':(out/r['path']).stat().st_size,'sha256':V.sha(out/r['path'])}for r in rows]};(out/'selection.json').write_text(json.dumps(new,indent=2)+'\n')
 return {'input_files':len(rows),'new_input_selection_sha256':V.sha(out/'selection.json'),'original_selection_sha256':V.sha(HERE/('evidence/Main/'+kind+'/controller/selection.json')),'execution_blocked':True,'authorization_granted':False,'app_executed':False,'original_input_or_result_relabelled':False,'required_before_any_future_run':'New output/namespace, fresh policy/seal, full preflight and separate explicit authorization; no old cue/claim supplied'}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--kind',choices=['catalogue26','backend-observer'],required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args();print(json.dumps(restore(a.kind,a.output),indent=2))
