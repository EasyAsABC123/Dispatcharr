#!/usr/bin/env python3
"""Bind explicit local rebuild/tooling paths; emit unauthorized inputs, never apps.

No historical/private source or executable fallback. Fixture authorization and
global slot review are separate from producing these concrete reviewable inputs.
"""
import argparse,hashlib,json,os
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SAFE=('PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN')
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def prepare(bundle,mode,source,build_record,elf,tooling_script,docker,compose,image_receipt,output,destination,root=ROOT):
 if bundle not in('physical','refusal','correction'):raise ValueError('Finite controller bundle')
 base=root/'controllers'/bundle;template=json.loads((base/'historical-proposed-execution-inputs.json').read_text());caseplan=json.loads((base/'case-plan.json').read_text())
 if mode not in caseplan['modes']or(bundle=='correction'and mode!='duplicate-casefold-movie'):raise ValueError('Finite declared mode')
 destination=Path(destination).resolve();output=Path(output).resolve();source=Path(source).resolve()
 if destination.exists()or output.exists():raise FileExistsError('Fresh inputs/output required')
 if source==output or source in output.parents or output in source.parents:raise ValueError('Separate source and evidence')
 mapping=root.parent/'source-inputs.json';binding=json.loads((root/'source-binding.json').read_text());expected=json.loads(mapping.read_text())
 if sha(mapping)!=binding['full_source_map_sha256']or len(expected)!=839:raise ValueError('Exact historical source map839')
 actual={p.relative_to(source).as_posix():sha(p)for p in source.rglob('*')if p.is_file()}
 if actual!=expected:raise ValueError('Every reconstructed source byte')
 build_record=Path(build_record).resolve();elf=Path(elf).resolve();record=json.loads(build_record.read_text())
 if not(record['all_waits_zero']is True and record['source_map_sha256']==sha(mapping)and all(c['settled']is True and type(c['exit'])is int and c['exit']==0 for c in record['commands'])):raise ValueError('Actual same-source build waits0')
 if elf.open('rb').read(4)!=b'\x7fELF'or record['outside_ELFs'][str(elf)]['sha256']!=sha(elf):raise ValueError('Explicit outside ELF bound to actual build')
 toolmap=json.loads((base/'executed-tool-map.json').read_text())
 if any(sha(base/'tools'/name)!=value for name,value in toolmap.items()):raise ValueError('Byte-exact historical controller tools')
 paths={k:Path(v).resolve()for k,v in [('tooling_environment_script',tooling_script),('compose_executable',compose),('Original_image_inspect_receipt',image_receipt)]}
 image=json.loads(paths['Original_image_inspect_receipt'].read_text())
 if not(image['actual_exit']==0 and image['process_settled']is True and image['image']['Id']==template['Original_image_id']):raise ValueError('Actual pinned Original image inspection')
 safe={k:os.environ.get(k)for k in SAFE}
 if not all(type(value)is str and value for value in safe.values()):raise ValueError('Source explicit six tooling exports first')
 if not safe['DOCKER_HOST'].startswith('unix://'):raise ValueError('Explicit owned local Docker daemon')
 template.update(authorized=False,status='PUBLIC_EXPLICIT_LOCAL_BINDINGS_UNAUTHORIZED',output=str(output),physical_mode=mode,Native_source_root=str(source),Native_source_map={'path':str(mapping.resolve()),'sha256':sha(mapping)},Native_build_record={'path':str(build_record),'sha256':sha(build_record)},Native_elf={'path':str(elf),'sha256':sha(elf)},tools_sha256=toolmap,case_plan_sha256=sha(base/'case-plan.json'),pinned_source_proof_sha256=sha(base/'pinned-source-proof.json'),Docker_host=safe['DOCKER_HOST'],docker_executable_sha256=sha(docker),tooling_environment=safe)
 for key,path in paths.items():template[key]={'path':str(path),'sha256':sha(path)}
 # Old keys are explicit historical aliases used only as provenance; rebind
 # them too so a generated receipt contains no hidden private-path fallback.
 template['Original_image_receipt_path']=str(paths['Original_image_inspect_receipt']);template['Original_image_receipt_sha256']=sha(paths['Original_image_inspect_receipt'])
 destination.parent.mkdir(parents=True,exist_ok=True);destination.write_text(json.dumps(template,indent=2)+'\n')
 return {'passed':True,'inputs':str(destination),'inputs_sha256':sha(destination),'authorized':False,'controller':str(base/'tools/run_source.py'),'support_required':'Reconstruct exact6148-file f0 via committed image54-checkpoint/reconstruct.py; supply --support-root explicitly','new_app_execution':False,'historical_binary_identity_claim':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--bundle',required=True,choices=('physical','refusal','correction'));p.add_argument('--mode',required=True)
 for key in('source','build-record','elf','tooling-script','docker','compose','image-receipt','output','inputs'):p.add_argument('--'+key,type=Path,required=True)
 a=p.parse_args();print(json.dumps(prepare(a.bundle,a.mode,a.source,a.build_record,a.elf,a.tooling_script,a.docker,a.compose,a.image_receipt,a.output,a.inputs)))
