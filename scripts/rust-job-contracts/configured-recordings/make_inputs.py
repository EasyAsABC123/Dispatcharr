#!/usr/bin/env python3
"""Prepare review-only, unauthorized portable Normal7/Known9 inputs.
Requires explicit verified support, tooling and outside-built historical peers.
No Docker query or fixture dispatch is performed; private artifact fallback is absent.
"""
import argparse,hashlib,json,os,shutil
from pathlib import Path
from reconstruct import validate_tree
HERE=Path(__file__).resolve().parent
SAFE=('PATH','DOCKER_HOST','DOCKER_CONFIG','CARGO_HOME','RUSTUP_HOME','RUSTUP_TOOLCHAIN')
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--kind',choices=['normal7','known9'],required=True);p.add_argument('--source',type=Path,required=True);p.add_argument('--build-record',type=Path,required=True);p.add_argument('--image-inspect-receipt',type=Path,required=True);p.add_argument('--environment-script',type=Path,required=True);p.add_argument('--compose-executable',type=Path,required=True);p.add_argument('--support-root',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--inputs',type=Path,required=True);a=p.parse_args();variant='qa9'if a.kind=='normal7'else'source10';folder=HERE/'fixtures'/a.kind;template=json.loads((folder/'proposed-execution-inputs.json').read_text());expected_map=HERE/(variant+'-source-inputs.json');validate_tree(a.source.resolve(),json.loads(expected_map.read_text()))
 if a.output.exists()or a.inputs.exists():raise ValueError('FreshOutputAndInputsRequired')
 for name,h in json.loads((folder/'tools/support-inputs.json').read_text()).items():
  if sha(a.support_root/name)!=h:raise ValueError('ExactExplicitSupport:'+name)
 built=json.loads(a.build_record.read_text());peer=built['binary'];source_map=sha(expected_map)
 if built.get('copied_after_all_waits_zero')is not True or built['source_map_sha256']!=source_map or peer['name']!='job-library' or sha(peer['path'])!=peer['sha256']or Path(peer['path']).open('rb').read(4)!=b'\x7fELF':raise ValueError('BoundHistoricalOutsidePeerRequired')
 docker=shutil.which('docker')
 if not docker or not all(os.environ.get(k)for k in SAFE):raise ValueError('ExplicitToolingEnvironmentRequired')
 value={**template,'status':'PORTABLE REVIEW-ONLY; actual execution requires separate reviewed owned-fixture authorization','authorized':False,'output':str(a.output.resolve()),'case_plan_sha256':sha(folder/'case-plan.json'),'tools_sha256':{name:sha(folder/'tools'/name)for name in template['tools_sha256']},'Native_source_map_sha256':source_map,'Native_elf':{'path':peer['path'],'sha256':peer['sha256']},'Native_build_record':{'path':str(a.build_record.resolve()),'sha256':sha(a.build_record)},'Docker_host':os.environ['DOCKER_HOST'],'docker_executable_sha256':sha(docker),'tooling_environment':{k:os.environ[k]for k in SAFE},'tooling_environment_script':{'path':str(a.environment_script.resolve()),'sha256':sha(a.environment_script)},'compose_executable':{'path':str(a.compose_executable.resolve()),'sha256':sha(a.compose_executable)},'Original_image_inspect_receipt':{'path':str(a.image_inspect_receipt.resolve()),'sha256':sha(a.image_inspect_receipt)},'Original_image_receipt_path':str(a.image_inspect_receipt.resolve()),'Original_image_receipt_sha256':sha(a.image_inspect_receipt)}
 a.inputs.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({'authorized':False,'inputs':str(a.inputs.resolve()),'sha256':sha(a.inputs),'command':['python',str(folder/'tools/run_pair.py'),'--repo','<explicit repository>','--support-root',str(a.support_root.resolve()),'--output',str(a.output.resolve()),'--execution-inputs',str(a.inputs.resolve())]}))
if __name__=='__main__':main()
