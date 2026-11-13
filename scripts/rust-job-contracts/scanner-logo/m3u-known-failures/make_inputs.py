#!/usr/bin/env python3
"""Bind an exact fresh build and public tool directory to disposable local rerun.
Performs only a read-only image identity query; starts no containers.
"""
import argparse,hashlib,json,os,shutil,subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
NAMES={'run_pair.py','source_pair.py','periodic_worker_observer.py','support-inputs.json','acceptance.py','pg_preflight_gate.py','reference.json','qa-policy.json','ledger_io.py','owned_pg_gate_proxy.py','compare_full.py','owned_redis_reply_proxy.py','fixture_runtime.py','source_physical.py'}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def inputs(build,tools,image,docker,host):
 expected=sha(HERE/'source-inputs.json')
 if build.get('passed')is not True or build.get('source_map_sha256')!=expected or not all(v.get('actual_exit')==0 and v.get('process_settled')is True for v in build.get('phases',[]))or len(build.get('phases',[]))!=2:raise ValueError('ExactSuccessfulHistoricalBuild')
 peers={v['name']:v for v in build['outside_peers']}
 if set(peers)!={'job-library','coordination-residual'}:raise ValueError('ExactTwoOutsideHarnesses')
 for row in peers.values():
  p=Path(row['path'])
  if p.read_bytes()[:4]!=b'\x7fELF'or sha(p)!=row['sha256']:raise ValueError('OutsideELFChanged')
 names=NAMES|({'acl_probes.py'}if(tools/'acl_probes.py').exists()else set());policy=json.loads((tools/'qa-policy.json').read_text())
 if policy['source_map_sha256']!=expected:raise ValueError('ToolRuntimeBinding')
 return{'authorized':True,'scope':'Explicit disposable local functional rerun; noMain/defaultgraph/performance','source_map_sha256':expected,'native_binary_sha256':peers['job-library']['sha256'],'coord_binary_sha256':peers['coordination-residual']['sha256'],'reference_sha256':sha(tools/'reference.json'),'tools_sha256':{name:sha(tools/name)for name in names},'Docker_host':host,'docker_executable_sha256':sha(docker),'Original_image_id':image}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--build-record',type=Path,required=True);p.add_argument('--tools',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--disposable-fixture',choices=['playback-fixture-only'],required=True);a=p.parse_args()
 if a.output.exists():raise ValueError('FreshInputRecordRequired')
 docker=shutil.which('docker')
 if not docker or not os.environ.get('DOCKER_HOST','').startswith('unix:///'):raise ValueError('ExplicitLocalDockerTooling')
 argv=[docker,'image','inspect','--format','{{json .Id}}',IMAGE];value=subprocess.run(argv,capture_output=True,text=True,timeout=30,check=True);image=json.loads(value.stdout)
 result=inputs(json.loads(a.build_record.read_text()),a.tools.resolve(),image,Path(docker),os.environ['DOCKER_HOST']);result['build_record_sha256']=sha(a.build_record);result['read_only_image_query']={'argv':argv,'actual_exit':value.returncode,'stdout':value.stdout,'stderr':value.stderr};a.output.write_text(json.dumps(result,indent=2)+'\n')
if __name__=='__main__':main()
