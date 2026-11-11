#!/usr/bin/env python3
"""Release test peer build from exact reconstructed Core8 inputs.
A rebuilt ELF is not silently substituted for the measured32ddd ELF.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import importlib.util
def local(name):
    spec=importlib.util.spec_from_file_location('_dispatcharr_core8_build_'+name,Path(__file__).resolve().with_name(name+'.py'))
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
_r=local('reconstruct');_s=local('schema')
reconstruct=_r.reconstruct;regular_map=_r.regular_map
BASE=_s.BASE;PRODUCTION=_s.PRODUCTION;BENCHMARK=_s.BENCHMARK;ELF=_s.ELF;digest=_s.digest;require=_s.require

def build(repo,output,target):
    output=Path(output).resolve();target=Path(target).resolve()
    require(not output.exists(),'Fresh build output required')
    require(not target.is_relative_to(output) and not output.is_relative_to(target),'Separate announced target and evidence output required')
    output.mkdir(parents=True)
    source=output/'source';reconstruct(repo,source)
    environment=dict(os.environ,CARGO_TARGET_DIR=str(target))
    command=['cargo','test','--manifest-path',str(source/'rust/Cargo.toml'),'--release','--locked','--offline',
             '-p','dispatcharr-stream-http','--lib','--no-run','--message-format=json']
    result=subprocess.run(command,env=environment,capture_output=True,text=True,timeout=1800)
    (output/'compiler.jsonl').write_text(result.stdout);(output/'compiler.log').write_text(result.stderr)
    require(result.returncode==0,'Release compilation failed; logs retained')
    products=[json.loads(line)for line in result.stdout.splitlines()if line.startswith('{')]
    peers=[row for row in products if row.get('reason')=='compiler-artifact' and row.get('executable')
           and row.get('target',{}).get('name')=='dispatcharr_stream_http' and row.get('profile',{}).get('test')is True]
    require(len(peers)==1,'Unique actual optimized test peer required')
    artifact=peers[0];profile=artifact['profile']
    require(profile['opt_level']=='3'and profile['debuginfo']==0 and profile['debug_assertions']is False
            and profile['overflow_checks']is False and profile['test']is True,'Optimized profile mismatch')
    require(digest(regular_map(source))==BENCHMARK,'Source changed during build')
    peer=output/'native-core-peer';shutil.copy2(artifact['executable'],peer);peer.chmod(0o755)
    binary=hashlib.sha256(peer.read_bytes()).hexdigest()
    record={'passed':True,'production_base':BASE,'production_map':PRODUCTION,
            'binary_sha256':binary,'profile':profile,'source_map':regular_map(source),
            'source_map_canonical_sha256':BENCHMARK,
            'compiler_artifact_record_sha256':hashlib.sha256(json.dumps(artifact,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
            'compiler_version':subprocess.check_output(['rustc','-vV'],env=environment,text=True),
            'command':command,'resource_samples_collected':False,'measured_ELF_byte_identity':binary==ELF,
            'production_unmodified_except_benchmark_peer_and_cfgtest_module':True}
    (output/'build-record.json').write_text(json.dumps(record,indent=2)+'\n')
    return record
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',required=True,type=Path);p.add_argument('--output',required=True,type=Path);p.add_argument('--target',required=True,type=Path);a=p.parse_args()
    record=build(a.repo,a.output,a.target);print(json.dumps({k:record[k]for k in ['passed','binary_sha256','source_map_canonical_sha256','measured_ELF_byte_identity']}))
