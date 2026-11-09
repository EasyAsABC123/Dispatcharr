#!/usr/bin/env python3
"""Execute original raw-input byte-reporting method as a clock/counter oracle.

Original Dispatcharr authors retain all application credit. Only time, Redis
increment storage and the ownership predicate are fixture adapters; the method
body is AST-extracted unchanged from the exact original Git archive.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from shared_ts import BASELINE,IMAGE
ORACLE=r'''
import ast,hashlib,json,random,sys,types
source=open('/source/manager.py').read();tree=ast.parse(source)
method=next(node for node in ast.walk(tree) if isinstance(node,ast.FunctionDef) and node.name=='_update_bytes_processed')
clock=types.SimpleNamespace(now=0,time=lambda:clock.now)
namespace={'time':clock,'logger':types.SimpleNamespace(debug=lambda *a,**kw:None,error=lambda *a,**kw:None),
 'RedisKeys':types.SimpleNamespace(channel_metadata=lambda channel:'live:channel:'+channel+':metadata'),
 'ChannelMetadataField':types.SimpleNamespace(TOTAL_BYTES='total_bytes')}
exec(compile(ast.Module(body=[method],type_ignores=[]),'/source/manager.py','exec'),namespace)
class Redis:
 def __init__(self):self.value=None
 def hincrby(self,key,field,value):self.value=(self.value or 0)+value
cases=[]
sequences=[(0.,[(0.,1),(4.9999,31),(5.,64),(9.9999,5),(10.,7)]),
 (1.0009,[(6.0001,1),(6.0009,2),(11.0008,3),(11.0009,4)]),
 (10.,[(8.,10),(9.,20),(14.999999,30),(15.,40),(14.,50),(20.,60)])]
rng=random.Random(734332)
for _ in range(100):
 start=rng.uniform(0,100000);at=start;reads=[]
 for _ in range(32):at+=rng.uniform(-2,4);reads.append((at,rng.randrange(1,65537)))
 sequences.append((start,reads))
for start,reads in sequences:
 redis=Redis();obj=types.SimpleNamespace(_upstream_may_continue=lambda:True,bytes_processed=0,last_bytes_update=start,bytes_update_interval=5,buffer=types.SimpleNamespace(redis_client=redis),channel_id='fixture')
 total=0;events=[]
 for at,size in reads:
  clock.now=at;namespace['_update_bytes_processed'](obj,size);total+=size
  events.append({'at':at,'size':size,'received':total,'reported':redis.value,'pending':obj.bytes_processed,'last':obj.last_bytes_update})
 cases.append({'start':start,'reads':events})
print(json.dumps({'scope':'unchanged original _update_bytes_processed method; successful Redis increments, finite clocks and bounded counters',
 'python':sys.version.split()[0],'source_sha256':hashlib.sha256(source.encode()).hexdigest(),
 'method_sha256':hashlib.sha256(ast.get_source_segment(source,method).encode()).hexdigest(),'cases':cases,'read_count':sum(len(case['reads']) for case in cases)}))
'''

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    original=subprocess.check_output(['git','show',BASELINE+':apps/proxy/live_proxy/input/manager.py'],cwd=ROOT)
    (out/'manager.py').write_bytes(original);(out/'oracle.py').write_text(ORACLE)
    result=subprocess.run([docker,'run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges',
        '-v',str(out)+':/source:ro','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/source/oracle.py'],capture_output=True,text=True,timeout=60,check=True)
    (out/'stderr.log').write_text(result.stderr);data=json.loads(result.stdout);(out/'source-vectors.json').write_text(json.dumps(data,indent=2)+'\n')
    dataset=ROOT/'rust/crates/stream-http/datasets/source-byte-reporting.json';dataset.write_text(json.dumps(data,separators=(',',':'))+'\n')
    shutil.copyfile(ROOT/'rust/crates/stream-http/src/byte_reporting.rs',out/'rust-byte-reporting.rs')
    shutil.copyfile(Path(__file__).resolve(),out/'captured-byte-reporting.py')
    report={'scope':'actual original Python raw-input byte-reporting counter/time vectors','passed':False,'application_performance_verified':False,'milestone_complete':False,
        'baseline_commit':BASELINE,'baseline_image':IMAGE,'source_sha256':hashlib.sha256(original).hexdigest(),'python':data['python'],'method_sha256':data['method_sha256'],
        'sequence_count':len(data['cases']),'read_count':data['read_count'],'dataset_sha256':hashlib.sha256(dataset.read_bytes()).hexdigest()}
    environment=dict(os.environ,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
    test=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(ROOT/'rust/Cargo.toml'),'-p','dispatcharr-stream-http','--lib','byte_reporting::tests','--locked','--offline'],env=environment,capture_output=True,text=True,timeout=120)
    (out/'rust-test.log').write_text(test.stdout+test.stderr);report['passed']=test.returncode==0
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path(__file__).resolve(),ROOT/'rust/crates/stream-http/src/byte_reporting.rs']}
    (out/'evidence.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));return 0 if report['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
