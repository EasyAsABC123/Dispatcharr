#!/usr/bin/env python3
"""Differential fMP4 framing against unchanged Dispatcharr helper methods."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import random
import struct
import subprocess
import types
ROOT=Path(__file__).resolve().parents[2]
REFERENCE=ROOT/'apps/proxy/live_proxy/output/fmp4/manager.py'

def reference():
    tree=ast.parse(REFERENCE.read_text());helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_find_moof_offset');cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='FMP4RemuxManager');cls.body=[n for n in cls.body if isinstance(n,ast.FunctionDef) and n.name=='_flush_complete_fragments']
    scope={'struct':struct,'MOOF_BOX_TYPE':b'moof','logger':types.SimpleNamespace(debug=lambda *args:None)}
    exec(compile(ast.Module(body=[helper,cls],type_ignores=[]),str(REFERENCE),'exec'),scope);return scope

def box(name,data):return struct.pack('>I',len(data)+8)+name+data

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--fixture',type=Path,action='append',default=[]);parser.add_argument('--binary',type=Path,default=ROOT/'rust/target/debug/examples/framing_parity');args=parser.parse_args()
    ref=reference();rng=random.Random(20261109);cases=[];expected=[]
    def scan(data,start):cases.append({'data':list(data),'scan':start});expected.append(ref['_find_moof_offset'](data,start))
    for i in range(500):
        data=bytes(rng.randrange(256) for _ in range(rng.randrange(0,40)))+box(b'moof',b'fixture');scan(data,rng.randrange(len(data)+1))
    def stream(data,width):
        manager=ref['FMP4RemuxManager']();out=[];index=0
        def put(payload):
            out.append({'fragment':list(payload)});manager.fmp4_buffer.index+=1
        manager.fmp4_buffer=types.SimpleNamespace(put_chunk=put,index=0);manager.channel_id='fixture';init=bytearray();fragments=bytearray();stored=False
        for offset in range(0,len(data),width):
            chunk=data[offset:offset+width]
            if not stored:
                init.extend(chunk);moof=ref['_find_moof_offset'](bytes(init))
                if moof>=0:out.append({'init':list(init[:moof])});fragments.extend(init[moof:]);init.clear();stored=True;manager._flush_complete_fragments(fragments)
            else:fragments.extend(chunk);manager._flush_complete_fragments(fragments)
        if fragments and stored:put(bytes(fragments))
        cases.append({'data':list(data),'width':width});expected.append(out)
    for count in range(20):
        data=box(b'ftyp',b'isom')+box(b'moov',b'codec')+b''.join(box(b'moof',bytes([i])*10)+box(b'mdat',bytes([i])*rng.randrange(1,100)) for i in range(1,8))
        for width in (1,7,64,65536):stream(data,width)
    for path in args.fixture:
        data=path.read_bytes()
        for width in (97,65536):stream(data,width)
    run=subprocess.run([str(args.binary)],input=''.join(json.dumps(c)+'\n' for c in cases),text=True,capture_output=True,check=True);actual=[json.loads(line) for line in run.stdout.splitlines()]
    failures=[i for i,(a,b) in enumerate(zip(actual,expected)) if a!=b]
    if len(actual)!=len(expected):failures.append('response-count')
    report={'scope':'fmp4-framing-component-parity','passed':not failures,'cases':len(cases),'failures':failures,'application_performance_verified':False,'milestone_completed':False,'reference_sha256':hashlib.sha256(REFERENCE.read_bytes()).hexdigest(),'source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__).resolve(),ROOT/'rust/crates/fmp4-core/src/lib.rs',ROOT/'rust/crates/fmp4-core/examples/framing_parity.rs']},'binary_sha256':hashlib.sha256(args.binary.read_bytes()).hexdigest(),'fixtures':[{str(p):hashlib.sha256(p.read_bytes()).hexdigest()}for p in args.fixture]}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));return bool(failures)
if __name__=='__main__':raise SystemExit(main())
