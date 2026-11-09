#!/usr/bin/env python3
"""Pinned Python msgpack byte oracle for bounded Rust realtime JSON values.

Original Dispatcharr authors retain full application credit. This validates the
wire codec, not WebSocket handshakes, worker delivery or browser parity.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[2]
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def vectors():
    values=[None,True,False,0,1,127,128,255,256,65535,65536,2**32-1,2**32,2**63-1,2**63,2**64-1,
            -1,-32,-33,-128,-129,-32768,-32769,-2**31,-2**31-1,-2**63,0.0,-0.0,1.5,1e-300,1e300,
            '', 'é', '𐀀', 'quote"slash\\newline\n']
    values.extend('a'*length for length in [31,32,255,256,65535,65536])
    values.extend(list(range(length)) for length in [0,15,16,100])
    values.extend({str(number):number for number in range(length)} for length in [0,15,16,100])
    values.extend([{'type':'update','data':{'type':'channel_stats','success':True,'stats':'{"channels":[]}'},
                    '__asgi_channel__':['specific.fixture!one','specific.fixture!two']},
                   {'type':'update','data':{'type':'system_notification','notification':{'id':1,'admin_only':True,'title':'fixture','created_at':'2026-01-01T00:00:00+00:00'}}}])
    rng=random.Random(20261109)
    def value(depth):
        if not depth:return rng.choice([None,True,False,rng.randint(-2**63,2**64-1),rng.random(),'', '東京','fixture'])
        kind=rng.randrange(3)
        if kind==0:return [value(depth-1) for _ in range(rng.randrange(5))]
        if kind==1:return {'key'+str(number):value(depth-1) for number in range(rng.randrange(5))}
        return value(0)
    values.extend(value(5) for _ in range(256))
    return values


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    values=vectors();script="import sys,json,msgpack,platform,hashlib;from pathlib import Path;values=json.load(sys.stdin);print(json.dumps({'python':platform.python_version(),'msgpack':msgpack.__version__,'codec_sha256':hashlib.sha256(Path(msgpack._cmsgpack.__file__).read_bytes()).hexdigest(),'vectors':[{'value':value,'msgpack_hex':msgpack.packb(value).hex()} for value in values]}))"
    command=[docker,'run','--rm','-i','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true',
             '--tmpfs','/tmp:rw,size=16777216','--memory','256m','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'-c',script]
    source=subprocess.run(command,input=json.dumps(values),text=True,capture_output=True,check=True,timeout=60)
    oracle=json.loads(source.stdout);raw=json.dumps(oracle['vectors']).encode();(out/'vectors.json').write_bytes(raw)
    environment=dict(os.environ,PATH='/tmp/dispatcharr-cargo/bin:'+os.environ['PATH'],CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
    rust=subprocess.run(['cargo','run','--manifest-path',str(ROOT/'rust/Cargo.toml'),'--locked','--offline','-p','dispatcharr-realtime','--example','codec_vectors'],input=raw,capture_output=True,env=environment,timeout=180)
    (out/'rust.log').write_bytes(rust.stderr+rust.stdout)
    result=json.loads(rust.stdout) if rust.returncode==0 else {'passed':False}
    report={'scope':'actual pinned Python msgpack to Rust byte-exact JSON codec vectors','passed':rust.returncode==0 and result['passed'],'application_performance_verified':False,'milestone_complete':False,'baseline_image':IMAGE,'source_runtime':{key:oracle[key] for key in ['python','msgpack','codec_sha256']},'vector_count':len(values),'vector_sha256':hashlib.sha256(raw).hexdigest(),'result':result,'tool_sha256':{str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in [Path(__file__).resolve(),ROOT/'rust/crates/realtime/src/codec.rs',ROOT/'rust/crates/realtime/examples/codec_vectors.rs']},'limitations':['JSON-compatible MessagePack values only; binary/extensions/nonfinite floats are deliberately refused','Not a handshake, transport, cached-IP or frontend proof']}
    (out/'evidence.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report));return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
