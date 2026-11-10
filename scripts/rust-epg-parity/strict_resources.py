#!/usr/bin/env python3
"""Isolated additive strict bridge sanitizers/heap bounds; not app performance.
Original Dispatcharr authors retain full application/algorithm credit.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import subprocess
ROOT=Path(__file__).resolve().parents[2]
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--goldens',type=Path,required=True);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--runtime-source-manifest',type=Path);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    tools=output/'tool-source/scripts/rust-epg-parity';tools.mkdir(parents=True)
    tool_hashes={}
    for name in ['strict_resources.py','frozen_runtime.py']:
        data=Path(__file__).with_name(name).read_bytes();(tools/name).write_bytes(data);tool_hashes['scripts/rust-epg-parity/'+name]=hashlib.sha256(data).hexdigest()
    source_root=ROOT;runtime_binding={}
    if args.runtime_source_manifest:
        import importlib.util
        spec=importlib.util.spec_from_file_location('strict_resource_frozen',tools/'frozen_runtime.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        captured,hashes,archive,manifest_sha=module.load_manifest(args.runtime_source_manifest)
        source_root=output/'frozen-source'
        for name,data in captured.items():target=source_root/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
        runtime_binding={'runtime_manifest_sha256':manifest_sha,'runtime_archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest()}
    original=json.loads(args.goldens.read_bytes());prefix=(source_root/'rust/crates/ingest-core/datasets/html4-doctype.xml').read_bytes();assert hashlib.sha256(prefix).hexdigest()==original['html_entity_doctype_sha256']
    candidates=sorted((ROOT/'rust/target/debug/build').glob('dispatcharr-xmltv-parser-*/out/libxml2-2.14.6/.libs/libxml2.a'));assert candidates,'Build the parser package first';library=candidates[-1];vendor=library.parent.parent
    source=source_root/'rust/crates/xmltv-parser/native/strict_probe.c';asan=output/'strict-asan';plain=output/'strict-resource';report={'format_version':1,'scope':'Standalone strict C bridge sanitizer/resource probe only','passed':False,'application_performance_verified':False,'goldens_sha256':hashlib.sha256(args.goldens.read_bytes()).hexdigest(),'source_sha256':{str(path.relative_to(source_root)):hashlib.sha256(path.read_bytes()).hexdigest()for path in [source,source.with_name('shim.c'),source.with_name('strict.inc'),source.with_name('memory.h')]},'libxml_archive_sha256':hashlib.sha256(library.read_bytes()).hexdigest(),'tool_source_sha256':tool_hashes,**runtime_binding,'compiler':subprocess.check_output(['cc','--version'],text=True).splitlines()[0],'limitations':['Shim/driver instrumented; private static libxml2 archive itself is not sanitizer-instrumented.','VmHWM applies only to standalone probe; no application or whole-service resource gain.']}
    env=dict(os.environ,ASAN_OPTIONS='detect_leaks=1:abort_on_error=1',UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1');status=1
    try:
        common=['cc','-std=c11','-g','-O1','-I',str(vendor/'include'),str(source),str(library),'-lm','-lpthread']
        for binary,flags in [(asan,['-fsanitize=address,undefined','-fno-omit-frame-pointer']),(plain,[])]:
            result=subprocess.run(common+flags+['-o',str(binary)],capture_output=True,text=True,check=True);(output/(binary.name+'-build.log')).write_text(result.stdout+'\n'+result.stderr)
        prepared=output/'prepared.xml';failures=[];checked=0
        for case in original['cases']:
            prepared.write_bytes(prefix+base64.b64decode(case['input_base64']));result=subprocess.run([str(asan),str(prepared)],capture_output=True,text=True,timeout=10,env=env)
            try:actual=json.loads(result.stdout)
            except ValueError:actual={}
            expected=0 if case['expected']['kind']=='ok'else 3
            if result.returncode or result.stderr or actual.get('status')!=expected or actual.get('roots')!=(1 if expected==0 else 0):failures.append({'case':case['name'],'exit_code':result.returncode,'stderr':result.stderr,'actual':actual})
            checked+=1
        resources=[]
        for name,body,heap in [('large-valid',b'<programme><title>'+b'x'*(2*1024*1024)+b'</title></programme>',16*1024*1024),('small-heap',b'<programme><title>'+b'x'*(1024*1024)+b'</title></programme>',65536),('node-cap',b'<programme>'+b'<!--x-->'*10000+b'</programme>',16*1024*1024),('entity-doctype-rejected',b'<!DOCTYPE programme [<!ENTITY a "x"><!ENTITY b "&a;&a;&a;&a;">]><programme>&b;</programme>',16*1024*1024)]:
            prepared.write_bytes(prefix+body);result=subprocess.run([str(plain),str(prepared),str(heap)],capture_output=True,text=True,timeout=10,env=env,check=True);value=json.loads(result.stdout);expected={'large-valid':0,'small-heap':2,'node-cap':2,'entity-doctype-rejected':3}[name];resources.append({'case':name,'passed':value['status']==expected and value.get('native_peak_bytes',0)<=heap,**value})
        prepared.unlink(missing_ok=True);report.update(passed=not failures and all(case['passed']for case in resources),sanitizer_cases=checked,sanitizer_failures=failures,resources=resources,binary_sha256={path.name:hashlib.sha256(path.read_bytes()).hexdigest()for path in [asan,plain]});status=0 if report['passed']else 1
    except(subprocess.SubprocessError,OSError,ValueError,AssertionError)as error:report['error_type']=type(error).__name__
    finally:(output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
