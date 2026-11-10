#!/usr/bin/env python3
"""Capture pinned original strict XML helper and optionally compare frozen Rust.
Original Dispatcharr authors retain all application and algorithm credit.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile
ROOT=Path(__file__).resolve().parents[2]
BASELINE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--native',action='store_true');parser.add_argument('--regression-goldens',type=Path);args=parser.parse_args();output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    tools=output/'tool-source';tool_map={}
    for relative in ['scripts/rust-epg-parity/strict_element_source.py','scripts/rust-epg-parity/run_strict_element.py','scripts/rust-epg-parity/parser.py']:
        target=tools/relative;target.parent.mkdir(parents=True,exist_ok=True);data=(ROOT/relative).read_bytes();target.write_bytes(data);tool_map[relative]=hashlib.sha256(data).hexdigest()
    source=output/'source/apps/epg';source.mkdir(parents=True);original=subprocess.check_output(['git','show',BASELINE+':apps/epg/tasks.py'],cwd=ROOT);(source/'tasks.py').write_bytes(original)
    report={'format_version':1,'scope':'Strict original XML single-element helper only','passed':False,'native_verified':False,'app_wired':False,'performance_verified':False,'baseline':BASELINE,'baseline_image':IMAGE,'original_source_sha256':hashlib.sha256(original).hexdigest(),'tool_source_sha256':tool_map}
    if args.native:
        captured={str(path.relative_to(ROOT)):path.read_bytes() for path in (ROOT/'rust').rglob('*') if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}}
        with tarfile.open(output/'rust-source.tar','w')as archive:
            for name,data in sorted(captured.items()):item=tarfile.TarInfo(name);item.size=len(data);item.mode=0o644;item.mtime=0;archive.addfile(item,io.BytesIO(data))
        report['rust_source_sha256']={name:hashlib.sha256(data).hexdigest()for name,data in captured.items()};report['rust_source_archive_sha256']=hashlib.sha256((output/'rust-source.tar').read_bytes()).hexdigest()
    status=1
    try:
        actual=json.loads(subprocess.check_output(['docker','image','inspect',IMAGE],timeout=30))[0];report['original_image_id']=actual['Id']
        command=['docker','run','--rm','--network','none','--read-only','--cap-drop','ALL','--security-opt','no-new-privileges:true','--tmpfs','/tmp:mode=1777,size=67108864','-e','PYTHONDONTWRITEBYTECODE=1','-v',str(output/'source')+':/source:ro','-v',str(tools/'scripts/rust-epg-parity')+':/strict-tools:ro','-v',str(output)+':/evidence','--entrypoint','/dispatcharrpy/bin/python',IMAGE,'/strict-tools/strict_element_source.py','--output','/evidence/goldens.json']
        result=subprocess.run(command,text=True,capture_output=True,timeout=120);(output/'source.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode();goldens=json.loads((output/'goldens.json').read_bytes())
        if goldens['source_sha256']!=report['original_source_sha256']or not goldens['controls']['helper_no_network_observed']:raise ValueError('Source version/network control differs')
        report.update(source_cases=len(goldens['cases']),goldens_sha256=hashlib.sha256((output/'goldens.json').read_bytes()).hexdigest(),source_controls=goldens['controls'])
        if args.native:
            frozen=output/'frozen-source'
            for name,data in captured.items():target=frozen/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
            env=dict(os.environ,CARGO_TARGET_DIR=str(output/'cargo-target'));result=subprocess.run(['cargo','run','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-xmltv-parser','--example','strict_element_parity','--locked','--offline'],input=''.join(json.dumps(case)+'\n'for case in goldens['cases']),text=True,capture_output=True,timeout=600,env=env);(output/'native.log').write_text(result.stdout+'\n'+result.stderr);result.check_returncode();responses=[json.loads(line)for line in result.stdout.splitlines()if line];assert len(responses)==len(goldens['cases']);failures=[{'case':case['name'],'expected':case['expected'],'actual':actual}for case,actual in zip(goldens['cases'],responses)if case['expected']!=actual];report.update(native_verified=not failures,native_cases=len(responses),failures=failures)
            if failures:raise ValueError('Strict native helper differs')
            if args.regression_goldens:
                build=subprocess.run(['cargo','build','--manifest-path',str(frozen/'rust/Cargo.toml'),'-p','dispatcharr-xmltv-parser','--example','xmltv_stream_parity','--locked','--offline'],text=True,capture_output=True,timeout=600,env=env);(output/'recover-build.log').write_text(build.stdout+'\n'+build.stderr);build.check_returncode()
                regression=subprocess.run([os.sys.executable,str(tools/'scripts/rust-epg-parity/parser.py'),'--goldens',str(args.regression_goldens.resolve()),'--binary',str(output/'cargo-target/debug/examples/xmltv_stream_parity'),'--output',str(output/'recover-regression.json')],text=True,capture_output=True,timeout=120);(output/'recover-regression.log').write_text(regression.stdout+'\n'+regression.stderr);regression.check_returncode();report['recover_regression']=json.loads((output/'recover-regression.json').read_bytes())
                if not report['recover_regression']['passed']:raise ValueError('Existing recover parser regression differs')
        report['passed']=True;status=0
    except(subprocess.SubprocessError,OSError,ValueError,AssertionError)as error:report['error_type']=type(error).__name__
    finally:(output/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'native_verified':report['native_verified'],'evidence':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
