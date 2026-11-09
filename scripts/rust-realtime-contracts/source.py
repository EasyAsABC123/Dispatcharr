#!/usr/bin/env python3
"""Actual original Daphne/Channels WebSocket contracts in isolated Compose.

Full original Dispatcharr authorship remains with upstream. Authentication data
stays inside the disposable Source process; reports never contain JWTs or keys.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from fixture_ports import write_port_overlay
from shared_ts import BASELINE,IMAGE,fixture_source,http,wait


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    report={'scope':'actual original Daphne/Channels handshake, auth, Origin, initial envelopes and per-event visibility','passed':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    project='dispatcharr-realtime-source-'+uuid.uuid4().hex[:12];report['project']=project
    tools=[Path(__file__).resolve(),Path(__file__).with_name('source_inside.py'),Path(__file__).with_name('ws_client.py'),ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ('seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini')],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in tools}
    overlay=out/'source-overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-realtime-contracts')+':/realtime:ro']}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
    with fixture_source(report,out,'source.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='60')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):
            report['commands'].append(values)
            return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180)
            base='http://'+command(compose+['port','baseline','5656']).strip()
            wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'Source fixture ready')
            values=compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/realtime/source_inside.py'];report['commands'].append(values)
            result=subprocess.run(values,cwd=ROOT,env=environment,input='{}',text=True,capture_output=True,timeout=90)
            (out/'inside.log').write_text(result.stderr)
            inside=json.loads(result.stdout);(out/'source-wire.json').write_text(json.dumps(inside,indent=2)+'\n')
            report['inside_exit_code']=result.returncode;report['source_wire']=inside;report['passed']=result.returncode==0 and inside['passed']
        except Exception as error:
            report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'output':str(out),'cases':report.get('source_wire',{}).get('case_count')}));return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
