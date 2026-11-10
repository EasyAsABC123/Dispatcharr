#!/usr/bin/env python3
"""UNEXECUTED original VOD REST oracle runner; no native or performance claim.
All application credit belongs to original Dispatcharr authors/contributors.
Each observation mode receives a fresh owned disposable Source Compose project.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration')]
from shared_ts import BASELINE,IMAGE,fixture_source
from fixture_ports import write_port_overlay


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--mode',choices=['factory','http','both'],default='both')
    args=parser.parse_args()
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    report={'scope':'Original VOD REST metadata-only capture; separate Factory and network fixtures',
        'passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,
        'native_verified':False,'provider_refresh_verified':False,'performance_verified':False,'modes':[]}
    modes=['factory','http']if args.mode=='both'else[args.mode]
    try:
        for mode in modes:
            evidence=out/mode;evidence.mkdir()
            files=[Path(__file__).resolve(),Path(__file__).with_name('source.py'),
                ROOT/'docker/compose.rust-playback.yml',ROOT/'scripts/rust-integration/shared_ts.py',
                ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'scripts/rust-baseline/dependencies.py',
                *[ROOT/'scripts/rust-playback-baseline'/name for name in
                  ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']]]
            tools=evidence/'tools';hashes={}
            for path in files:
                target=tools/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True)
                data=path.read_bytes();target.write_bytes(data);hashes[str(path.relative_to(ROOT))]=hashlib.sha256(data).hexdigest()
            project='dispatcharr-vod-api-'+mode+'-'+uuid.uuid4().hex[:10]
            result={'scope':'Original '+mode+' VOD REST source capture','passed':False,
                'project':project,'tool_sha256':hashes,'observation':mode,'commands':[]}
            with fixture_source(result,evidence,'source-run.json')as temporary:
                source=Path(temporary)/'source';source.mkdir()
                archive=subprocess.check_output(['git','archive',BASELINE],cwd=ROOT)
                result['baseline_archive_sha256']=hashlib.sha256(archive).hexdigest()
                subprocess.run(['tar','-x','--no-same-owner','-C',str(source)],input=archive,check=True)
                (source/'media').mkdir(exist_ok=True)
                environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(evidence),
                    FIXTURE_SECONDS='900',DISPATCHARR_ENV='aio')
                ports=evidence/'ports.yml';result['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
                overlay=evidence/'oracle.yml';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[
                    str(tools/'scripts/rust-vod-api-contracts')+':/vod-api-probe:ro']}}}))
                compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
                def command(argv,timeout=180):
                    result['commands'].append(argv)
                    completed=subprocess.run(argv,cwd=ROOT,env=environment,text=True,capture_output=True,timeout=timeout)
                    if completed.returncode:
                        (evidence/'failed-command.log').write_text(completed.stdout+'\n'+completed.stderr)
                        completed.check_returncode()
                    return completed
                try:
                    command(compose+['up','-d','--no-build','baseline','origin'])
                    # Use a Docker exec health probe: no host public-interface
                    # assumptions or retry of observed API workloads.
                    deadline=time.monotonic()+180
                    while True:
                        ready=subprocess.run(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',
                            "from urllib.request import urlopen;assert urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],
                            cwd=ROOT,env=environment,text=True,capture_output=True,timeout=10)
                        if ready.returncode==0:break
                        if time.monotonic()>=deadline:raise RuntimeError('Original fixture readiness deadline')
                        time.sleep(.2)
                    captured=command(compose+['exec','-T','-e','VOD_API_DISPOSABLE_FIXTURE=playback-fixture-only',
                        '-e','VOD_API_OBSERVATION='+mode,'baseline','/dispatcharrpy/bin/python','/vod-api-probe/source.py'],timeout=300)
                    (evidence/'source-worker.log').write_text(captured.stderr)
                    corpus=json.loads(captured.stdout.splitlines()[-1]);data=json.dumps(corpus,indent=2,ensure_ascii=False)+'\n'
                    (evidence/'source-corpus.json').write_text(data)
                    result.update(passed=corpus['capture_complete'],cases=len(corpus['cases']),
                        corpus_sha256=hashlib.sha256(data.encode()).hexdigest(),provider_trap_attempts=len(corpus['provider_trap_attempts']),
                        provider_refresh_rows_unchanged=corpus['provider_refresh_rows_unchanged'])
                    assert result['passed'],'Incomplete Source metadata capture or unexpected provider refresh'
                except Exception as error:
                    result.update(failure=type(error).__name__,failure_message=str(error));result['passed']=False
                finally:
                    try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],timeout=60);result['cleanup']='owned Compose project removed'
                    except Exception:result['cleanup_error']=True;result['passed']=False
            report['modes'].append({'mode':mode,'passed':result['passed'],'cases':result.get('cases'),
                'evidence':str(evidence/'source-run.json'),'cleanup':result.get('cleanup')})
            if not result['passed']:break
        report['passed']=len(report['modes'])==len(modes)and all(row['passed']for row in report['modes'])
    finally:
        (out/'source-run.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'modes':report['modes'],'output':str(out)}))
    return 0 if report['passed']else 1

if __name__=='__main__':raise SystemExit(main())
