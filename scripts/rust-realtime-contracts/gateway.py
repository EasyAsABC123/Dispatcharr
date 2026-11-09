#!/usr/bin/env python3
"""Native WebSocket wire proof using original Django/Redis/PostgreSQL controls.

Full application credit remains with original Dispatcharr authors. This tests
an isolated native component binary, not an app-wired image or browser workflow.
Authentication material stays in subprocess memory and is never evidence.
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
from shared_ts import BASELINE,IMAGE,fixture_source,http,wait
from fixture_ports import write_port_overlay
from interop import Peer
from gateway_cases import exercise


class Gateway(Peer):
    def call(self,value):
        self.process.stdin.write(value+'\n');self.process.stdin.flush();return self.read()
    def close(self):
        if self.process.poll() is None:
            try:
                self.closed=self.call('close');self.process.wait(timeout=8)
            except Exception:self.process.kill();self.process.wait(timeout=3)
        self.log.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);parser.add_argument('--browser',action='store_true');parser.add_argument('--source-live-stats',action='store_true')
    args=parser.parse_args();docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    report={'scope':'native WebSocket component against original Django controls','passed':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    paths=[Path(__file__).resolve(),*[Path(__file__).with_name(name) for name in ['gateway_cases.py','gateway_control.py','ws_client.py','interop.py','browser_peer.js']],*[path for path in sorted((ROOT/'rust').rglob('*')) if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}],ROOT/'LICENSE',ROOT/'ATTRIBUTION.md',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in paths:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    source_hashes={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in paths if path.is_relative_to(ROOT/'rust')}
    report['native_source_tree_sha256']=hashlib.sha256(json.dumps(source_hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    report['native_source_scope']='immutable complete Rust tree, LICENSE and ATTRIBUTION; component example, not app image'
    environment=dict(os.environ,PATH='/tmp/dispatcharr-cargo/bin:'+os.environ['PATH'],CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
    build=subprocess.run(['cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-realtime','--example','gateway_peer'],env=environment,capture_output=True,text=True,timeout=180)
    (out/'build.log').write_text(build.stdout+build.stderr)
    if build.returncode:raise RuntimeError('native gateway build failed')
    binary=out/'gateway-peer';shutil.copyfile(ROOT/'rust/target/debug/examples/gateway_peer',binary);binary.chmod(0o700);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
    project='dispatcharr-realtime-gateway-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-realtime-contracts')+':/realtime:ro']}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379],'origin':[8080]})
    peers=[]
    with fixture_source(report,out,'gateway.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='60')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):
            report['commands'].append(values)
            return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180)
            base='http://'+command(compose+['port','baseline','5656']).strip()
            wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'fixture ready')
            environment['REDIS_TEST_URL']='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0'
            environment['DATABASE_TEST_URL']='postgres://dispatch:playback-fixture-only@'+command(compose+['port','baseline','5432']).strip()+'/dispatcharr'
            environment['SECRET_KEY']='playback-fixture-only'
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/realtime/gateway_control.py'],environment,out/'control.log');peers.append(control)
            report['original']=control.call({'op':'source_info'})
            gateway=Gateway([str(binary)],environment,out/'native.log');peers.append(gateway)
            browser=None
            if args.browser:
                node='/home/justin/.local/share/paseo-runtime/node-v22.23.2-linux-x64/bin/node'
                browser=Peer([node,str(copies/'scripts/rust-realtime-contracts/browser_peer.js')],environment,out/'browser.log');peers.append(browser)
                report['browser_runtime']=browser.ready
                report['browser_executable_sha256']=hashlib.sha256(Path('/usr/bin/chromium').read_bytes()).hexdigest()
            second=Gateway([str(binary)],environment,out/'second-native.log');peers.append(second)
            report['wire']=exercise('127.0.0.1',gateway.ready['port'],control.call,browser=browser,
                                    second_port=second.ready['port'],diagnostics=lambda:[peer.call('diagnostics') for peer in [gateway,second]],source_live_stats=args.source_live_stats)
            second.close();report['second_shutdown']=second.closed;assert second.closed['clean']
            if browser:browser.close()
            report['diagnostics_before_shutdown']=gateway.call('diagnostics')
            gateway.close();report['shutdown']=gateway.closed
            assert report['shutdown']['clean']
            assert control.call({'op':'groups'})['members']==0
            report['passed']=True
        except Exception as error:
            report['failure']=type(error).__name__;report['failure_message']=str(error)
            if hasattr(error,'partial_wire_cases'):report['partial_wire_cases']=error.partial_wire_cases
        finally:
            for peer in reversed(peers):peer.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'output':str(out),'cases':report.get('wire',{}).get('case_count')}))
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
