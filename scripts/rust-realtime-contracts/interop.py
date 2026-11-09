#!/usr/bin/env python3
"""Real original Python↔Rust ChannelsRedis protocol and two-process fanout.

Original Dispatcharr authors retain full application credit. No fake transport,
local-only broadcast, WebSocket route or resource-gain claim is used here.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import time
import uuid

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from shared_ts import BASELINE,IMAGE,fixture_source,http,wait
from fixture_ports import write_port_overlay


class Peer:
    def __init__(self,command,environment,log):
        self.log=log.open('w');self.process=subprocess.Popen(command,env=environment,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=self.log,text=True,bufsize=1)
        self.ready=self.read()
        assert self.ready['ready']
    def read(self):
        selector=selectors.DefaultSelector();selector.register(self.process.stdout,selectors.EVENT_READ)
        try:
            if not selector.select(10):raise TimeoutError('protocol peer response deadline')
            line=self.process.stdout.readline()
            if not line:raise RuntimeError('protocol peer exited')
            if len(line)>2*1024*1024:raise ValueError('peer reply exceeds2MiB')
            return json.loads(line)
        finally:selector.close()
    def call(self,value):
        self.process.stdin.write(json.dumps(value)+'\n');self.process.stdin.flush();return self.read()
    def close(self):
        if self.process.poll() is None:
            try:self.call({'op':'close'});self.process.wait(timeout=5)
            except Exception:self.process.kill();self.process.wait(timeout=3)
        self.log.close()


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    report={'scope':'real original Python and two Rust process ChannelsRedis interoperability','passed':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'checks':[],'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('python_peer.py'),ROOT/'rust/crates/realtime/src/bus.rs',ROOT/'rust/crates/realtime/src/codec.rs',ROOT/'rust/crates/realtime/examples/bus_peer.rs',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in tools}
    environment=dict(os.environ,PATH='/tmp/dispatcharr-cargo/bin:'+os.environ['PATH'],CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
    build=subprocess.run(['cargo','build','--manifest-path',str(ROOT/'rust/Cargo.toml'),'--locked','--offline','-p','dispatcharr-realtime','--example','bus_peer'],env=environment,capture_output=True,text=True,timeout=180)
    (out/'build.log').write_text(build.stdout+build.stderr)
    if build.returncode:raise RuntimeError('Rust protocol peer build failed')
    binary=ROOT/'rust/target/debug/examples/bus_peer';report['rust_peer_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
    project='dispatcharr-realtime-interop-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-realtime-contracts')+':/realtime:ro']}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379],'origin':[8080]})
    peers=[]
    with fixture_source(report,out,'interop.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='60')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def size():return int(command(compose+['exec','-T','baseline','redis-cli','ZCARD','asgi:group:updates']))
        def expected(peer,event):
            deadline=time.monotonic()+4
            while time.monotonic()<deadline:
                reply=peer.call({'op':'receive','timeout_ms':max(1,int((deadline-time.monotonic())*1000))})
                report['last_reply']=reply
                if reply.get('message')==event:return
                if reply.get('timeout'):raise TimeoutError('correlated probe event not received')
                message=reply['message'];data=message.get('data') or {}
                if isinstance(data,dict) and data.get('fixture_probe'):
                    raise AssertionError('probe envelope differs or arrived out of order')
                report.setdefault('ambient_messages',[]).append({'type':message.get('type'),'data_type':data.get('type') if isinstance(data,dict) else None})
            raise TimeoutError('correlated probe event deadline')
        def absent(peer):
            deadline=time.monotonic()+.15
            while time.monotonic()<deadline:
                reply=peer.call({'op':'receive','timeout_ms':max(1,int((deadline-time.monotonic())*1000))})
                if reply.get('timeout'):return
                data=reply['message'].get('data') or {}
                assert not isinstance(data,dict) or not data.get('fixture_probe'), 'non-target peer received correlated event'
                report.setdefault('ambient_messages',[]).append({'type':reply['message'].get('type'),'data_type':data.get('type') if isinstance(data,dict) else None})

        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip()
            wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'fixture ready')
            address=command(compose+['port','baseline','6379']).strip();environment['REDIS_TEST_URL']='redis://:playback-fixture-only@'+address+'/0'
            first=Peer([str(binary)],environment,out/'first.log');peers.append(first)
            second=Peer([str(binary)],environment,out/'second.log');peers.append(second)
            python=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/realtime/python_peer.py'],environment,out/'python.log');peers.append(python)
            report['python_runtime']={key:python.ready[key] for key in ['channels_redis','msgpack']};report['initial_group_size']=size();assert report['initial_group_size']==3, 'expected three group members'
            for number,(sender,path) in enumerate([(python,'sync'),(python,'async'),(first,None),(second,None)]):
                event={'type':'update','data':{'type':'fixture_event','fixture_probe':'interop-'+str(number),'number':number,'fraction':1.5,'unicode':'東京é','flags':[True,False,None],'integer':2**63+1}}
                report['stage']='send-'+str(number);assert sender.call({'op':'send','path':path,'message':event})=={'sent':True}
                for receiver_index,receiver in enumerate([first,second,python]):
                    report['stage']='receive-'+str(number)+'-'+str(receiver_index)
                    expected(receiver,event)
                report['checks'].append({'sender':path or ('rust-first' if sender is first else 'rust-second'),'three_processes_received_exact_envelope':True})
            direct={'type':'update','data':{'type':'direct_fixture','fixture_probe':'interop-direct','number':42}}
            assert python.call({'op':'send','path':'direct','channel':first.ready['channel'],'message':direct})=={'sent':True}
            expected(first,direct);absent(second);absent(python)
            report['checks'].append({'case':'Python direct-channel string selector','only_target_received':True})
            first.close();assert size()==2
            event={'type':'update','data':{'type':'after_disconnect','fixture_probe':'interop-after-close'}};python.call({'op':'send','path':'sync','message':event})
            for receiver in [second,python]:expected(receiver,event)
            report['checks'].append({'case':'Rust disconnect preserves Python and other Rust members','passed':True})
            second.close();python.close();assert size()==0;report['group_members_after_close']=0;report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            for peer in peers:peer.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'output':str(out),'checks':len(report['checks'])}));return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
