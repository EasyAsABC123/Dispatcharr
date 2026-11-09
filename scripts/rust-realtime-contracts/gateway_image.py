#!/usr/bin/env python3
"""Frozen packaged app WebSocket proof against actual original Django controls.

Full original Dispatcharr authors retain application credit. No full React,
regex preview, all-producer or resource-performance claim follows this probe.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import uuid
from urllib.parse import urlencode

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from shared_ts import BASELINE,IMAGE,fixture_source,http,wait
from fixture_ports import write_port_overlay
from interop import Peer
from gateway_cases import exercise,INITIAL
from ws_client import Client


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--live-stats',action='store_true');parser.add_argument('--candidate-image',required=True);parser.add_argument('--candidate-build-manifest',type=Path,required=True)
    args=parser.parse_args();docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',args.candidate_image):parser.error('invalid image reference')
    manifest_bytes=args.candidate_build_manifest.read_bytes();manifest=json.loads(manifest_bytes)
    hashes=manifest.get('source_sha256',{})
    if manifest.get('passed') is not True or not hashes or manifest.get('source_tree_sha256')!=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest():parser.error('invalid passing source-bound manifest')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    report={'scope':'packaged two-replica native app WebSocket routes and original event interoperability','passed':False,
            'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,
            'candidate_image':args.candidate_image,'candidate_source_tree_sha256':manifest['source_tree_sha256'],
            'candidate_build_manifest_sha256':hashlib.sha256(manifest_bytes).hexdigest(),'commands':[]}
    (out/'candidate-build.json').write_bytes(manifest_bytes)
    component=json.loads((ROOT/'scripts/rust-realtime-contracts/verified-native-gateway-fanout-1.json').read_text())
    matching={name:value==hashes.get(name) for name,value in component['tool_sha256'].items() if name.startswith('rust/crates/realtime/')}
    report['separate_component_source_match']=matching
    paths=[Path(__file__).resolve(),*[Path(__file__).with_name(name) for name in ['gateway_cases.py','gateway_control.py','ws_client.py','interop.py','browser_peer.js']],ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',ROOT/'docker/compose.rust-replicas.yml',ROOT/'scripts/rust-integration/fixture_ports.py',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in paths:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    project='dispatcharr-realtime-image-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-realtime-contracts')+':/realtime:ro']},**{service:{'image':args.candidate_image} for service in ['candidate','candidate-replica']}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080],'candidate':[9192],'candidate-replica':[9192],'python-replica':[5656]})
    peers=[];held=[]
    with fixture_source(report,out,'gateway-image.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='90')
        compose=[docker,'compose','-p',project,*[value for path in [ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',ROOT/'docker/compose.rust-replicas.yml',overlay,ports] for value in ['-f',str(path)]]]
        def command(values,timeout=120):
            report['commands'].append(values)
            return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def endpoint(service,port):
            host,port=command(compose+['port',service,str(port)]).strip().rsplit(':',1);return host,int(port)
        try:
            inspected=json.loads(command([docker,'image','inspect',args.candidate_image]))[0]
            assert inspected['Id']==manifest['image_id']
            assert inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==manifest['source_tree_sha256']
            report['candidate_image_id']=inspected['Id']
            command(compose+['up','-d','--no-build','baseline','origin'],180)
            host,port=endpoint('baseline',5656);base='http://'+host+':'+str(port)
            wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'Source fixture ready')
            command(compose+['up','-d','--no-build','candidate','candidate-replica'],120)
            targets=[endpoint(service,9192) for service in ['candidate','candidate-replica']]
            for host,port in targets:wait(lambda:http('http://'+host+':'+str(port)+'/health/ready')[0]==200,60,'native app ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/realtime/gateway_control.py'],environment,out/'control.log');peers.append(control)
            browser=Peer(['/home/justin/.local/share/paseo-runtime/node-v22.23.2-linux-x64/bin/node',str(copies/'scripts/rust-realtime-contracts/browser_peer.js')],environment,out/'browser.log');peers.append(browser)
            report['browser_runtime']=browser.ready;report['original']=control.call({'op':'source_info'})
            report['wire']=exercise(*targets[0],control.call,browser=browser,second_port=targets[1][1],slow_value_bytes=3*1024*1024,source_live_stats=args.live_stats,live_stats_target='http://candidate:9192' if args.live_stats else None,live_stats_peer='http://candidate-replica:9192' if args.live_stats else None)
            browser.close()
            token=control.call({'op':'token','role':'admin'})['access']
            for host,port in targets:
                client=Client(host,port,'/ws/?'+urlencode([('token',token)]));held.append(client)
                assert client.status==101 and client.receive()=={'message':INITIAL}
            started=time.monotonic();command(compose+['stop','--timeout','15','candidate','candidate-replica'],40)
            report['shutdown_seconds']=time.monotonic()-started
            report['held_socket_shutdown']=[client.receive() for client in held]
            assert report['held_socket_shutdown']==[{'closed':1001},{'closed':1001}]
            report['group_after_shutdown']=control.call({'op':'groups'});assert report['group_after_shutdown']['members']==0
            exits=[]
            for service in ['candidate','candidate-replica']:
                container=command(compose+['ps','-a','-q',service]).strip();state=json.loads(command([docker,'inspect',container]))[0]['State']
                exits.append({'service':service,'exit_code':state['ExitCode'],'oom_killed':state['OOMKilled']})
                assert state['ExitCode']==0 and not state['OOMKilled']
                logs=command(compose+['logs','--no-color',service]);(out/(service+'-logs.txt')).write_text(logs)
                assert 'Shutdown deadline reached' not in logs and 'Media drain deadline reached' not in logs
            report['exit_states']=exits;report['passed']=True
        except Exception as error:
            report['failure']=type(error).__name__;report['failure_message']=str(error)
            if hasattr(error,'partial_wire_cases'):report['partial_wire_cases']=error.partial_wire_cases
            if 'control' in locals():
                try:report['failure_group_state']=control.call({'op':'groups'})
                except Exception:pass
            for service in ['candidate','candidate-replica']:
                try:(out/(service+'-failure-logs.txt')).write_text(command(compose+['logs','--no-color',service]))
                except Exception:pass
        finally:
            for client in held:client.close()
            for peer in reversed(peers):peer.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'output':str(out),'cases':report.get('wire',{}).get('case_count')}))
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
