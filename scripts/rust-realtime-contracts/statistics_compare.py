#!/usr/bin/env python3
"""Actual original Python basic-channel statistics versus native Redis projection.

Original Dispatcharr authors retain full application credit. The original
helper runs unchanged; only its wall clock is fixed for deterministic fields.
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


def fixtures():
    rows=[]
    for index in range(8):
        key=str(uuid.uuid5(uuid.NAMESPACE_URL,'dispatcharr-statistics-'+str(index)))
        metadata={'state':'active','init_time':'1794000000','url':'http://origin:8080/live/'+str(index)+'.ts','owner':'foreign-original-worker','stream_profile':'3','channel_name':'Fixture 東京 '+str(index),'stream_name':'Fixture stream '+str(index),'stream_id':str(index+1),'m3u_profile':'2'}
        if index%2==0:metadata.update(total_bytes='1253376',logo_id='4',source_fps='29.97',ffmpeg_speed='1.03',audio_channels='2',video_codec='h264',audio_codec='aac',resolution='1280x720',stream_type='MPEG-TS')
        if index==1:metadata={'state':'connecting'}
        if index==3:metadata.update(init_time='0',total_bytes='0')
        if index==5:metadata.pop('state');metadata.pop('owner')
        clients=[]
        for number in range(index%4):
            fields={'user_agent':'fixture é','ip_address':'127.0.0.1','connected_at':'1794000001.25','user_id':str(number),'output_format':['mpegts','hls','fmp4'][number%3],'output_profile_id':['None','000','-1'][number%3]}
            if number==1:fields.update(user_agent='',ip_address='',output_format='')
            if index==7 and number==2:fields.pop('user_agent')
            clients.append({'id':'fixture-client-'+str(number),'fields':fields})
        clients.append({'id':'expired-fixture-client','missing':True})
        rows.append({'id':key,'metadata':metadata,'clients':clients,**({'index':index*17} if index%2==0 else {})})
    return rows

def normalized(value):
    value=json.loads(json.dumps(value))
    for channel in value['channels']:channel['clients'].sort(key=lambda row:row['client_id'])
    value['channels'].sort(key=lambda row:row['channel_id']);return value

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True)
    if any(out.iterdir()):parser.error('output must be empty')
    report={'scope':'actual original basic live statistics and bounded native global Redis projection','passed':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    paths=[Path(__file__).resolve(),Path(__file__).with_name('gateway_control.py'),Path(__file__).with_name('interop.py'),ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',*[path for path in sorted((ROOT/'rust').rglob('*')) if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}],ROOT/'LICENSE',ROOT/'ATTRIBUTION.md',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in paths:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in paths}
    report['native_source_tree_sha256']=hashlib.sha256(json.dumps({key:value for key,value in report['tool_sha256'].items() if key.startswith('rust/')},sort_keys=True,separators=(',',':')).encode()).hexdigest()
    environment=dict(os.environ,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
    build=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-rs','--example','statistics_projection'],env=environment,capture_output=True,text=True,timeout=180)
    (out/'build.log').write_text(build.stdout+build.stderr)
    if build.returncode:raise RuntimeError('statistics projection build failed')
    binary=out/'statistics-projection';shutil.copyfile(ROOT/'rust/target/debug/examples/statistics_projection',binary);binary.chmod(0o700);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
    project='dispatcharr-statistics-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-realtime-contracts')+':/realtime:ro']}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379],'origin':[8080]});control=None
    with fixture_source(report,out,'statistics.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='60')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):
            report['commands'].append(values)
            return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip()
            wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'Source ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/realtime/gateway_control.py'],environment,out/'control.log')
            rows=fixtures();(out/'fixtures.json').write_text(json.dumps(rows,indent=2)+'\n');assert control.call({'op':'metadata_seed','channels':rows})=={'seeded':8}
            at=1794000010.0;expected=control.call({'op':'statistics_at','at':at})
            environment['REDIS_TEST_URL']='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0'
            result=subprocess.run([str(binary)],env=environment,input=json.dumps({'at':at}),text=True,capture_output=True,timeout=30,check=True)
            actual=json.loads(result.stdout);(out/'native.log').write_text(result.stderr)
            report['expected']=normalized(expected);report['actual']=normalized(actual);assert report['actual']==report['expected']
            assert actual['count']==8;report['compared_channels']=8;report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'output':str(out),'channels':report.get('compared_channels')}));return 0 if report['passed'] else 1

if __name__=='__main__':raise SystemExit(main())
