#!/usr/bin/env python3
"""Full Compose source/Rust encoded H264 transport, cookie and quota contracts.

Full original Dispatcharr authorship remains with upstream. This is media/byte
and resource-cleanup evidence, not a performance comparison.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import urllib.request
import uuid
from shared_ts import ROOT,BASELINE,IMAGE,Reader,fixture_source,http,wait
from fixture_ports import write_port_overlay


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--docker',default='docker');parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--candidate-image');parser.add_argument('--candidate-build-manifest',type=Path);parser.add_argument('--backends',nargs='+',choices=['baseline','candidate'],default=['baseline','candidate']);args=parser.parse_args()
    if 'candidate' in args.backends and (not args.candidate_image or not args.candidate_build_manifest):parser.error('candidate requires immutable image/manifest')
    docker=shutil.which(args.docker)
    if not docker:parser.error('Docker not found')
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('output must be empty')
    report={'scope':'full Compose encoded H264 byte/decoder, attempt-cookie and provider-quota contracts','passed':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'source_worker_scope':'fresh source worker per mode after separately preserved reopen failure; Rust process reused across all modes to verify cookie isolation','cases':[],'commands':[]}
    manifest=None
    if args.candidate_build_manifest:
        raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw);hashes=manifest.get('source_sha256',{})
        if not manifest.get('passed') or not hashes or hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()!=manifest.get('source_tree_sha256'):parser.error('invalid source-bound manifest')
        report.update(candidate_build_manifest=str(args.candidate_build_manifest),candidate_build_manifest_sha256=hashlib.sha256(raw).hexdigest(),candidate_image_id=manifest['image_id'],candidate_source_tree_sha256=manifest['source_tree_sha256'])
    project='dispatcharr-coded-media-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=output/'encoded-overlay.json';services={'origin':{'entrypoint':['/dispatcharrpy/bin/python','/integration/encoded_origin.py'],'volumes':[str(ROOT/'scripts/rust-integration')+':/integration:ro']}}
    if args.candidate_image:services['candidate']={'image':args.candidate_image}
    overlay.write_text(json.dumps({'services':services}));ports=output/'ports.yml';mapping={'baseline':[5656],'origin':[8080]}
    if 'candidate' in args.backends:mapping['candidate']=[9192]
    report['ports']=write_port_overlay(ports,mapping)
    tools=[Path(__file__).resolve(),Path(__file__).with_name('encoded_origin.py'),Path(__file__).with_name('shared_ts.py'),Path(__file__).with_name('playback_methods.py'),Path(__file__).with_name('fixture_ports.py'),ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ('origin.py','range_http.py','seed.py','start.sh','supervisor.py','uwsgi.ini')]]
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in tools};readers=[]
    with fixture_source(report,output,'upstream-media.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),BENCHMARK_SHUTDOWN_DELAY='2',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(ROOT/'docker/compose.rust-candidate.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def endpoint(service,port):return 'http://'+command(compose+['port',service,str(port)]).strip()
        def redis(script,*values):return json.loads(command(compose+['exec','-T','baseline','redis-cli','--json','EVAL',script,'0',*map(str,values)],15))
        def snapshot(channel,profile):
            values=redis("local p='live:channel:'..ARGV[1];return {redis.call('GET',p..':owner') or '',redis.call('GET','profile_connections:'..ARGV[2]) or '0',redis.call('SCARD',p..':clients'),redis.call('HLEN',p..':metadata'),redis.call('GET',p..':input:buffer:index') or ''}",channel,profile)
            return {'owner':values[0] or None,'slots':int(values[1]),'clients':values[2],'metadata_fields':values[3],'input_index':int(values[4]) if values[4] else None}
        def stats():return http(origin+'/stats')[1]
        def control(path,mode):
            request=urllib.request.Request(origin+'/control',data=json.dumps({'path':path,'mode':mode}).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(request,timeout=3) as response:assert json.load(response)['applied']
        def clean(channel,profile):
            state=snapshot(channel,profile);return not state['owner'] and state['slots']==0 and state['clients']==0 and state['metadata_fields']==0 and state['input_index'] is None and stats()['active']==0
        def decode(path):
            media='/evidence/'+str(path.relative_to(output));probe=json.loads(command(compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json',media],30))
            log=command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',media,'-t','3','-map','0:v:0','-map','0:a:0','-f','null','-'],30);path.with_suffix('.decode.log').write_text(log)
            frames=max((int(line.split('=')[1]) for line in log.splitlines() if line.startswith('frame=')),default=0)
            return {'frames':frames,'audio_video':{'audio','video'}<={row['codec_type'] for row in probe['streams']}}
        try:
            command(compose+['up','-d','baseline','origin'],180);base=endpoint('baseline',5656);origin=endpoint('origin',8080)
            wait(lambda:(output/'seed.json').exists() and http(origin+'/health')[0]==200 and http(base+'/proxy/ts/stream/missing')[0]==404,300,'fixture ready')
            seed=json.loads((output/'seed.json').read_text());report['fixture']=stats()
            setup="import os,sys,json;sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from apps.m3u.models import M3UAccount;p=M3UAccount.get_custom_account().profiles.get(is_default=True);p.max_streams=1;p.save(update_fields=['max_streams']);print(json.dumps({'profile':p.pk}))"
            profile=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',setup]))['profile'];report['provider_max_streams']=1
            for backend in args.backends:
                command(compose+['stop','--timeout','15','candidate'],60);command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']);command(compose+['exec','-T','baseline','redis-cli','FLUSHDB'])
                if backend=='baseline':command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','start']);target=base
                else:
                    command(compose+['up','-d','--no-build','--force-recreate','candidate']);target=endpoint('candidate',9192);wait(lambda:http(target+'/health/ready')[0]==200,60)
                    container=command(compose+['ps','-q','candidate']).strip();inspection=json.loads(command([docker,'inspect',container]))[0]
                    assert inspection['Image']==manifest['image_id'] and inspection['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256')==manifest['source_tree_sha256']
                for number,mode in enumerate(('cookie-gzip','identity-clean','deflate','raw','stacked')):
                    if backend=='baseline' and number:
                        command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])
                        assert stats()['active']==0
                        command(compose+['exec','-T','baseline','redis-cli','FLUSHDB'])
                        command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','start'])
                        wait(lambda:http(base+'/proxy/ts/stream/missing')[0]==404,60,'independent source mode worker ready')
                    selected=seed['channels'][number];channel=selected['channel_uuid'];case={'backend':backend,'mode':mode,'independent_source_worker':backend=='baseline','rust_process_reused_across_modes':backend=='candidate'};report['cases'].append(case)
                    for path in (selected['origin_path'],selected['backup_origin_path']):control(path,mode)
                    before=stats();file=output/(backend+'-'+mode+'.ts');reader=Reader(target+selected['path'],file);readers.append(reader)
                    wait(lambda:reader.snapshot()['bytes']>=1_500_000,25,backend+'/'+mode+' real media')
                    case['during']=snapshot(channel,profile);assert case['during']['slots']==1 and case['during']['clients']==1
                    if mode=='cookie-gzip':
                        before_quota=stats();status,body=http(target+seed['channels'][9]['path']);case['quota_rejection']={'status':status,'body':body};assert status==503 and stats()['connections']==before_quota['connections']
                    reader.close();wait(lambda:clean(channel,profile),15,'provider/client/input cleanup');case['after']=snapshot(channel,profile)
                    case['reader']=reader.snapshot();case['prefix_sha256']=hashlib.sha256(file.read_bytes()[:report['fixture']['prefix_bytes']]).hexdigest();case['prefix_reference_equal']=case['prefix_sha256']==report['fixture']['prefix_sha256'];assert case['prefix_reference_equal']
                    case['decoder']=decode(file);assert case['decoder']['frames']>=50 and case['decoder']['audio_video'];assert case['reader']['sync_errors']==0
                    after=stats();case['requests']=after['requests'][len(before['requests']):];case['connections']=after['connections']-before['connections'];assert case['connections']==1
                    if mode=='cookie-gzip':assert len(case['requests'])==2 and case['requests'][-1]['cookie_valid']
                    else:assert all(not request['cookie_present'] for request in case['requests'])
                    case['passed']=True
            pairs=[]
            if len(args.backends)==2:
                for mode in ('cookie-gzip','identity-clean','deflate','raw','stacked'):
                    rows=[row for row in report['cases'] if row['mode']==mode];pairs.append({'mode':mode,'prefix_equal':rows[0]['prefix_sha256']==rows[1]['prefix_sha256']})
            report['comparisons']=pairs;report['passed']=len(report['cases'])==5*len(args.backends) and all(row.get('passed') for row in report['cases']) and all(row['prefix_equal'] for row in pairs)
        except Exception as error:
            report['failure']=type(error).__name__;report['failure_message']=str(error)
            try:report['last_origin']=stats();report['last_state']=snapshot(channel,profile);report['last_readers']=[reader.snapshot() for reader in readers]
            except Exception:pass
        finally:
            for reader in readers:
                try:reader.close()
                except Exception:report['passed']=False
            try:(output/'compose.log').write_text(command(compose+['logs','--no-color'],30))
            except Exception:pass
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
            report['changed_tool_sources']=[name for name,digest in report['tool_sha256'].items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest]
            if report['changed_tool_sources']:report['passed']=False
            (output/'upstream-media.json').write_text(json.dumps(report,indent=2)+'\n')
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
