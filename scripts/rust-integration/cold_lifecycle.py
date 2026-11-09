#!/usr/bin/env python3
"""Observe original cold/warm client/provider lifecycle and pre-stream errors.

Configuration-only ghost fixture variants do not modify original application
source. Full original Dispatcharr authorship and credit remain unchanged.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import threading
import time
import urllib.request
import uuid
from shared_ts import ROOT, BASELINE, IMAGE, Reader, fixture_source, http, wait
from cold_ts import capture


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker',default='docker')
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--candidate-image')
    parser.add_argument('--candidate-build-manifest',type=Path)
    parser.add_argument('--warm-only',action='store_true')
    parser.add_argument('--routes-only',action='store_true')
    parser.add_argument('--cold-only',action='store_true')
    parser.add_argument('--cold-mode',choices=['stall','fail','empty'],default='stall')
    parser.add_argument('--ghost-seconds',type=int,default=0,help='0 preserves defaults;1..30 is an explicit fixture-only class override')
    args=parser.parse_args()
    if bool(args.candidate_image)!=bool(args.candidate_build_manifest) or (args.candidate_image and not args.routes_only):parser.error('candidate characterization requires routes-only, image and manifest')
    if not 0<=args.ghost_seconds<=30:parser.error('ghost seconds must be0..30')
    args.docker=shutil.which(args.docker)
    if not args.docker:parser.error('Docker not found')
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=True)
    if any(output.iterdir()):parser.error('output must be empty')
    report={'scope':'original TS cold/warm resource and rejection wire contracts','passed':False,'application_performance_verified':False,'milestone_complete':False,
            'baseline_commit':BASELINE,'baseline_image':IMAGE,'fixture_ghost_seconds':args.ghost_seconds or 50,'default_class_config':not bool(args.ghost_seconds),'cold_mode':args.cold_mode,'observations':[],'snapshots':[],'commands':[]}
    manifest=None
    if args.candidate_build_manifest:
        raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw)
        hashes=manifest.get('source_sha256',{})
        digest=hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()
        if not manifest.get('passed') or not hashes or digest!=manifest.get('source_tree_sha256'):parser.error('invalid passing source-bound candidate manifest')
        report.update(backend='candidate',candidate_build_manifest=str(args.candidate_build_manifest),candidate_build_manifest_sha256=hashlib.sha256(raw).hexdigest(),candidate_image_id=manifest['image_id'])
    project='dispatcharr-cold-life-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=output/'cold-overlay.json'
    services={'origin':{'entrypoint':['/dispatcharrpy/bin/python','/integration/cold_origin.py'],'volumes':[str(ROOT/'scripts/rust-integration')+':/integration:ro']}}
    if args.candidate_image:services['candidate']={'image':args.candidate_image}
    overlay.write_text(json.dumps({'services':services}))
    paths=[Path(__file__).resolve(),*[Path(__file__).with_name(name) for name in ('shared_ts.py','cold_ts.py','cold_origin.py','cold_config.py','cold_supervisor.py','seed_accounts.py')],
           ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ('seed.py','start.sh','supervisor.py','uwsgi.ini','origin.py')]]
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    live_readers=[];threads=[]
    with fixture_source(report,output,'cold-lifecycle.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as stream:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=stream,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest()
        subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True,timeout=30);(source/'media').mkdir(exist_ok=True)
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),BENCHMARK_SHUTDOWN_DELAY='2',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='0')
        compose=[args.docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(ROOT/'docker/compose.rust-candidate.yml'),'-f',str(overlay)]
        def command(values,timeout=120):
            report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=environment,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def endpoint(service,port):return 'http://'+command(compose+['port',service,str(port)]).strip()
        def python(code):return command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',code])
        def snapshot(channel,profile):
            script="""local p='live:channel:'..ARGV[1];local ids=redis.call('SMEMBERS',p..':clients');if #ids>16 then return redis.error_reply('fixture client bound') end;local rows={};for _,id in ipairs(ids) do local k=p..':clients:'..id;table.insert(rows,{id=id,last_active=redis.call('HGET',k,'last_active') or '',user=redis.call('HGET',k,'user_id') or '',connected_at=redis.call('HGET',k,'connected_at') or ''}) end;return cjson.encode({owner=redis.call('GET',p..':owner') or '',profile_slots=tonumber(redis.call('GET','profile_connections:'..ARGV[2]) or '0'),client_count=#ids,clients=rows,state=redis.call('HGET',p..':metadata','state') or '',error_message=redis.call('HGET',p..':metadata','error_message') or '',input_index=tonumber(redis.call('GET',p..':input:buffer:index') or '0')})"""
            return json.loads(json.loads(command(compose+['exec','-T','baseline','redis-cli','--json','EVAL',script,'0',channel,str(profile)],15)))
        def control(path,mode):
            request=urllib.request.Request(origin+'/control',data=json.dumps({'path':path,'mode':mode}).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(request,timeout=3) as response:assert json.load(response)['applied'] is True
        def observe(label,url,headers=None,max_bytes=188*32):
            result={'label':label,**capture(url,75,headers,max_bytes)};report['observations'].append(result);(output/(label+'.bin')).write_bytes(bytes.fromhex(result['body_hex']));return result
        custom=False
        try:
            command(compose+['up','-d','baseline','origin'],180);base,origin=endpoint('baseline',5656),endpoint('origin',8080)
            wait(lambda:(output/'seed.json').exists() and http(origin+'/health')[0]==200 and http(base+'/proxy/ts/stream/missing')[0]==404,300)
            seed=json.loads((output/'seed.json').read_text());accounts=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/integration/seed_accounts.py']))
            setup="""import os,sys,json;sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();from apps.m3u.models import M3UAccount;from apps.accounts.models import User;from core.models import CoreSettings,USER_LIMITS_SETTINGS_KEY;p=M3UAccount.get_custom_account().profiles.get(is_default=True);p.max_streams=1;p.save(update_fields=['max_streams']);User.objects.filter(username='rust-probe-viewer').update(stream_limit=1);CoreSettings.objects.update_or_create(key=USER_LIMITS_SETTINGS_KEY,defaults={'name':'Isolated cold limit policy','value':{'terminate_on_limit_exceeded':False,'prioritize_single_client_channels':True,'ignore_same_channel_connections':False,'terminate_oldest':True}});print(json.dumps({'profile':p.pk}))"""
            profile=json.loads(python(setup))['profile'];report['provider_max_streams']=1;report['provider_profile_id']=profile
            command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop']);command(compose+['exec','-T','baseline','redis-cli','FLUSHDB'])
            if args.ghost_seconds:
                command(compose+['exec','-d','-e','COLD_GHOST_SECONDS='+str(args.ghost_seconds),'baseline','/dispatcharrpy/bin/python','/integration/cold_supervisor.py','serve']);custom=True
                wait(lambda:http(base+'/proxy/ts/stream/missing')[0]==404,60)
            else:
                command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','start'])
            if args.candidate_image:
                command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop'])
                command(compose+['up','-d','--no-build','--force-recreate','candidate'])
                base=endpoint('candidate',9192)
                wait(lambda:http(base+'/health/ready')[0]==200,60,'candidate ready')
                container=command(compose+['ps','-q','candidate']).strip()
                inspection=json.loads(command([args.docker,'inspect',container]))[0]
                if inspection['Image']!=manifest['image_id'] or inspection['Config']['Labels'].get('org.opencontainers.image.source-tree-sha256')!=manifest['source_tree_sha256']:raise RuntimeError('candidate immutable image binding mismatch')
            if not args.warm_only and not args.routes_only:
                selected=seed['channels'][0];channel=selected['channel_uuid'];token=accounts['viewer']['access'];headers={'Authorization':'Bearer '+token}
                for path in (selected['origin_path'],selected['backup_origin_path']):control(path,args.cold_mode)
                start=time.monotonic();fresh={};warm={}
                a=threading.Thread(target=lambda:fresh.update(observe('fresh-stalled',base+selected['path'],headers)),daemon=True);a.start();threads.append(a)
                wait(lambda:snapshot(channel,profile)['client_count']==1 and snapshot(channel,profile)['profile_slots']==1,10,'fresh client reserved provider')
                b=threading.Thread(target=lambda:warm.update(observe('existing-initializing',base+selected['path'])),daemon=True);b.start();threads.append(b)
                report['snapshots'].append({'at_seconds':time.monotonic()-start,**snapshot(channel,profile)})
                observe('user-limit-rejection',base+seed['channels'][1]['path'],headers)
                observe('provider-full-rejection',base+seed['channels'][2]['path'])
                observe('invalid-auth-rejection',base+seed['channels'][3]['path'],{'Authorization':'Bearer invalid-fixture-token'})
                checkpoints=[5,8,12,20,30,40,50,55,60,70] if not args.ghost_seconds else [3,5,6,7,8,10,12]
                for checkpoint in checkpoints:
                    if not a.is_alive() and not b.is_alive():break
                    while time.monotonic()-start<checkpoint and (a.is_alive() or b.is_alive()):time.sleep(.1)
                    report['snapshots'].append({'at_seconds':time.monotonic()-start,**snapshot(channel,profile)})
                a.join(80);b.join(80)
                if a.is_alive() or b.is_alive():raise RuntimeError('cold capture threads exceed observation bound')
                wait(lambda:snapshot(channel,profile)['profile_slots']==0,40,'cold provider slot cleanup')
            if args.cold_only:
                report['passed']=len(report['observations'])==5 and all(row['status'] is not None for row in report['observations'])
            elif args.routes_only:
                paths=[seed['channels'][0]['path'], '/proxy/hls/missing/index.m3u8', '/proxy/hls/missing/1.ts', '/proxy/ts/status']
                for path in paths:
                    for label,headers in [('anonymous',{}),('invalid',{'Authorization':'Bearer invalid-fixture-token'}),('valid',{'Authorization':'Bearer '+accounts['admin']['access']})]:
                        for method in ('HEAD','GET') if '/hls/' in path else ('HEAD',):
                            result={'label':label,'path':path,'method':method,**capture(base+path,5,headers,method=method)}
                            report['observations'].append(result)
                for label,token in [('anonymous',None),('invalid','invalid-fixture-token'),('viewer',accounts['viewer']['access']),('standard',accounts['standard']['access']),('admin',accounts['admin']['access'])]:
                    for method in ('HEAD','DELETE'):
                        headers={'Authorization':'Bearer '+token} if token else {}
                        report['observations'].append({'label':label,'path':'/api/proxy/hls/sessions/missing/','method':method,
                            **capture(base+'/api/proxy/hls/sessions/missing/',5,headers,method=method)})
                report['origin_after_routes']=http(origin+'/stats')[1]
                for row in report['observations']:
                    if '/api/' in row['path']:
                        expected=401 if row['label'] in ('anonymous','invalid') else 403 if row['label']=='viewer' else 405 if row['method']=='HEAD' else 404
                    else:
                        expected=401 if row['label']=='invalid' or (row['path']=='/proxy/ts/status' and row['label']=='anonymous') else 405 if row['method']=='HEAD' else 410
                    row['expected_status']=expected
                    row['contract_passed']=row['status']==expected and (row['method']!='HEAD' or row['bytes']==0) and (expected!=401 or row.get('headers',{}).get('WWW-Authenticate')=='Bearer realm="api"')
                report['passed']=report['origin_after_routes']['connections']==0 and all(row['contract_passed'] for row in report['observations'])
            else:
                # Reach an actual active state with valid media, then observe a new
                # head-position viewer while the origin sends no further payload.
                selected=seed['channels'][4];channel=selected['channel_uuid'];
                for path in (selected['origin_path'],selected['backup_origin_path']):control(path,'warm')
                warm_start=time.monotonic();reader=Reader(base+selected['path'],output/'warm-owner.ts');live_readers.append(reader)
                report['warm_transitions']=[];last_transition=None
                def ready():
                    nonlocal last_transition
                    state=snapshot(channel,profile);progress=reader.snapshot()
                    key=(state['state'],state['input_index'],bool(progress['bytes']))
                    if key!=last_transition:
                        report['warm_transitions'].append({'at_seconds':time.monotonic()-warm_start,**state,'reader':progress});last_transition=key
                    phase=http(origin+'/stats')[1]['paths'].get(selected['origin_path'],{}).get('phase')
                    return phase=='stalled' and state['state'] in ('active','waiting_for_clients')
                wait(ready,20,'real media becomes active before warm no-data request')
                before=snapshot(channel,profile);result=observe('existing-active-head',base+selected['path'],max_bytes=188);result['state_before_request']=before
                reader.close();wait(lambda:snapshot(channel,profile)['profile_slots']==0,40,'warm provider cleanup')
                report['passed']=len(report['observations'])==(1 if args.warm_only else 6) and all(row['status'] is not None for row in report['observations'])
            report['interpretation']='Characterization only; distinguish200TS error initialization from429 user-limit,503 provider-full/registration and401 invalid-auth JSON.'
        except Exception as error:
            report['failure']=type(error).__name__;report['failure_message']=str(error)
            try:report['last_origin']=http(origin+'/stats')[1];report['last_state']=snapshot(channel,profile);report['last_readers']=[reader.snapshot() for reader in live_readers]
            except Exception:pass
        finally:
            for reader in live_readers:
                try:reader.close()
                except Exception:report['passed']=False
            try:
                if custom:command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/integration/cold_supervisor.py','stop'])
                (output/'compose.log').write_text(command(compose+['logs','--no-color'],30))
            except Exception:pass
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception as error:report['cleanup_error']=type(error).__name__;report['passed']=False
            report['changed_tool_sources']=[name for name,digest in report['tool_sha256'].items() if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=digest]
            if report['changed_tool_sources']:report['passed']=False
            (output/'cold-lifecycle.json').write_text(json.dumps(report,indent=2)+'\n')
    return 0 if report['passed'] else 1


if __name__=='__main__':raise SystemExit(main())
