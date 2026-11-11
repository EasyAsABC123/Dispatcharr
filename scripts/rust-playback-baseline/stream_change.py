#!/usr/bin/env python3
"""Owned Original admin stream-change boundary. No sampler or Native execution.
Original Dispatcharr authors retain application/algorithm credit and licenses.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'rust-integration'))
from shared_ts import Reader,wait
from fixture_ports import write_port_overlay
ROOT=Path(__file__).resolve().parents[2]
SOURCE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'

def finite_state_matches(state,clients,upstreams):
    return state['profile']['max_streams']==1 and state['profile']['is_active'] is True and state['provider_slots']==upstreams and state['clients']==clients and state['origin']['active']==upstreams

def refusal_matches(status,body,before,after):
    return status==503 and isinstance(body,dict) and isinstance(body.get('error'),str) and after['provider_slots']==before['provider_slots']==1 and after['origin']['connections']==before['origin']['connections'] and after['origin']['active']==1

def get(url,timeout=10):
    try:response=urllib.request.urlopen(url,timeout=timeout)
    except urllib.error.HTTPError as e:response=e
    with response:
        data=response.read(65537)
        if len(data)>65536:raise ValueError('Control response exceeds64KiB')
        return response.status,data

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',required=True,type=Path);parser.add_argument('--policy',required=True,type=Path);args=parser.parse_args()
    policy=json.loads(args.policy.read_text());assert policy['provider_cap']==0 and policy['Source']==SOURCE
    out=args.output.resolve()
    if out.exists() and any(out.iterdir()):parser.error('output must be empty')
    out.mkdir(parents=True,exist_ok=True)
    project='dispatcharr-m0-switch-'+uuid.uuid4().hex[:12]
    report={'scope':policy['scope'],'passed':False,'fully_verified':False,'resource_acceptance':False,'Source_budget_acceptance':None,'project':project,'source_revision':SOURCE,'image':IMAGE,'policy':policy,'policy_sha256':hashlib.sha256(args.policy.read_bytes()).hexdigest(),'states':[],'checks':[],'commands':[],'readers':[]}
    readers=[]
    def check(label,passed,details=None):
        report['checks'].append({'label':label,'passed':bool(passed),'details':details})
        if not passed:raise AssertionError(label)
    with tempfile.TemporaryDirectory(prefix='dispatcharr-m0-switch-') as temporary:
        src=Path(temporary)/'source';src.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb')as f:subprocess.run(['git','archive',SOURCE],cwd=ROOT,stdout=f,check=True,timeout=30)
        report['Source_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();assert report['Source_archive_sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
        subprocess.run(['tar','-xf',str(archive),'-C',str(src),'--no-same-owner'],check=True,timeout=30);(src/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(src),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='300',FIXTURE_CODEC='h264',BENCHMARK_SHUTDOWN_DELAY='5',BENCHMARK_RETENTION_SECONDS='60',BENCHMARK_STARTUP_BEHIND='5')
        ports=out/'fixture-ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
        compose=['docker','compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(ports)]
        def cmd(values,timeout=90):
            report['commands'].append(values);r=subprocess.run(values,cwd=ROOT,env=env,capture_output=True,text=True,timeout=timeout)
            if r.returncode:raise RuntimeError('fixture command: '+r.stderr[-4096:])
            return r.stdout
        def py(text):return cmd(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',text],30)
        prefix="import os,sys,json;sys.path.insert(0,'/app');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings');import django;django.setup();"
        def redis(lua,*arguments):return json.loads(cmd(compose+['exec','-T','baseline','redis-cli','--json','EVAL',lua,'0',*map(str,arguments)],15))
        try:
            cmd(compose+['up','-d','baseline','origin'],180)
            base='http://'+cmd(compose+['port','baseline','5656']).strip();origin='http://'+cmd(compose+['port','origin','8080']).strip()
            wait(lambda:(out/'seed.json').exists() and get(origin+'/health')[0]==200 and get(base+'/proxy/ts/stream/missing')[0]==404,300,'startup')
            seed=json.loads((out/'seed.json').read_text());channels=seed['channels'][:2]
            profile=json.loads(py(prefix+"from apps.m3u.models import M3UAccount;p=M3UAccount.get_custom_account().profiles.get(is_default=True);assert p.max_streams==0;print(json.dumps({'id':p.pk,'max_streams':p.max_streams}))"));report['configured_profile']=profile
            ids=cmd(compose+['ps','-q','baseline','origin']).split();inspect=json.loads(cmd(['docker','inspect',*ids]));report['container_bindings']=[{'id':x['Id'],'image':x['Image'],'StartedAt':x['State']['StartedAt'],'NanoCpus':x['HostConfig']['NanoCpus'],'Memory':x['HostConfig']['Memory']}for x in inspect]
            def state(label):
                start=time.monotonic();profile_now=json.loads(py(prefix+f"from apps.m3u.models import M3UAccountProfile;p=M3UAccountProfile.objects.get(pk={profile['id']});print(json.dumps({{'id':p.pk,'max_streams':p.max_streams,'is_active':p.is_active}}))"))
                rows=redis("local r={};for i=2,#ARGV do local p='live:channel:'..ARGV[i];table.insert(r,{redis.call('SCARD',p..':clients'),redis.call('GET',p..':owner')or'',redis.call('HLEN',p..':metadata'),redis.call('GET',p..':input:buffer:index')or'',redis.call('ZCARD',p..':input:buffer:chunk_timestamps')})end;return {tonumber(redis.call('GET','profile_connections:'..ARGV[1])or'0'),r}",profile['id'],*[x['channel_uuid']for x in channels])
                orig=json.loads(get(origin+'/stats')[1]);s={'label':label,'profile':profile_now,'provider_slots':rows[0],'clients':sum(x[0]for x in rows[1]),'channels':rows[1],'origin':orig,'started_monotonic':start,'completed_monotonic':time.monotonic()};report['states'].append(s);return s
            def light():
                row=redis("return {tonumber(redis.call('GET','profile_connections:'..ARGV[1])or'0'),redis.call('SCARD','live:channel:'..ARGV[2]..':clients'),redis.call('SCARD','live:channel:'..ARGV[3]..':clients')}",profile['id'],*[x['channel_uuid']for x in channels]);orig=json.loads(get(origin+'/stats')[1]);return row==[0,0,0] and orig['active']==0
            def reader(index,name):
                r=Reader(base+channels[index]['path'],out/(name+'.ts'));readers.append(r);wait(lambda:r.snapshot()['bytes']>=750000,25,name+' media');return r
            before=state('before');check('before-empty',before['clients']==before['provider_slots']==before['origin']['active']==0)
            owner=reader(0,'original-body');active=state('active');check('single-unlimited-upstream',active['clients']==1 and active['origin']['active']==1 and active['provider_slots']==0 and active['profile']['max_streams']==0)
            tokens=json.loads(py(prefix+"from apps.accounts.models import User;from rest_framework_simplejwt.tokens import RefreshToken;u=User.objects.create(username='m0-owned-switch-admin',user_level=10,is_active=True,is_staff=True,is_superuser=True,stream_limit=0);print(json.dumps({'token':str(RefreshToken.for_user(u).access_token)}))"));token=tokens['token']
            control=base+'/proxy/ts/change_stream/'+channels[0]['channel_uuid']
            def control_request(method,authenticated):
                payload=json.dumps({'stream_id':channels[0]['backup_stream_pk']}).encode() if method=='POST' else None
                headers={'Content-Type':'application/json'}
                if authenticated:headers['Authorization']='Bearer '+token
                try:response=urllib.request.urlopen(urllib.request.Request(control,data=payload,headers=headers,method=method),timeout=15)
                except urllib.error.HTTPError as error:response=error
                with response:
                    raw=response.read(65537)
                    if len(raw)>65536:raise ValueError('Control response bound')
                    return {'method':method,'authenticated':authenticated,'status':response.status,'headers':response.getheaders(),'body':json.loads(raw),'body_sha256':hashlib.sha256(raw).hexdigest()}
            report['method_probe']=control_request('GET',True);check('GET-not-allowed',report['method_probe']['status']==405,report['method_probe'])
            report['anonymous_probe']=control_request('POST',False);check('anonymous-admin-denied',report['anonymous_probe']['status']in[401,403],report['anonymous_probe'])
            after_denial=state('denied-control');check('denied-controls-no-origin-change',after_denial['origin']['connections']==active['origin']['connections'] and after_denial['origin']['active']==1 and after_denial['provider_slots']==0)
            report['switch']=control_request('POST',True);check('authenticated-switch-success',report['switch']['status']==200 and report['switch']['body'].get('stream_id')==channels[0]['backup_stream_pk'],report['switch'])
            wait(lambda:json.loads(get(origin+'/stats')[1])['paths'].get(channels[0]['backup_origin_path'],{}).get('bytes',0)>0,15,'backup origin receives bytes')
            count=owner.snapshot()['media_bytes'];wait(lambda:owner.snapshot()['media_bytes']>=count+750000,15,'original body continues after app switch')
            switched=state('after-switch');report['target_metadata']=redis("return redis.call('HGETALL','live:channel:'..ARGV[1]..':metadata')",channels[0]['channel_uuid']);check('new-origin-active-unlimited-counter0',switched['origin']['active']==1 and switched['provider_slots']==0)
            decoded=cmd(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','http://127.0.0.1:5656'+channels[0]['path'],'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],45);(out/'post-switch.decode.log').write_text(decoded);check('post-switch-fresh-body-audio-video-decode',any(x.startswith('frame=')and int(x.split('=')[1])>=50 for x in decoded.splitlines()))
            owner.close();state('post-physical-close');time.sleep(70);final=state('retention-settled');check('same-run-retention-settled',final['clients']==final['provider_slots']==final['origin']['active']==0,final)
            for r in readers:
                snap=r.snapshot();check('original-body-valid-TS',snap['sync_errors']==0 and not snap['errors'] and snap['media_bytes']>0,snap);report['readers'].append(snap)
            report['passed']=True
        except Exception as error:report['failure']={'type':type(error).__name__,'message':str(error)}
        finally:
            for reader in readers:
                try:reader.close()
                except Exception as error:report.setdefault('cleanup_errors',[]).append(str(error))
            try:(out/'compose.log').write_text(cmd(compose+['logs','--no-color'],30))
            except Exception as error:report['log_error']=str(error)
            try:cmd(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='owned project removed'
            except Exception as error:report['passed']=False;report['cleanup_error']=str(error)
            paths=[Path(__file__),ROOT/'docker/compose.rust-playback.yml',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'scripts/rust-playback-baseline/seed.py'];report['tools_sha256']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()for p in paths}
            (out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'failure':report.get('failure'),'project':project}));raise SystemExit(0 if report['passed'] else 1)
if __name__=='__main__':main()
