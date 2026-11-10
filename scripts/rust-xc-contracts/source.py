#!/usr/bin/env python3
"""Capture actual original XC account/live read contracts, without native claims.

Full application credit remains with original Dispatcharr authors/contributors.
Credentials never enter command arguments or persisted responses.
"""
import argparse,hashlib,http.client,json,os,shutil,subprocess,sys,time,uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer

def exercise(base,control):
    seed=control.call({'op':'seed'});rows=[]
    def request(name,role='standard',action=None,path='/player_api.php',extra=(),headers=None,method='GET',credentials=True):
        user=seed['users'][role];pairs=([('username',user['username']),('password',user['password'])] if credentials else [])
        if action is not None:pairs.append(('action',action))
        pairs.extend(extra);target=path+('?' +urlencode(pairs) if pairs else '')
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=10);values={'Host':'xc.fixture.invalid:8088'};values.update(headers or {})
        before=time.time();conn.request(method,target,headers=values);response=conn.getresponse();body=response.read(2*1024*1024);after=time.time();assert len(body)<2*1024*1024
        result={'case':name,'path':path,'action':action,'method':method,'status':response.status,'headers':{key.lower():value for key,value in response.getheaders() if key.lower() in ['content-type','allow','retry-after','content-disposition','cache-control']},'body_bytes':len(body)}
        if body:
            try:decoded=json.loads(body)
            except (ValueError,UnicodeError):result['body_sha256']=hashlib.sha256(body).hexdigest()
            else:
                if isinstance(decoded,dict) and 'user_info' in decoded:
                    info=decoded['user_info'];server=decoded['server_info'];assert info['password']==user['password'] and info['username']==user['username'];info['password']='<fixture-secret>'
                    stamp=server['timestamp_now'];assert before-1<=stamp<=after+1;expiry=int(info['exp_date']);assert before+90*86400-1<=expiry<=after+90*86400+1
                    parsed=datetime.strptime(server['time_now'],'%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp();assert abs(parsed-stamp)<=1
                    result['volatile_time_validation']={'timestamp_in_request_window':True,'expiry_plus_90_days':True,'utc_string_matches_timestamp':True}
                    info['exp_date']='<request-time+90days>';server['timestamp_now']='<request-time>';server['time_now']='<UTC request time>'
                result['json']=decoded
        conn.close();rows.append(result);return result
    for role in ['viewer','standard','admin','limited','inactive']:
        for action in [None,'get_live_categories','get_live_streams']:
            request(role+' '+str(action),role,action)
        request(role+' full panel',role,path='/panel_api.php')
    for action in ['get_account_info','unknown_action','']:
        request('fallback '+repr(action),action=action)
    for group in [*seed['groups'],0,999999]:request('category '+str(group),action='get_live_streams',extra=[('category_id',str(group))])
    for role in ['standard','admin']:
        for value in [True,'false',False]:
            control.call({'op':'preferences','user':role,'values':{'hide_adult_content':value}});request(role+' adult preference '+repr(value),role,'get_live_streams');request(role+' categories preference '+repr(value),role,'get_live_categories')
        control.call({'op':'preferences','user':role,'values':{}})
    for value in [False,0,True]:
        control.call({'op':'preferences','user':'standard','values':{'catchup_enabled':value}});request('user catchup '+repr(value),action='get_live_streams')
    control.call({'op':'preferences','user':'standard','values':{}})
    control.call({'op':'system','values':{'catchup_enabled':False}});request('system catchup disabled',action='get_live_streams');control.call({'op':'system','values':{'catchup_enabled':True}})
    for state in [None,'timestamp','user','type']:
        control.call({'op':'counts','present':True,'bad':state});request('global counts '+str(state));request('limited counts '+str(state),'limited')
    control.call({'op':'counts','present':False})
    for unlimited in [False,True]:
        expected=control.call({'op':'tuner','unlimited':unlimited})['expected']
        result=request('tuner unlimited='+str(unlimited));assert result['json']['user_info']['max_connections']==str(expected)
        result=request('limited ignores tuner unlimited='+str(unlimited),'limited');assert result['json']['user_info']['max_connections']=='2'
    control.call({'op':'reset_tuner'})
    for name,headers in [('no port',{'Host':'xc.fixture.invalid'}),('https implicit',{'Host':'xc.fixture.invalid','X-Forwarded-Proto':'https'}),('https explicit',{'Host':'xc.fixture.invalid:443','X-Forwarded-Proto':'https,http'}),('forwarded host ignored',{'Host':'xc.fixture.invalid','X-Forwarded-Host':'ignored.invalid:8443','X-Forwarded-Proto':'https'}),('uppercase proto',{'Host':'xc.fixture.invalid','X-Forwarded-Proto':'HTTPS'}),('IPv6 host',{'Host':'[::1]:8088'})]:request('server '+name,headers=headers)
    request('invalid Bearer ignored',headers={'Authorization':'Bearer invalid'});request('invalid API key ignored',headers={'X-API-Key':'invalid'})
    for method in ['HEAD','OPTIONS','POST']:request('method '+method,method=method)
    request('HEAD live categories',action='get_live_categories',method='HEAD')
    request('HEAD live streams',action='get_live_streams',method='HEAD')
    request('HEAD panel',path='/panel_api.php',method='HEAD')
    request('browser accept ignored',action='get_live_streams',headers={'Accept':'text/html'})
    csrf='A'*32
    request('POST valid CSRF',method='POST',headers={'Cookie':'csrftoken='+csrf,'X-CSRFToken':csrf})
    request('missing credentials',credentials=False)
    request('wrong credentials',extra=[('password','invalid')])
    control.call({'op':'preferences','user':'standard','values':{'allowed_networks':{'XC_API':'192.0.2.0/24'}}});request('per-user network denied');control.call({'op':'preferences','user':'standard','values':{}})
    control.call({'op':'network','values':{'XC_API':'192.0.2.0/24'}});request('global network denied');control.call({'op':'network','values':{'XC_API':'0.0.0.0/0,::/0'}})
    return rows,control.call({'op':'side_effects'})

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--docker',default='docker');parser.add_argument('--native',action='store_true');args=parser.parse_args();docker=shutil.which(args.docker);assert docker
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir())
    report={'scope':globals().get('SOURCE_SCOPE','actual original XC account/live read HTTP characterization only'),'passed':False,'native_verified':False,'application_performance_verified':False,'milestone_complete':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    tools=[Path(__file__).resolve(),*[Path(__file__).with_name(name) for name in ['control.py','numbers_source.py','counts_source.py']],ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    tools.extend(globals().get('EXTRA_TOOLS',[]))
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    binary=None
    if args.native:
        for path in sorted((ROOT/'rust').rglob('*')):
            if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}:
                target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        env_build=dict(os.environ,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
        build=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-xc-http','--example','peer'],env=env_build,capture_output=True,text=True,timeout=180)
        (out/'build.log').write_text(build.stdout+build.stderr)
        if build.returncode:
            report.update(failure='native XC peer build failed',cleanup='no Compose project was started')
            (out/'xc-source.json').write_text(json.dumps(report,indent=2)+'\n')
            raise RuntimeError('native XC peer build failed')
        binary=out/'xc-peer';shutil.copyfile(ROOT/'rust/target/debug/examples/peer',binary);binary.chmod(0o700);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
        report['native_source_sha256']={str(path.relative_to(copies)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted((copies/'rust').rglob('*')) if path.is_file()}
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    project='dispatcharr-xc-source-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-xc-contracts')+':/xc-probe:ro']}}}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,5432,6379],'origin':[8080]});control=None;native=None
    with fixture_source(report,out,'xc-source.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='90')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',overlay,ports] for value in ['-f',str(path)]]]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xc-probe/control.py'],env,out/'control.log');report['cases'],report['side_effects']=exercise(base,control)
            report['default_group_helper']=control.call({'op':'default_group_probe'})
            report['connection_count_helper']=json.loads(command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xc-probe/counts_source.py']))
            assert report['connection_count_helper']['owned_keys_removed']
            command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xc-probe/numbers_source.py','--source','/app','--output','/evidence/xc-numbers.json'])
            report['number_assignment_helper']=json.loads((out/'xc-numbers.json').read_text())
            if args.native:
                env.update(DATABASE_TEST_URL='postgres://dispatch:playback-fixture-only@'+command(compose+['port','baseline','5432']).strip()+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0',SECRET_KEY='playback-fixture-only')
                native=Peer([str(binary)],env,out/'native.log');report['actual'],report['native_side_effects']=exercise('http://127.0.0.1:'+str(native.ready['port']),control)
                assert len(report['actual'])==len(report['cases'])
                report['differences']=[{'case':expected['case'],'expected':expected,'actual':actual} for expected,actual in zip(report['cases'],report['actual']) if expected!=actual]
                assert report['side_effects']==report['native_side_effects'],'Default Group relationship side effects differ'
                assert not report['differences'],str(len(report['differences']))+' differing XC cases'
                env_tests=dict(env,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
                tests=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-xc-http','--lib','--','--ignored','--test-threads=1'],env=env_tests,capture_output=True,text=True,timeout=180)
                (out/'resource-tests.log').write_text(tests.stdout+tests.stderr);report['resource_tests_passed']=tests.returncode==0
                assert tests.returncode==0,'actual PostgreSQL/Redis resource tests failed'
                report['native_verified']=True
            report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            if native:
                native.close();report['native_exit_code']=native.process.returncode
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'native_verified':report['native_verified'],'cases':len(report.get('cases',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
