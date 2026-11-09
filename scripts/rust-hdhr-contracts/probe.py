#!/usr/bin/env python3
"""Actual pinned Dispatcharr HDHR HTTP contracts; original authors retain credit."""
import argparse,hashlib,http.client,json,os,shutil,subprocess,sys,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer

def exercise(base,control,rows=None):
    seed=control.call({'op':'seed'});rows=[] if rows is None else rows
    def request(name,path,method='GET',role=None,body=None,headers=None):
        address=base.removeprefix('http://');conn=http.client.HTTPConnection(address,timeout=10)
        values={'Host':'hdhr.fixture.invalid:8088','Accept':'application/json'}
        if role:values['Authorization']='Bearer '+seed['users'][role]['access']
        values.update(headers or {})
        if body is not None and not isinstance(body,bytes):body=json.dumps(body).encode();values['Content-Type']='application/json'
        conn.request(method,path,body=body,headers=values);res=conn.getresponse();payload=res.read(2*1024*1024);assert len(payload)<2*1024*1024
        result={'name':name,'method':method,'path':path,'status':res.status,'headers':{key.lower():value for key,value in res.getheaders() if key.lower() in ['content-type','allow','www-authenticate','location','vary','content-length']}}
        if payload:
            try:result['json']=json.loads(payload)
            except (ValueError,UnicodeError):result['text']=payload.decode('utf-8')
        conn.close();rows.append(result);return result
    for prefix in ['/hdhr/','/api/hdhr/']:
        for route in ['discover.json','lineup.json','lineup_status.json','device.xml']:
            request(prefix+route,prefix+route)
    for route in ['discover.json','lineup.json','lineup_status.json','device.xml']:
        request('browser Accept '+route,'/hdhr/'+route,headers={'Accept':'text/html'})
    for profile in [seed['profile'],*seed['aliases'],'unknown']:
        encoded=__import__('urllib.parse',fromlist=['quote']).quote(profile,safe='')
        for route in ['discover.json','lineup.json','lineup_status.json']:request('profile '+profile+' '+route,'/hdhr/'+encoded+'/'+route)
    for output in [seed['active_output'],seed['inactive_output'],0,99999]:
        for route in ['discover.json','lineup.json']:request('output '+str(output)+' '+route,f'/hdhr/output_profile/{output}/{route}')
    for role in ['viewer','standard','admin','inactive']:
        request('auth discovery '+role,'/hdhr/discover.json',role=role)
        request('auth device list '+role,'/hdhr/devices/',role=role)
        request('auth options '+role,'/hdhr/devices/',method='OPTIONS',role=role)
    for path in ['/hdhr/discover.json','/hdhr/devices/']:
        request('invalid auth '+path,path,headers={'Authorization':'Bearer invalid'})
        request('invalid key '+path,path,headers={'X-API-Key':'invalid'})
    for method in ['HEAD','OPTIONS','POST','PUT','DELETE']:
        request('discovery method '+method,'/hdhr/discover.json',method=method)
    request('xml negotiation','/hdhr/device.xml',headers={'Accept':'application/xml'})
    request('format xml','/hdhr/discover.json?format=xml')
    request('redirect','/hdhr?x=1')
    for value in [seed['active_output'],seed['inactive_output'],None,'invalid',True,1.5]:
        control.call({'op':'default_output','value':value});request('default output '+repr(value),'/hdhr/lineup.json')
    control.call({'op':'default_output','value':None})
    for payload in [{},{'device_id':'fixture-one'},{'device_id':'fixture-one'},{'device_id':''},{'device_id':None},{'device_id':'x'*33},{'device_id':' fixture-two ','friendly_name':' x ','tuner_count':0},{'device_id':'three','tuner_count':-1},{'device_id':'four','tuner_count':'3.0'},{'device_id':'five','tuner_count':True},{'device_id':'six','friendly_name':''},{'device_id':'seven','friendly_name':False}]:
        request('create '+repr(payload),'/hdhr/devices/','POST','admin',payload)
    for name,body,mime in [('malformed existing',b'{','application/json'),('unsupported parser',b'hello','text/plain'),('array body',b'[]','application/json'),('null body',b'null','application/json'),('form body',b'device_id=form-device&tuner_count=2','application/x-www-form-urlencoded')]:
        request(name,'/hdhr/devices/1/' if name=='malformed existing' else '/hdhr/devices/','PATCH' if name=='malformed existing' else 'POST','admin',body,{'Content-Type':mime})
    for number in [2147483647,2147483648,-1,3.0,3.5,'1e2','+4',' 004 ',None,{},False]:
        request('integer '+repr(number),'/hdhr/devices/1/','PATCH','admin',{'tuner_count':number})
    for route in ['/hdhr/devices.json','/hdhr/devices/1.json','/api/hdhr/devices.json','/api/hdhr/']:
        request('router alias '+route,route,role='admin')
    for route in ['/hdhr/HDHR%20fixture/output_profile/'+str(seed['active_output'])+'/lineup.json','/hdhr/output_profile/000'+str(seed['active_output'])+'/discover.json','/hdhr/devices/output_profile/'+str(seed['active_output'])+'/lineup.json']:
        request('combined '+route,route)
    for value in [{},[],{'default_user_agent':None}]:
        control.call({'op':'stream_map','value':value});request('legacy stream map '+repr(value),'/hdhr/lineup.json')
    control.call({'op':'default_output','value':None})
    control.call({'op':'tuner_limited'});request('limited tuner count','/hdhr/discover.json')
    for unlimited,expected in [(False,27),(True,31)]:
        control.call({'op':'tuner_fixture','unlimited':unlimited});value=request('tuner active/inactive/custom unlimited='+str(unlimited),'/hdhr/discover.json');assert value['json']['TunerCount']==expected
    control.call({'op':'user_ui','value':'192.0.2.0/24'});request('user UI deny','/hdhr/devices/',role='standard');request('user UI irrelevant public','/hdhr/discover.json',role='standard')
    control.call({'op':'user_ui','value':'0.0.0.0/0'})
    request('stored discovery','/hdhr/discover.json');request('stored XML','/hdhr/device.xml')
    for method,body in [('GET',None),('HEAD',None),('OPTIONS',None),('PATCH',{'friendly_name':'Updated & café','unknown':1}),('PUT',{'friendly_name':'missing id'}),('PUT',{'device_id':'replaced','tuner_count':7}),('DELETE',None),('GET',None)]:
        request('detail '+method,'/hdhr/devices/1/',method,'admin',body)
    request('missing malformed','/hdhr/devices/9999/','PUT','admin',b'{',{'Content-Type':'application/json'})
    request('root admin','/hdhr/',role='admin');request('root anonymous','/hdhr/')
    control.call({'op':'network','values':{'M3U_EPG':'192.0.2.0/24'}})
    request('global deny','/hdhr/discover.json');request('global deny admin','/hdhr/discover.json',role='admin');request('deny options','/hdhr/discover.json',method='OPTIONS')
    control.call({'op':'reset'});return rows

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--docker',default='docker');p.add_argument('--native',action='store_true');args=p.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir())
    docker=shutil.which(args.docker);assert docker
    report={'scope':'actual original HDHR HTTP contracts; native implementation not yet compared','passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'application_performance_verified':False,'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('control.py'),ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    if args.native:
        for path in sorted((ROOT/'rust').rglob('*')):
            if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}:
                target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        env_build=dict(os.environ,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
        build=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-hdhr-http','--example','peer'],env=env_build,capture_output=True,text=True,timeout=180)
        (out/'build.log').write_text(build.stdout+build.stderr)
        if build.returncode:raise RuntimeError('native build failed')
        binary=out/'hdhr-peer';shutil.copyfile(ROOT/'rust/target/debug/examples/peer',binary);binary.chmod(0o700);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
        report['native_source_sha256']={str(path.relative_to(copies)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted((copies/'rust').rglob('*')) if path.is_file()}
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    project='dispatcharr-hdhr-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-hdhr-contracts')+':/hdhr-probe:ro']}}}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379,5432],'origin':[8080]});control=None;native=None;env=dict(os.environ)
    with fixture_source(report,out,'hdhr.json') as temp:
        source=Path(temp)/'source';source.mkdir();archive=Path(temp)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env.update(BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='60')
        compose=[docker,'compose','-p',project,'-f',str(ROOT/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(values,timeout=120):
            report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/hdhr-probe/control.py'],env,out/'control.log');report['cases']=[];exercise(base,control,report['cases'])
            if args.native:
                env.update(DATABASE_TEST_URL='postgres://dispatch:playback-fixture-only@'+command(compose+['port','baseline','5432']).strip()+'/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0',SECRET_KEY='playback-fixture-only')
                native=Peer([str(binary)],env,out/'native.log');report['actual']=exercise('http://127.0.0.1:'+str(native.ready['port']),control)
                def normalize(rows):
                    rows=json.loads(json.dumps(rows))
                    for row in rows:row['headers'].pop('content-length',None)
                    return rows
                report['differences']=[{'case':expected['name'],'expected':expected,'actual':actual} for expected,actual in zip(normalize(report['cases']),normalize(report['actual'])) if expected!=actual]
                assert not report['differences'],str(len(report['differences']))+' differing cases'
                env_tests=dict(env,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0')
                tests=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-hdhr-http','--lib','--','--ignored','--test-threads=1'],env=env_tests,capture_output=True,text=True,timeout=120)
                (out/'resource-tests.log').write_text(tests.stdout+tests.stderr);report['resource_tests_passed']=tests.returncode==0;assert tests.returncode==0,'actual DB resource tests failed'
            report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            if native:native.close()
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'cases':len(report.get('cases',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
