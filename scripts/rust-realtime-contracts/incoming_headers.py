#!/usr/bin/env python3
"""Raw incoming header Source/native ACL and metadata regression.

Original Dispatcharr authors retain full credit. Credentials stay in memory.
The before mode records a vulnerability witness, not application acceptance.
"""
import argparse,hashlib,http.client,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'scripts/rust-integration'))
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer
PRIVATE='127.0.0.0/8,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16'

def exercise(base,control,seed,duplicates=False,scoped=False):
    def request(path,headers=(),auth=True,method='GET'):
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=5);conn.putrequest(method,path,skip_host=True);conn.putheader('Host','header.fixture.invalid:8088');conn.putheader('Accept','application/json')
        if auth:conn.putheader('Authorization','Bearer '+seed['users']['admin']['access'])
        for key,value in headers:conn.putheader(key,value)
        conn.endheaders();response=conn.getresponse();body=response.read(256*1024);assert len(body)<256*1024
        result={'status':response.status}
        try:
            decoded=json.loads(body)
            if isinstance(decoded,dict):
                if 'access' in decoded and 'refresh' in decoded:result['token_pair']=True
                for key in ['detail','error','username','BaseURL']:
                    if key in decoded:result[key]=decoded[key]
        except ValueError:pass
        conn.close();return result
    rows=[]
    forwarded=[('ascii-public',b'203.0.113.42'),('public-invalid-last',b'203.0.113.42, \xff'),('invalid-first-public',b'\xff, 203.0.113.42'),('invalid-only',b'\xff'),('public-private-invalid',b'203.0.113.42, 10.0.0.1, \xff'),('public-utf8-garbage',b'203.0.113.42, caf\xc3\xa9')]
    if duplicates:forwarded.extend([('duplicate-private-public',[b'10.0.0.1',b'203.0.113.42']),('duplicate-public-private',[b'203.0.113.42',b'10.0.0.1']),('duplicate-invalid-public',[b'\xff',b'203.0.113.42']),('alias-underscore',b'203.0.113.42')])
    if scoped:forwarded.extend([('scope-public',b'2001:db8::1%header-scope'),('scope-mapped',b'::ffff:203.0.113.42%header-scope'),('scope-empty',b'2001:db8::1%'),('scope-double',b'2001:db8::1%a%b'),('scope-real',b'2001:db8::2%header-scope'),('scope-real-mapped',b'::ffff:203.0.113.42%header-scope'),('scope-real-linklocal',b'fe80::1%header-scope')])
    for name,value in forwarded:
        for path,method in [('/hdhr/discover.json','GET'),('/api/accounts/users/me/','GET'),('/proxy/ts/status','GET'),('/api/proxy/hls/sessions/'+'a'*32+'/','DELETE'),('/proxy/catchup/stats/','GET')]:
            values=value if isinstance(value,list) else [value];rows.append({'case':name+' '+path,'xff_hex':[item.hex() for item in values] if isinstance(value,list) else value.hex(),**request(path,[('X-Real-IP' if name.startswith('scope-real') else ('X_Forwarded_For' if name=='alias-underscore' else 'X-Forwarded-For'),item) for item in values],method=method)})
    rows.append({'case':'latin1 proxy username',**request('/api/accounts/auth/proxy-login/',[('X-Header-Fixture-User',b'header-caf\xe9')],auth=False,method='POST')})
    if duplicates:rows.append({'case':'duplicate proxy identity',**request('/api/accounts/auth/proxy-login/',[('X-Header-Fixture-User',b'header-caf\xe9'),('X-Header-Fixture-User',b'not-a-real-user')],auth=False,method='POST')})
    rows.append({'case':'latin1 forwarded origin',**request('/hdhr/discover.json',[('X-Forwarded-Host',b'header-caf\xe9.invalid'),('X-Forwarded-Proto',b'http')],auth=False)})
    for label,value in [('latin1',b'header-agent-caf\xe9'),('utf8-bytes',b'header-agent-caf\xc3\xa9'),*([('duplicate',[b'header-agent-one',b'header-agent-two'])] if duplicates else [])]:
        before=control.call({'op':'checkpoint'})['id'];result=request('/output/m3u',[('X-Forwarded-For',b'203.0.113.42'),*[(('User-Agent'),item) for item in (value if isinstance(value,list) else [value])]],auth=False);events=control.call({'op':'events','after':before})['events'];rows.append({'case':'agent '+label,**result,'events':[{key:value for key,value in row.items() if key!='id'} for row in events]})
    if scoped:
        for label,value in [('scoped',b'2001:db8::1%header-scope'),('mapped',b'::ffff:203.0.113.42%header-scope')]:
            before=control.call({'op':'checkpoint'})['id'];result=request('/output/m3u',[('X-Real-IP',value),('User-Agent',b'header-scope-agent')],auth=False);events=control.call({'op':'events','after':before})['events'];rows.append({'case':'scope metadata '+label,**result,'events':[{key:value for key,value in row.items() if key!='id'} for row in events]})
    if scoped:
        for value,canonical in [(b'2001:db8::3%history-scope','2001:db8::3%history-scope'),(b'::ffff:203.0.113.43%history-scope','203.0.113.43')]:
            before=control.call({'op':'auth_history','ip':canonical})['entries'];result=request('/hdhr/discover.json',[('X-Real-IP',value),('X-API-Key',b'header-scope-invalid')],auth=False);after=control.call({'op':'auth_history','ip':canonical})['entries'];rows.append({'case':'scope auth bucket '+canonical,**result,'history_delta':after-before,'canonical_ip':canonical})
    for key,value in [('Authorization',b'Bearer \xff'),('X-API-Key',b'\xff')]:
        result=request('/hdhr/discover.json',[(key,value)],auth=False);result.pop('detail',None);rows.append({'case':'invalid auth '+key,**result})
    return rows

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--docker',default='docker');p.add_argument('--expect-before',action='store_true');p.add_argument('--candidate-image');p.add_argument('--candidate-build-manifest',type=Path);p.add_argument('--native',action='store_true');p.add_argument('--duplicates',action='store_true');p.add_argument('--scoped',action='store_true');p.add_argument('--expect-scoped-before',action='store_true');args=p.parse_args();assert args.native != bool(args.candidate_image)
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir());docker=shutil.which(args.docker);assert docker
    report={'scope':'actual raw incoming-header ACL/identity/metadata Source/native regression','passed':False,'before_vulnerability_witness':args.expect_before,'baseline_commit':BASELINE,'baseline_image':IMAGE,'application_performance_verified':False,'milestone_complete':False,'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('headers_control.py'),Path(__file__).with_name('interop.py'),ROOT/'scripts/rust-hdhr-contracts/control.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    if args.native:
        for path in sorted((ROOT/'rust').rglob('*')):
            if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}:
                target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        report['native_source_sha256']={str(path.relative_to(copies)):hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted((copies/'rust').rglob('*')) if path.is_file()}
        build=subprocess.run(['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(ROOT/'rust/target'),'--locked','--offline','-p','dispatcharr-rs'],env=dict(os.environ,CARGO_HOME='/tmp/dispatcharr-cargo',RUSTUP_HOME='/tmp/dispatcharr-rustup',RUSTUP_TOOLCHAIN='1.90.0'),text=True,capture_output=True,timeout=180);(out/'build.log').write_text(build.stdout+build.stderr);assert build.returncode==0
        binary=out/'dispatcharr-rs';shutil.copyfile(ROOT/'rust/target/debug/dispatcharr-rs',binary);binary.chmod(0o700);report['native_binary_sha256']=hashlib.sha256(binary.read_bytes()).hexdigest()
    else:
        assert args.candidate_build_manifest and re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',args.candidate_image)
        raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw);assert manifest['passed'];assert hashlib.sha256(json.dumps(manifest['source_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest()==manifest['source_tree_sha256'];(out/'candidate-build.json').write_bytes(raw);report.update(candidate_image=args.candidate_image,candidate_image_id=manifest['image_id'],candidate_source_tree_sha256=manifest['source_tree_sha256'])
    project='dispatcharr-headers-'+uuid.uuid4().hex[:12];report['project']=project
    services={'baseline':{'environment':{'DISPATCHARR_TRUSTED_PROXIES':PRIVATE},'volumes':[str(copies/'scripts/rust-hdhr-contracts')+':/hdhr-probe:ro',str(copies/'scripts/rust-realtime-contracts')+':/headers:ro']}}
    if not args.native:services['candidate']={'image':args.candidate_image,'environment':{'DISPATCHARR_TRUSTED_PROXIES':PRIVATE}}
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':services}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379,5432],'origin':[8080],**({'candidate':[9192]} if not args.native else {})});control=None;native=None;log=None
    with fixture_source(report,out,'incoming-headers.json') as temp:
        source=Path(temp)/'source';source.mkdir();archive=Path(temp)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='90')
        files=[copies/'docker/compose.rust-playback.yml',*([copies/'docker/compose.rust-candidate.yml'] if not args.native else []),overlay,ports];compose=[docker,'compose','-p',project,*[item for path in files for item in ['-f',str(path)]]]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            if not args.native:
                inspected=json.loads(command([docker,'image','inspect',args.candidate_image]))[0];assert inspected['Id']==manifest['image_id'];assert inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==manifest['source_tree_sha256']
            command(compose+['up','-d','--no-build','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/headers/headers_control.py'],env,out/'control.log');seed=control.call({'op':'seed'});report['original_functions']=seed['source_functions'];report['expected']=exercise(base,control,seed,args.duplicates,args.scoped)
            if args.native:
                allocated=write_port_overlay(out/'native-port.yml',{'native':[9192]});report['native_port']=allocated;port=allocated['services']['native'][0]['host'];target='http://127.0.0.1:'+str(port)
                env_native=dict(env,DATABASE_URL='postgres://dispatch:playback-fixture-only@'+command(compose+['port','baseline','5432']).strip()+'/dispatcharr',REDIS_URL='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0',DJANGO_SECRET_KEY='playback-fixture-only',DISPATCHARR_TRUSTED_PROXIES=PRIVATE,RUST_BIND='127.0.0.1:'+str(port),TOKIO_WORKER_THREADS='1');log=(out/'native.log').open('w');native=subprocess.Popen([str(binary)],env=env_native,stdout=log,stderr=log)
            else:command(compose+['up','-d','--no-build','candidate'],120);target='http://'+command(compose+['port','candidate','9192']).strip()
            wait(lambda:ready_http(target+'/health/ready')[0]==200,60,'native ready');report['fixture_login_history_reset']=control.call({'op':'reset_login_history'});report['actual']=exercise(target,control,seed,args.duplicates,args.scoped)
            report['differences']=[{'case':expected['case'],'expected':expected,'actual':actual} for expected,actual in zip(report['expected'],report['actual']) if expected!=actual]
            if args.expect_before:
                witness=next(row for row in report['differences'] if row['case']=='public-invalid-last /hdhr/discover.json');assert witness['expected']['status']==403 and witness['actual']['status']==200;report['vulnerability_reproduced']=True
            elif args.expect_scoped_before:
                witness=next(row for row in report['differences'] if row['case']=='scope-public /hdhr/discover.json');assert witness['expected']['status']==403 and witness['actual']['status']==200;report['scoped_vulnerability_reproduced']=True
            else:assert not report['differences'],str(len(report['differences']))+' differing cases'
            report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            if native:
                native.terminate()
                try:native.wait(timeout=15)
                except subprocess.TimeoutExpired:native.kill();native.wait(timeout=5);report['native_killed']=True;report['passed']=False
                report['native_exit']=native.returncode
                if log:log.close()
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'before_witness':args.expect_before,'cases':len(report.get('actual',[])),'differences':len(report.get('differences',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
