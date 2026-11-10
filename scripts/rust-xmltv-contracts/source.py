#!/usr/bin/env python3
"""Actual original XMLTV HTTP/cache characterization; original author credit.

No native equivalence is claimed. Credentials remain memory-only.
"""
import argparse,concurrent.futures,hashlib,http.client,json,os,shutil,subprocess,sys,threading,time,uuid,xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urlencode,quote
ROOT=Path(__file__).resolve().parents[2]
TARGET_DIR=Path(os.environ.get('XMLTV_CARGO_TARGET_DIR',str(ROOT/'rust/target')))
sys.path[:0]=[str(Path(__file__).resolve().parent),str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer

def exercise(base,control,out):
    seed=control.call({'op':'seed'});rows=[]
    def request(name,path='/output/epg',params=(),role=None,headers=None,method='GET'):
        pairs=list(params)
        if role:
            user=seed['users'][role];pairs.extend([('username',user['username']),('password',user['password'])])
        conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=20);target=path+('?' +urlencode(pairs) if pairs else '')
        conn.request(method,target,headers={'Host':'xmltv.fixture.invalid:8088',**(headers or {})});response=conn.getresponse();error=None
        try:body=response.read(8*1024*1024)
        except http.client.IncompleteRead as failure:body=failure.partial;error='IncompleteRead'
        assert len(body)<8*1024*1024
        row={'case':name,'path':path,'parameters':list(params),'role':role,'method':method,'status':response.status,'headers':{key.lower():value for key,value in response.getheaders() if key.lower() in ['content-type','content-disposition','cache-control','allow','retry-after']},'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()}
        if error:row['read_error']=error
        if body.lstrip().startswith(b'<?xml'):
            filename='body-'+str(len(rows)).zfill(3)+'.xml';(out/filename).write_bytes(body);row['body_file']=filename
            try:tree=ET.fromstring(body)
            except ET.ParseError:row['xml_complete']=False
            else:
                row.update(xml_complete=True,root_attributes=tree.attrib,channel_ids=[node.attrib['id'] for node in tree.findall('channel')],programmes=len(tree.findall('programme')),programme_titles=[node.findtext('title') for node in tree.findall('programme')])
        elif body:
            try:row['json']=json.loads(body)
            except (UnicodeError,ValueError):pass
        conn.close();rows.append(row);return row
    snapshots=[]
    def snapshot(name):snapshots.append({'after':name,**control.call({'op':'cache'})})
    snapshot('seed')
    request('normal HEAD cold',method='HEAD');snapshot('normal HEAD cold')
    first=request('normal first');snapshot('normal first')
    second=request('normal replay');assert first['body_sha256']==second['body_sha256'];snapshot('normal replay')
    request('normal HEAD cached',method='HEAD')
    for path in ['/output/epg/','/output/epg/'+quote('XC assigned profile',safe=''),'/output/epg/'+quote('XC assigned profile',safe='')+'/','/output/epg/missing-profile']:
        request('path '+path,path=path)
    request('HEAD missing profile',path='/output/epg/missing-profile',method='HEAD')
    for params in [[('cachedlogos','false')],[('cachedlogos','FALSE')],[('cachedlogos','0')],[('tvg_id_source','tvg_id')],[('tvg_id_source','gracenote')],[('tvg_id_source','invalid')],[('days','1')],[('days','0')],[('days','bad')],[('days','-1')],[('days','١')],[('days','999999999999999999999999')],[('prev_days','1')],[('days','1'),('prev_days','1')],[('prev_days','bad')]]:
        request('query '+repr(params),params=params)
    for role in ['viewer','standard','admin','limited','inactive']:
        request('XC '+role,path='/xmltv.php',role=role)
    request('XC HEAD cold days2',path='/xmltv.php',role='standard',method='HEAD',params=[('days','2')]);snapshot('XC HEAD cold days2')
    request('XC HEAD',path='/xmltv.php',role='standard',method='HEAD');snapshot('XC HEAD')
    request('XC invalid Bearer ignored',path='/xmltv.php',role='standard',headers={'Authorization':'Bearer invalid'})
    request('normal invalid Bearer ignored',headers={'Authorization':'Bearer invalid'})
    request('normal browser accept ignored',headers={'Accept':'text/html'})
    for method in ['OPTIONS','POST']:request('normal method '+method,method=method)
    csrf='A'*32;request('normal valid-CSRF POST',method='POST',headers={'Cookie':'csrftoken='+csrf,'X-CSRFToken':csrf})
    request('XC missing credentials',path='/xmltv.php')
    for values in [{'epg_days':1,'epg_prev_days':1},{'epg_days':0,'epg_prev_days':0}]:
        control.call({'op':'preferences','user':'standard','values':values});request('XC saved window '+repr(values),path='/xmltv.php',role='standard')
    control.call({'op':'preferences','user':'standard','values':{}})
    control.call({'op':'mutate_program','title':'Mutated guide title'})
    cached=request('mutation before invalidation');assert cached['body_sha256']==first['body_sha256'];snapshot('mutation before invalidation')
    snapshots[-1]['event_dedup_reset']=control.call({'op':'clear_anon_download_dedup'})
    control.call({'op':'invalidate'});fresh=request('mutation after invalidation');assert fresh['body_sha256']!=first['body_sha256'];snapshot('mutation after invalidation')
    control.call({'op':'network','values':{'M3U_EPG':'192.0.2.0/24'}})
    request('normal network deny');request('normal HEAD network deny',method='HEAD');request('XC bypasses M3U_EPG',path='/xmltv.php',role='standard')
    control.call({'op':'network','values':{'M3U_EPG':'0.0.0.0/0,::/0','XC_API':'192.0.2.0/24'}})
    request('XC network deny',path='/xmltv.php',role='standard');request('normal ignores XC_API')
    control.call({'op':'network','values':{'XC_API':'0.0.0.0/0,::/0'}});snapshot('finished')
    return rows,snapshots

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--native-cache',action='store_true');parser.add_argument('--native-content',action='store_true');parser.add_argument('--native-http',action='store_true');parser.add_argument('--xc-epg-source',action='store_true');parser.add_argument('--native-xc-epg',action='store_true');parser.add_argument('--xc-epg-http-source',action='store_true');parser.add_argument('--native-xc-epg-http',action='store_true');parser.add_argument('--grid-datetime-fuzz',action='store_true');parser.add_argument('--shared-guide-cache',action='store_true');parser.add_argument('--cold-cache-diagnostics',action='store_true');args=parser.parse_args();args.shared_guide_cache=args.shared_guide_cache or args.cold_cache_diagnostics;args.native_http=args.native_http or args.shared_guide_cache;args.native_xc_epg=args.native_xc_epg or args.native_xc_epg_http;args.xc_epg_http_source=args.xc_epg_http_source or args.native_xc_epg_http;docker=shutil.which('docker');assert docker
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir())
    report={'scope':'actual original XMLTV HTTP/cache/real+defaultdummy characterization only','passed':False,'native_verified':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('control.py'),Path(__file__).with_name('render_source.py'),Path(__file__).with_name('cache_source.py'),Path(__file__).with_name('window_source.py'),Path(__file__).with_name('content_source.py'),Path(__file__).with_name('grid_datetime_fuzz.py'),ROOT/'scripts/rust-xc-contracts/control.py',ROOT/'scripts/rust-xc-contracts/epg_source.py',ROOT/'scripts/rust-xc-contracts/epg_http.py',ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py',ROOT/'scripts/rust-guide-perf/fixture.py']
    copies=out/'tools'
    for path in tools:target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    if args.native_cache or args.native_content or args.native_http or args.native_xc_epg:
        for path in (ROOT/'rust').rglob('*'):
            if path.is_file() and not set(path.relative_to(ROOT/'rust').parts)&{'target','.git','__pycache__'}:
                target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
        report['native_source_manifest_sha256']=hashlib.sha256(json.dumps({str(path.relative_to(copies/'rust')):hashlib.sha256(path.read_bytes()).hexdigest() for path in (copies/'rust').rglob('*') if path.is_file()},sort_keys=True,separators=(',',':')).encode()).hexdigest()
        (out/'frozen-source-ready.json').write_text(json.dumps({'rust_manifest_sha256':report['native_source_manifest_sha256']}))
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    project='dispatcharr-xmltv-'+uuid.uuid4().hex[:12];report['project']=project;control=None;native=None
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-xmltv-contracts')+':/xmltv-probe:ro',str(copies/'scripts/rust-xc-contracts')+':/xc-probe:ro',str(copies/'scripts/rust-guide-perf')+':/guide-probe:ro']}}}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656,6379,5432],'origin':[8080]})
    with fixture_source(report,out,'xmltv-source.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='900')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',overlay,ports] for value in ['-f',str(path)]]]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            command(compose+['up','-d','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xmltv-probe/control.py'],env,out/'control.log');report['cases'],report['cache_snapshots']=exercise(base,control,out);report['render_vectors']=control.call({'op':'render_vectors'});report['cache_vectors']=control.call({'op':'cache_vectors'});report['window_vectors']=control.call({'op':'window_vectors'});report['content_vectors']=control.call({'op':'content_vectors'});(out/'content-source.json').write_text(json.dumps(report['content_vectors'],ensure_ascii=False));report['passed']=True
            if args.grid_datetime_fuzz:
                fuzz=control.call({'op':'grid_datetime_fuzz','start':0,'count':64})
                for start in range(64,fuzz['total'],64):fuzz['cases'].extend(control.call({'op':'grid_datetime_fuzz','start':start,'count':64})['cases'])
                (out/'grid-datetime-source.json').write_text(json.dumps(fuzz,ensure_ascii=False));report['grid_datetime_source']={'cases':len(fuzz['cases']),'source_sha256':fuzz['source_sha256'],'django_dateparse_sha256':fuzz['django_dateparse_sha256']}
            if args.xc_epg_http_source:
                sys.path.insert(0,str(copies/'scripts/rust-xc-contracts'))
                from epg_http import exercise as epg_http_exercise
                report['xc_epg_http_original']=epg_http_exercise(base,control)
            if args.xc_epg_source or args.native_xc_epg:
                captured=control.call({'op':'xc_epg_vectors','start':0,'count':32});total=captured['total']
                for start in range(32,total,32):captured['cases'].extend(control.call({'op':'xc_epg_vectors','start':start,'count':32})['cases'])
                captured['hash_characterization']=control.call({'op':'xc_epg_hash'});(out/'xc-epg-source.json').write_text(json.dumps(captured,ensure_ascii=False));report['xc_epg_source']={'cases':len(captured['cases']),'source_sha256':captured['source_sha256'],'hash_characterization':captured['hash_characterization'],'native_verified':False}
            if args.native_cache or args.native_content or args.native_http or args.native_xc_epg:
                env_test=dict(env,CARGO_HOME=os.environ.get('CARGO_HOME','/tmp/dispatcharr-cargo'),RUSTUP_HOME=os.environ.get('RUSTUP_HOME','/tmp/dispatcharr-rustup'),RUSTUP_TOOLCHAIN='1.90.0',REDIS_TEST_URL='redis://:playback-fixture-only@'+command(compose+['port','baseline','6379']).strip()+'/0')
                env_test.update(DATABASE_TEST_URL='postgres://dispatch:playback-fixture-only@'+command(compose+['port','baseline','5432']).strip()+'/dispatcharr',XMLTV_CONTENT_SOURCE=str(out/'content-source.json'))
                cmd=['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xmltv-http','--lib','--','--ignored','--nocapture','--test-threads=1'];report['native_command']=cmd
                with (out/'native-cache-tests.log').open('w') as native_log:tested=subprocess.run(cmd,env=env_test,text=True,stdout=native_log,stderr=subprocess.STDOUT,timeout=180)
                report['native_cache_component_verified']=tested.returncode==0;assert tested.returncode==0,'native cache component tests failed'
                if args.native_xc_epg:
                    xc_command=['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xc-http','--lib','epg::tests','--','--ignored','--test-threads=1']
                    with (out/'native-xc-epg-tests.log').open('w')as xc_log:xc_tested=subprocess.run(xc_command,env=dict(env_test,XC_EPG_SOURCE=str(out/'xc-epg-source.json')),stdout=xc_log,stderr=subprocess.STDOUT,text=True,timeout=180)
                    report['native_xc_epg_verified']=xc_tested.returncode==0;assert xc_tested.returncode==0,'native XC EPG source corpus comparison failed'
                    resource_command=['/tmp/dispatcharr-cargo/bin/cargo','test','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xc-http','--lib','gateway::tests::epg_sql_cancellation_and_detached_body_keep_bounded_lifetimes','--','--ignored','--test-threads=1']
                    with (out/'native-xc-epg-resource-tests.log').open('w')as resource_log:resource_tested=subprocess.run(resource_command,env=dict(env_test,SECRET_KEY='playback-fixture-only'),stdout=resource_log,stderr=subprocess.STDOUT,text=True,timeout=90)
                    report['native_xc_epg_resources_verified']=resource_tested.returncode==0;assert resource_tested.returncode==0,'native XC EPG resource gate'
                    if args.native_xc_epg_http:
                        epg_build=['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xc-http','--example','epg_peer']
                        with (out/'native-xc-epg-peer-build.log').open('w')as epg_log:epg_built=subprocess.run(epg_build,env=env_test,stdout=epg_log,stderr=subprocess.STDOUT,text=True,timeout=180)
                        assert epg_built.returncode==0,'native XC EPG peer build'
                        executable=out/'xc-epg-peer';shutil.copy2(ROOT/'rust/target/debug/examples/epg_peer',executable)
                        with executable.open('rb')as elf:report['native_xc_epg_peer_sha256']=hashlib.file_digest(elf,'sha256').hexdigest()
                        native=Peer([str(executable)],dict(env_test,SECRET_KEY='playback-fixture-only'),out/'native-xc-epg-peer.log')
                        report['xc_epg_http_native']=epg_http_exercise('http://127.0.0.1:'+str(native.ready['port']),control)
                        report['xc_epg_http_differences']=[{'case':original['case'],'original':original,'native':actual}for original,actual in zip(report['xc_epg_http_original'],report['xc_epg_http_native'],strict=True)if original!=actual]
                        report['native_xc_epg_shutdown']=native.call({'op':'close'});native.close();native=None
                        assert report['native_xc_epg_shutdown']['drained'] and report['native_xc_epg_shutdown']['active_requests']==0,'native XC EPG shutdown'
                        assert not report['xc_epg_http_differences'],'native XC EPG HTTP mismatch'
                if args.native_http:
                    build=cmd[:cmd.index('test')]+['build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xmltv-http','--example','peer']
                    built=subprocess.run(build,env=env_test,text=True,capture_output=True,timeout=180);(out/'native-peer-build.log').write_text(built.stdout+built.stderr);assert built.returncode==0,'native peer build'
                    native=Peer([str(TARGET_DIR/'debug/examples/peer')],dict(env_test,SECRET_KEY='playback-fixture-only'),out/'native-peer.log')
                    # The component generation-isolation test intentionally advances
                    # the owned global counter. Prime the CURRENT generation with
                    # the actual Python endpoint before checking native cache reads.
                    conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=10);conn.request('GET','/output/epg',headers={'Host':'xmltv.fixture.invalid:8088'});primed=conn.getresponse();primed_body=primed.read(8*1024*1024);conn.close();assert primed.status==200
                    before=control.call({'op':'cache'})
                    conn=http.client.HTTPConnection('127.0.0.1',native.ready['port'],timeout=10);conn.request('GET','/output/epg',headers={'Host':'xmltv.fixture.invalid:8088'});legacy=conn.getresponse();legacy_body=legacy.read(8*1024*1024);conn.close()
                    after=control.call({'op':'cache'})
                    report['python_cache_interop']={'status':legacy.status,'body_sha256':hashlib.sha256(legacy_body).hexdigest(),'expected_sha256':hashlib.sha256(primed_body).hexdigest(),'actual_python_endpoint_primed_current_generation':True,'events_unchanged':before['epg_download_events']==after['epg_download_events']}
                    assert legacy.status==200 and report['python_cache_interop']['body_sha256']==report['python_cache_interop']['expected_sha256'] and report['python_cache_interop']['events_unchanged'],'Python-written cache interoperability'
                    report['phase_reset']=control.call({'op':'reset_phase'})
                    target=out/'native';target.mkdir()
                    report['native_cases'],report['native_cache_snapshots']=exercise('http://127.0.0.1:'+str(native.ready['port']),control,target)
                    differences=[]
                    for source_case,native_case in zip(report['cases'],report['native_cases'],strict=True):
                        keys=['case','status','headers','body_sha256','body_bytes','xml_complete','json','read_error']
                        changes={key:{'source':source_case.get(key),'native':native_case.get(key)}for key in keys if source_case.get(key)!=native_case.get(key)}
                        if changes:differences.append({'case':source_case['case'],'changes':changes})
                    report['event_count_comparison']=[{'after':source['after'],'source':[source['epg_download_events'],source['epg_blocked_events']],'native':[native_row['epg_download_events'],native_row['epg_blocked_events']]}for source,native_row in zip(report['cache_snapshots'],report['native_cache_snapshots'],strict=True)]
                    assert all(row['source']==row['native']for row in report['event_count_comparison']),'Source audit/dedup event counts'
                    report['native_http_differences']=differences
                    report['native_shutdown']=native.call({'op':'close'});native.close();native=None
                    assert report['native_shutdown']['drained'] and report['native_shutdown']['active_requests']==0,'native shutdown'
                    assert not differences,'native HTTP contract mismatch'
                    report['native_http_component_verified']=True
                    if args.shared_guide_cache:
                        probe_build=['/tmp/dispatcharr-cargo/bin/cargo','build','--manifest-path',str(copies/'rust/Cargo.toml'),'--target-dir',str(TARGET_DIR),'--locked','--offline','-p','dispatcharr-xmltv-http','--example','cache_probe']
                        with (out/'cache-probe-build.log').open('w')as log:
                            built=subprocess.run(probe_build,env=env_test,stdout=log,stderr=subprocess.STDOUT,text=True,timeout=180)
                        built.check_returncode()
                        def guide_helper(op):
                            result=subprocess.run(compose+['exec','-T','-e','GUIDE_PERF_DISPOSABLE=playback-fixture-only','baseline','/dispatcharrpy/bin/python','/guide-probe/fixture.py'],env=env,text=True,input=json.dumps({'op':op}),capture_output=True,timeout=120,check=True)
                            return json.loads(result.stdout.splitlines()[-1])
                        report['shared_guide_fixture_counts']={key:value for key,value in guide_helper('seed').items()if key in ['channels','guides','stored_programmes']}
                        guide_helper('clear_cache')
                        def guide_request(target_base,validate=True):
                            conn=http.client.HTTPConnection(target_base.removeprefix('http://'),timeout=30)
                            conn.request('GET','/output/epg?days=1&prev_days=0',headers={'Host':'guide-perf.invalid:8088','Accept-Encoding':'identity'})
                            response=conn.getresponse();data=response.read(32*1024*1024+1);conn.close()
                            assert response.status==200 and len(data)<=32*1024*1024
                            if validate:
                                tree=ET.fromstring(data);assert len(tree.findall('channel'))==500 and len(tree.findall('programme'))==22500
                            return {'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
                        expected_guide=guide_request(base)
                        def probe_cache(label):
                            before=guide_helper('cache_state')
                            cache_base='epg_content:all:anonymous:d=1:p=0:logos=True:tvgid=channel_number:origin=http://guide-perf.invalid:8088:g='+before['generation']
                            result=subprocess.run([str(TARGET_DIR/'debug/examples/cache_probe')],env=dict(env_test,XMLTV_CACHE_DISPOSABLE_FIXTURE='playback-fixture-only'),text=True,input=json.dumps({'base':cache_base}),capture_output=True,timeout=60)
                            (out/(label+'-cache-probe.log')).write_text(result.stdout+'\n'+result.stderr);result.check_returncode()
                            data=json.loads(result.stdout.splitlines()[-1]);after=guide_helper('cache_state')
                            assert all(before[key]==after[key]for key in ['generation','chunks','bytes','ready','status','locked'])
                            assert all(row['bytes']==expected_guide['bytes'] and row['sha256']==expected_guide['sha256']for row in data['policies'])
                            return data
                        report['source_shared_guide_ready_cache']=probe_cache('source-shared-guide')
                        def cold_phase(target_base,label,peer=None):
                            guide_helper('clear_cache')
                            before=control.call({'op':'redis_commandstats'})
                            local_before=peer.call({'op':'stats'})if peer else None
                            barrier=threading.Barrier(10)
                            def fetch(_):
                                barrier.wait(timeout=10)
                                return guide_request(target_base,validate=False)
                            with concurrent.futures.ThreadPoolExecutor(max_workers=10)as pool:
                                responses=list(pool.map(fetch,range(10)))
                            assert all(value==expected_guide for value in responses)
                            after=control.call({'op':'redis_commandstats'})
                            local_after=peer.call({'op':'stats'})if peer else None
                            delta={key:{field:after['commands'].get(key,{}).get(field,0)-before['commands'].get(key,{}).get(field,0)for field in ['calls','usec']}for key in set(before['commands'])|set(after['commands'])}
                            row={'scope':'Functional actual10-reader cold-cache diagnostic; not performance acceptance','backend':label,'consumers':10,'body':expected_guide,'redis_command_deltas':delta,'local_before':local_before,'local_after':local_after,'cache_after':guide_helper('cache_state')}
                            (out/(label+'-cold10-diagnostic.json')).write_text(json.dumps(row,indent=2)+'\n')
                            return row
                        if args.cold_cache_diagnostics:report['source_cold10_diagnostic']=cold_phase(base,'source')
                        guide_helper('clear_cache')
                        native=Peer([str(TARGET_DIR/'debug/examples/peer')],dict(env_test,SECRET_KEY='playback-fixture-only'),out/'native-shared-guide-peer.log')
                        actual_guide=guide_request('http://127.0.0.1:'+str(native.ready['port']));assert actual_guide==expected_guide
                        report['native_shared_guide_ready_cache']=probe_cache('native-shared-guide')
                        if args.cold_cache_diagnostics:
                            report['native_cold10_diagnostic']=cold_phase('http://127.0.0.1:'+str(native.ready['port']),'native',native)
                        report['shared_guide_complete_xml']=actual_guide
                        report['native_shared_guide_shutdown']=native.call({'op':'close'});native.close();native=None
                        assert report['native_shared_guide_shutdown']['drained']
                        report['shared_guide_ready_diagnostics_verified']=True

        except Exception as error:report.update(passed=False,failure=type(error).__name__,failure_message=str(error))
        finally:
            if native:native.close()
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='owned Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'cases':len(report.get('cases',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
