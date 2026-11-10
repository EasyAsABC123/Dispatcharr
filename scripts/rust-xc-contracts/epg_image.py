#!/usr/bin/env python3
"""Immutable packaged XC EPG750/HTTP88/resource probe. Original author credit."""
import argparse,hashlib,http.client,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
from urllib.parse import urlencode
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(Path(__file__).resolve().parent),str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer
from epg_http import exercise

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--candidate-image',required=True);parser.add_argument('--candidate-build-manifest',type=Path,required=True);args=parser.parse_args();docker=shutil.which('docker');assert docker
    assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',args.candidate_image)
    raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw);assert manifest['passed'] and hashlib.sha256(json.dumps(manifest['source_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest()==manifest['source_tree_sha256']
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir());(out/'candidate-build.json').write_bytes(raw)
    report={'scope':'Packaged XC short/simple EPG750 original helper cases,88 original HTTP cases, physical admission/cancellation and response cap; custom regex excluded','passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'candidate_image':args.candidate_image,'candidate_source_tree_sha256':manifest['source_tree_sha256'],'candidate_build_manifest_sha256':hashlib.sha256(raw).hexdigest(),'performance_verified':False,'milestone_complete':False,'commands':[]}
    files=[Path(__file__).resolve(),*[ROOT/'scripts/rust-xmltv-contracts'/name for name in ['control.py','render_source.py','cache_source.py','window_source.py','content_source.py']],*[Path(__file__).with_name(name)for name in ['control.py','epg_source.py','epg_http.py']],ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in files:target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest()for path in files}
    project='dispatcharr-xc-epg-image-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-xmltv-contracts')+':/xmltv-probe:ro',str(copies/'scripts/rust-xc-contracts')+':/xc-probe:ro']},'candidate':{'image':args.candidate_image,'environment':{'RUST_MAX_XC_REQUESTS':'2'}},'candidate-small':{'extends':{'file':str(copies/'docker/compose.rust-candidate.yml'),'service':'candidate'},'image':args.candidate_image,'environment':{'RUST_MAX_XC_REQUESTS':'2','RUST_XC_RESPONSE_BYTES':'64','PYTHONHASHSEED':'1'}}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080],'candidate':[9192],'candidate-small':[9192]});control=None;connections=[]
    with fixture_source(report,out,'xc-epg-image.json')as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb')as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='180')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',copies/'docker/compose.rust-candidate.yml',overlay,ports]for value in ['-f',str(path)]]]
        def command(values,timeout=120):report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def fetch(base,parameters):
            conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=15);conn.request('GET','/player_api.php?'+urlencode(parameters),headers={'Host':'xc.fixture.invalid:8088'});response=conn.getresponse();body=response.read(16*1024*1024);assert len(body)<16*1024*1024;conn.close();return response.status,body
        try:
            info=json.loads(command([docker,'image','inspect',args.candidate_image]))[0];assert info['Id']==manifest['image_id']and info['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==manifest['source_tree_sha256'];report['candidate_image_id']=info['Id']
            command(compose+['up','-d','--no-build','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists()and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xmltv-probe/control.py'],env,out/'control.log');seed=control.call({'op':'seed'})
            corpus=control.call({'op':'xc_epg_vectors','start':0,'count':32})
            for start in range(32,corpus['total'],32):corpus['cases'].extend(control.call({'op':'xc_epg_vectors','start':start,'count':32})['cases'])
            assert len(corpus['cases'])==750;(out/'original-helper.json').write_text(json.dumps(corpus,ensure_ascii=False));report['original_source_sha256']=corpus['source_sha256']
            report['original_http']=exercise(base,control)
            command(compose+['up','-d','--no-build','candidate'],120);target='http://'+command(compose+['port','candidate','9192']).strip();wait(lambda:ready_http(target+'/health/ready')[0]==200,60,'native ready')
            report['native_http']=exercise(target,control);report['http_differences']=[{'case':original['case'],'original':original,'native':actual}for original,actual in zip(report['original_http'],report['native_http'],strict=True)if original!=actual];assert not report['http_differences'],'packaged88 HTTP mismatch'
            report['helper_http_checks']=[]
            for index,case in enumerate(corpus['cases']):
                account=seed['users'][case['role']];parameters=[*[(key,value)for key,value in case['params']],('action','get_short_epg'if case['short']else'get_simple_data_table'),('username',account['username']),('password',account['password'])];status,body=fetch(target,parameters)
                if 'error'in case:
                    expected=404 if case['error']=='Http404' else 500;reference=next(row for row in report['original_http']if row['status']==expected);assert status==expected and hashlib.sha256(body).hexdigest()==reference['body_sha256'],f'helper error case{index}'
                else:
                    assert status==200,f'helper status case{index}'
                    if case['synthetic_indices']:
                        value=json.loads(body)
                        for at in case['synthetic_indices']:
                            row=value['epg_listings'][at];assert isinstance(row['id'],str)and row['id'].isdigit()and int(row['id'])<=2**63;row['id']='<process-random-synthetic>'
                        comparison=json.dumps(value)
                    else:comparison=body.decode('utf-8')
                    assert comparison==case['comparison_json'],f'helper payload case{index}'
                report['helper_http_checks'].append({'case':index,'status':status,'passed':True})
            account=seed['users']['standard'];credentials=[('username',account['username']),('password',account['password'])];epg=[*credentials,('action','get_short_epg'),('stream_id',str(seed['channels'][0]))]
            report['metadata_lock']=control.call({'op':'lock_xc_epg_metadata'})
            for _ in range(2):
                conn=http.client.HTTPConnection(target.removeprefix('http://'),timeout=10);conn.request('GET','/player_api.php?'+urlencode(epg));connections.append(conn)
            wait(lambda:control.call({'op':'xc_epg_waiting'})['waiting']==2,5,'two physical native metadata waiters')
            connections[0].close();time.sleep(.05);report['cancelled_socket_waiters']=control.call({'op':'xc_epg_waiting'})['waiting'];assert report['cancelled_socket_waiters']==2
            status,body=fetch(target,credentials);report['protected_admission']={'status':status,'configured_requests':2,'physical_waiters_after_cancel':2};assert status==503
            report['metadata_unlock']=control.call({'op':'unlock_xc_epg_metadata'});response=connections[1].getresponse();assert response.status==200;response.read(1024*1024);connections[1].close();connections=[]
            wait(lambda:control.call({'op':'xc_epg_waiting'})['waiting']==0,5,'physical workers recover');assert fetch(target,epg)[0]==200
            command(compose+['up','-d','--no-build','candidate-small'],120);small='http://'+command(compose+['port','candidate-small','9192']).strip();wait(lambda:ready_http(small+'/health/ready')[0]==200,60,'small-budget native ready');status,body=fetch(small,epg);report['response_cap']={'configured_bytes':64,'status':status};assert status==503
            status,body=fetch(small,[*credentials,('action','get_short_epg'),('stream_id',str(seed['channels'][3]))]);report['nonzero_hash_guard']={'configured_seed':'1','status':status};assert status==501
            started=time.monotonic();command(compose+['stop','--timeout','15','candidate','candidate-small'],30);report['shutdown_seconds']=time.monotonic()-started
            logs=command(compose+['logs','--no-color','candidate','candidate-small']);(out/'candidate.log').write_text(logs);assert 'drain deadline reached'not in logs
            report['shutdown_states']=[]
            for service in ['candidate','candidate-small']:
                container=command(compose+['ps','-aq',service]).strip();state=json.loads(command([docker,'inspect',container]))[0]['State'];assert state['ExitCode']==0 and not state['OOMKilled'];report['shutdown_states'].append({'service':service,'ExitCode':state['ExitCode'],'OOMKilled':state['OOMKilled']})
            report['passed']=True
        except Exception as error:report.update(failure=type(error).__name__,failure_message=str(error))
        finally:
            for conn in connections:conn.close()
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='owned Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'helper_cases':len(report.get('helper_http_checks',[])),'http_cases':len(report.get('native_http',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
