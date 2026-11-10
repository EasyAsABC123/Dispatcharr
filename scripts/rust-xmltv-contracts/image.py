#!/usr/bin/env python3
"""Packaged XMLTV original/native HTTP/cache comparison. Original author credit.

App images are built by the parent milestone runner, never by this probe.
"""
import argparse,hashlib,http.client,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(Path(__file__).resolve().parent),str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http as ready_http,wait
from fixture_ports import write_port_overlay
from interop import Peer
from source import exercise

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--candidate-image',required=True)
    parser.add_argument('--candidate-build-manifest',type=Path,required=True)
    args=parser.parse_args();docker=shutil.which('docker');assert docker
    assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',args.candidate_image)
    raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(raw)
    assert manifest['passed'] and hashlib.sha256(json.dumps(manifest['source_sha256'],sort_keys=True,separators=(',',':')).encode()).hexdigest()==manifest['source_tree_sha256']
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir())
    (out/'candidate-build.json').write_bytes(raw)
    report={'scope':'Packaged XMLTV47 actual original/native HTTP and shared Python-cache replay; custom source-property regex excluded','passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'candidate_image':args.candidate_image,'candidate_source_tree_sha256':manifest['source_tree_sha256'],'candidate_build_manifest_sha256':hashlib.sha256(raw).hexdigest(),'application_performance_verified':False,'milestone_complete':False,'commands':[]}
    tools=[Path(__file__).resolve(),*[Path(__file__).with_name(name)for name in ['source.py','control.py','render_source.py','cache_source.py','window_source.py','content_source.py']],ROOT/'scripts/rust-xc-contracts/control.py',ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest()for path in tools}
    project='dispatcharr-xmltv-image-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-xmltv-contracts')+':/xmltv-probe:ro',str(copies/'scripts/rust-xc-contracts')+':/xc-probe:ro']},'candidate':{'image':args.candidate_image}}}))
    ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080],'candidate':[9192]});control=None
    with fixture_source(report,out,'xmltv-image.json') as temporary:
        source=Path(temporary)/'source';source.mkdir();archive=Path(temporary)/'source.tar'
        with archive.open('wb')as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='180')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',copies/'docker/compose.rust-candidate.yml',overlay,ports]for value in ['-f',str(path)]]]
        def command(values,timeout=120):
            report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        def legacy_request(base):
            conn=http.client.HTTPConnection(base.removeprefix('http://'),timeout=15);conn.request('GET','/output/epg',headers={'Host':'xmltv.fixture.invalid:8088'});response=conn.getresponse();body=response.read(8*1024*1024);assert len(body)<8*1024*1024;conn.close();return response.status,hashlib.sha256(body).hexdigest()
        try:
            inspected=json.loads(command([docker,'image','inspect',args.candidate_image]))[0]
            assert inspected['Id']==manifest['image_id'] and inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==manifest['source_tree_sha256'];report['candidate_image_id']=inspected['Id']
            command(compose+['up','-d','--no-build','baseline','origin'],180)
            base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists()and ready_http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/xmltv-probe/control.py'],env,out/'control.log')
            expected=out/'original';expected.mkdir();report['original_cases'],report['original_cache_snapshots']=exercise(base,control,expected)
            command(compose+['up','-d','--no-build','candidate'],120);target='http://'+command(compose+['port','candidate','9192']).strip();wait(lambda:ready_http(target+'/health/ready')[0]==200,60,'native ready')
            status,sha=legacy_request(base);assert status==200;before=control.call({'op':'cache'});native_status,native_sha=legacy_request(target);after=control.call({'op':'cache'})
            report['python_cache_interop']={'source_status':status,'native_status':native_status,'source_sha256':sha,'native_sha256':native_sha,'events_unchanged':before['epg_download_events']==after['epg_download_events']};assert native_status==200 and sha==native_sha and report['python_cache_interop']['events_unchanged']
            report['phase_reset']=control.call({'op':'reset_phase'})
            actual=out/'native';actual.mkdir();report['native_cases'],report['native_cache_snapshots']=exercise(target,control,actual)
            differences=[]
            for original,native in zip(report['original_cases'],report['native_cases'],strict=True):
                changes={key:{'original':original.get(key),'native':native.get(key)}for key in ['case','status','headers','body_sha256','body_bytes','xml_complete','json','read_error']if original.get(key)!=native.get(key)}
                if changes:differences.append({'case':original['case'],'changes':changes})
            report['differences']=differences;assert not differences,'packaged XMLTV HTTP mismatch'
            report['event_count_comparison']=[{'after':original['after'],'original':[original['epg_download_events'],original['epg_blocked_events']],'native':[native['epg_download_events'],native['epg_blocked_events']]}for original,native in zip(report['original_cache_snapshots'],report['native_cache_snapshots'],strict=True)]
            assert all(row['original']==row['native']for row in report['event_count_comparison']),'audit count mismatch'
            started=time.monotonic();command(compose+['stop','--timeout','15','candidate'],30);report['shutdown_seconds']=time.monotonic()-started
            logs=command(compose+['logs','--no-color','candidate']);(out/'candidate.log').write_text(logs);assert 'drain deadline reached'not in logs
            container=command(compose+['ps','-aq','candidate']).strip();state=json.loads(command([docker,'inspect',container]))[0]['State'];assert state['ExitCode']==0 and not state['OOMKilled'];report['candidate_shutdown']={key:state[key]for key in ['ExitCode','OOMKilled']};report['passed']=True
        except Exception as error:report.update(failure=type(error).__name__,failure_message=str(error))
        finally:
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='owned Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'cases':len(report.get('native_cases',[])),'output':str(out)}));return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
