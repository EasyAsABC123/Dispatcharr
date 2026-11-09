#!/usr/bin/env python3
"""Frozen packaged HDHR HTTP original/native comparison; original author credit retained."""
import argparse,hashlib,json,os,re,shutil,subprocess,sys,time,uuid
from pathlib import Path
from urllib.parse import urlsplit
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'scripts/rust-integration'),str(ROOT/'scripts/rust-realtime-contracts')]
from shared_ts import BASELINE,IMAGE,fixture_source,http,wait,Reader
from fixture_ports import write_port_overlay
from interop import Peer
from probe import exercise

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);p.add_argument('--docker',default='docker');p.add_argument('--candidate-image',required=True);p.add_argument('--candidate-build-manifest',type=Path,required=True);args=p.parse_args();docker=shutil.which(args.docker);assert docker
    if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9._:/@-]{0,255}',args.candidate_image):p.error('invalid image')
    manifest_raw=args.candidate_build_manifest.read_bytes();manifest=json.loads(manifest_raw);hashes=manifest['source_sha256'];assert manifest['passed'] and hashlib.sha256(json.dumps(hashes,sort_keys=True,separators=(',',':')).encode()).hexdigest()==manifest['source_tree_sha256']
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=True);assert not any(out.iterdir());(out/'candidate-build.json').write_bytes(manifest_raw)
    report={'scope':'packaged HDHR original/native HTTP and generated native TS URL decoder proof','passed':False,'baseline_commit':BASELINE,'baseline_image':IMAGE,'candidate_image':args.candidate_image,'candidate_source_tree_sha256':manifest['source_tree_sha256'],'candidate_build_manifest_sha256':hashlib.sha256(manifest_raw).hexdigest(),'application_performance_verified':False,'milestone_complete':False,'commands':[]}
    tools=[Path(__file__).resolve(),Path(__file__).with_name('probe.py'),Path(__file__).with_name('control.py'),ROOT/'scripts/rust-realtime-contracts/interop.py',ROOT/'scripts/rust-integration/shared_ts.py',ROOT/'scripts/rust-integration/fixture_ports.py',ROOT/'docker/compose.rust-playback.yml',ROOT/'docker/compose.rust-candidate.yml',*[ROOT/'scripts/rust-playback-baseline'/name for name in ['seed.py','origin.py','range_http.py','start.sh','supervisor.py','uwsgi.ini']],ROOT/'scripts/rust-baseline/dependencies.py']
    copies=out/'tools'
    for path in tools:
        target=copies/path.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(path,target)
    report['tool_sha256']={str(path.relative_to(ROOT)):hashlib.sha256((copies/path.relative_to(ROOT)).read_bytes()).hexdigest() for path in tools}
    project='dispatcharr-hdhr-image-'+uuid.uuid4().hex[:12];report['project']=project
    overlay=out/'overlay.json';overlay.write_text(json.dumps({'services':{'baseline':{'volumes':[str(copies/'scripts/rust-hdhr-contracts')+':/hdhr-probe:ro']},'candidate':{'image':args.candidate_image}}}));ports=out/'ports.yml';report['ports']=write_port_overlay(ports,{'baseline':[5656],'origin':[8080],'candidate':[9192]});control=None;reader=None
    with fixture_source(report,out,'hdhr-image.json') as temp:
        source=Path(temp)/'source';source.mkdir();archive=Path(temp)/'source.tar'
        with archive.open('wb') as file:subprocess.run(['git','archive',BASELINE],cwd=ROOT,stdout=file,check=True,timeout=30)
        report['baseline_archive_sha256']=hashlib.sha256(archive.read_bytes()).hexdigest();subprocess.run(['tar','-xf',str(archive),'-C',str(source),'--no-same-owner'],check=True);(source/'media').mkdir(exist_ok=True)
        env=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(out),FIXTURE_SECONDS='180',FIXTURE_CODEC='h264')
        compose=[docker,'compose','-p',project,*[value for path in [copies/'docker/compose.rust-playback.yml',copies/'docker/compose.rust-candidate.yml',overlay,ports] for value in ['-f',str(path)]]]
        def command(values,timeout=120):
            report['commands'].append(values);return subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,check=True,timeout=timeout).stdout
        try:
            inspected=json.loads(command([docker,'image','inspect',args.candidate_image]))[0];assert inspected['Id']==manifest['image_id'];assert inspected['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==manifest['source_tree_sha256'];report['candidate_image_id']=inspected['Id']
            command(compose+['up','-d','--no-build','baseline','origin'],180);base='http://'+command(compose+['port','baseline','5656']).strip();wait(lambda:(out/'seed.json').exists() and http(base+'/proxy/ts/stream/missing')[0]==404,300,'original ready')
            control=Peer(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','/hdhr-probe/control.py'],env,out/'control.log');report['expected']=[];exercise(base,control,report['expected'])
            command(compose+['up','-d','--no-build','candidate'],120);native='http://'+command(compose+['port','candidate','9192']).strip();wait(lambda:http(native+'/health/ready')[0]==200,60,'native ready');report['actual']=[];exercise(native,control,report['actual'])
            def normalize(rows):
                rows=json.loads(json.dumps(rows))
                for row in rows:row['headers'].pop('content-length',None)
                return rows
            report['differences']=[{'case':expected['name'],'expected':expected,'actual':actual} for expected,actual in zip(normalize(report['expected']),normalize(report['actual'])) if expected!=actual];assert len(report['expected'])==len(report['actual']);assert not report['differences'],str(len(report['differences']))+' differing cases'
            lineup=next(row['json'] for row in report['actual'] if row['name']=='/hdhr/lineup.json');url=urlsplit(lineup[0]['URL']);sample=out/'generated-lineup.ts';reader=Reader(native+url.path,sample);wait(lambda:reader.snapshot()['media_bytes']>=1024*1024,60,'generated lineup stream body');reader.close();report['media_reader']=reader.snapshot();assert report['media_reader']['sync_errors']==0
            def media_command(values,name):
                report['commands'].append(values);result=subprocess.run(values,cwd=ROOT,env=env,text=True,capture_output=True,timeout=30);(out/(name+'.stdout')).write_text(result.stdout);(out/(name+'.stderr')).write_text(result.stderr);assert result.returncode==0;return result
            probed=media_command(compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json','/evidence/generated-lineup.ts'],'ffprobe');probe=json.loads(probed.stdout);report['media_streams']=probe;report['ffprobe_tail_stderr_bytes']=len(probed.stderr);assert {'video','audio'}<={stream['codec_type'] for stream in probe['streams']}
            decoded=media_command(compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i','/evidence/generated-lineup.ts','-t','3','-map','0:v:0','-map','0:a:0','-f','null','-'],'complete-prefix-decode');frames=max([int(line.split('=')[1]) for line in decoded.stdout.splitlines() if line.startswith('frame=')],default=0);assert frames>=50 and not decoded.stderr.strip();report['decode']={'frames':frames,'video_and_audio':True,'stderr_bytes':len(decoded.stderr),'sample_sha256':hashlib.sha256(sample.read_bytes()).hexdigest()}
            started=time.monotonic();command(compose+['stop','--timeout','15','candidate'],30);report['shutdown_seconds']=time.monotonic()-started;logs=command(compose+['logs','--no-color','candidate']);(out/'candidate.log').write_text(logs);assert 'drain deadline reached' not in logs;container=command(compose+['ps','-aq','candidate']).strip();info=json.loads(command([docker,'inspect',container]))[0]['State'];assert info['ExitCode']==0 and not info['OOMKilled'];report['candidate_shutdown']={key:info[key] for key in ['ExitCode','OOMKilled']};report['passed']=True
        except Exception as error:report['failure']=type(error).__name__;report['failure_message']=str(error)
        finally:
            if reader:reader.close()
            if control:control.close()
            try:command(compose+['down','--volumes','--remove-orphans','--timeout','5'],60);report['cleanup']='isolated Compose project removed'
            except Exception:report['cleanup_error']=True;report['passed']=False
    print(json.dumps({'passed':report['passed'],'cases':len(report.get('actual',[])),'output':str(out)}));return 0 if report['passed'] else 1
if __name__=='__main__':raise SystemExit(main())
