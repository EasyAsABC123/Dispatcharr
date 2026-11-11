#!/usr/bin/env python3
"""Isolated transition prototype from exact170 base plus selected overlay only.
Original Dispatcharr and redis-rs authors retain application/client credit.
"""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import tarfile
import time
import uuid
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'


def sha(data):return hashlib.sha256(data).hexdigest()
def main():
    p=argparse.ArgumentParser(description=__doc__)
    base_group=p.add_mutually_exclusive_group(required=True)
    base_group.add_argument('--base-proof',type=Path)
    base_group.add_argument('--git-revision')
    p.add_argument('--overlay',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--only-target',action='append',choices=['lookup_read','recovery_generation','recovery_generation_physical','generation_scope','generation_scope_physical','http_recovery','vod','session_diagnostic'])
    a=p.parse_args();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    overlay=json.loads(a.overlay.read_text())
    if a.git_revision:
        revision=subprocess.check_output(['git','rev-parse',a.git_revision+'^{commit}'],text=True).strip()
        if overlay.get('base_git_revision')!=revision:raise ValueError('Wrong committed base')
        archive=subprocess.check_output(['git','archive',revision,'rust'])
        with tarfile.open(fileobj=io.BytesIO(archive))as source:
            captured={member.name:source.extractfile(member).read()for member in source if member.isfile()}
        base={'git_revision':revision,'rust_source_archive_sha256':sha(archive),
              'rust_source_sha256':{name:sha(data)for name,data in captured.items()}}
    else:
        base=json.loads(a.base_proof.read_text())
        if not base['passed'] or not base['cleanup_passed']:raise ValueError('Passing immutable base required')
        if overlay['base_archive_sha256']!=base['rust_source_archive_sha256']:raise ValueError('Wrong base')
        archive=(a.base_proof.resolve().parent/'rust-source.tar').read_bytes()
        if sha(archive)!=base['rust_source_archive_sha256']:raise ValueError('Changed base archive')
        with tarfile.open(fileobj=io.BytesIO(archive))as source:
            captured={member.name:source.extractfile(member).read()for member in source if member.isfile()}
        if {name:sha(data)for name,data in captured.items()}!=base['rust_source_sha256']:raise ValueError('Invalid base map')
    for item in overlay['selected_files']:
        target=PurePosixPath(item['path'])
        if target.is_absolute()or '..'in target.parts or target.parts[0]!='rust':raise ValueError('Invalid overlay target')
        data=Path(item['frozen_path']).read_bytes()
        if sha(data)!=item['sha256']:raise ValueError('Changed explicit overlay')
        captured[str(target)]=data
    source=out/'native-source';source.mkdir()
    with tarfile.open(out/'rust-source.tar','w')as archive:
        for name,data in sorted(captured.items()):
            path=source/name;path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(data)
            item=tarfile.TarInfo(name);item.size=len(data);item.mode=0o644;item.mtime=0;archive.addfile(item,io.BytesIO(data))
    tools=out/'tool-source';tools.mkdir();shutil.copyfile(__file__,tools/Path(__file__).name)
    (out/'overlay.json').write_bytes(a.overlay.read_bytes())
    report={'passed':False,'scope':'Isolated app-unwired generation transition prototype; no actual HTTP/body/event recovery claim',
        'base_proof_sha256':sha(a.base_proof.read_bytes())if a.base_proof else None,'base_git_revision':(base.get('git_revision')or base.get('base_git_revision')),'base_archive_sha256':base['rust_source_archive_sha256'],
        'explicit_overlay':overlay,'rust_source_sha256':{name:sha(data)for name,data in captured.items()},
        'rust_source_archive_sha256':sha((out/'rust-source.tar').read_bytes()),'runner_sha256':sha(Path(__file__).read_bytes()),
        'image':IMAGE,'tests':[],'performance_verified':False}
    (out/'frozen-source-ready.json').write_text(json.dumps({'closed':True,'sha256':report['rust_source_archive_sha256']}))
    binaries=out/'binaries';binaries.mkdir(mode=0o755);binaries.chmod(0o755)
    env=dict(os.environ,CARGO_TARGET_DIR=str(out/'cargo-target'),CARGO_PROFILE_DEV_DEBUG='0',CARGO_PROFILE_TEST_DEBUG='0',CARGO_INCREMENTAL='0')
    report['build_environment']={key:env[key]for key in ['CARGO_TARGET_DIR','CARGO_PROFILE_DEV_DEBUG','CARGO_PROFILE_TEST_DEBUG','CARGO_INCREMENTAL']}
    name='dispatcharr-generation-'+uuid.uuid4().hex[:12]
    try:
        targets=[('dispatcharr-vod-http','lookup_read',6),('dispatcharr-coordination','redis',10),('dispatcharr-coordination','acquire_cancel',2),
                 ('dispatcharr-clients','vod',2),('dispatcharr-vod-http','recovery_generation',11),('dispatcharr-vod-http','recovery_generation_physical',2),('dispatcharr-vod-http','generation_scope',12),('dispatcharr-vod-http','generation_scope_physical',8),('dispatcharr-vod-http','http_recovery',4),('dispatcharr-vod-http','session_diagnostic',1)]
        if a.only_target:targets=[item for item in targets if item[1]in a.only_target]
        report['selected_targets']=[item[1]for item in targets]
        for package,target,count in targets:
            built=subprocess.run(['cargo','test','--manifest-path',str(source/'rust/Cargo.toml'),'-p',package,
                *(['--lib']if target in ['session_diagnostic','lookup_read']else['--test',target]),'--no-run','--message-format=json','--locked','--offline'],env=env,capture_output=True,text=True,timeout=600)
            (out/(target+'-build.jsonl')).write_text(built.stdout);(out/(target+'-build.log')).write_text(built.stderr);built.check_returncode()
            paths=[json.loads(line)['executable']for line in built.stdout.splitlines()if line.startswith('{')and json.loads(line).get('reason')=='compiler-artifact'and json.loads(line).get('executable')]
            if len(paths)!=1:raise ValueError('One binary expected')
            shutil.copyfile(paths[0],binaries/target);(binaries/target).chmod(0o755)
            report['tests'].append({'target':target,'expected_count':count,'binary_sha256':sha((binaries/target).read_bytes())})
        media_mounts=[]
        if any(test['target']=='http_recovery'for test in report['tests']):
            media=out/'media';media.mkdir(mode=0o777);media.chmod(0o777)
            evidence=out/'response-prefixes';evidence.mkdir(mode=0o777);evidence.chmod(0o777)
            ffmpeg=['-hide_banner','-loglevel','error','-f','lavfi','-i','testsrc2=size=320x180:rate=25',
                    '-f','lavfi','-i','sine=frequency=440:sample_rate=48000','-t','30','-c:v','libx264','-preset','ultrafast',
                    '-threads','1','-pix_fmt','yuv420p','-g','50','-keyint_min','50','-sc_threshold','0','-b:v','1000k',
                    '-c:a','aac','-b:a','128k','-flags:v','+bitexact','-flags:a','+bitexact','-fflags','+bitexact',
                    '-movflags','+faststart','-f','mp4','/media/fixture.mp4']
            fixture_docker=['docker','run','--rm','--network','none','--read-only','--user','65532:65532','--cpus','1','--memory','256m',
                            '--tmpfs','/tmp:rw,size=16m,mode=1777','--mount',f'type=bind,src={media},dst=/media']
            generated=subprocess.run(fixture_docker+['--entrypoint','ffmpeg',IMAGE]+ffmpeg,env=env,capture_output=True,text=True,timeout=90)
            (out/'fixture-generation.log').write_text(generated.stdout+generated.stderr);generated.check_returncode()
            report['media_fixture']={'image':IMAGE,'command':['ffmpeg']+ffmpeg,'sha256':sha((media/'fixture.mp4').read_bytes()),'bytes':(media/'fixture.mp4').stat().st_size}
            media_mounts=['--mount',f'type=bind,src={media},dst=/media,readonly','--mount',f'type=bind,src={evidence},dst=/evidence',
                          '-e','VOD_RECOVERY_MEDIA=/media/fixture.mp4']
            report['scope']='Actual loopback HTTP/valid MP4 key-loss opt-in recovery; fixture resolver only, no SQL/auth/real-server-restart/full-application acceptance'
        subprocess.run(['docker','run','-d','--rm','--name',name,'--network','none','--read-only','--user','65532:65532',
            '--cpus','1','--memory','256m','--tmpfs','/tmp:rw,size=16m,mode=1777','--workdir','/tmp',
            '--mount',f'type=bind,src={binaries},dst=/native-peers,readonly',
            '-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0',
            '-e','REDIS_RESTART_DISPOSABLE_FIXTURE=playback-fixture-only','-e','RESTART_PROBE_TASK=owned_redis_restart_diagnostic']+media_mounts+['--entrypoint','redis-server',IMAGE,
            '--save','','--appendonly','no','--bind','127.0.0.1','--requirepass','playback-fixture-only'],
            env=env,check=True,capture_output=True,timeout=30)
        for attempt in range(100):
            ready=subprocess.run(['docker','exec','-e','REDISCLI_AUTH=playback-fixture-only',name,'redis-cli','--no-auth-warning','ping'],
                env=env,capture_output=True,text=True,timeout=5)
            if ready.returncode==0 and ready.stdout.strip()=='PONG':break
            time.sleep(.05)
        else:raise TimeoutError('Redis readiness')
        for test in report['tests']:
            result=subprocess.run(['docker','exec',name,'/native-peers/'+test['target'],'--ignored','--nocapture','--test-threads=1']+(['physical_']if test['target']=='recovery_generation_physical'else['local_scope_']if test['target']=='generation_scope'else['local_physical_']if test['target']=='generation_scope_physical'else['receipt_actor_failure_']if test['target']=='session_diagnostic'else['lookup_read_failure_']if test['target']=='lookup_read'else[]),
                env=env,capture_output=True,text=True,timeout=180)
            text=result.stdout+result.stderr;(out/(test['target']+'-tests.log')).write_text(text)
            test['passed']=result.returncode==0 and f"test result: ok. {test['expected_count']} passed;"in text
        if any(test['target']=='http_recovery'and test.get('passed')for test in report['tests']):
            report['prefix_decoders']=[]
            for prefix in ['old-prefix.mp4','young-prefix.mp4']:
                decoded=subprocess.run(['docker','exec',name,'ffmpeg','-hide_banner','-loglevel','error','-xerror',
                    '-i','/evidence/'+prefix,'-t','1','-f','null','-'],env=env,capture_output=True,text=True,timeout=30)
                (out/(prefix+'-decode.log')).write_text(decoded.stdout+decoded.stderr)
                report['prefix_decoders'].append({'path':prefix,'passed':decoded.returncode==0,
                    'sha256':sha((out/'response-prefixes'/prefix).read_bytes()),'duration_seconds':1})
        clippy=subprocess.run(['cargo','clippy','--manifest-path',str(source/'rust/Cargo.toml'),'-p','dispatcharr-coordination',
            '-p','dispatcharr-clients','-p','dispatcharr-vod-http','--all-targets','--locked','--offline','--','-D','warnings'],
            env=env,capture_output=True,text=True,timeout=300)
        (out/'strict-clippy.log').write_text(clippy.stdout+clippy.stderr);report['strict_clippy_passed']=clippy.returncode==0
        report['passed']=report['strict_clippy_passed']and all(t.get('passed')for t in report['tests'])and all(item['passed']for item in report.get('prefix_decoders',[]))
    except Exception as error:report['error']=type(error).__name__+': '+str(error)[:1024]
    finally:
        removed=subprocess.run(['docker','rm','-f',name],env=env,capture_output=True,text=True,timeout=20)
        report['cleanup_passed']=removed.returncode==0;report['passed']&=report['cleanup_passed']
        (out/'proof.json').write_text(json.dumps(report,indent=2)+'\n')
    return 0 if report['passed']else 1
if __name__=='__main__':raise SystemExit(main())
