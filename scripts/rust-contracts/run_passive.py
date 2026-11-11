#!/usr/bin/env python3
"""Owned pinned Source passive resolver metadata fixture.
Original Dispatcharr/Django/DRF authors retain credit/licenses. No parity claim.
Requires --root checkout explicitly; never infer a private SUPPORT location.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

BASELINE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();root=args.root.resolve();output=args.output.resolve()
    output.mkdir(parents=True,exist_ok=False)
    own=Path(__file__).resolve().parent
    sys.path.insert(0,str(root/'scripts/rust-integration'))
    # Avoid the already observed generic `run` module collision in other tools.
    spec=importlib.util.spec_from_file_location('_m0_passive_integration_support',root/'scripts/rust-integration/run.py')
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
    from tool_snapshot import snapshot_sources
    from fixture_ports import write_port_overlay
    if helper.BASELINE!=BASELINE:raise RuntimeError('Pinned Source mismatch')
    tools=output/'tool-source';hashes=snapshot_sources(root,tools)
    private=tools/'m0-passive';private.mkdir(parents=True)
    for name in ['passive_source.py','run_passive.py']:
        data=(own/name).read_bytes();(private/name).write_bytes(data);hashes['m0-passive/'+name]=hashlib.sha256(data).hexdigest()
    expected=(root/'docs/rust-contract-register.json').read_bytes()
    (private/'expected-register.json').write_bytes(expected)
    project='dispatcharr-passive-'+uuid.uuid4().hex[:10]
    report={'passed':False,'scope':'Passive Source framework metadata only; bootstrap/readiness and audit observer SQL separate',
            'probe_network_mode':'none','probe_root_read_only':True,'project':project,'original_revision':BASELINE,'expected_register_sha256':hashlib.sha256(expected).hexdigest(),
            'tools_sha256':hashes,'HTTP_parity_acceptance':False,'metadata_capture_executed':False}
    status=1
    with helper.fixture_source(report,False,output) as temporary:
        source=Path(temporary)/'source';source.mkdir(mode=0o755)
        archive=subprocess.check_output(['git','-C',str(root),'archive',BASELINE])
        if hashlib.sha256(archive).hexdigest()!=ARCHIVE:raise RuntimeError('Original archive mismatch')
        subprocess.run(['tar','-x','--same-permissions','--no-same-owner','-C',str(source)],input=archive,check=True)
        (source/'media').mkdir(exist_ok=True)
        environment=dict(os.environ,BASELINE_SOURCE_DIR=str(source),BASELINE_EVIDENCE_DIR=str(output),DISPATCHARR_ENV='aio')
        overlay=output/'passive.yml'
        overlay.write_text(json.dumps({'services':{
            'baseline':{'volumes':[str(tools/'scripts/rust-integration')+':/backup-tools:ro']},
            # A separate network-none child cannot reach fixture PG/Redis even
            # through a C extension or subprocess bypassing Python guards.
            'passive':{
                'image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f',
                'entrypoint':['/dispatcharrpy/bin/python','/passive-tools/passive_source.py'],
                'working_dir':'/app','network_mode':'none','read_only':True,
                'cap_drop':['ALL'],'security_opt':['no-new-privileges:true'],
                'tmpfs':['/tmp:rw,nosuid,nodev,size=16777216','/data:rw,nosuid,nodev,size=16777216'],
                'environment':{'CONTRACT_PASSIVE_DISPOSABLE_FIXTURE':'playback-fixture-only',
                    'DJANGO_SECRET_KEY':'passive-metadata-disposable-fixture','PYTHONDONTWRITEBYTECODE':'1','DJANGO_SETTINGS_MODULE':'dispatcharr.settings','DISPATCHARR_ENV':'aio'},
                'volumes':[str(source)+':/app:ro',str(private)+':/passive-tools:ro']}
            },'networks':{'default':{'internal':True}}}))
        ports=output/'ports.yml';write_port_overlay(ports,{'baseline':[5656],'origin':[8080]})
        compose=['docker','compose','-p',project,'-f',str(tools/'docker/compose.rust-playback.yml'),'-f',str(overlay),'-f',str(ports)]
        def command(argv,body=None,timeout=180):
            return subprocess.check_output(argv,input=body,env=environment,text=True,stderr=subprocess.PIPE,timeout=timeout)
        def audit():
            raw=command(compose+['exec','-T','-e','POSTGRES_DB=dispatcharr','-e','BACKUP_DISPOSABLE_FIXTURE=playback-fixture-only',
                'baseline','/dispatcharrpy/bin/python','/backup-tools/backup_fixture.py'],json.dumps({'action':'baseline_manifest'}))
            return json.loads(raw.splitlines()[-1])
        before=None
        try:
            command(compose+['up','-d','baseline']);deadline=time.monotonic()+180
            while True:
                try:
                    command(compose+['exec','-T','baseline','/dispatcharrpy/bin/python','-c',
                        "import urllib.request; assert urllib.request.urlopen('http://127.0.0.1:5656/api/core/version/',timeout=2).status==200"],timeout=5)
                    break
                except subprocess.SubprocessError:
                    if time.monotonic()>deadline:raise RuntimeError('Source readiness failed')
                    time.sleep(.2)
            before=audit();(output/'before-private.json').write_text(json.dumps(before))
            # No handler request is sent by this child. Its SQL/network guards
            # start before django.setup; fixture setup above is distinct.
            result=subprocess.run(compose+['run','--rm','--no-deps','-T','passive'],
                env=environment,capture_output=True,text=True,timeout=120)
            (output/'probe-private.stderr').write_text(result.stderr)
            (output/'probe-stdout.json').write_text(result.stdout)
            data=json.loads(result.stdout.splitlines()[-1]);report['metadata_capture_executed']=True
            report['data_sha256']=hashlib.sha256(result.stdout.encode()).hexdigest()
            report['child_exit_code']=result.returncode
            after=audit();(output/'after-private.json').write_text(json.dumps(after))
            report['default_database_unchanged']=before==after
            report['route_count']=len(data.get('routes',[]));report['metadata_observed']=data.get('metadata_observed')
            report['violations']=data.get('violations',[])
            report['passed']=(result.returncode==0 and data.get('passed') is True and before==after
                and len(data['routes'])==592 and data['unresolved_rows']==322 and not data['violations']
                and data['handler_invocations']==0 and data['dispatch_executions']==0)
            status=0 if report['passed'] else 1
        except (OSError,ValueError,RuntimeError,subprocess.SubprocessError) as error:
            report['error_type']=type(error).__name__
        finally:
            if before is not None and 'default_database_unchanged' not in report:
                try:report['default_database_unchanged']=before==audit()
                except Exception as error:report['audit_error_type']=type(error).__name__
            try:command(compose+['down','--volumes','--remove-orphans']);report['cleanup']='owned project removed'
            except subprocess.SubprocessError:report['cleanup']='failed';report['passed']=False;status=1
            (output/'integration.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'passed':report['passed'],'output':str(output)}));return status
if __name__=='__main__':raise SystemExit(main())
