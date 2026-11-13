"""Blocked finite image57 notification normal-Bearer Main fixture, no Source HTTP oracle."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
import uuid
HERE=Path(__file__).resolve().parent
ROOT=HERE.parent
BASE='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
SUPPORT='aca6d900ad13700ab86b2898134a61e4dba57bc1'


def save(path,value):Path(path).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')


def main():
    raise RuntimeError('NOTIFICATION_MAIN57_BLOCKED: reviewed activation and fresh Root cue required')


def execute_reviewed_fixture(repo,out):
    import bounded_process as bounded
    from candidate_inputs import bound_candidate
    repo=Path(repo).resolve();out=Path(out).resolve();b=json.loads((ROOT/'candidate-binding.json').read_text())
    proof=bound_candidate(repo/b['manifest'],b['image_id'],HERE/'expected-runtime.json',repo/b['archive'])
    out.mkdir(parents=True,exist_ok=False);shutil.copytree(ROOT,out/'controller',ignore=shutil.ignore_patterns('__pycache__'))
    original=out/'original-source';original.mkdir()
    raw=subprocess.check_output(['git','-C',str(repo),'archive',BASE])
    if hashlib.sha256(raw).hexdigest()!=ARCHIVE:raise ValueError('ExactOriginalFullArchive')
    subprocess.run(['tar','-x','--no-same-owner','-C',str(original)],input=raw,check=True);(original/'media').mkdir(exist_ok=True)
    support=out/'support';support.mkdir();raw=subprocess.check_output(['git','-C',str(repo),'archive',SUPPORT,'scripts','docker']);subprocess.run(['tar','-x','--no-same-owner','-C',str(support)],input=raw,check=True)
    save(out/'support-binding.json',{'revision':SUPPORT,'archive_sha256':hashlib.sha256(raw).hexdigest()})
    spec=importlib.util.spec_from_file_location('notification_ports',support/'scripts/rust-integration/fixture_ports.py');ports=importlib.util.module_from_spec(spec);spec.loader.exec_module(ports)
    nonce=uuid.uuid4().hex;project='dispatcharr-notification-main-'+uuid.uuid4().hex[:10];db='rust_pending_notif_'+uuid.uuid4().hex[:10]
    overlay=out/'fixture.json'
    save(overlay,{'services':{'baseline':{'volumes':[str(out/'controller/tools')+':/notification-tools:ro',str(out/'controller')+':/notification-root:ro'],'environment':{'NOTIFICATION_MAIN_FIXTURE':'notification-fixture-only','NOTIFICATION_MAIN_NONCE':nonce}},'candidate':{'image':b['image_id'],'environment':{'DATABASE_URL':'postgresql://dispatch:playback-fixture-only@baseline:5432/'+db,'REDIS_URL':'redis://:playback-fixture-only@baseline:6379/0'}}}})
    portfile=out/'ports.yml';ports.write_port_overlay(portfile,{'baseline':[5656],'candidate':[9192]})
    compose=['docker','compose','-p',project,'-f',str(support/'docker/compose.rust-playback.yml'),'-f',str(support/'docker/compose.rust-candidate.yml'),'-f',str(overlay),'-f',str(portfile)]
    env=dict(os.environ,BASELINE_SOURCE_DIR=str(original),BASELINE_EVIDENCE_DIR=str(out));index=0;before=None
    result={'passed':False,'project':project,'candidate':proof,'Source8_is_framework_not_TCP':True,'Source_notification_requests_executed':False,'default_workers_activated':False,'resource_sampler':False,'full_endpoint_or_performance_acceptance':False}
    def stop_owned():subprocess.run(compose+['kill'],env=env,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15,check=True)
    def command(argv,timeout=180):
        nonlocal index
        index+=1;return bounded.Collected(argv,env,8388608,stop_owned,stderr_path=out/('command-'+str(index)+'.stderr')).wait(timeout)
    def run(*args,timeout=180):return command(compose+list(args),timeout)
    def helper(database,action):return json.loads(run('exec','-T','-e','POSTGRES_DB='+database,'baseline','/dispatcharrpy/bin/python','/notification-tools/db.py',action).splitlines()[-1])
    def ready(url):
        until=time.monotonic()+120
        while time.monotonic()<until:
            try:run('exec','-T','baseline','/dispatcharrpy/bin/python','-c','import urllib.request;assert urllib.request.urlopen('+repr(url)+',timeout=2).status==200',timeout=5);return
            except bounded.CollectionFailure:time.sleep(.1)
        raise TimeoutError('OwnedNotificationReady')
    try:
        # No media/provider origin is needed or started for this SQL-only fixture.
        run('up','-d','--no-build','--no-deps','baseline');ready('http://127.0.0.1:5656/api/core/version/')
        run('exec','-T','baseline','/dispatcharrpy/bin/python','/verification/supervisor.py','control','stop')
        before=helper('dispatcharr','default-audit');result['default_before']=before
        run('exec','-T','-e','PGPASSWORD=playback-fixture-only','baseline','psql','-h','127.0.0.1','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db)
        helper(db,'migrate');helper(db,'seed');result['seed_snapshot']=helper(db,'snapshot')
        run('up','-d','--no-build','--no-deps','candidate');cid=run('ps','-q','candidate').decode().strip();inspection=json.loads(command(['docker','inspect',cid]))[0]
        if inspection['Image']!=b['image_id']or inspection['Config']['User']!='65532:65532':raise ValueError('ActualNormalMainImageUID')
        result['actual_Main_image_id']=inspection['Image'];result['actual_Main_Config_User']=inspection['Config']['User'];ready('http://candidate:9192/api/core/version/')
        run('exec','-T','-e','POSTGRES_DB='+db,'baseline','/dispatcharrpy/bin/python','/notification-tools/phase.py',timeout=100)
        result['phase']=json.loads((out/'notification-Main-private.json').read_text());result['passed']=result['phase']['qualification']['normal_Bearer_Main_notification_slice_qualified']is True
    except Exception as error:result['error']=type(error).__name__+':'+str(error)[:512]
    finally:
        def cleanup(action,label):
            try:return action()
            except Exception as error:result['passed']=False;result.setdefault('cleanup_errors',[]).append(label+':'+type(error).__name__)
        cleanup(lambda:run('stop','--timeout','15','candidate',timeout=30),'MainStop')
        if before is not None:
            after=cleanup(lambda:helper('dispatcharr','default-audit'),'DefaultAudit');result['default_after']=after;result['default_eleven_named_tables_sequences_unchanged']=after==before
            if after!=before:result['passed']=False
        cleanup(lambda:run('down','--volumes','--remove-orphans','--timeout','10',timeout=120),'ComposeDown')
        result['remaining']={}
        for kind,args in {'containers':['ps','-aq'],'networks':['network','ls','-q'],'volumes':['volume','ls','-q']}.items():
            raw=cleanup(lambda args=args:command(['docker']+args+['--filter','label=com.docker.compose.project='+project]),'IndependentInventory:'+kind)
            result['remaining'][kind]=raw.decode().strip()if raw is not None else'UNVERIFIED'
        if any(result['remaining'].values()):result['passed']=False
        save(out/'summary-private.json',result)
    return 0 if result['passed']else 1


if __name__=='__main__':raise SystemExit(main())
