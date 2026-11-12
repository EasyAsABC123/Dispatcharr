#!/usr/bin/env python3
"""UNEXECUTED, review-gated eight-request Original notification controller.
Default invocation only checks local sealed bytes. No application on import.
"""
import argparse,hashlib,json,os,re,subprocess,sys,time,uuid,tarfile
from pathlib import Path
from bounded_process import Collected
from fixture_guards import database,unchanged_default,require
from effects import compare
HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def sealed_paths(root):
    paths=[p for directory in ('controller','inputs') for p in (root/directory).rglob('*') if p.is_file() and '__pycache__'not in p.parts]
    return sorted(paths+[root/'execution-plan.json',root/'README.md'])
def check_seal(path):
    value=json.loads(path.read_text());root=HERE.parent
    require(value['state']=='SEALED_UNEXECUTED_REQUIRES_ROOT_REVIEW','UnexecutedSealState')
    require(value['Original_revision']==REVISION and value['image']==IMAGE and value['fixed_request_count']==8,'PinnedPlanIdentity')
    require({row['path']for row in value['files']}=={str(p.relative_to(root))for p in sealed_paths(root)},'CompleteSealedInputInventory')
    require(digest(root/'inputs/original-source.tar')=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive')
    for row in value['files']:
        relative=Path(row['path']);require(not relative.is_absolute() and '..'not in relative.parts,'SealRelativePath')
        require((root/relative).stat().st_size==row['bytes'] and digest(root/relative)==row['sha256'],'SealedInputDrift:'+str(relative))
    with tarfile.open(root/'inputs/original-source.tar')as archive:
        authorities=json.loads((root/'inputs/pinned-Original-files.json').read_text())
        require(authorities['revision']==REVISION,'OriginalAuthoritiesRevision')
        for row in authorities['files']:
            handle=archive.extractfile(row['original_path']);require(handle is not None,'OriginalAuthorityFile')
            require(hashlib.sha256(handle.read()).hexdigest()==row['sha256']==digest(root/row['path']),'ExactArchivedOriginalAuthority')
    return value
def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seal',type=Path,default=HERE.parent/'seal.json')
    parser.add_argument('--execute-reviewed-seal',help='Exact seal SHA256 explicitly reviewed by Root; omitted means local validation only')
    parser.add_argument('--output',type=Path);parser.add_argument('--docker',default='docker')
    args=parser.parse_args();seal=check_seal(args.seal)
    if args.execute_reviewed_seal is None:
        print(json.dumps({'seal_valid':True,'seal_sha256':digest(args.seal),'Source_executed':False,'requests_executed':0}));return 0
    require(args.execute_reviewed_seal==digest(args.seal),'ReviewedSealHashBinding')
    require(args.output is not None,'NewOwnedOutputRequired')
    out=args.output.resolve();out.mkdir(mode=0o700,parents=True,exist_ok=False)
    nonce=uuid.uuid4().hex[:10];db=database('rust_probe_notif_'+nonce);project='dispatcharr-notif-source-'+nonce
    environment={k:v for k,v in os.environ.items() if not k.startswith(('COMPOSE_','POSTGRES_','REDIS_','CELERY_','DJANGO_','DATABASE_'))}
    source=out/'source';source.mkdir();compose=out/'compose.json'
    spec={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/notification-tools/start.sh'],
        'environment':{'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'notification-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/notification-tools:/app','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},
        'volumes':[str(source)+':/app:ro',str(HERE)+':/notification-tools:ro',str(out)+':/evidence'],
        'tmpfs':['/data:mode=1777,size=2147483648','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=536870912'],
        'cpus':1,'mem_limit':'1g','pids_limit':256,'networks':['default']}},'networks':{'default':{'internal':True}}}
    save(compose,spec);base=[args.docker,'compose','-p',project,'-f',str(compose)]
    report={'passed':False,'state':'ACTUAL_EXECUTION_ATTEMPT','project':project,'copied_database':db,'Original_revision':REVISION,'image':IMAGE,'reviewed_seal_sha256':digest(args.seal),'Source_only':True,'real_credentials':False,'real_TCP':False,'registered':False,'performance_claimed':False,'web_workers_media_started':False}
    serial=0;container=None;before=None;jobs=[];status=1;project_claimed=False
    def stop_owned():
        # Exact project command uses only our random project and generated file.
        # Killing the fixture acknowledges container descendants, unlike killing
        # the host docker-exec client alone. Outer finally verifies absence.
        if not project_claimed:return
        result=subprocess.run(base+['stop','--timeout','10'],env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        require(result.returncode==0,'OwnedStopAcknowledged')
    def command(argv,timeout=180,body=None):
        nonlocal serial
        serial+=1;tag=f'command-{serial:03d}'
        job=Collected(argv,environment,1048576,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'),body=body)
        jobs.append(job);job.wait(timeout)
        return (out/(tag+'.stdout')).read_text()
    def exec_args(dbname,stage,label=None):
        env=['POSTGRES_DB='+dbname,'NOTIFICATION_FIXTURE_DATABASE='+dbname,'NOTIFICATION_FIXTURE_STAGE='+stage,'NOTIFICATION_FIXTURE_AUTHORITY=owned-source-only','REDIS_DB='+('1'if stage=='exercise'else'0')]
        if label is not None:env+=['NOTIFICATION_AUDIT_LABEL='+label]
        return base+['exec','-T']+[part for entry in env for part in ('-e',entry)]+['baseline','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy; runpy.run_path('/notification-tools/source_fixture.py',run_name='__main__')"]
    def audit(label):
        command(exec_args('dispatcharr','audit',label),timeout=120)
        captured=json.loads((out/('audit-'+label+'-private.json')).read_text())
        require(captured['passed']is True and captured['capture_valid']is True and captured['connections_close_called']is True,'CompleteDefaultAudit')
        return captured['audit']
    def inventories():
        values={}
        for kind in ['container','network','volume']:
            argv=[args.docker,kind,'ls','--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'if kind!='volume'else'{{.Name}}']
            raw=command(argv,timeout=30);values[kind]=raw.splitlines()
        return values
    try:
        command(['tar','-xf',str(HERE.parent/'inputs/original-source.tar'),'--no-same-owner','-C',str(source)])
        (source/'media').mkdir(exist_ok=True)
        require(not any(inventories().values()),'RandomOwnedProjectInitiallyAbsent')
        project_claimed=True
        command(base+['up','-d','--no-build','--pull','never','baseline'])
        deadline=time.monotonic()+180
        while not (out/'bootstrap-ready').exists():
            require(time.monotonic()<deadline,'OwnedBootstrapDeadline');time.sleep(.2)
        ids=command(base+['ps','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedService');container=ids[0]
        running=json.loads(command([args.docker,'inspect',container]));require(running[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedContainerLabel')
        save(out/'owned-container-inspect-private.json',running)
        # Read-only default audit occurs before clone creation/owned SQL seeds.
        before=audit('before');save(out/'default-before-private.json',before)
        # No web/worker exists; Original migrate process and audit shell exited,
        # so template copying has no retained application connection.
        command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr'])
        command(exec_args(db,'exercise'),timeout=180)
        actual=json.loads((out/'notification-source-private.json').read_text());qualified=compare(actual)
        qualified['actual_Source_result']=True;qualified['input_origin']='sealed owned Source controller capture';save(out/'effect-validation.json',qualified)
        report['effects_passed']=qualified['passed'];report['actual_request_count']=actual['actual_request_count'];report['passed']=qualified['passed'];status=0 if qualified['passed']else 1
    except BaseException as error:
        report['error_type']=type(error).__name__;report['error_message']=str(error)[:4096];status=1;report['passed']=False
    finally:
        # Preserve and compare a partial/failed capture too; never retry it.
        capture=out/'notification-source-private.json'
        if capture.exists():
            try:
                actual=json.loads(capture.read_text());report['actual_request_count']=actual.get('actual_request_count')
                qualified=compare(actual);qualified['actual_Source_result']=True;qualified['input_origin']='sealed owned Source controller capture'
                save(out/'effect-validation.json',qualified)
                if not qualified['passed']:report['passed']=False;status=1
            except BaseException as error:
                report['capture_preservation_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        # Always audit default before teardown, including partial exercise.
        if before is not None:
            try:
                after=audit('after');save(out/'default-after-private.json',after);report['default_eleven_named_tables_and_sequences_unchanged']=unchanged_default(before,after)
                require(report['default_eleven_named_tables_and_sequences_unchanged'],'DefaultNamedAuditChanged')
            except BaseException as error:report['default_audit_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        if project_claimed:
            try:
                try:
                    bootstrap=command(base+['logs','--no-color','--tail','2000','baseline'],timeout=30);(out/'bootstrap-private.log').write_text(bootstrap)
                except BaseException as error:
                    report['bootstrap_log_capture_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
                # Capture process listing before removing PID namespace; no unknown
                # web, task, decoder or owned shell process may remain running.
                if container is not None:
                    try:
                        processes=command([args.docker,'top',container,'-eo','pid,comm,args'],timeout=30);(out/'precleanup-processes-private.txt').write_text(processes)
                        require(not re.search(r'(?im)^.*\b(uwsgi|celery|daphne|ffmpeg|manage.py shell)\b',processes),'NoUnexpectedFixtureProcesses')
                    except BaseException as error:
                        report['precleanup_process_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
                command(base+['down','--volumes','--remove-orphans'],timeout=60)
                cleanup=inventories();save(out/'cleanup.json',cleanup);require(not any(cleanup.values()),'IndependentOwnedProjectAbsence')
                if container is not None:
                    # Exact former PID namespace's container ID absent, independently
                    # of label selectors; docker inspect must fail with no object.
                    probe=subprocess.run([args.docker,'inspect',container],env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
                    require(probe.returncode!=0 and probe.stdout.strip()==b'[]' and b'No such object'in probe.stderr,'ExactOwnedContainerAbsent')
                require(all(job.physically_settled or(job.process.poll()is not None and not any(t.is_alive()for t in job.threads))for job in jobs),'OwnedHostChildrenAndPipesSettled')
                report['owned_cleanup_independently_verified']=True
            except BaseException as error:
                report['cleanup_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        else:
            report['project_not_claimed_no_cleanup_mutation']=True
        save(out/'execution.json',report)
    print(json.dumps({'passed':report['passed'],'output':str(out),'actual_requests':report.get('actual_request_count'),'registered':False}));return status
if __name__=='__main__':raise SystemExit(main())
