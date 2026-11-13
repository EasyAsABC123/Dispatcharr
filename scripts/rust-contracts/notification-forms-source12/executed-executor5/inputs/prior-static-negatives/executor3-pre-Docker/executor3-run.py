#!/usr/bin/env python3
"""UNEXECUTED, review-gated 12-request real-auth Original notification forms controller.
Default invocation only checks local sealed bytes. No application on import.
"""
import argparse,datetime,hashlib,json,os,re,subprocess,sys,time,uuid,tarfile,threading,math
from bounded_stop import collect as bounded_stop
from definition import validate as validate_definition
from pathlib import Path
from bounded_process import Collected
from fixture_guards import database,unchanged_default,require
from effects import compare
PLAN=json.loads((Path(__file__).resolve().parent.parent/'inputs/plan.json').read_text())
HERE=Path(__file__).resolve().parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
def digest(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def sealed_paths(root):
    return sorted([p for directory in ('controller','inputs')for p in(root/directory).rglob('*')if p.is_file()and'__pycache__'not in p.parts]+[root/'execution-plan.json',root/'README.md',root/'Original-LICENSE',root/'local-controls.log'])
def check_seal(path):
    value=json.loads(path.read_text());root=HERE.parent
    require(value['state']=='SEALED_UNEXECUTED_SOURCE12_REQUIRES_ROOT_REVIEW'and value['Original_revision']==REVISION and value['image']==IMAGE and value['fixed_request_count']==12,'ExactSource12SealIdentity')
    require({r['path']for r in value['files']}=={str(p.relative_to(root))for p in sealed_paths(root)},'CompleteSealedInputInventory')
    for row in value['files']:
        relative=Path(row['path']);require(not relative.is_absolute()and'..'not in relative.parts,'SealRelativePath');p=root/relative;require(p.is_file()and not p.is_symlink()and p.stat().st_size==row['bytes']and digest(p)==row['sha256'],'SealedInputDrift:'+str(relative))
    require(digest(root/'inputs/definition-seal.json')=='9a32203d9b5b35c9c348fc0a796866baffe4ae2c0da70c3e3def41bb2c19b994','ExactCorrectedDefinition3');definition=json.loads((root/'inputs/definition-seal.json').read_text())
    for name,row in definition['files'].items():
        original=root/name if name.startswith('inputs/')else root/'inputs/definition3-top'/name;require(original.is_file()and original.stat().st_size==row['bytes']and digest(original)==row['sha256'],'FullDefinition3Reconstruction:'+name)
    validate_definition(PLAN)
    for group in('auth23','parser23'):
        proof=json.loads((root/'inputs'/(group+'-proof.json')).read_text());require(proof['passed']is True and proof['state']=='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'and proof['container_started']is False and proof['container_program_executions']==0 and proof['exact_CID_absent']is True and proof['all_project_containers_absent']is True,'NeverStartedActualByteCapture:'+group);require(proof['created_inspect']['State']['Pid']==proof['final_before_remove']['State']['Pid']==0,'PID0BeforeAfterCopy')
    proof=json.loads((root/'inputs/serializer9-proof.json').read_text());require(proof['passed']is True and proof['Source_started']is False and proof['app_imports']is False and proof['all_nine_equal']is True and proof['exact_container_absent']is True,'PriorExactSerializer9Bytes');require(proof['created_inspect']['State']['Pid']==proof['final_before_remove']['Pid']==0 and proof['final_before_remove']['Status']=='created'and proof['final_before_remove']['Running']is False,'SerializerPID0')
    return value
def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def proc_start(pid):
    try:return Path('/proc',str(pid),'stat').read_text().rsplit(')',1)[1].split()[19]
    except FileNotFoundError:return None
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
    nonce=uuid.uuid4().hex[:10];db=database('rust_probe_notifforms_'+nonce);project='dispatcharr-notifforms-source-'+nonce
    environment={k:v for k,v in os.environ.items() if not k.startswith(('COMPOSE_','POSTGRES_','REDIS_','CELERY_','DJANGO_','DATABASE_'))}
    source=out/'source';source.mkdir();compose=out/'compose.json'
    spec={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/notification-tools/start.sh'],
        'environment':{'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'notification-forms-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','CELERY_BROKER_URL':'redis://:playback-fixture-only@127.0.0.1:6379/2','CELERY_RESULT_BACKEND':'redis://:playback-fixture-only@127.0.0.1:6379/3','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/notification-tools:/app','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},
        'volumes':[str(source)+':/app:ro',str(HERE)+':/notification-tools:ro',str(HERE.parent/'inputs')+':/form-inputs:ro',str(HERE.parent)+':/form-root:ro',str(out)+':/evidence'],
        'tmpfs':['/data:mode=1777,size=2147483648','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=536870912'],
        'cpus':1,'mem_limit':'1g','pids_limit':256,'networks':['default']}},'networks':{'default':{'internal':True}}}
    save(compose,spec);base=[args.docker,'compose','-p',project,'-f',str(compose)]
    report={'passed':False,'state':'ACTUAL_EXECUTION_ATTEMPT','project':project,'copied_database':db,'Original_revision':REVISION,'image':IMAGE,'reviewed_seal_sha256':digest(args.seal),'Source_only':True,'real_credentials':True,'real_TCP':False,'registered':False,'performance_claimed':False,'web_workers_media_started':False}
    serial=0;container=None;before=None;jobs=[];status=1;project_claimed=False;cleanup_deadline=None;cleanup_started=None;owned_pids=[];stop_started=False;stop_done=threading.Event();began=time.monotonic()
    def begin_cleanup():
        nonlocal cleanup_started,cleanup_deadline
        if cleanup_started is None:cleanup_started=time.monotonic();cleanup_deadline=cleanup_started+90;report['cleanup_started_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat();report['cleanup_budget_seconds']=90
    def stop_owned():
        nonlocal stop_started
        if not project_claimed:return
        if stop_started:require(stop_done.wait(35)and report.get('owned_stop_receipt',{}).get('acknowledged')is True,'ExistingOwnedStopNotAcknowledged');return
        stop_started=True;receipt={};report['owned_stop_receipt']=receipt
        try:bounded_stop(base+['stop','--timeout','10'],environment,out,receipt,timeout=15 if cleanup_deadline is None else min(15,max(.01,cleanup_deadline-time.monotonic())),save=save)
        finally:stop_done.set()
    def command(argv,timeout=180,body=None):
        nonlocal serial
        remaining=(cleanup_deadline-time.monotonic())if cleanup_deadline is not None else 1200-(time.monotonic()-began);timeout=min(timeout,max(.01,remaining))
        serial+=1;tag=f'command-{serial:03d}'
        job=Collected(argv,environment,1048576,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'),body=body)
        jobs.append(job);identity=proc_start(job.process.pid);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:job.wait(timeout)
        finally:save(out/(tag+'.json'),{'argv':argv,'pid':job.process.pid,'starttime':identity,'began_utc':began,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exit':job.process.poll(),'collection_invalid':job.invalid,'physically_settled':job.physically_settled,'pipe_threads_settled':not any(t.is_alive()for t in job.threads)})
        return (out/(tag+'.stdout')).read_text()
    def exec_args(dbname,stage,label=None):
        env=['POSTGRES_DB='+dbname,'NOTIFICATION_FIXTURE_DATABASE='+dbname,'NOTIFICATION_FIXTURE_STAGE='+stage,'NOTIFICATION_FORMS_AUTHORITY=owned-source-forms-only','REDIS_DB='+('1'if stage=='exercise'else'0')]
        if label is not None:env+=['NOTIFICATION_AUDIT_LABEL='+label]
        return base+['exec','-T']+[part for entry in env for part in ('-e',entry)]+['baseline','/dispatcharrpy/bin/python','manage.py','shell','-c',"import runpy; runpy.run_path('/notification-tools/source_fixture.py',run_name='__main__')"]
    def audit(label):
        command(exec_args('dispatcharr','audit',label),timeout=120)
        captured=json.loads((out/('audit-'+label+'-private.json')).read_text())
        require(captured['passed']is True and captured['capture_valid']is True and captured['connections_close_called']is True,'CompleteDefaultAudit')
        return {'named11':captured['audit'],'public62':captured['public_manifest']}
    def inventories():
        values={}
        for kind in ['container','network','volume']:
            argv=[args.docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format','{{.ID}}'if kind!='volume'else'{{.Name}}']
            raw=command(argv,timeout=30);values[kind]=raw.splitlines()
        return values
    try:
        command(['tar','-xf',str(HERE.parent/'inputs/original-source.tar'),'--no-same-owner','-C',str(source)])
        (source/'media').mkdir(exist_ok=True)
        require(not any(inventories().values()),'RandomOwnedProjectInitiallyAbsent')
        project_claimed=True
        command(base+['up','-d','--no-build','--pull','never','baseline'])
        ids=command(base+['ps','--all','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedService');container=ids[0]
        running=json.loads(command([args.docker,'inspect',container]));require(running[0]['Config']['Labels']['com.docker.compose.project']==project and running[0]['Config']['Image']==IMAGE,'ExactOwnedContainerLabelImage');save(out/'owned-container-inspect-private.json',running)
        deadline=time.monotonic()+180
        while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'OwnedBootstrapDeadline');time.sleep(.2)
        # Read-only default audit occurs before clone creation/owned SQL seeds.
        before=audit('before');save(out/'default-before-private.json',before)
        # No web/worker exists; Original migrate process and audit shell exited,
        # so template copying has no retained application connection.
        absent=command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-At','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+db+"'"])
        require(absent.strip()=='0','OwnedDatabaseInitiallyAbsent')
        command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr'])
        report['Source_process_command_next']=serial+1
        command(exec_args(db,'exercise'),timeout=240)
        begin_cleanup()
        actual=json.loads((out/'notification-forms-private.json').read_text());qualified=compare(actual,PLAN)
        qualified['actual_Source_result']=True;qualified['input_origin']='sealed owned Source controller capture';save(out/'effect-validation.json',qualified)
        report['effects_passed']=qualified['passed'];report['actual_request_count']=actual['actual_request_count'];report['passed']=qualified['passed'];status=0 if qualified['passed']else 1
    except BaseException as error:
        report['error_type']=type(error).__name__;report['error_message']=str(error)[:4096];status=1;report['passed']=False
    finally:
        begin_cleanup()
        # Preserve and compare a partial/failed capture too; never retry it.
        capture=out/'notification-forms-private.json'
        if capture.exists():
            try:
                actual=json.loads(capture.read_text());report['actual_request_count']=actual.get('actual_request_count')
                qualified=compare(actual,PLAN);qualified['actual_Source_result']=True;qualified['input_origin']='sealed owned Source controller capture'
                save(out/'effect-validation.json',qualified)
                if not qualified['passed']:report['passed']=False;status=1
            except BaseException as error:
                report['effect_validation_negative']={'type':type(error).__name__,'message':str(error)[:4096],'message_bytes':len(str(error).encode()),'message_sha256':hashlib.sha256(str(error).encode()).hexdigest(),'message_truncated':len(str(error).encode())>4096};report['passed']=False;status=1
        if before is not None:
            try:
                backend=command(base+['exec','-T','-e','PGOPTIONS=-c default_transaction_read_only=on -c statement_timeout=5000','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,backend_start,state,wait_event_type,wait_event FROM pg_stat_activity WHERE datname='"+db+"') t"],timeout=10);values=json.loads(backend);save(out/'owned-backends-after-Source.json',values);require(values==[],'ActualOwnedBackendAbsenceAfterSourceExit');report['owned_backends_absent']=True
            except BaseException as error:report['backend_cleanup_error']=type(error).__name__+':'+str(error)[:4096];report['passed']=False;status=1
        # Always audit default before teardown, including partial exercise.
        if before is not None:
            try:
                after=audit('after');save(out/'default-after-private.json',after);report['default_named11_and_full62tables59sequences_unchanged']=unchanged_default(before['named11'],after['named11'])and before['public62']==after['public62']
                require(report['default_named11_and_full62tables59sequences_unchanged'],'DefaultNamedAuditChanged')
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
                        for line in processes.splitlines()[1:]:
                            if line.strip():
                                pid=int(line.split()[0]);identity=proc_start(pid);require(identity is not None,'DockerTopPIDHostStartNotObserved');owned_pids.append({'pid':pid,'starttime':identity,'PID_namespace':os.readlink('/proc/self/ns/pid'),'source':'Docker top with actual current-host /proc start observed; not an inner container PID file'})
                        save(out/'precleanup-PIDs.json',owned_pids)
                        require(not re.search(r'(?im)^.*\b(uwsgi|celery|daphne|ffmpeg|manage.py shell)\b',processes),'NoUnexpectedFixtureProcesses')
                    except BaseException as error:
                        report['precleanup_process_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
                try:command(base+['down','--volumes','--remove-orphans'],timeout=60)
                except BaseException as error:report['down_error']=type(error).__name__+':'+str(error)[:4096];report['passed']=False;status=1
                cleanup=inventories();save(out/'cleanup.json',cleanup);require(not any(cleanup.values()),'IndependentOwnedProjectAbsence')
                absence=[{**row,'after_starttime':proc_start(row['pid']),'original_process_absent':proc_start(row['pid'])is None or proc_start(row['pid'])!=row['starttime']}for row in owned_pids];save(out/'cleanup-PID-absence.json',absence);require(all(row['original_process_absent']for row in absence),'ActualOwnedPIDsAbsent')
                if container is not None:
                    raw=command([args.docker,'container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],timeout=15);save(out/'exact-container-absence.json',{'id':container,'raw':raw});require(not raw.strip(),'ExactOwnedContainerAbsent')
                require(all(job.physically_settled or(job.process.poll()is not None and not any(t.is_alive()for t in job.threads))for job in jobs),'OwnedHostChildrenAndPipesSettled')
                report['owned_cleanup_independently_verified']=True
            except BaseException as error:
                report['cleanup_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        else:
            report['project_not_claimed_no_cleanup_mutation']=True
        report['cleanup_elapsed_seconds']=time.monotonic()-cleanup_started;report['cleanup_within90']=math.isfinite(report['cleanup_elapsed_seconds'])and 0<=report['cleanup_elapsed_seconds']<=90
        if not report['cleanup_within90']:report['passed']=False;status=1
        report['controller_exit']=status;report['settled_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        report['host_children']=[{'pid':j.process.pid,'exit':j.process.poll(),'physically_settled':j.physically_settled,'pipes_settled':not any(t.is_alive()for t in j.threads)}for j in jobs]
        if 'Source_process_command_next'in report:
            record=out/('command-'+str(report['Source_process_command_next']).zfill(3)+'.json')
            if record.exists():report['Source_process_result']=json.loads(record.read_text())
        save(out/'execution.json',report)
    print(json.dumps({'passed':report['passed'],'output':str(out),'actual_requests':report.get('actual_request_count'),'registered':False}));return status
if __name__=='__main__':raise SystemExit(main())
