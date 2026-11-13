#!/usr/bin/env python3
"""UNEXECUTED, review-gated 16-request real-auth Original backup job controller.
Default invocation only checks local sealed bytes. No application on import.
"""
import argparse,datetime,hashlib,json,os,re,subprocess,sys,time,uuid,tarfile
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
    paths=[p for directory in ('controller','inputs','dependency-capture') for p in (root/directory).rglob('*') if p.is_file() and '__pycache__'not in p.parts]
    return sorted(paths+[root/'execution-plan.json',root/'README.md',root/'Original-LICENSE',root/'local-controls.log',root/'capture_dependencies.py',root/'dependency-capture-plan.json'])
def check_seal(path):
    value=json.loads(path.read_text());root=HERE.parent
    require(value['state']=='SEALED_UNEXECUTED_REQUIRES_ROOT_REVIEW','UnexecutedSealState')
    require(value['Original_revision']==REVISION and value['image']==IMAGE and value['fixed_request_count']==16,'PinnedPlanIdentity')
    require({row['path']for row in value['files']}=={str(p.relative_to(root))for p in sealed_paths(root)},'CompleteSealedInputInventory')
    for row in value['files']:
        relative=Path(row['path']);require(not relative.is_absolute()and'..'not in relative.parts,'SealRelativePath');require((root/relative).is_file()and not(root/relative).is_symlink()and(root/relative).stat().st_size==row['bytes']and digest(root/relative)==row['sha256'],'SealedInputDrift:'+str(relative))
    require(digest(root/'inputs/original-source.tar')=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','ExactOriginalArchive')
    require(digest(root/'inputs/definition-seal.json')=='1cdffe74d6fc1c706c526f2fb1e5490fa645d5ae428d6237bee2c6a0dea3a101','Exact16Definition')
    definition=json.loads((root/'inputs/definition-seal.json').read_text());require(digest(root/'inputs/plan.json')==definition['inputs']['plan.json'],'ExactReviewedDefinitionPlan')
    for name,expected in definition['inputs'].items():
        actual=root/'inputs'/name if name.startswith('Original/')or name=='plan.json' else root/'inputs/definition-support'/name
        require(digest(actual)==expected,'CompleteOriginalDefinitionClosure:'+name)
    with tarfile.open(root/'inputs/original-source.tar')as archive:
        for rel,expected in PLAN['Original_inputs_sha256'].items():require(hashlib.sha256(archive.extractfile(rel).read()).hexdigest()==expected==digest(root/'inputs/Original'/rel),'ExactOriginalBlob:'+rel)
    for proof_name,count in [('dependency-proof.json',11),('auth-dependency-proof.json',23)]:
        proof=json.loads((root/'inputs'/proof_name).read_text());require(proof['state']=='EXACT_NEVER_STARTED_IMAGE_BYTES_CAPTURED'and proof['passed']is True and proof['container_started']is False and proof['container_program_executions']==0 and len(proof['files'])==count and proof['exact_CID_absent']is True,'ActualDependencyClosure')
        require(proof['created_inspect']['State']['Pid']==proof['final_before_remove']['State']['Pid']==0 and proof['created_inspect']['State']['Status']==proof['final_before_remove']['State']['Status']=='created','NeverStartedDependencyCapture')
        for row in proof['files']:require(digest(root/row['path'])==row['sha256'],'ExactActualImagePrimaryBytes')
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
    nonce=uuid.uuid4().hex[:10];db=database('rust_probe_backupjob_'+nonce);project='dispatcharr-backupjob-source-'+nonce
    environment={k:v for k,v in os.environ.items() if not k.startswith(('COMPOSE_','POSTGRES_','REDIS_','CELERY_','DJANGO_','DATABASE_'))}
    source=out/'source';source.mkdir();compose=out/'compose.json'
    spec={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/app','entrypoint':['bash','/backup-tools/start.sh'],
        'environment':{'DISPATCHARR_ENV':'aio','DJANGO_SECRET_KEY':'backup-job-fixture-only','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_HOST':'127.0.0.1','REDIS_PORT':'6379','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':'/backup-tools:/app','PUID':'1000','PGID':'1000','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},
        'volumes':[str(source)+':/app:ro',str(HERE)+':/backup-tools:ro',str(HERE.parent/'inputs')+':/backup-inputs:ro',str(out)+':/evidence'],
        'tmpfs':['/data:mode=1777,size=2147483648','/app/media:mode=1777,size=67108864','/tmp:mode=1777,size=536870912'],
        'cpus':1,'mem_limit':'1536m','pids_limit':256,'networks':['default']}},'networks':{'default':{'internal':True}}}
    save(compose,spec);base=[args.docker,'compose','-p',project,'-f',str(compose)]
    report={'passed':False,'state':'ACTUAL_EXECUTION_ATTEMPT','project':project,'copied_database':db,'Original_revision':REVISION,'image':IMAGE,'reviewed_seal_sha256':digest(args.seal),'Source_only':True,'owned_worker_only':True,'real_credentials':True,'real_TCP':False,'registered':False,'performance_claimed':False,'web_media_started':False,'full_startup_default_routing_claim':False}
    serial=0;container=None;before=None;jobs=[];status=1;project_claimed=False;cleanup_deadline=None;owned_pids=[];worker=None;worker_finished=False;active_deadline=time.monotonic()+1200
    def stop_owned():
        # Exact project command uses only our random project and generated file.
        # Killing the fixture acknowledges container descendants, unlike killing
        # the host docker-exec client alone. Outer finally verifies absence.
        if not project_claimed:return
        argv=base+['stop','--timeout','10'];result=subprocess.run(argv,env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
        save(out/('owned-stop-'+uuid.uuid4().hex+'.json'),{'argv':argv,'exit':result.returncode,'stdout_hex':result.stdout.hex(),'stderr_hex':result.stderr.hex(),'utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
        require(result.returncode==0,'OwnedStopAcknowledged')
    def command(argv,timeout=180,body=None):
        nonlocal serial
        if cleanup_deadline is None:
            remaining=active_deadline-time.monotonic();require(remaining>0,'FixtureOverallDeadline');timeout=min(timeout,remaining)
        if cleanup_deadline is not None:
            remaining=cleanup_deadline-time.monotonic();require(remaining>0,'OwnedCleanupOverallDeadline');timeout=min(timeout,remaining)
        serial+=1;tag=f'command-{serial:03d}'
        job=Collected(argv,environment,8388608,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'),body=body)
        jobs.append(job);identity=proc_start(job.process.pid);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:job.wait(timeout)
        finally:save(out/(tag+'.json'),{'argv':argv,'pid':job.process.pid,'starttime':identity,'began_utc':began,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'exit':job.process.poll(),'collection_invalid':job.invalid,'physically_settled':job.physically_settled,'pipe_threads_settled':not any(t.is_alive()for t in job.threads)})
        return (out/(tag+'.stdout')).read_text()
    def exec_args(dbname,stage,label=None):
        env=['POSTGRES_DB='+dbname,'BACKUP_FIXTURE_NONCE='+nonce,'REDIS_DB='+('0'if stage=='audit'else'1'),'REDIS_URL=','CELERY_BROKER_URL=redis://:playback-fixture-only@127.0.0.1:6379/2','CELERY_RESULT_BACKEND=redis://:playback-fixture-only@127.0.0.1:6379/3','BACKUP_ROOT=/data/backups/'+db,'PLUGINS_DIR=/data/plugins/'+db,'DISPATCHARR_PLUGINS_DIR=/data/plugins/'+db,'USE_NGINX_ACCEL=false']
        if label is not None:env+=['BACKUP_AUDIT_LABEL='+label]
        return base+['exec','-T']+[part for entry in env for part in ('-e',entry)]+['baseline','/dispatcharrpy/bin/python','/backup-tools/source_fixture.py',stage]
    def finish_worker():
        nonlocal worker_finished
        if worker is None or worker_finished:return
        try:
            if worker.process.poll()is None:command(exec_args(db,'stop-worker'),timeout=15)
            worker.wait(30)
        finally:
            worker_finished=True;save(out/'worker-host-process.json',{'pid':worker.process.pid,'exit':worker.process.poll(),'physically_settled':worker.physically_settled,'pipes_settled':not any(t.is_alive()for t in worker.threads),'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
    def audit(label):
        command(exec_args('dispatcharr','audit',label),timeout=120)
        captured=json.loads((out/('audit-'+label+'-private.json')).read_text())
        require(captured['passed']is True and captured['capture_valid']is True and captured['connections_close_called']is True,'CompleteDefaultAudit')
        return captured['audit']
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
        absent=command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-At','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+db+"'"])
        require(absent.strip()=='0','OwnedDatabaseInitiallyAbsent')
        command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db+' TEMPLATE dispatcharr'])
        report['Source_before_command_next']=serial+1
        command(exec_args(db,'before'),timeout=180)
        worker_argv=exec_args(db,'worker');worker_argv[-2:]=['/backup-tools/worker_fixture.py']
        worker=Collected(worker_argv,environment,8388608,stop_owned,stdout_path=out/'worker.stdout',stderr_path=out/'worker.stderr');jobs.append(worker)
        save(out/'worker-host-start.json',{'argv':worker_argv,'pid':worker.process.pid,'starttime':proc_start(worker.process.pid),'started_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
        deadline=time.monotonic()+30
        while not(out/'worker-ready.json').exists():
            require(worker.process.poll()is None and worker.invalid is None and time.monotonic()<deadline,'ActualWorkerReadyDeadline');time.sleep(.1)
        poll_receipts=[];deadline=time.monotonic()+90
        while True:
            require(time.monotonic()<deadline and worker.process.poll()is None and worker.invalid is None,'ActualJobCompletionDeadline')
            command(exec_args(db,'poll'),timeout=min(15,max(.01,deadline-time.monotonic())))
            captured=json.loads((out/'backend-poll.json').read_text());poll_receipts.append(captured);save(out/'backend-poll-ledger.json',poll_receipts)
            if captured['raw_hex']is not None:
                meta=json.loads(bytes.fromhex(captured['raw_hex']))
                if meta.get('status')in('SUCCESS','FAILURE','REVOKED'):break
            time.sleep(.1)
        command(exec_args(db,'after'),timeout=120)
        finish_worker()
        command(exec_args(db,'snapshot'),timeout=60)
        backends=command(base+['exec','-T','baseline','psql','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-At','-v','ON_ERROR_STOP=1','-c',"SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json)::text FROM (SELECT pid,usename,state,application_name,backend_type FROM pg_stat_activity WHERE datname='"+db+"') t"],timeout=30);save(out/'owned-backends-after-worker.json',{'database':db,'actual_backends':json.loads(backends)});require(json.loads(backends)==[],'ActualOwnedSQLBackendsAbsentAfterWorkerAndHelpers')
        actual=json.loads((out/'backup-job-private.json').read_text());qualified=compare(actual,PLAN,*[json.loads((out/name).read_text())for name in ('worker-ready.json','worker-prerun.json','worker-postrun.json','worker-returned.json','post-worker-snapshot.json')]);save(out/'effect-validation.json',qualified)
        report['effects_passed']=qualified['passed'];report['actual_request_count']=actual['actual_request_count'];report['passed']=qualified['passed'];status=0 if qualified['passed']else 1
    except BaseException as error:
        report['error_type']=type(error).__name__;report['error_message']=str(error)[:4096];status=1;report['passed']=False
    finally:
        # Partial captures and worker output remain literal; no capture retry.
        try:finish_worker()
        except BaseException as error:report['worker_settlement_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        capture=out/'backup-job-private.json'
        if capture.exists():
            try:report['actual_request_count']=json.loads(capture.read_text()).get('actual_request_count')
            except BaseException as error:report['capture_read_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        # Always audit default before teardown, including partial exercise.
        if before is not None:
            try:
                after=audit('after');save(out/'default-after-private.json',after);report['default_eleven_named_tables_and_sequences_unchanged']=unchanged_default(before,after)
                require(report['default_eleven_named_tables_and_sequences_unchanged'],'DefaultNamedAuditChanged')
            except BaseException as error:report['default_audit_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        if project_claimed:
            cleanup_deadline=time.monotonic()+90
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
                            if line.strip():pid=int(line.split()[0]);owned_pids.append({'pid':pid,'starttime':proc_start(pid)})
                        save(out/'precleanup-PIDs.json',owned_pids)
                        require(not re.search(r'(?im)^.*\b(uwsgi|celery|daphne|ffmpeg|manage.py shell)\b',processes),'NoUnexpectedFixtureProcesses')
                    except BaseException as error:
                        report['precleanup_process_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
                command(base+['down','--volumes','--remove-orphans'],timeout=60)
                cleanup=inventories();save(out/'cleanup.json',cleanup);require(not any(cleanup.values()),'IndependentOwnedProjectAbsence')
                absence=[{**row,'after_starttime':proc_start(row['pid']),'original_process_absent':proc_start(row['pid'])is None or proc_start(row['pid'])!=row['starttime']}for row in owned_pids];save(out/'cleanup-PID-absence.json',absence);require(all(row['original_process_absent']for row in absence),'ActualOwnedPIDsAbsent')
                if container is not None:
                    # Exact former PID namespace's container ID absent, independently
                    # of label selectors; docker inspect must fail with no object.
                    probe=subprocess.run([args.docker,'inspect',container],env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=min(15,max(.01,cleanup_deadline-time.monotonic())))
                    save(out/'exact-container-absence.json',{'id':container,'exit':probe.returncode,'stdout':probe.stdout.decode(errors='replace'),'stderr':probe.stderr.decode(errors='replace')})
                    require(probe.returncode!=0 and json.loads(probe.stdout)==[],'ExactOwnedContainerAbsent')
                for job in jobs:
                    if not job.physically_settled:
                        try:job.wait(10)
                        except BaseException as error:report.setdefault('retained_child_wait_negatives',[]).append(type(error).__name__+':'+str(error)[:512]);report['passed']=False;status=1
                require(all(job.physically_settled and job.process.poll()is not None and not any(t.is_alive()for t in job.threads)for job in jobs),'OwnedHostChildrenAndPipesSettled')
                report['owned_cleanup_independently_verified']=True
            except BaseException as error:
                report['cleanup_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        else:
            report['project_not_claimed_no_cleanup_mutation']=True
        report['controller_exit']=status;report['settled_utc']=datetime.datetime.now(datetime.timezone.utc).isoformat()
        report['host_children']=[{'pid':j.process.pid,'exit':j.process.poll(),'physically_settled':j.physically_settled,'pipes_settled':not any(t.is_alive()for t in j.threads)}for j in jobs]
        save(out/'execution.json',report)
    print(json.dumps({'passed':report['passed'],'output':str(out),'actual_requests':report.get('actual_request_count'),'registered':False}));return status
if __name__=='__main__':raise SystemExit(main())
