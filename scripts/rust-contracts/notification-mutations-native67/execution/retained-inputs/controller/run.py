#!/usr/bin/env python3
"""Native mutation build-review/execution controller. Default verifies hashes only.
BUILD_REVIEW_ONLY state cannot execute. Future completed ELF bindings and a NEW
Root-reviewed execution seal are required; no automatic retry or Source app run.
"""
import argparse,datetime,hashlib,json,os,re,subprocess,time,uuid
from pathlib import Path
from bounded_process import Collected
from validate_native import TABLES,owned,audit,mutations,legacy,require
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
TESTS={'mutations':('notifications-mutations-sql-test','mutations_match_retained_source48_and_native_order_controls'),'legacy':('notifications-legacy-sql-test','create_and_sequential_repeat_match_retained_source_effects')}
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def save(p,v):p.write_text(json.dumps(v,indent=2)+'\n')
def proc_start(pid):
    try:
        text=Path('/proc',str(pid),'stat').read_text();return text[text.rfind(')')+2:].split()[19]
    except FileNotFoundError:return None

def paths():return sorted([p for d in ['controller','inputs']for p in (ROOT/d).rglob('*')if p.is_file()and'__pycache__'not in p.parts]+[ROOT/'PLAN.md'])
def check_seal(path):
    seal=json.loads(path.read_text());require(seal['state']in ['BUILD_REVIEW_ONLY_NO_EXECUTABLES','SEALED_NATIVE_FIXTURE_REQUIRES_ROOT_EXECUTION_REVIEW'],'KnownSealState');require(seal['image']==IMAGE and seal['Source48_cases']==48 and seal['Native_order_controls']==3 and seal['legacy_cases']==16,'FiniteFixtureIdentity')
    require(len(seal['files'])==len({r['path']for r in seal['files']})and{r['path']for r in seal['files']}=={str(p.relative_to(ROOT))for p in paths()},'ExactLocalInputInventory')
    for row in seal['files']:
        rel=Path(row['path']);require(not rel.is_absolute()and'..'not in rel.parts,'RelativeInput');require((ROOT/rel).stat().st_size==row['bytes']and sha(ROOT/rel)==row['sha256'],'ExactLocalInput:'+row['path'])
    source=Path(seal['native_source_root']);expected=json.loads((ROOT/'inputs/full797-map.json').read_text());require(len(expected)==797 and sum(k.startswith('rust/')for k in expected)==792,'Exact792Plus5Map')
    actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()};require(actual==expected,'FrozenSource792AndCoupled5')
    require(sha(ROOT/'inputs/Source48.json')=='68542935c46471505dfbc9e1442e45fa9cbe498ef2c587fe0c257fb2ebd8b1bd','ExactSource48Raw');require(sha(ROOT/'inputs/Source48-plan.json')=='fe2f80cee17e477676f9e65e094a4242bbe123f397baa350c16d9f3822f65566','ExactReviewedSourcePlan')
    receipts=json.loads((ROOT/'inputs/executable-bindings.json').read_text());require(set(receipts)==set(TESTS),'ExactTwoExecutableSlots')
    if seal['state']=='BUILD_REVIEW_ONLY_NO_EXECUTABLES':require(all(v['state']=='UNBUILT_NOT_EXECUTABLE'for v in receipts.values()),'NoUnreviewedELF')
    else:
        for name,r in receipts.items():
            require(r['state']=='COMPLETED_COMPILE_BOUND'and r['compile_exit']==0 and r['wait_completed']is True and r['copy_after_successful_compile_and_wait']is True,'CompletedCompileRequired')
            require(r['full797_map_sha256']==sha(ROOT/'inputs/full797-map.json')and r['compiled_source_root']==str(source),'ELFSourceBinding')
            require(sha(ROOT/'inputs'/TESTS[name][0])==r['executable_sha256'],'ExactCompletedELF')
    return seal

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--seal',type=Path,default=ROOT/'seal.json');p.add_argument('--execute-reviewed-seal');p.add_argument('--output',type=Path);p.add_argument('--docker',default='docker');args=p.parse_args();seal=check_seal(args.seal)
    if args.execute_reviewed_seal is None:print(json.dumps({'passed':True,'seal_sha256':sha(args.seal),'state':seal['state'],'Source_Native_or_Docker_executed':False}));return 0
    require(seal['state']=='SEALED_NATIVE_FIXTURE_REQUIRES_ROOT_EXECUTION_REVIEW','BuildReviewExecutionForbidden');require(args.execute_reviewed_seal==sha(args.seal),'ExactReviewedExecutionSeal');require(args.output is not None,'FreshOutputRequired')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False,mode=0o700);project='dispatcharr-notifmut-native-'+uuid.uuid4().hex[:10];databases={n:owned('rust_probe_notif_native_'+uuid.uuid4().hex[:10])for n in TESTS};require(len(set(databases.values()))==2,'TwoDistinctOwnedDatabases');source=Path(seal['native_source_root']);environment={k:v for k,v in os.environ.items()if not k.startswith(('COMPOSE_','POSTGRES_','REDIS_','DATABASE_','DJANGO_','CELERY_'))}
    spec={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/tmp','entrypoint':['bash','/native-tools/start.sh'],'environment':{'DISPATCHARR_ENV':'aio','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PUID':'1000','PGID':'1000','PYTHONDONTWRITEBYTECODE':'1','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(HERE)+':/native-tools:ro',str(ROOT/'inputs/pg-init')+':/pg-bootstrap:ro',str(ROOT/'inputs')+':/native-inputs:ro',str(source)+':/native-source:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1g','pids_limit':256,'networks':['default']}},'networks':{'default':{'internal':True}}};save(out/'compose.json',spec)
    compose=[args.docker,'compose','-p',project,'-f',str(out/'compose.json')];serial=0;claimed=False;container=None;before=None;jobs=[];pid_records=[];started=[];cleanup_deadline=None;status=1
    report={'passed':False,'project':project,'databases':databases,'reviewed_seal_sha256':sha(args.seal),'full797_map_sha256':sha(ROOT/'inputs/full797-map.json'),'Source_runs':0,'Main_runs':0,'Source_force_auth_equals_Native_credentials':False,'renderer500_body_parity_claimed':False,'forms_multipart_qualified':False,'operation_registrations':0,'native_processes':{}}
    def stop_owned():
        if claimed:
            q=subprocess.run(compose+['stop','--timeout','10'],env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30);require(q.returncode==0,'OwnedStopAcknowledged')
    def command(argv,timeout=120,kind=None):
        nonlocal serial
        if cleanup_deadline is not None:remaining=cleanup_deadline-time.monotonic();require(remaining>0,'OverallCleanupDeadline');timeout=min(timeout,remaining)
        serial+=1;tag=f'command-{serial:03d}';job=Collected(argv,environment,1048576,stop_owned,stdout_path=out/(tag+'.stdout'),stderr_path=out/(tag+'.stderr'));jobs.append(job);start=proc_start(job.process.pid);began=datetime.datetime.now(datetime.timezone.utc).isoformat()
        try:job.wait(timeout)
        finally:
            receipt={'argv':argv,'kind':kind,'pid':job.process.pid,'starttime':start,'exit':job.process.poll(),'physically_settled':job.physically_settled,'pipes_settled':not any(t.is_alive()for t in job.threads),'began_utc':began,'completed_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()};save(out/(tag+'.json'),receipt)
            if kind in TESTS:report['native_processes'][kind]=receipt
        return(out/(tag+'.stdout')).read_text()
    def collect(stage,label,target):
        command(compose+['exec','-T','-e','NOTIFICATION_NATIVE_AUDIT=owned-native-only','-e','NOTIFICATION_AUDIT_STAGE='+stage,'-e','NOTIFICATION_AUDIT_DATABASE='+target,'-e','NOTIFICATION_AUDIT_LABEL='+label,'baseline','/dispatcharrpy/bin/python','/native-tools/audit.py']);value=json.loads((out/(label+'.json')).read_text());require(value['passed']is True and value['Django_or_Source_application_loaded']is False,'ReadonlyCollectionComplete');return value['value']
    def inventories():
        result={}
        for kind in ['container','network','volume']:
            argv=[args.docker,kind,'ls']+(['--all','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}'if kind=='volume'else'{{.ID}}'];result[kind]=command(argv,30).splitlines()
        return result
    try:
        require(not any(inventories().values()),'OwnedProjectInitiallyAbsent');claimed=True;command(compose+['up','-d','--no-build','--pull','never','baseline']);deadline=time.monotonic()+180
        while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'BootstrapDeadline');time.sleep(.2)
        ids=command(compose+['ps','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedService');container=ids[0];inspection=json.loads(command([args.docker,'inspect',container]));require(inspection[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactContainerProject');save(out/'owned-container-inspect.json',inspection)
        before=collect('default','default-before','dispatcharr');audit(before)
        for name,(binary,test)in TESTS.items():
            db=databases[name];loader=command(compose+['exec','-T','baseline','ldd','/native-inputs/'+binary],30);require('not found'not in loader,'ELFLoaderClosure')
            absent=command(compose+['exec','-T','baseline','psql','-XAtq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c',"SELECT count(*) FROM pg_database WHERE datname='"+db+"'"]);require(absent.strip()=='0','OwnedDatabaseInitiallyAbsent');command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+db]);command(compose+['exec','-T','baseline','redis-cli','--raw','SET','rust-probe-notification-native-owner',db]);collect('cache',name+'-cache-before',db)
            argv=compose+['exec','-T','-e','NOTIFICATION_NATIVE_FIXTURE=owned-native-only','-e','NOTIFICATION_NATIVE_DATABASE='+db,'-e','NOTIFICATION_NATIVE_REPORT=/evidence/'+name+'-report.json','-e','NOTIFICATION_MUTATION_SOURCE48=/native-inputs/Source48.json','-e','DATABASE_TEST_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+db,'-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','baseline','/native-inputs/'+binary,test,'--exact','--ignored','--nocapture','--test-threads=1'];started.append(name);command(argv,240,name)
            actual=json.loads((out/(name+'-report.json')).read_text());projection=mutations(actual,json.loads((ROOT/'inputs/Source48.json').read_text()))if name=='mutations'else legacy(actual);save(out/(name+'-validation.json'),projection);collect('credentials',name+'-credentials-after',db);collect('cache',name+'-cache-after',db)
        report['passed']=True;status=0
    except BaseException as error:report['error_type']=type(error).__name__;text=str(error).encode();report['error_message']=text[:4096].decode(errors='replace');report['error_message_bytes']=len(text);report['error_message_sha256']=hashlib.sha256(text).hexdigest();report['error_message_complete']=len(text)<=4096
    finally:
        for name in started:
            capture=out/(name+'-report.json')
            if capture.exists():
                try:
                    actual=json.loads(capture.read_text());projection=mutations(actual,json.loads((ROOT/'inputs/Source48.json').read_text()))if name=='mutations'else legacy(actual);save(out/(name+'-validation.json'),projection)
                except BaseException as error:report[name+'_partial_or_validation_failure']=type(error).__name__+':'+str(error)[:4096];report['passed']=False;status=1
            try:collect('credentials',name+'-final-credentials',databases[name]);collect('cache',name+'-final-cache',databases[name])
            except BaseException as error:report[name+'_final_collection_error']=str(error)[:4096];report['passed']=False;status=1
        if before is not None:
            try:after=collect('default','default-after','dispatcharr');require(audit(before)==audit(after)and before==after,'Default11Exact');report['default11_exact']=True
            except BaseException as error:report['default_after_error']=str(error)[:4096];report['passed']=False;status=1
        if claimed:
            cleanup_deadline=time.monotonic()+60
            try:
                try:
                    command(compose+['logs','--no-color','--tail','2000','baseline'],30)
                    if container:
                        listing=command([args.docker,'top',container,'-eo','pid,comm,args'],30);require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|notifications-mutations-sql-test|notifications-legacy-sql-test)\b',listing),'NoApplicationProcessLeft')
                        for line in listing.splitlines()[1:]:
                            if line.strip():pid=int(line.split()[0]);pid_records.append({'pid':pid,'starttime':proc_start(pid)})
                        save(out/'precleanup-PIDs.json',pid_records)
                except BaseException as error:report['precleanup_observation_error']=str(error)[:4096];report['passed']=False;status=1
                command(compose+['down','--volumes','--remove-orphans'],60);inventory=inventories();save(out/'cleanup-project-inventories.json',inventory);require(not any(inventory.values()),'IndependentAllProjectResourcesAbsent')
                if container:exact=command([args.docker,'container','ls','--all','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],15);require(not exact.strip(),'ExactFormerCIDAbsent')
                absence=[{**p,'after_starttime':proc_start(p['pid']),'original_process_absent':proc_start(p['pid'])is None or(p['starttime']is not None and proc_start(p['pid'])!=p['starttime'])}for p in pid_records];save(out/'cleanup-PID-absence.json',absence);require(all(x['original_process_absent']for x in absence),'RecordedPIDsAbsent');require(all(j.process.poll()is not None and not any(t.is_alive()for t in j.threads)for j in jobs),'AllHostChildrenPipesSettled');report['owned_cleanup_verified']=True
            except BaseException as error:report['cleanup_error']=str(error)[:4096];report['passed']=False;status=1
        try:check_seal(args.seal);report['input_bytes_unchanged_after']=True
        except BaseException as error:report['final_input_error']=str(error)[:4096];report['passed']=False;status=1
        report['controller_exit']=status;save(out/'execution.json',report)
    print(json.dumps({'passed':report['passed'],'Source_runs':0,'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
