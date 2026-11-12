#!/usr/bin/env python3
"""Sealed Native-only notification SQL/router fixture; default is local check.
No Django/Source application or Main binary is run. Root reviews the exact seal
before the single SQL/router test invocation. No automatic retry.
"""
import argparse,datetime,hashlib,json,os,re,subprocess,time,uuid
from pathlib import Path
from bounded_process import Collected
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
TEST='create_and_sequential_repeat_match_retained_source_effects'
TABLES=('accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings','core_systemnotification','core_notificationdismissal')
def require(value,label):
    if not value:raise ValueError(label)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def owned_database(name):
    require(type(name)is str and re.fullmatch(r'rust_probe_notif_native_[a-f0-9]{10}',name),'OwnedNativeDatabase');return name
def audit(value):
    require(value['database']=='dispatcharr' and set(value['tables'])==set(TABLES),'DefaultElevenTables')
    require(set(value['sequences'])=={'public.'+name+'_id_seq'for name in TABLES},'DefaultElevenSequences')
    for row in value['tables'].values():require(type(row['rows'])is int and 0<=row['rows']<=10000 and re.fullmatch('[a-f0-9]{64}',row['sha256']),'DefaultFingerprintShape')
    for row in value['sequences'].values():require(type(row['last_value'])is int and row['last_value']>=1 and type(row['is_called'])is bool,'DefaultSequenceShape')
    require(hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest()==value['dataset_sha256'],'DefaultDigestBinding')
    return value['dataset_sha256']
def validate_isolation(actual,database):
    empty={'notifications':[],'dismissals':[]}
    require(actual['isolation_database']==owned_database(database),'IsolatedTargetDatabaseBinding')
    require(actual['source_initial_notification_rows_before_isolation']==empty and actual['owned_notification_rows_after_isolation']==empty,'ActualSourceEmptyIsolationMatch')
    initial=actual['native_initialized_notification_rows_before_isolation']
    require(type(initial['notifications'])is list and type(initial['dismissals'])is list and len(initial['notifications'])==1 and initial['notifications'][0]['id']==1 and initial['notifications'][0]['notification_key']=='network_security_m3u_epg','RetainedNativeInitializedNotice')
    expected=actual['source_seed_notification_rows'];observed=actual['native_seed_notification_rows_before_requests']
    require(set(expected)==set(observed)=={'notifications','dismissals'} and expected['dismissals']==observed['dismissals']==[] and len(expected['notifications'])==len(observed['notifications'])==1,'IsolatedSeedShape')
    left,right=observed['notifications'][0],expected['notifications'][0];require(set(left)==set(right),'ExactSeedFieldSurface')
    for key,value in right.items():
        if key in ('created_at','updated_at'):
            require(datetime.datetime.fromisoformat(left[key].replace('Z','+00:00'))==datetime.datetime.fromisoformat(value.replace('Z','+00:00')),'ExactCopiedSourceSeedClock')
        else:require(left[key]==value,'ExactCopiedSourceSeedField:'+key)
    require(actual['native_seed_notice_sequence']=={'last_value':1,'is_called':True},'SourceSeedSequenceBinding')
    return True
def local_paths():
    return sorted([p for directory in ('controller','inputs') for p in(ROOT/directory).rglob('*')if p.is_file()and'__pycache__'not in p.parts]+[ROOT/'PLAN.md'])
def check_seal(path):
    value=json.loads(path.read_text());require(value['state']=='SEALED_NATIVE_FIXTURE_REQUIRES_ROOT_REVIEW','PendingRootReviewState')
    require(value['image']==IMAGE and value['test_filter']==TEST and value['expected_Source8_cases']==8 and value['expected_extra_Native_cases']==8,'FiniteFixtureIdentity')
    require(len(value['files'])==len({r['path']for r in value['files']}),'UniqueSealedInventory')
    require({r['path']for r in value['files']}=={str(p.relative_to(ROOT))for p in local_paths()},'ExactControllerInputInventory')
    for row in value['files']:
        p=Path(row['path']);require(not p.is_absolute()and'..'not in p.parts,'RelativeSealedPath');require((ROOT/p).stat().st_size==row['bytes'] and sha(ROOT/p)==row['sha256'],'SealedInputDrift:'+str(p))
    source=Path(value['native_source_root']);require(source.is_absolute()and source.is_dir(),'FrozenNativeSourceRoot')
    expected=json.loads((ROOT/'inputs/final790-map.json').read_text());require(len(expected)==790,'Finite790Map')
    actual={str(p.relative_to(source)):sha(p)for p in source.rglob('*')if p.is_file()};require(actual==expected,'Frozen790InputDrift')
    receipt=json.loads((ROOT/'inputs/executable-receipt.json').read_text());require(receipt['copy_after_successful_compile_and_wait']is True and receipt['compile_exit']==0 and receipt['compiled_final790_map_sha256']==sha(ROOT/'inputs/final790-map.json') and receipt['executable_sha256']==sha(ROOT/'inputs/notifications-sql-test'),'ActualCompletedExecutableBinding')
    return value
def save(path,value):path.write_text(json.dumps(value,indent=2)+'\n')
def proc_start(pid):
    try:
        raw=Path('/proc',str(pid),'stat').read_text();return raw[raw.rfind(')')+2:].split()[19]
    except FileNotFoundError:return None

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--seal',type=Path,default=ROOT/'seal.json');parser.add_argument('--execute-reviewed-seal');parser.add_argument('--output',type=Path);parser.add_argument('--docker',default='docker');args=parser.parse_args();seal=check_seal(args.seal)
    if args.execute_reviewed_seal is None:
        print(json.dumps({'seal_valid':True,'seal_sha256':sha(args.seal),'Native_SQL_or_Source_executed':False}));return 0
    require(args.execute_reviewed_seal==sha(args.seal),'ReviewedSealHashBinding');require(args.output is not None,'NewOwnedOutputRequired')
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False,mode=0o700);nonce=uuid.uuid4().hex[:10];database=owned_database('rust_probe_notif_native_'+nonce);project='dispatcharr-notif-native-'+nonce
    source=Path(seal['native_source_root']);environment={k:v for k,v in os.environ.items()if not k.startswith(('COMPOSE_','POSTGRES_','REDIS_','DATABASE_','DJANGO_','CELERY_'))}
    spec={'services':{'baseline':{'image':IMAGE,'pull_policy':'never','platform':'linux/amd64','working_dir':'/tmp','entrypoint':['bash','/native-tools/start.sh'],'environment':{'DISPATCHARR_ENV':'aio','POSTGRES_DB':'dispatcharr','POSTGRES_USER':'dispatch','POSTGRES_PASSWORD':'playback-fixture-only','POSTGRES_HOST':'/var/run/postgresql','POSTGRES_PORT':'5432','POSTGRES_DIR':'/data/db','REDIS_PASSWORD':'playback-fixture-only','REDISCLI_AUTH':'playback-fixture-only','PUID':'1000','PGID':'1000','PYTHONDONTWRITEBYTECODE':'1','LANG':'C.UTF-8','LC_ALL':'C.UTF-8'},'volumes':[str(HERE)+':/native-tools:ro',str(ROOT/'inputs/pg-init')+':/pg-bootstrap:ro',str(ROOT/'inputs')+':/native-inputs:ro',str(source)+':/native-source:ro',str(out)+':/evidence'],'tmpfs':['/data:mode=1777,size=2147483648','/tmp:mode=1777,size=536870912'],'cpus':1,'mem_limit':'1g','pids_limit':256,'networks':['default']}},'networks':{'default':{'internal':True}}};save(out/'compose.json',spec)
    compose=[args.docker,'compose','-p',project,'-f',str(out/'compose.json')];serial=0;claimed=False;container=None;before=None;native_started=False;marker=False;jobs=[];pid_records=[]
    report={'passed':False,'state':'NATIVE_ONLY_ATTEMPT','project':project,'database':database,'reviewed_seal_sha256':sha(args.seal),'compiled_input_map_sha256':sha(ROOT/'inputs/final790-map.json'),'native_executable_sha256':sha(ROOT/'inputs/notifications-sql-test'),'Source_application_or_capture_runs':0,'Main_binary_runs':0,'Source_credentials_or_HTTP_TCP_acceptance':False,'performance_or_full_endpoint_acceptance':False,'operation_registration_changes':0,'Source_force_auth_distinct_from_Native_API_key_machinery':True};status=1
    def stop_owned():
        if not claimed:return
        result=subprocess.run(compose+['stop','--timeout','10'],env=environment,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30);require(result.returncode==0,'OwnedStopAcknowledged')
    def command(argv,timeout=120,kind=None):
        nonlocal serial
        serial+=1;prefix=f'command-{serial:03d}';save(out/(prefix+'.json'),{'argv':argv,'kind':kind,'timeout_seconds':timeout})
        job=Collected(argv,environment,1048576,stop_owned,stdout_path=out/(prefix+'.stdout'),stderr_path=out/(prefix+'.stderr'));jobs.append(job)
        try:job.wait(timeout)
        finally:
            if kind=='Native-SQL-test':
                report['native_process_exit']=job.process.poll();save(out/'native-process-exit.json',{'returncode':job.process.poll(),'physically_settled':job.physically_settled})
        return(out/(prefix+'.stdout')).read_text()
    def collect(stage,label,target):
        argv=compose+['exec','-T','-e','NOTIFICATION_NATIVE_AUDIT=owned-native-only','-e','NOTIFICATION_AUDIT_STAGE='+stage,'-e','NOTIFICATION_AUDIT_DATABASE='+target,'-e','NOTIFICATION_AUDIT_LABEL='+label,'baseline','/dispatcharrpy/bin/python','/native-tools/audit.py']
        command(argv);value=json.loads((out/(label+'.json')).read_text());require(value['passed']is True and value['stage']==stage and value['Django_or_Source_application_loaded']is False,'ReadOnlyCollectionComplete');return value['value']
    def inventories():
        values={}
        for kind in ('container','network','volume'):
            argv=[args.docker,kind,'ls']+(['-a','--no-trunc']if kind=='container'else[])+['--filter','label=com.docker.compose.project='+project,'--format','{{.Name}}'if kind=='volume'else'{{.ID}}'];values[kind]=command(argv,timeout=30).splitlines()
        return values
    try:
        require(not any(inventories().values()),'OwnedProjectInitiallyAbsent');claimed=True;command(compose+['up','-d','--no-build','--pull','never','baseline'])
        deadline=time.monotonic()+180
        while not(out/'bootstrap-ready').exists():require(time.monotonic()<deadline,'OwnedBootstrapDeadline');time.sleep(.2)
        ids=command(compose+['ps','-q','baseline']).splitlines();require(len(ids)==1,'OneOwnedContainer');container=ids[0]
        inspection=json.loads(command([args.docker,'inspect',container]));require(inspection[0]['Config']['Labels']['com.docker.compose.project']==project,'ExactOwnedContainerBinding');save(out/'owned-container-inspect.json',inspection)
        loader=command(compose+['exec','-T','baseline','ldd','/native-inputs/notifications-sql-test'],timeout=30);(out/'native-loader.log').write_text(loader);require('not found'not in loader,'NativeImageELFClosure')
        before=collect('default','default-before','dispatcharr');audit(before)
        command(compose+['exec','-T','baseline','psql','-Xq','-h','/var/run/postgresql','-U','dispatch','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database])
        command(compose+['exec','-T','baseline','redis-cli','--raw','SET','rust-probe-notification-native-owner',database]);marker=True;collect('cache','native-cache-before',database)
        argv=compose+['exec','-T','-e','NOTIFICATION_NATIVE_FIXTURE=owned-native-only','-e','NOTIFICATION_NATIVE_DATABASE='+database,'-e','NOTIFICATION_NATIVE_REPORT=/evidence/native-notification-report.json','-e','DATABASE_TEST_URL=postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+database,'-e','REDIS_TEST_URL=redis://:playback-fixture-only@127.0.0.1:6379/0','baseline','/native-inputs/notifications-sql-test',TEST,'--exact','--ignored','--nocapture','--test-threads=1']
        native_started=True;command(argv,timeout=240,kind='Native-SQL-test')
        actual=json.loads((out/'native-notification-report.json').read_text());report['owned_seed_isolation_and_Source_match_verified']=validate_isolation(actual,database);require(actual['passed']is True and len(actual['cases'])==8 and len(actual['extra_cases'])==8 and actual['owned_server_aborted_and_pool_closed']is True,'NativeEightPlusEightComplete');report['Native_cases']=8;report['Native_definition_derived_extra_cases']=8;report['passed']=True;status=0
    except BaseException as error:report['error_type']=type(error).__name__;report['error_message']=str(error)[:4096];report['passed']=False;status=1
    finally:
        if native_started:
            try:
                credentials=collect('credentials','native-credential-rows-after',database)
                report['native_credential_raw_tables_retained']=True;report['native_credential_event_rows']=credentials['tables']['core_systemevent']['rows']
                require(credentials['tables']['core_systemevent']['rows']==0,'UnexpectedNativeCredentialEvent')
            except BaseException as error:report['native_credential_collection_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        if marker:
            try:collect('cache','native-cache-after',database);report['raw_Native_cache_and_TTL_retained_without_Source_equivalence_claim']=True
            except BaseException as error:report['native_cache_collection_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        if before is not None:
            try:
                after=collect('default','default-after','dispatcharr');report['default_eleven_tables_and_sequences_exact']=audit(before)==audit(after) and before==after;require(report['default_eleven_tables_and_sequences_exact'],'DefaultAuditChanged')
            except BaseException as error:report['default_audit_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        if claimed:
            try:
                try:
                    logs=command(compose+['logs','--no-color','--tail','2000','baseline'],timeout=30);(out/'bootstrap.log').write_text(logs)
                    if container:
                        listing=command([args.docker,'top',container,'-eo','pid,comm,args'],timeout=30);(out/'precleanup-processes.txt').write_text(listing)
                        for line in listing.splitlines()[1:]:
                            if line.strip():pid=int(line.split()[0]);pid_records.append({'pid':pid,'starttime':proc_start(pid)})
                        save(out/'precleanup-PIDs.json',pid_records)
                        require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|notifications-sql-test)\b',listing),'NoApplicationOrNativeTestLeftRunning')
                except BaseException as error:report['precleanup_observation_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
                command(compose+['down','--volumes','--remove-orphans'],timeout=60);cleanup=inventories();save(out/'cleanup-label-inventories.json',cleanup);require(not any(cleanup.values()),'IndependentOwnedProjectAbsence')
                if container:
                    exact=command([args.docker,'container','ls','-a','--no-trunc','--filter','id='+container,'--format','{{.ID}}'],timeout=30);(out/'cleanup-exact-ID.txt').write_text(exact);require(not exact.strip(),'ExactFormerContainerAbsent')
                require(container is None or len(pid_records)>0,'RecordedOwnedPIDsRequired')
                absence=[{**r,'after_starttime':proc_start(r['pid']),'original_process_absent':proc_start(r['pid'])is None or(r['starttime']is not None and proc_start(r['pid'])!=r['starttime'])}for r in pid_records];save(out/'cleanup-PID-absence.json',absence);require(all(r['original_process_absent']for r in absence),'OwnedRecordedPIDsAbsent')
                require(all(job.process.poll()is not None and not any(t.is_alive()for t in job.threads)and(job.writer is None or not job.writer.is_alive())for job in jobs),'HostChildrenAndPipesReaped');report['owned_cleanup_independently_verified']=True
            except BaseException as error:report['cleanup_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        else:report['project_not_claimed_no_cleanup_mutation']=True
        try:check_seal(args.seal);report['sealed_inputs_unchanged_after']=True
        except BaseException as error:report['post_input_error']=type(error).__name__+':'+str(error)[:512];report['passed']=False;status=1
        save(out/'execution.json',report)
    print(json.dumps({'passed':report['passed'],'native_process_exit':report.get('native_process_exit'),'Source_runs':0,'output':str(out)}));return status
if __name__=='__main__':raise SystemExit(main())
