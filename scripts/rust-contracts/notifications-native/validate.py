#!/usr/bin/env python3
"""Read-only retained Native2/Source8 comparison. No compiler, SQL or app runs."""
import datetime,hashlib,importlib.util,json,re,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
MANIFEST='14f48d7c2d8ef48835094dd12b6975bd669dbcd6174b80e523edc173fbea07d6'
MAP='c3b6a496827d4d595bad2b881a684aa96827cdfc5a3344d4eb118a9298808b38'
ELF='26c59f64695dc1eebc00bea090bbdacbadc264ae30b4118fedc8b3baff1246d7'
SEAL='0b670f70ee88f6a2b887a9bc877c296aff5ca9cb445d0179115388855236be1c'
EXTRA=('duplicate-key-no-sequence','required-fields-no-mutation','standard-permission-before-invalid-body','repeat-truthy-update-only','repeat-falsey-retains-action','other-crud-put-stays-guarded','other-crud-patch-stays-guarded','other-crud-delete-stays-guarded')
def require(value,label):
    if not value:raise ValueError(label)
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def executed_controller(root):
    path=root/'executed/controller';sys.path.insert(0,str(path))
    try:
        spec=importlib.util.spec_from_file_location('_retained_notification_native_controller',path/'run.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
    finally:sys.path.remove(str(path))
def instant(value):
    result=datetime.datetime.fromisoformat(value.replace('Z','+00:00'));require(result.tzinfo is not None,'AwareClock');return result
def load(root=HERE):
    require(sha(root/'raw-manifest.json')==MANIFEST,'ExactRawManifest')
    manifest=read(root,'raw-manifest.json');require(len(manifest['files'])==67,'Exact67RawFiles')
    for row in manifest['files']:
        relative=Path(row['path']);require(not relative.is_absolute()and'..'not in relative.parts,'RelativeRawPath');require((root/relative).stat().st_size==row['bytes']and sha(root/relative)==row['sha256'],'RawHashBinding:'+row['path'])
    names=('source-observations','source-negative-controller','native2-observations','native2-execution','native2-process-exit','default-before','default-after','native-credential-rows-after','native-cache-before','native-cache-after','cleanup-label-inventories','precleanup-PIDs','cleanup-PID-absence','owned-container-inspect','native1-negative-outcome','native1-negative-execution','native1-process-exit','Root-preexecution-review','Root-retained-review','native2-seal','cleanup-exact-ID-command')
    values={name:read(root,'raw/'+name+'.json')for name in names}
    for name in ('native2-controller-exit.txt','native2-controller-stdout.log','cleanup-exact-ID.txt','cleanup-exact-ID-command.stdout','legacy-store-lib-sandbox.log','legacy-store-lib-host-loopback.log','legacy-store-lib-host-loopback.exit'):values[name]=(root/'raw'/name).read_text()
    return values

def derive(values,controller,identity=None,root_review=True):
    expected_seal,expected_map,expected_elf=identity or(SEAL,MAP,ELF)
    d=values['native2-observations'];s=values['source-observations'];e=values['native2-execution'];require(d['passed']is True and d['owned_server_aborted_and_pool_closed']is True,'NativeReportPassed')
    require(len(d['cases'])==len(d['extra_cases'])==8 and s['capture_valid']is True and s['actual_request_count']==8,'EightPlusEight')
    require([c['status']for c in d['cases']]==[401,200,403,200,200,200,200,201],'ActualSource8Statuses')
    require([c['status']for c in d['extra_cases']]==[400,400,403,200,200,501,501,501]and tuple(c['name']for c in d['extra_cases'])==EXTRA,'ActualNativeExtraCases')
    require(e['passed']is True and e['native_process_exit']==0 and values['native2-process-exit']=={'returncode':0,'physically_settled':True}and values['native2-controller-exit.txt'].strip()=='0','ActualProcessControllerExit0')
    require(json.loads(values['native2-controller-stdout.log'])['passed']is True,'ControllerStdoutPassed')
    require(e['reviewed_seal_sha256']==expected_seal and e['compiled_input_map_sha256']==expected_map and e['native_executable_sha256']==expected_elf,'ExecutedInputIdentity')
    require(e['Source_application_or_capture_runs']==e['Main_binary_runs']==e['operation_registration_changes']==0 and e['Source_credentials_or_HTTP_TCP_acceptance']is False and e['performance_or_full_endpoint_acceptance']is False and e['Source_force_auth_distinct_from_Native_API_key_machinery']is True,'BoundedClaims')
    require(d['Source_controller_exit_preserved']==1 and values['source-negative-controller']['passed']is False,'OriginalSourceControllerFailureRetained')
    require(values['native1-negative-outcome']['passed']is False and values['native1-negative-outcome']['controller_actual_exit']==1 and values['native1-negative-outcome']['native_process_actual_exit']==101 and values['native1-negative-outcome']['native_router_requests_reached']==0 and values['native1-negative-execution']['passed']is False and values['native1-process-exit']['returncode']==101,'Native1SetupFailureRetained')
    require(controller.validate_isolation(d,e['database'])and e['owned_seed_isolation_and_Source_match_verified']is True,'TargetIsolationAndExactSourceSeed')
    clock_pairs={};changed_paths=[]
    def compare(a,b,native,source,path=''):
        if isinstance(a,dict):
            require(isinstance(b,dict)and set(a)==set(b),'SQLOrJSONFieldSurface:'+path)
            for k in a:compare(a[k],b[k],native,source,path+'/'+k)
        elif isinstance(a,list):
            require(isinstance(b,list)and len(a)==len(b),'SQLOrJSONListShape:'+path)
            for index,(x,y)in enumerate(zip(a,b)):compare(x,y,native,source,path+'/'+str(index))
        elif path.rsplit('/',1)[-1]in('created_at','updated_at','dismissed_at')and a is not None:
            old,new=instant(a),instant(b)
            if old==new:return
            if old in clock_pairs:require(new==clock_pairs[old],'CrossRequestClockPreserved:'+path);return
            require(instant(source['began_utc'])<=old<=instant(source['completed_utc']),'SourceOwnClock:'+path)
            require(instant(native['began_utc'])<=new<=instant(native['completed_utc']),'NativeOwnClock:'+path)
            clock_pairs[old]=new;changed_paths.append(path)
        else:require(type(a)is type(b)and a==b,'StableValue:'+path)
    allcases=d['cases']+d['extra_cases'];previous=None
    for c in allcases:
        body=bytes.fromhex(c['body_hex']);require(json.loads(body)==c['body'],'RawBodyBoundToJSON:'+c['name'])
        if'body_sha256'in c:require(hashlib.sha256(body).hexdigest()==c['body_sha256'],'BodySHA256:'+c['name'])
        require(instant(c['began_utc'])<=instant(c['completed_utc']),'RequestClockOrder')
        require(set(c['before']['tables'])==set(c['after']['tables'])==set(controller.TABLES)and set(c['before']['sequences'])==set(c['after']['sequences'])==set(controller.TABLES),'All11NativeStateFields')
        if previous is not None:require(c['before']==previous,'CrossRequestSQLChain')
        for table in controller.TABLES:
            if table not in('core_notificationdismissal','core_systemnotification'):
                require(c['before']['tables'][table]==c['after']['tables'][table]and c['before']['sequences'][table]==c['after']['sequences'][table],'OtherNineTablesAndSequences:'+table)
        previous=c['after']
    for c,orig in zip(d['cases'],s['requests']):
        require(c['name']==orig['name']and c['status']==orig['actual_status'],'ActualSourceCaseIdentity')
        compare(orig['json'],c['body'],c,orig)
        for source_name,table in [('notifications','core_systemnotification'),('dismissals','core_notificationdismissal')]:
            compare(orig['after'][source_name],c['after']['tables'][table],c,orig,'/'+table)
            require(c['after']['sequences'][table]==orig['after_named_audit']['sequences']['public.'+table+'_id_seq'],'BothNotificationSequencesExact')
    require(d['cases'][4]['before']==d['cases'][4]['after'],'SequentialRepeatAll11Exact')
    for c in d['extra_cases']:
        if c['name']!='repeat-truthy-update-only':require(c['before']==c['after'],'ExtraNoMutation:'+c['name'])
    truthy=d['extra_cases'][3];expected=json.loads(json.dumps(truthy['before']));expected['tables']['core_notificationdismissal'][0]['action_taken']='changed';require(expected==truthy['after'],'TruthyActionOnlyNoSequenceUse')
    before,after=values['default-before']['value'],values['default-after']['value'];require(controller.audit(before)==controller.audit(after)and before==after and e['default_eleven_tables_and_sequences_exact']is True,'Default11Exact')
    for label in('default-before','default-after'):
        require(values[label]['passed']is True and values[label]['stage']=='default'and values[label]['Django_or_Source_application_loaded']is False,'ReadonlyDefaultAuditScope')
    credentials=values['native-credential-rows-after']['value'];require(credentials['tables']['core_systemevent']['rows']==0 and credentials['raw_rows']['core_systemevent']==[] and credentials['raw_rows']['accounts_user']==allcases[-1]['after']['tables']['accounts_user'],'NativeCredentialRowsRetainedNotNormalized')
    require(e['native_credential_event_rows']==0 and e['raw_Native_cache_and_TTL_retained_without_Source_equivalence_claim']is True,'NativeCredentialCacheScope')
    for label in('native-cache-before','native-cache-after'):
        row=values[label];require(row['passed']is True and row['stage']=='cache'and row['Django_or_Source_application_loaded']is False and row['value']['capture_bytes']<=1048576,'NativeRawCacheBound')
        for item in row['value']['rows']:require(len(bytes.fromhex(item['key_hex']))<=1024 and len(bytes.fromhex(item['dump_hex']))<=65536 and type(item['pttl'])is int,'NativeRawCacheEntry')
    require(values['cleanup-label-inventories']=={'container':[],'network':[],'volume':[]}and values['cleanup-exact-ID.txt']==values['cleanup-exact-ID-command.stdout']=='','OwnedProjectAndExactIDAbsent')
    container=values['owned-container-inspect'][0];cid=container['Id'];argv=values['cleanup-exact-ID-command']['argv'];require(argv==['docker','container','ls','-a','--no-trunc','--filter','id='+cid,'--format','{{.ID}}'],'ExactFormerContainerSelector')
    require(container['Config']['Labels']['com.docker.compose.project']==e['project'],'OwnedProjectLabelBinding')
    require(len(values['precleanup-PIDs'])==len(values['cleanup-PID-absence'])>0,'RecordedOwnedPIDs')
    for old,new in zip(values['precleanup-PIDs'],values['cleanup-PID-absence']):require(old['pid']==new['pid']and old['starttime']==new['starttime']and new['original_process_absent']is True and(new['after_starttime']is None or new['after_starttime']!=old['starttime']),'ExactPIDStarttimeAbsence')
    require(e['owned_cleanup_independently_verified']is True and e['sealed_inputs_unchanged_after']is True,'OwnedCleanupAndInputsFinal')
    if root_review:
        review=values['Root-retained-review'];require(review['passed']is True and review['Root_cleanup_check_pending']is False and review['Root_actual_owned_inventory']=={'container':[],'network':[],'volume':[]},'IndependentRootReview')
    require(values['legacy-store-lib-host-loopback.exit'].strip()=='0'and'test result: ok. 184 passed; 0 failed; 58 ignored;'in values['legacy-store-lib-host-loopback.log'],'LegacyStore184Passed58Ignored')
    require('180 passed; 4 failed; 58 ignored;'in values['legacy-store-lib-sandbox.log']and'Operation not permitted'in values['legacy-store-lib-sandbox.log'],'SandboxSocketFailuresRetained')
    return {'format_version':1,'passed':True,'meaning':'Retained Native2/router effects qualify this bounded slice; no Source credentials or HTTP TCP pairing','Source_observation_cases':8,'Native_extra_definition_controls':8,'Source_controller_exit_preserved':1,'Native1_setup_process_exit_preserved':101,'Native1_controller_exit_preserved':1,'Native2_process_exit':0,'Native2_controller_exit':0,'Native2_isolation_Source_seed_match':True,'new_own_clock_paths':changed_paths,'Source8_both_notification_sequences_exact':True,'sequential_repeat_all11_rows_and_sequences_exact':True,'truthy_repeat_only_action_changed':True,'default_named_tables':11,'default_named_sequences':11,'default_audits_exact':True,'Native_credential_event_rows':0,'raw_clocks_users_cache_retained':True,'Source_force_auth_equals_Native_credentials_claimed':False,'cleanup_project_exactID_PID_verified':True,'legacy_Store_tests_passed':184,'legacy_Store_ignored':58,'executed_seal_sha256':expected_seal,'source_map_sha256':expected_map,'ELF_sha256':expected_elf,'operation_registrations':0,'Main_Source_app_or_performance_runs':0,'full_role_endpoint_or_M0_M1_acceptance':False}

def union_values(root,base):
    values=dict(base);folder=root/'raw/union791'
    for file in folder.glob('*.json'):values[file.stem]=json.loads(file.read_text())
    for name in('native2-controller-exit.txt','native2-controller-stdout.log','cleanup-exact-ID.txt','cleanup-exact-ID-command.stdout'):values[name]=(folder/name).read_text()
    return values
UNION=('f82486e6f86fec30df5e82abf71e8d9eae0f5d32bf1f40179bc99f9450a78fe5','a729127b8f57f261433152d958b60695fb9e25ce824b65678b614378ac6bc8c5','da6d0b89a7e735ccdebf02ed4d3bc00746edeac5f32afac09739a3a5b1e65db0')
def project(root=HERE):
    values=load(root);controller=executed_controller(root);component=derive(values,controller);union=derive(union_values(root,values),controller,UNION,True)
    return {'format_version':1,'component_Native2':component,'union791_same8plus8':union,'Source_reruns':0,'operation_registrations':0,'Source_controller_negative_and_Native1_setup_failure_preserved':True}
def validate(root=HERE):
    values=load(root);projection=project(root)
    require(sha(root/'inputs/final790-map.json')==MAP,'Exact790Map');mapping=read(root,'inputs/final790-map.json');require(len(mapping)==790,'790Inputs')
    for relative in('rust/crates/store/src/notifications.rs','rust/crates/store/tests/notifications_sql.rs','rust/crates/store/tests/fixtures/notification-source8.json'):require(sha(root/'snapshot'/relative)==mapping[relative],'SelectedRuntimeQAFixtureByteBinding')
    require((root/'raw/source-observations.json').read_bytes()==(root/'snapshot/rust/crates/store/tests/fixtures/notification-source8.json').read_bytes(),'SourceFixtureByteExact')
    require(sha(root/'raw/native2-seal.json')==SEAL and read(root,'inputs/executable-receipt.json')['executable_sha256']==ELF,'OriginalExecutedSealAndELFIdentity')
    require(sha(root/'inputs/union791-map.json')==UNION[1]and len(read(root,'inputs/union791-map.json'))==791,'ExactUnion791Map');require(read(root,'projection.json')==projection,'ExactPortableProjection');return projection
if __name__=='__main__':print(json.dumps(validate(),sort_keys=True))
