"""Recompute this finite Source100 / Native7-prefix publication. No app access."""
import hashlib
import json
import math
from pathlib import Path
import re

HERE = Path(__file__).resolve().parent
AUTHORITY_SHA256 = '0995ca7dc7517511dc8d0284c4ae2b538b2f3715b6715d7d41fededac0ccc556'

def need(value, label):
    if not value: raise ValueError(label)
def read(name): return json.loads((HERE/name).read_text())
def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def canonical(value): return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()).hexdigest()
def integer(value): return type(value) is int
def finite(value): return type(value) in (int,float) and math.isfinite(value)
def hex64(value): return type(value) is str and re.fullmatch('[0-9a-f]{64}',value) is not None

def manifest(value, scope, authority):
    need(value['database_scope']==scope,'SQLDatabaseScope')
    need(sorted(value['tables'])==authority['tables'] and sorted(value['sequences'])==authority['sequences'],'FullSQLSchemaCoverage')
    for row in value['tables'].values():
        need(set(row)=={'rows','sha256'} and integer(row['rows']) and 0<=row['rows']<=10000 and hex64(row['sha256']),'SQLTableRecord')
    for row in value['sequences'].values():
        need(set(row)=={'last_value','is_called'} and integer(row['last_value']) and type(row['is_called']) is bool,'SQLSequenceRecord')

def response(row):
    need(type(row['body_hex']) is str and len(row['body_hex'])<=2*1048576,'BodyCap')
    raw=bytes.fromhex(row['body_hex'])
    need(hashlib.sha256(raw).hexdigest()==row['body_sha256'],'ActualBodyHash')
    need(type(row['headers']) is list and len(row['headers'])>0,'ActualResponseHeaders')
    for pair in row['headers']:
        need(type(pair) is list and len(pair)==2 and all(type(v) is str for v in pair),'HeaderPair')
        need(pair[0].casefold() not in {'cookie','set-cookie','authorization','proxy-authorization'},'NoSensitiveResponseHeaders')
    if row['status'] in [200,401]:
        return json.loads(raw,parse_constant=lambda _: (_ for _ in ()).throw(ValueError('NonfiniteBody')))
    need(row['status']==404 and b'<title>Not Found</title>' in raw,'LiteralSource404Page')
    return None

def projection_privacy(value):
    forbidden={'nonce','boot_id','env','environment','selected_environment','xc_password','api_key','jwt','access_token','refresh_token','authorization','cookie','password'}
    if isinstance(value,dict):
        need(not(set(value)&forbidden),'NoPrivateAuthorizationOrDerivationFields')
        for item in value.values():projection_privacy(item)
    elif isinstance(value,list):
        for item in value:projection_privacy(item)

def validate(data,authority):
    projection_privacy(data)
    scope=data['scope']
    required_false=['single_combined_Source_fixture_pass','Native_overall_fixture_pass','process_delivery_qualified','Source_Native_epoch_body_identity_claimed','Main_acceptance','worker_graph_activation','performance_acceptance','full_M3_M4_M7_acceptance']
    need(all(scope[k] is False for k in required_false),'ScopeCannotExpand')
    need(scope['Source_observations']==100 and scope['Native_qualified_test_prefix']==7,'ExactScopedCounts')
    for key,value in {'Source_argv24_returned':21,'Source_argv24_ValueError':3,'Factory38_status200':32,'Factory38_status401':2,'Factory38_Http404_exception':4,'TCP38_status200':32,'TCP38_status401':2,'TCP38_status404':4,'Factory_actual_SQL_calls':92}.items(): need(integer(scope[key]) and scope[key]==value,'ExactScopeCounter:'+key)
    argv=data['argv']['cases']
    need([r['name'] for r in argv]==authority['Source24_names'] and len(argv)==24,'ExactSource24Coverage')
    returned=raised=0
    for row in argv:
        need(row['SQL_journal_invalid'] is None and len(row['actual_SQL'])==1 and row['actual_SQL'][0]['sql'].startswith('SELECT ') and 'core_outputprofile'in row['actual_SQL'][0]['sql'],'ActualModelSQLReload')
        observed=row['observed']
        if observed['kind']=='return':
            returned+=1
            need(type(observed['argv']) is list and len(observed['argv'])>=1 and all(type(v) is str for v in observed['argv']),'ActualArgvType')
            need(observed['argv'][0]==row['SQL_command'],'UnsplitCommandFirstArgument')
        else:
            raised+=1
            need(observed['kind']=='raise' and observed['exception_type']=='ValueError' and observed['message'] in ['No closing quotation','No escaped character'],'LiteralModelException')
        need(canonical(row)==authority['case_projection_sha256']['argv24'][row['name']],'ExactArgv24RecordBinding')
    need((returned,raised)==(21,3),'Literal21Return3ValueError')
    counts={}
    for label in ['factory','tcp']:
        doc=data[label];rows=doc['cases'];need([r['name'] for r in rows]==authority['XC38_names'] and len(rows)==38,'ExactXC38Coverage:'+label)
        actual={200:0,401:0,404:0,'Http404':0};queries=0
        for row in rows:
            manifest(row['before'],'owned',authority);manifest(row['after'],'owned',authority)
            need(row['before']==row['after'],'PerCaseSQLSequencesExact')
            need(finite(row['began_seconds']) and finite(row['completed_seconds']) and 0<=row['began_seconds']<=row['completed_seconds'],'ActualCaseClocks')
            if label=='factory':
                need(row['SQL_journal_invalid'] is None and type(row['actual_SQL']) is list,'FactorySQLJournal')
                queries+=len(row['actual_SQL'])
            if 'status' in row:
                need(integer(row['status']) and row['status'] in [200,401,404],'LiteralHTTPStatus')
                actual[row['status']]+=1;response(row)
            else:
                need(label=='factory' and row['exception_type']=='Http404','FactoryExceptionNotHTTP')
                actual['Http404']+=1
            if label=='tcp':
                need(finite(doc['phase_seconds']) and 0<doc['phase_seconds']<=90 and row['completed_seconds']<=doc['phase_seconds'],'TCPPhaseClock')
                observation=row['absolute_observation'];physical=row['physical_close']
                need(finite(observation['deadline_seconds']) and finite(observation['completed_seconds']) and row['began_seconds']<=observation['completed_seconds']<=observation['deadline_seconds']<=row['began_seconds']+8.01,'AbsoluteObservationDeadline')
                need(observation['timer_joined'] is True and physical['timer_joined'] is True and integer(physical['fd_after']) and physical['fd_after']==-1 and physical['original_inode_absent'] is True,'ActualFDAndTimerSettlement')
            need(canonical(row)==authority['case_projection_sha256'][label+'38'][row['name']],'ExactCaseProjectionBinding')
        need(actual==({200:32,401:2,404:0,'Http404':4} if label=='factory' else {200:32,401:2,404:4,'Http404':0}),'LiteralSourceOutcomeCounts')
        if label=='factory': need(queries==92,'Actual92FactorySQLCalls')
        counts[label]=actual
    need(data['source_attempts']['single_combined_fixture_pass'] is False and data['source_attempts']['Source_observations']==100,'NoCombinedSourceFixtureClaim')
    attempts=data['source_attempts']['attempts']
    need(len(attempts)==3 and [r['attempt'] for r in attempts]==[1,2,3] and [r['actual_host_exit'] for r in attempts]==[1,1,0] and [r['original_passed'] for r in attempts]==[False,False,True],'SourceFailedAttemptsRetained')
    for row in attempts:
        manifest(row['default_before'],'default',authority);manifest(row['default_after'],'default',authority)
        need(row['default_before']==row['default_after'],'DefaultSQLSequencesExact')
        need(set(row['independent_cleanup'])=={'containers','networks','volumes'} and set(row['controller_remaining'])=={'containers','networks','volumes'},'AllOwnedResourceKinds')
        for result in row['independent_cleanup'].values():need(result['actual_exit']==0 and result['stdout']==result['stderr']=='','IndependentOwnedEmpty')
        need(all(v=='' for v in row['controller_remaining'].values()),'ControllerOwnedEmpty')
    need(attempts[0]['cleanup_errors']==['source-redis:CollectionFailure'],'Source1RedisNegativeRetained')
    need('first TCP request issued' in attempts[1]['scope_boundary'],'Source2IssuedRequestNotZeroRequests')
    native=data['native'];rows=native['gates']
    need(native['actual_host_exit']==1 and native['original_fixture_passed'] is False,'NativeOverallFailureRetained')
    need(all(native[key] is False for key in ['performance_acceptance','Main_acceptance','full_M7_acceptance']),'NativeScopeCannotExpand')
    need(native['expected_gates']==[{'role':'native-'+str(i+1),'filter':name,'command_seconds':45} for i,name in enumerate(authority['Native_filters'])],'ExactExpectedNativeGates')
    need(len(rows)==7 and [r['filter'] for r in rows]==authority['Native_filters'] and [r['role'] for r in rows]==['native-'+str(i) for i in range(1,8)],'AllSevenFixedNativeGates')
    need(finite(native['phase_seconds']) and 0<native['phase_seconds']<=180,'NativeFinitePhase')
    for row in rows:
        need(row['actual_exit']==0 and row['actual_passed_tests']==1 and row['actual_filtered_tests']==19,'ActualOneSelectedNativeTest')
        need(finite(row['started_seconds']) and finite(row['completed_seconds']) and 0<=row['started_seconds']<=row['completed_seconds']<=native['phase_seconds'],'NativeActualStageClock')
        need(finite(row['Rust_reported_seconds']) and 0<=row['Rust_reported_seconds']<=45,'NativeFiniteTestClock')
        path=Path(row['stdout_path']);need(not path.is_absolute() and '..'not in path.parts and sha(HERE/path)==row['stdout_sha256'],'ActualNativeStdoutHash')
        lines=(HERE/path).read_text().splitlines()
        need(lines.count('test '+row['filter']+' ... ok')==1 and sum(line.startswith('test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 19 filtered out;') for line in lines)==1,'ActualNativeTestTranscript')
        command=row['command'];need(command['actual_exit']==0 and command['invalid'] is None and all(command[k] is True for k in ['physically_settled','actual_identity_observed','stdout_pipe_closed','stderr_pipe_closed','pipe_reader_threads_settled','stdin_writer_settled']),'ActualNativeHostWaitAndPipes')
        owned=row['owned_process'];need(owned['role']==row['role'] and hex64(owned['identity_sha256']) and owned['identity_sha256']==owned['terminal_identity_sha256'] and owned['terminal_state']=='absent' and owned['physical_process_stop_observed'] is True,'SameOwnedNativeProcessStopped')
    for prefix,db in [('owned','owned'),('default','default')]:
        manifest(native[prefix+'_before'],db,authority);manifest(native[prefix+'_after'],db,authority);need(native[prefix+'_before']==native[prefix+'_after'],'NativeSQLSequencesExact')
    need(native['Redis']['actual_exit']==0 and all(native['Redis'][k] is True for k in ['actual_identity_observed','stdout_pipe_closed','stderr_pipe_closed','pipe_reader_threads_settled','stdin_writer_settled','pipes_physically_settled']),'RedisActualSettlement')
    need(set(native['controller_remaining'])=={'containers','networks','volumes'} and all(v=='' for v in native['controller_remaining'].values()) and native['exact_CID_NotFound'] is True,'NativeOwnedCleanup')
    origin=native['terminal_origin_audit'];need(origin['qualified'] is False and origin['journal_captured'] is False and origin['static_baseline_depends_on_origin'] is True and origin['static_referenced_origin_script_missing'] is True and origin['origin_exit_or_log_captured'] is False,'OriginFailureCannotBecomeNoContactClaim')
    negative=data['native_attempts'];need(negative['overall_fixture_pass'] is False and negative['no_seven_case_rerun'] is True and len(negative['attempts'])==2,'BothNativeNegativesRetained')
    second=negative['attempts'][1];need(second['attempt']==2 and second['actual_host_exit']==1 and second['original_passed'] is False and second['Native_tests_executed']==second['Native_tests_actual_exit0']==7,'NativeSecondFailureAndPrefix')
    first=negative['attempts'][0];need(first['actual_host_exit']==1 and first['original_passed'] is False and first['Docker_creations']==first['Native_tests_executed']==0 and first['commands_executed']==1 and first['archive_actual_exit']==0 and first['archive_bytes']==authority['Original_full_archive_bytes'] and first['archive_sha256']==authority['Original']['Original_full_archive_sha256'],'CorrectArchiveEmptyBufferFailureRetained')
    need(canonical(data['native'])==authority['Native_prefix_projection_sha256'],'ExactNativePrefixClockAndRecordAuthority')
    need(canonical(data['source_attempts'])==authority['Source_attempts_projection_sha256'],'ExactSourceAttemptAuthority')
    need(canonical(data['native_attempts'])==authority['Native_attempts_projection_sha256'],'ExactNativeAttemptAuthority')
    need(canonical(data['factory']['metadata'])==authority['Factory_metadata_projection_sha256'],'ExactObservedFactoryRuntimeMetadata')
    return {'Source_literal_observations':100,'Source_return_or_status_counts':{'argv_return':21,'argv_ValueError':3,'Factory200':32,'Factory401':2,'FactoryHttp404':4,'TCP200':32,'TCP401':2,'TCP404':4},'Native_qualified_prefix':7,'single_Source_fixture_pass':False,'Native_overall_fixture_pass':False,'origin_contact_journal_qualified':False,'Main_or_full_M3_M4_M7_or_performance_acceptance':False}

def load_data():
    return {key:read(name) for key,name in [('scope','scope.json'),('argv','source-argv24.json'),('factory','source-factory38.json'),('tcp','source-tcp38.json'),('source_attempts','source-attempts.json'),('native','native-seven-prefix.json'),('native_attempts','native-attempts.json')]}
def main():
    need(sha(HERE/'authority.json')==AUTHORITY_SHA256,'ImmutableFiniteAuthority')
    authority=read('authority.json')
    need(sha(HERE/'prototype-inputs.json')==authority['Prototype_manifest_sha256'],'ExactPrototypeManifest')
    prototype=read('prototype-inputs.json')
    for name,digest in prototype['overlays'].items(): need(sha(HERE/'prototype-overlay'/name)==digest,'ExactHistoricalPrototypeOverlay')
    report=validate(load_data(),authority)
    print(json.dumps(report,indent=2))
if __name__=='__main__':main()
