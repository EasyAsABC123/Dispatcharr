"""Offline Native67 observation verification. Controller1 stays a retained failure."""
import gzip,hashlib,importlib.util,json,re
from pathlib import Path
HERE=Path(__file__).resolve().parent
AUTHORITY='d658a763a693e0094cff59a8a3af3a459bfb02900cff7ead7d56495e7fcda070'
def require(v,label):
    if not v:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def read(p):
    p=Path(p);require(p.is_file()and not p.is_symlink()and p.stat().st_size<=4194304,'BoundedRegularInput');return json.loads(p.read_bytes())
def compressed(p):
    p=Path(p);require(p.is_file()and not p.is_symlink()and p.stat().st_size<=4194304,'BoundedCompressedInput')
    with gzip.open(p,'rb')as stream:raw=stream.read(33554433)
    require(len(raw)<=33554432,'BoundedExpandedInput');return json.loads(raw)
def comparer():
    s=importlib.util.spec_from_file_location('retained_native67_compare',HERE/'compare.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def sentinel(cache):
    key=b'rust-probe-notification-native-owner'.hex();rows=[r for r in cache['rows']if r['key_hex']==key];require(len(rows)==1,'OneActualSentinelDUMP')
    raw=bytes.fromhex(rows[0]['dump_hex']);require(len(raw)>=12 and raw[0]==0 and raw[1]<64 and len(raw)==raw[1]+12,'BoundedLiteralStringDUMP')
    return raw[2:2+raw[1]].decode()

def observations(mutations,legacy,source,execution,collector1,collector2,timeline):
    c=comparer()
    for report,expected_users in [(mutations,3),(legacy,2)]:
        for row in report['cases']+report['extra_cases']:
            for state in [row['before'],row['after']]:
                users=state['tables']['accounts_user'];require(type(users)is list and len(users)==expected_users,'MandatoryCredentialSQLRows')
                for user in users:
                    for key in ['password','api_key']:
                        marker=user.get(key);require(type(marker)is dict and set(marker)=={'private_field_sha256'}and re.fullmatch('[0-9a-f]{64}',marker['private_field_sha256'])is not None,'CredentialDigestProjectionRequired')
    m=c.mutations(mutations,source);l=c.legacy(legacy)
    require(execution.get('passed')is False and execution.get('controller_exit')==1 and execution.get('mutations_final_collection_error')=='CommandExit1','PreserveControllerNegative')
    require(execution.get('Source_runs')==execution.get('Main_runs')==execution.get('operation_registrations')==0 and execution.get('renderer500_body_parity_claimed')is False and execution.get('forms_multipart_qualified')is False,'KeepUnsupportedScopes')
    require(set(execution['native_processes'])=={'mutations','legacy'},'ExactlyTwoRustProcesses')
    for row in execution['native_processes'].values():require(row['exit']==0 and row['physically_settled']is True and row['pipes_settled']is True and c.clock(row['began_utc'])<=c.clock(row['completed_utc']),'RustActualExitAndSettlement')
    db1,db2=execution['databases']['mutations'],execution['databases']['legacy'];require(db1!=db2 and mutations['database']==db1 and legacy['database']==db2,'DistinctSQLCacheEpochs')
    c.named_cache(mutations['final_cache'],source,source['requests'][-1]['after_condition_cache'],db1)
    require(mutations['final_cache']['named']['expected_database']==db1 and bytes.fromhex(mutations['final_cache']['named']['sentinel']['raw_hex']).decode()==db1,'InsideRustEpoch1FinalCache')
    for value,db in[(collector1,db1),(collector2,db2)]:
        require(value['passed']is True and value['stage']=='cache'and value['Django_or_Source_application_loaded']is False,'ValidReadonlyPrimaryCollection');c.raw_cache(value['value']);require(sentinel(value['value'])==db,'ExactCollectorEpochOwner')
    # DUMP payload equality of epoch1 completion is exact; decreasing PTTLs are
    # kept literal and bounded rather than falsely compared across capture times.
    inner={r['key_hex']:r['dump_hex']for r in mutations['final_cache']['rows']};outer={r['key_hex']:r['dump_hex']for r in collector1['value']['rows']};require(inner==outer,'Epoch1PrimaryAfterMatchesRustFinalDUMPs')
    require(type(timeline)is list and len(timeline)==35 and [x['command_file']for x in timeline]==['command-'+str(i).zfill(3)+'.json'for i in range(1,36)],'CompleteCommandTimeline')
    current=None;sets=[];refused=[];primary=[]
    for row in timeline:
        require(c.clock(row['began_utc'])<=c.clock(row['completed_utc'])and row['physically_settled']is True and row['pipes_settled']is True,'CommandClockPhysicalSettlement')
        if 'epoch_transition_SET_owner'in row:
            require(row['exit']==0,'SuccessfulOwnerTransition');current=row['epoch_transition_SET_owner'];sets.append(current)
        if row.get('collection',{}).get('stage')=='cache':
            requested=row['collection']['database']
            if requested!=current:
                require(row['command_file']=='command-025.json'and requested==db1 and current==db2 and row['exit']==1 and row['collection']['label']=='mutations-final-cache','ExactLateEpochRefusal');refused.append(row)
            else:require(row['exit']==0,'PrimaryEpochCollectionSuccess');primary.append(row['collection']['label'])
    require(sets==[db1,db2]and len(refused)==1 and 'mutations-cache-after'in primary and 'legacy-final-cache'in primary,'BothEpochsAndPreservedLateNegative')
    return {'observation_requests_qualified':67,'Source48_plus_Native_controls':51,'legacy8_plus8':16,'Rust_actual_exits':[0,0],'controller_actual_exit':1,'overall_fixture_passed':False,'all51_raw_named_cache_projections_checked':True,'generated_own_clock_mappings':len(mutations['clock_mapping']),'valid_epoch1_completion_reference':'primary mutations-cache-after and inside-Rust final_cache','late_epoch1_recapture_claimed':False,'renderer500_forms_physical_SQLcancel_Main_Source_credentials_graph_performance_qualified':False,'comparison_results':[m,l]}

def cleanup(execution,before,after,pre,pids,root,identity):
    c=comparer();require(before.get('passed')is True and after.get('passed')is True and before.get('Django_or_Source_application_loaded')is False and after.get('Django_or_Source_application_loaded')is False,'ReadonlyDefaultAudits');c.audit(before['value']);c.audit(after['value']);require(before==after and execution['default11_exact']is True,'ExactDefault11RowsSequences')
    require(type(pre)is list and pre and type(pids)is list and len(pids)==len(pre),'AllOwnedPIDCoverage')
    require({(x['pid'],x['starttime'])for x in pre}=={(x['pid'],x['starttime'])for x in pids},'ExactPIDStarttimeBindings')
    require(all(x.get('original_process_absent')is True and x.get('after_starttime')is None for x in pids),'ActualPIDAbsence')
    require(root.get('observation_qualified')is True and root.get('controller_passed')is False and root.get('actual_controller_exit')==1,'RootObservationNotControllerPass')
    queries=root['owned_cleanup_queries'];require(set(queries)=={'container','network','volume','exact_CID'},'CompleteIndependentCleanup')
    for key in ['container','network','volume']:
        q=queries[key];require(q['exit']==0 and q['stdout']==q['stderr']==''and 'label=com.docker.compose.project='+execution['project']in q['argv'],'IndependentProjectEmpty')
    exact=queries['exact_CID'];require(exact['id']==identity['Id']and exact['exit']==1 and exact['stdout']=='[]\n'and 'no such object'in exact['stderr'],'ExactCIDNoSuchObjectReceipt')
    return {'default11_exact':True,'owned_PID_count':len(pids),'CID_PID_project_cleanup_verified':True,'controller_negative_preserved':True}

def builds(build,peers,workspace,full):
    require(build['passed']is True and build['state']=='COMPLETED'and build['runtime792_count']==792 and build['coupled_count']==5,'Build792Plus5')
    require(len(build['commands'])==6 and all(r['exit']==r['poll']==0 and r['wait_completed']is True and r['state']=='SETTLED'for r in build['commands']),'AllSixBuildCommandsWaitedZero')
    require(set(peers)=={'mutations','legacy'},'TwoHistoricalELFBindings')
    for row in peers.values():require(row['compile_exit']==0 and row['wait_completed']is True and row['copy_after_successful_compile_and_wait']is True and row['all_six_authorized_commands_waited_exit0']is True and row['full797_map_sha256']==full,'ELFPostSuccessBindingNoCopy')
    require(workspace['passed']is True and workspace['fmt_non_test_strict_workspace_actual_exits']==[0,0,0]and workspace['unchanged_full_default_host_exit']==0 and workspace['Root_parsed_failed']==0 and workspace['full797_map_sha256']==full and workspace['all4_pre_post797_maps_equal_frozen_QA2']is True and workspace['sandbox101four_ingestHTTP_socketEPERM_preserved']is True and workspace['Native67_controller_actual_exit_remains']==1,'AdditiveWorkspaceReceipt')
    return {'compile_commands':6,'source_Rust':792,'coupled5':5,'binaries_distributed':False,'workspace_defaults_aggregate':workspace['Root_parsed_passed'],'workspace_ignored':workspace['Root_parsed_ignored'],'result_blocks':workspace['Root_parsed_result_blocks'],'aggregate_is_not_unique_contract_count':True,'sandbox_attempt_preserved':True}

def legacy_source8(actual,source):
    c=comparer();require(actual['Source_fixture_raw_sha256']=='fadc5eb12f7b56e5717439da46cf754cc134c94e9bafb8314ed0a804c677c3d9'and len(source['requests'])==8,'ActualLegacySource8Authority')
    mapping={};previous=None
    for row,reference in zip(actual['cases'],source['requests']):
        require(row['name']==reference['name']and row['status']==reference['actual_status'],'LegacySourceCaseIdentity')
        c.body(row,'body');c.shape(row['before']);c.shape(row['after'])
        if previous is not None:require(c.exact(row['before'],previous),'LegacySourceCrossRequestContinuity')
        previous=row['after']
        c.compare_values(reference['json'],row['body'],reference,row,mapping,'/response')
        for key,table in [('notifications',c.NOTICE),('dismissals',c.DISMISS)]:
            c.compare_values(reference['before'][key],row['before']['tables'][table],reference,row,mapping,'/before/'+table)
            c.compare_values(reference['after'][key],row['after']['tables'][table],reference,row,mapping,'/after/'+table)
            require(row['after']['sequences'][table]==reference['after_named_audit']['sequences']['public.'+table+'_id_seq'],'LegacySourceExactSequences')
        for table in c.TABLES:
            if table not in(c.NOTICE,c.DISMISS):require(c.exact(row['before']['tables'][table],row['after']['tables'][table]),'LegacyOtherNineUnchanged')
    return {'first8_Source_responses_and_SQL_effects_checked':True,'own_clock_mappings':len(mapping),'Source_force_auth_equals_Native_credentials':False}

def verify():
    require(sha(HERE/'data-authority.json')==AUTHORITY,'ExactDataAuthority');authority=read(HERE/'data-authority.json')
    for rel,wanted in authority['files'].items():require(sha(HERE/rel)==wanted,'RetainedInputHash:'+rel)
    o=HERE/'observations';execution=read(o/'execution-projection.json');mutations=compressed(o/'mutations-projection.json.gz');legacy=compressed(o/'legacy-projection.json.gz');source=compressed(o/'Source48-projection.json.gz')
    result=observations(mutations,legacy,source,execution,read(o/'mutations-cache-after.json'),read(o/'legacy-final-cache.json'),read(o/'command-timeline.json'))
    for name,expected in [('mutations-credentials-after',mutations['final_credentials']),('mutations-final-credentials',mutations['final_credentials'])]:
        row=read(o/(name+'.json'));require(row['passed']is True and row['Django_or_Source_application_loaded']is False and row['value']['database']==execution['databases']['mutations']and row['value']['raw_rows']['accounts_user']==expected['accounts_user']and row['value']['raw_rows']['core_systemevent']==expected['core_systemevent']and row['value']['sequences']['public.core_systemevent_id_seq']==expected['core_systemevent_sequence'],'IndependentEpoch1SQLCredentials:'+name)
    legacy_a,legacy_b=read(o/'legacy-credentials-after.json'),read(o/'legacy-final-credentials.json')
    require(legacy_a==legacy_b and legacy_a['passed']is True and legacy_a['Django_or_Source_application_loaded']is False,'LegacySQLCredentialsAfterAndFinal')
    result['legacy_Source8_detail']=legacy_source8(legacy,read(o/'legacy-source8-reference.json'))
    result['cleanup']=cleanup(execution,read(o/'default-before.json'),read(o/'default-after.json'),read(o/'precleanup-PIDs.json'),read(o/'cleanup-PID-absence.json'),read(o/'Root-retained-observation-review.json'),read(o/'container-identity-projection.json'))
    result['build_binding']=builds(read(HERE/'build/build.json'),read(HERE/'source/executable-bindings.json'),read(HERE/'workspace/Root-independent-review.json'),sha(HERE/'source/full797-map.json'))
    require('ValueError: OwnedRedisMarker'in(o/'late-collector-refusal.stderr').read_text(),'StrictRefusalRetained')
    result.update(passed=True,observation_qualified=True,application_replayed=False,source_runtime_still_private_not_packaged=True)
    return result
if __name__=='__main__':print(json.dumps(verify(),indent=2))
