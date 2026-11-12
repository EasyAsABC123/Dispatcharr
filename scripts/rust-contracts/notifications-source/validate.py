#!/usr/bin/env python3
"""Read-only public eight-request Source observation/effect packet validation.
No Django, Docker, Source process, registration, filesystem writes or I/O outside
this packet. Original application/algorithm credit belongs to its contributors.
"""
import hashlib,json,re
from pathlib import Path
from effects import compare,CASES
from fixture_guards import require,dataset,unchanged_default
HERE=Path(__file__).resolve().parent
MANIFEST_SHA256='2f3c4f5d830310301356606e268af6b878399b07bbfc883c086190c6fa34d4d1'
SEAL_SHA256='2004c823a728d3ecdb7f7750867b0b0d3385fdac5779b4a074afb899b892ba59'
REVISION='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IMAGE='ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
CONTAINER='906682ce8908b1a21d9337624ac0b68ef9aa634c8a9b95cb6774c63e3686b832'
PROJECT='dispatcharr-notif-source-6380fe41f2';DATABASE='rust_probe_notif_6380fe41f2'
CANDIDATES=(('route-025c0d74fa99218d4728:GET','api/core/^notifications/$','GET','list',(0,1,5,6)),('route-025c0d74fa99218d4728:POST','api/core/^notifications/$','POST','create',(2,7)),('route-1577c7cc68b36d7ee6f4:POST','api/core/^notifications/(?P<pk>[^/.]+)/dismiss/$','POST','dismiss',(3,4)))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def read(root,name):return json.loads((root/name).read_text())
def load(root=HERE):
    manifest=root/'raw-manifest.json';require(sha(manifest)==MANIFEST_SHA256,'ExactRawManifest')
    value=json.loads(manifest.read_text());seen=set()
    for row in value['files']:
        relative=Path(row['path']);require(not relative.is_absolute() and '..'not in relative.parts and row['path']not in seen,'UniqueRelativeRawPath');seen.add(row['path'])
        require((root/relative).is_file() and (root/relative).stat().st_size==row['bytes'] and sha(root/relative)==row['sha256'],'RawHashBinding:'+row['path'])
    require(len(seen)==47,'Exact47RawAndProvenanceFiles')
    names=['source-observations','executed-effect-validation','controller-execution','execution-recovery','default-before','default-after','audit-before-report','audit-after-report','independent-cleanup','independent-process-absence','controller-label-cleanup','postexecution-seal-check','Root-preexecution-review','preexecution-seal-check','executed-seal','executed-compose']
    result={name:read(root,'raw/'+name+'.json')for name in names}
    result['controller-exit']=(root/'raw/controller-exit.txt').read_text();result['controller-stdout']=(root/'raw/controller-stdout.log').read_text();result['precleanup-processes']=(root/'raw/precleanup-processes.txt').read_text()
    return result

def derive(values):
    """Pure reconstruction; mutations fail even without consulting file hashes."""
    observed=values['source-observations'];execution=values['controller-execution'];effects=compare(observed)
    require(observed['database']==DATABASE and execution['copied_database']==DATABASE and execution['project']==PROJECT,'ActualOwnedIdentity')
    require(observed['capture_valid']is True and observed['actual_request_count']==8 and observed['passed']is False and observed['effect_qualification']=='PENDING_OFFLINE_VALIDATOR','CaptureDistinctFromControllerOrEffects')
    require(effects['passed']is True and len(effects['checks'])==213,'Recomputed213Effects')
    old=values['executed-effect-validation'];require(old['passed']is True and old['checks']==effects['checks'] and old['actual_Source_result']is True,'ExecutedEffectReportBinding')
    require(execution['passed']is False and execution['effects_passed']is True and execution['actual_request_count']==8 and execution['cleanup_error']=='ValueError:ExactOwnedContainerAbsent','NegativeControllerPreserved')
    require(values['controller-exit'].strip()=='1','ActualControllerExit1')
    stdout=json.loads(values['controller-stdout']);require(stdout['passed']is False and stdout['actual_requests']==8 and stdout['registered']is False,'ActualNegativeStdout')
    require(execution['Original_revision']==REVISION and execution['image']==IMAGE and execution['reviewed_seal_sha256']==SEAL_SHA256,'PinnedExecutionIdentity')
    require(execution['Source_only']is True and execution['real_credentials']is False and execution['real_TCP']is False and execution['registered']is False and execution['performance_claimed']is False and execution['web_workers_media_started']is False,'NarrowExecutionScope')
    before,after=values['default-before'],values['default-after'];require(unchanged_default(before,after) and before==after,'ExactDefaultElevenAudit')
    for name,audit in [('audit-before-report',before),('audit-after-report',after)]:
        report=values[name];require(report['audit']==audit and report['database']=='dispatcharr'and report['stage']=='audit'and report['passed']is True and report['capture_valid']is True and report['actual_request_count']==0 and report['connections_close_called']is True,'ReadOnlyAuditReportBinding')
    require(execution['default_eleven_named_tables_and_sequences_unchanged']is True,'ControllerDefaultAuditBinding')
    for name in ('preexecution-seal-check','postexecution-seal-check'):
        row=values[name];require(row['seal_valid']is True and row['seal_sha256']==SEAL_SHA256 and row['Source_executed']is False and row['requests_executed']==0,'BeforeAfterSealedBytesUnchanged')
    review=values['Root-preexecution-review'];require(review['passed']is True and review['seal_sha256']==SEAL_SHA256 and review['sealed_inputs']==29,'RootPreexecutionReview')
    cleanup=values['independent-cleanup'];require(len(cleanup)==4,'IndependentCleanupFourCommands')
    exact=cleanup[0];require(exact['argv']==['docker','inspect',CONTAINER] and exact['returncode']==1 and bytes.fromhex(exact['stdout_hex'])==exact['stdout_text'].encode()==b'[]\n' and bytes.fromhex(exact['stderr_hex'])==exact['stderr_text'].encode() and exact['stderr_text']=='error: no such object: '+CONTAINER+'\n','ExactIDCleanupNegativeInspect')
    require('No such object'not in exact['stderr_text'] and 'no such object'in exact['stderr_text'],'DockerCapitalizationLimitationPreserved')
    for index,kind in enumerate(('container','network','volume'),1):
        row=cleanup[index];require(row['returncode']==0 and row['stdout_hex']==row['stderr_hex']==row['stdout_text']==row['stderr_text']=='','IndependentProjectInventoryEmpty')
        require(row['argv'][:3]==['docker',kind,'ls'] and 'label=com.docker.compose.project='+PROJECT in row['argv'],'ExactOwnedProjectSelector')
        if kind=='container':require('-a'in row['argv']and'--no-trunc'in row['argv'],'AllContainersIncludingStopped')
    require(values['controller-label-cleanup']=={'container':[],'network':[],'volume':[]},'ControllerLabelInventoryPreserved')
    pids=[int(row.split()[0])for row in values['precleanup-processes'].splitlines()[1:]if row.strip()]
    absence=values['independent-process-absence'];require(len(pids)==len(set(pids))==len(absence)==8 and [r['pid']for r in absence]==pids and all(r['proc_exists']is False for r in absence),'ExactRecordedPIDsAbsent')
    require(not re.search(r'\b(uwsgi|celery|daphne|ffmpeg|manage.py shell)\b',values['precleanup-processes']),'NoWebWorkerMediaOrShellAtTeardown')
    recovery=values['execution-recovery'];require(recovery['controller_passed']is False and recovery['controller_actual_exit']==1 and recovery['actual_requests']==8 and recovery['effect_checks']==recovery['effect_checks_passed']==213 and recovery['independent_container_network_volume_absence']is True and recovery['independent_recorded_process_PIDs_absent']==8 and recovery['Source_fixture_retries']==recovery['sealed_input_edits']==recovery['registration_changes']==0,'RecoveryDoesNotRewriteNegativeExecution')
    compose=values['executed-compose'];service=compose['services']['baseline'];require(list(compose['services'])==['baseline'] and service['image']==IMAGE and service['pull_policy']=='never' and 'ports'not in service and compose['networks']['default']['internal']is True,'NoPublishedOrExternalFixtureNetwork')
    require(observed['no_network_setting_mutation']is True and len(observed['seed_users'])==2 and [(u['user_level'],u['is_active'],u['is_superuser'])for u in observed['seed_users']]==[(1,True,False),(10,True,False)],'ExplicitRealOwnedRoleSlice')
    cases=[]
    for index,row in enumerate(observed['requests']):
        cases.append({'case_index':index,'name':row['name'],'role':row['role'],'authentication':'anonymous APIClient'if row['role']=='anonymous'else'real owned user with force_authenticate; JWT/API-key backend bypassed','method':row['method'],'path_exact':row['path'],'resolver_action':row['resolver_action'],'actual_status':row['actual_status'],'body_bytes':row['body_bytes'],'body_sha256':row['body_sha256'],'raw_evidence_pointer':'raw/source-observations.json#/requests/'+str(index),'notification_rows_before_after':[len(row[k]['notifications'])for k in ('before','after')],'dismissal_rows_before_after':[len(row[k]['dismissals'])for k in ('before','after')],'cache_keys_before_after':[len(row[k]['rows'])for k in ('before_owned_cache','after_owned_cache')],'Source_clock_interval':[row['began_utc'],row['completed_utc']]})
    candidates=[{'candidate_contract_id':cid,'pattern_exact':pattern,'method':method,'source_handler':'core.api_views.SystemNotificationViewSet','source_action':action,'case_indices':list(indices),'registered':False,'fully_verified':False}for cid,pattern,method,action,indices in CANDIDATES]
    return {'format_version':1,'scope':'One Source-only in-process real-resolver APIClient attempt; force-auth framework routing/permissions/SQL/cache observations only','Original_revision':REVISION,'image':IMAGE,'executed_seal_sha256':SEAL_SHA256,'observation_capture_complete':True,'request_count':8,'effect_comparison_success':True,'recomputed_effect_checks':213,'executed_controller_success':False,'executed_controller_exit':1,'controller_cleanup_assertion':'ValueError:ExactOwnedContainerAbsent','independent_cleanup_recovered':True,'Docker_capitalization_limitation':'Executed case-sensitive No such object check rejected actual lower-case error: no such object; executed result remains exit1','default_named_audit':{'tables':11,'sequences':11,'before_after_exact':True,'dataset_sha256':before['dataset_sha256'],'full_database_audit':False},'raw_cache_scope':'Owned Django Redis database DUMP/PTTL observations including unrelated timezone/group activity; no global silence or Channels/Celery delivery claim','actual_database':DATABASE,'actual_project':PROJECT,'actual_container_id':CONTAINER,'recorded_container_PIDs_absent':pids,'cases':cases,'candidate_operations_unregistered':candidates,'new_Source_attempts':1,'automatic_retries':0,'registered_operations':0,'fully_verified_added':0,'acceptance':{'credentials':False,'HTTP_TCP':False,'Native':False,'performance':False,'full_role_matrix':False,'full_endpoint':False,'M0_M1':False}}

def provenance(root,values):
    require(sha(root/'raw/executed-seal.json')==SEAL_SHA256,'ExecutedSealExactBytes')
    seal=values['executed-seal'];require(seal['state']=='SEALED_UNEXECUTED_REQUIRES_ROOT_REVIEW'and len(seal['files'])==29,'OriginalPreexecutionSealStatePreserved')
    omitted={'inputs/original-source.tar','inputs/proposal-PLAN.md','inputs/proposal-plan.json','inputs/proposal-source-evidence.json'};verified=[]
    for row in seal['files']:
        path=root/'executed'/row['path']
        if row['path']in omitted:continue
        require(path.is_file()and path.stat().st_size==row['bytes']and sha(path)==row['sha256'],'ExactExecutedPublicInput:'+row['path']);verified.append(row['path'])
    require(len(verified)==25,'Exact25PublishedExecutedInputs')
    archive=next(row for row in seal['files']if row['path']=='inputs/original-source.tar');require(archive['sha256']=='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'and archive['bytes']==16609280,'RetainedFullArchiveIdentity')
    original=read(root,'executed/inputs/pinned-Original-files.json');require(original['revision']==REVISION and len(original['files'])==15,'Pinned15OriginalAuthorities')
    for row in original['files']:require(sha(root/'executed'/row['path'])==row['sha256'],'PinnedOriginalAuthorityBytes')
    require((root/'effects.py').read_bytes()==(root/'executed/controller/effects.py').read_bytes()and(root/'fixture_guards.py').read_bytes()==(root/'executed/controller/fixture_guards.py').read_bytes(),'PortableComparatorIsExecutedComparator')
    return {'published_executed_inputs_byte_exact':25,'pinned_Original_authority_files':15,'sealed_input_files_not_copied_publicly':sorted(omitted),'full_application_reexecution_package_claimed':False}

def validate(root=HERE):
    values=load(root);projection=derive(values);proof=provenance(root,values)
    require(read(root,'projection.json')==projection,'ExactReadOnlyProjection')
    return {'passed':True,'meaning':'Public evidence and negative-controller report validated; does not mean executed controller passed','requests':8,'effect_checks':213,'controller_exit_preserved':1,'registered_operations':0,'provenance':proof,'Source_or_Docker_executed_by_validator':False}
if __name__=='__main__':print(json.dumps(validate(),sort_keys=True))
