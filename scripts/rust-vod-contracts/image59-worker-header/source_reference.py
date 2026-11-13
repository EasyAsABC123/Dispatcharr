"""Additive retained Source6 validation, never rewrite the failed Source7 record."""
import copy
import hashlib
import json
import math
from pathlib import Path
from qualify import phase,media_total
from origin_coverage import validate as origin_validate
from identity import worker


def validate(source,summary,auth,stderr,policy):
    original=['head405','invalid_get401','no_provider503','bad_uuid404','full200','range206','provider404_to500']
    if [x.get('case')for x in source.get('cases',[])]!=original:raise ValueError('ExactHistoricalSourceSeven')
    if source.get('qualification')!={'capture_valid':False,'worker_header_branch_scope_qualified':False,'full_header_or_M4_parity':False}or source.get('qualification_error')!='ValueError:FixedBranchStatus:bad_uuid404':raise ValueError('PreserveOriginalDerivedFailure')
    if summary.get('passed')is not False or summary.get('error')!='CollectionFailure:CommandExit1' or 'native'in summary:raise ValueError('PreserveOriginalRunFailureNoNativePhase')
    if summary.get('remaining')!={'containers':'','networks':'','volumes':''}or summary.get('default_named_tables_sequences_unchanged')is not True or summary.get('default_before')!=summary.get('default_after'):raise ValueError('OriginalCleanupAndNamedAudit')
    if auth.get('status')!=200 or type(auth.get('expected_real_SQL_user_id'))is not int or auth.get('auth_response_user_id')!=auth['expected_real_SQL_user_id'] or auth.get('auth_response_user_level')!=10 or auth.get('auth_response_stream_limit')!=0 or auth.get('physical_close',{}).get('fd_after')!=-1 or auth['physical_close'].get('original_inode_absent')is not True or auth.get('absolute_observation',{}).get('timer_joined')is not True:raise ValueError('RetainedSQLBearerIdentity')
    if summary.get('cleanup_errors'):raise ValueError('HistoricalCleanupError')
    origin_validate(summary['origin_terminal'],source['fixture_nonce_sha256'],terminal=True)
    if summary['origin_terminal'].get('server_thread_settled')is not True:raise ValueError('OriginalOriginThreadSettlement')
    if summary.get('fixture_nonce_sha256')!=source.get('fixture_nonce_sha256')or summary.get('Source_HOSTNAME_canary_configured')is not True:raise ValueError('HistoricalFixtureIdentityAndCanary')
    excluded=source['cases'][3]
    if excluded.get('status')!=500 or 'django.template.exceptions.TemplateDoesNotExist: index.html'not in stderr:raise ValueError('LiteralMissingCompiledTemplateDiagnostic')
    if excluded.get('transport_error')or excluded.get('physical_close',{}).get('fd_after')!=-1 or excluded['physical_close'].get('original_inode_absent')is not True or excluded.get('absolute_observation',{}).get('timer_joined')is not True or worker(excluded['headers'])is not None:raise ValueError('ExcludedDiagnosticPhysicalClosure')
    start,end=excluded.get('request_started'),excluded.get('request_completed')
    if any(type(x)not in(int,float)or not math.isfinite(x)for x in(start,end))or not source['phase_started']<=start<=end<=source['phase_completed']or end-start>8:raise ValueError('ExcludedDiagnosticBoundedClock')
    origin_validate(excluded['origin_before'],source['fixture_nonce_sha256']);origin_validate(excluded['origin_after'],source['fixture_nonce_sha256'])
    if media_total(excluded['origin_before'])!=media_total(excluded['origin_after']):raise ValueError('ExcludedUnroutedBranchContactedProvider')
    projected=copy.deepcopy(source);projected['cases']=[x for x in projected['cases']if x['case']!='bad_uuid404']
    result=phase(projected,policy,'source')
    if result['worker_header_branch_scope_qualified']is not True:raise ValueError('RoutedSourceHeaderSubscope')
    return {'Source_routed_six_header_subscope_offline_valid':True,'actual_routed_cases':[x['case']for x in projected['cases']],'actual_routed_statuses':[x['status']for x in projected['cases']],'original_Source7_run_passed':False,'original_Source7_qualification_preserved_false':True,'Source_VOD_request_rerun':False,'excluded_unrouted_SPA_diagnostic':{'case':'bad_uuid404','status':500,'missing_template':'index.html','VOD_handler_authority':False,'application_route_parity_claim':False,'Native404_change_authorized':False},'error_body_or_full_header_or_M4_parity':False}


def validate_frozen_reference(root):
    root=Path(root);inputs=json.loads((root/'retained-inputs.json').read_text())
    for name,sha in inputs['copied_reference_files'].items():
        if hashlib.sha256((root/name).read_bytes()).hexdigest()!=sha:raise ValueError('RetainedReferenceHash')
    ref=root/'retained-source'
    source=json.loads((ref/'worker-header-source-private.json').read_text())
    if json.loads((ref/'kernel-source.json').read_text())!=source['kernel_witness']:raise ValueError('LiteralKernelRecordBinding')
    if json.loads((ref/'Source-client-config.json').read_text())!={'port':6390,'socket_timeout':60,'socket_connect_timeout':5,'Source_methods_replaced':False}:raise ValueError('OriginalUnmodifiedClientConfiguration')
    return validate(json.loads((ref/'worker-header-source-private.json').read_text()),json.loads((ref/'summary-private.json').read_text()),json.loads((ref/'auth-identity-projection.json').read_text()),(ref/'source-web.stderr').read_text(),json.loads((root/'policy.json').read_text()))
