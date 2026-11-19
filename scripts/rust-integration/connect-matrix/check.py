"""Validate real paired Connect evidence while leaving the existing register read-only."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import sys

from requirements import MATRIX_SEAL, compile_requirements, sha

REQUIREMENTS_SHA='a60d5c53819d4dd4ed6b8ca7891eac90e0a8f004b31f5206d53c6eda27e6b951'
BUNDLE_SCOPE='inner_owned_Connect_matrix_terminal_and_cleanup'
CAPTURE_SCOPE='representative_paired_connect_method_role_matrix'
FILES=(
    'binding.json','receipt.json','execution.json','execution.stdout','execution.stderr',
    'independent-cleanup.json','evidence/integration.json','evidence/owned-before-compose-down.json',
)
PHASES=('source-bare','source-json','native-bare','native-json')
ROLE_NAMES=('baseline','candidate','data-init','origin')
SHA=re.compile('[0-9a-f]{64}')
PROJECT=re.compile('dispatcharr-connect-matrix-[0-9a-f]{12}')
CREDENTIAL_ROLE_FIELDS=('role','path_template','method','resolver_ordinal')
EMPTY_SHA=hashlib.sha256(b'').hexdigest()


class Unqualified(ValueError):
    """A concrete missing custody, shape, or physical acceptance condition."""


def need(ok, reason):
    if not ok:
        raise Unqualified(reason)


def strict_json(raw):
    def unique(pairs):
        value={}
        for name,item in pairs:
            need(name not in value,'duplicate_json_key')
            value[name]=item
        return value
    def invalid_constant(_):
        raise Unqualified('invalid_nonfinite_json')
    return json.loads(raw,object_pairs_hook=unique,parse_constant=invalid_constant)


def bounded(path,limit=8*1024*1024):
    with path.open('rb') as f:
        body=f.read(limit+1)
    need(len(body)<=limit,'document_exceeds_bounded_intake')
    return body


def digest_file(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f,'sha256').hexdigest()


def verify_requirements(matrix):
    requirements=compile_requirements(matrix)
    raw=(json.dumps(requirements,indent=2,sort_keys=True)+'\n').encode()
    need(sha(raw)==REQUIREMENTS_SHA,'requirement_drift')
    result=strict_json(raw)
    need(result['matrix_fixture_seal_sha256']==MATRIX_SEAL and
         not result['whole_operation_policy_approved'],'unexpected_requirement_policy')
    return result


def load_matrix(matrix):
    sys.path.insert(0,str(matrix))
    spec=importlib.util.spec_from_file_location('sealed_connect_matrix_launch',matrix/'run.py')
    driver=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    return driver


def load_custody(approval, trusted_hash, folder):
    need(type(trusted_hash) is str and SHA.fullmatch(trusted_hash),'trusted_external_custody_digest_required')
    raw=bounded(approval,128*1024)
    need(sha(raw)==trusted_hash,'externally_pinned_custody_digest_mismatch')
    note=strict_json(raw)
    candidate106='evidence_sha256' in note
    if candidate106:
        need(note.keys()=={'passed','physical_http_cells_attempted','per_phase','actual_matrix_child_exit',
                           'exact_cleanup_verified','evidence_sha256'},'unexpected_coordinator_custody_schema')
        need(type(note['passed']) is bool and note['physical_http_cells_attempted']==240 and
             note['exact_cleanup_verified'] is True and type(note['actual_matrix_child_exit']) is int and
             note['actual_matrix_child_exit'] in (0,1) and set(note['per_phase'])==set(PHASES)
             and all(row['attempted']==60 for row in note['per_phase'].values()),'incomplete_coordinator_custody')
        refs=note['evidence_sha256']
    else:
        need(note.keys()=={'format_version','scope','matrix_fixture_seal_sha256','project','candidate_revision',
                           'candidate_image','build_sha256','files_sha256','approved_actual_run'},'unexpected_custody_schema')
        need(type(note['format_version']) is int and note['format_version']==1 and note['scope']==BUNDLE_SCOPE
             and note['approved_actual_run'] is True and note['matrix_fixture_seal_sha256']==MATRIX_SEAL,'custody_scope')
        refs=note['files_sha256']
    need(set(refs)==set(FILES),'custody_dependency_closure')
    documents={}
    for name,wanted in refs.items():
        concrete=folder/name
        need(concrete.is_file() and concrete.resolve().is_relative_to(folder.resolve()),'evidence_outside_owned_bundle')
        need(isinstance(wanted,str) and SHA.fullmatch(wanted),'invalid_custody_digest')
        raw=bounded(concrete)
        need(sha(raw)==wanted,'retained_artifact_hash_mismatch')
        if name.endswith('.json'):
            documents[name]=strict_json(raw)
    if candidate106:
        binding=documents['binding.json']
        note={
            'project':binding['project'],'candidate_revision':binding['candidate_revision'],
            'candidate_image':binding['image_id'],'build_sha256':binding['build_sha256'],
        }
    need(isinstance(note['project'],str) and PROJECT.fullmatch(note['project']),'owned_project_namespace')
    return note, documents


def common_identity(required,matrix,custody,records,build):
    binding=records['binding.json']
    terminal=records['execution.json']
    cleanup=records['independent-cleanup.json']
    report=records['evidence/integration.json']
    receipt=records['receipt.json']
    before=records['evidence/owned-before-compose-down.json']
    driver=load_matrix(matrix)
    freeze=driver.verify_inputs()
    need(digest_file(matrix/'freeze.json')==MATRIX_SEAL,'fixture_or_source_inputs_changed')
    manifest,archive=driver.verify_build(build,custody['candidate_revision'])
    build_hash=digest_file(build)
    need(build_hash==binding['build_sha256']==report['build_manifest_sha256']==custody['build_sha256'],
         'candidate_build_receipt_mismatch')
    need(digest_file(archive)==binding['source_archive_sha256']==manifest['source_archive_sha256'],
         'candidate_source_archive_mismatch')
    project=custody['project']
    candidate=custody['candidate_revision']
    image=custody['candidate_image']
    need(all(row['project']==project for row in (binding,receipt,cleanup,report)),'run_project_mismatch')
    need(receipt['scope']==binding['scope']==CAPTURE_SCOPE and
         binding['frozen_inputs_sha256']==MATRIX_SEAL,'fixture_boundary_mismatch')
    need(binding['candidate_revision']==receipt['candidate_revision']==manifest['git_revision']==candidate,
         'candidate_revision_mismatch')
    need(binding['image_id']==receipt['image_id']==manifest['image_id']==image,'candidate_image_mismatch')
    need(report['candidate_image_binding']=={'image_id':image,'source_tree_sha256':manifest['source_tree_sha256'],
                                             'manifest_verified':True},'physical_running_image_binding')
    need(report['source_baseline']==receipt['source_revision']==required['original_revision']
         and report['baseline_archive_sha256']==required['original_archive_sha256'],'original_authority_mismatch')
    expected={name:checksum for name,checksum in freeze['copied_inputs'].items()
              if name!='original-source.tar' and not name.startswith('scripts/rust-milestone-batches/')}
    need(report['tool_source_sha256']==expected and report['contract_suites']==['connect_matrix'],'captured_phase_tool_drift')
    need(type(terminal['actual_exit']) is int and terminal['actual_exit'] in (0,1)
         and receipt['tracked_runner_actual_exit']==terminal['actual_exit']
         and terminal['failure'] is None and driver.load_coordinator().settled(terminal),'actual_terminal_eof_or_group_missing')
    need(type(report['passed']) is bool and receipt['passed'] is report['passed'] and
         terminal['actual_exit']==(0 if report['passed'] else 1),'literal_terminal_and_report_disagree')
    need(report.get('failure') is None and report.get('owned_capture_error') is None and
         report['cleanup']=='project removed' and report['resources_retained'] is False,'physical_report_or_owned_cleanup_failed')
    need(receipt['capture_completed'] is True and receipt['performance_verified'] is False and
         receipt['full_milestone_accepted'] is False and receipt['fully_verified_register_operations']==0,
         'missing_scoped_capture_receipt')
    need(cleanup['passed'] is True and cleanup['inventories']=={'container':[],'network':[],'volume':[]},
         'independent_owned_inventories_not_empty')
    after=cleanup['owned_container_generations']
    need(tuple(sorted(row['service'] for row in before))==tuple(sorted(row['service'] for row in after))==ROLE_NAMES,
         'owned_compose_role_roster_mismatch')
    core=('id','service','pid','generation')
    identities=lambda rows:{tuple(row[k] for k in core) for row in rows}
    need(identities(before)==identities(after) and len(identities(after))==4 and
         all(row['container_absent'] is True and row['host_generation_absent'] is True for row in after),
         'owned_container_or_process_generation_mismatch')
    need(all(SHA.fullmatch(row['id']) and type(row['pid']) is int and
             (row['service']=='data-init' or row['pid']>0 and str(row['generation']).isdigit()) for row in after),
         'missing_exact_owned_process_generation')
    need(driver.lifecycle_foundation(report['checks'])['passed'] is True,'scoped_foundation_failed')
    return report,receipt


def evidence_cell(observed, record, role):
    status=record['representative_role_cells'][role]
    fields=(role,record['path_template'],record['method'],record['resolver_ordinal'])
    need(tuple(observed[name] for name in CREDENTIAL_ROLE_FIELDS)==fields and observed['expected_stock_status']==status,
         'method_path_or_role_cell_changed')
    need(type(observed['actual_status']) is int and 100<=observed['actual_status']<=599 and
         observed['transport_error'] is None,'missing_actual_role_response')
    need(isinstance(observed['raw_response_sha256'],str) and SHA.fullmatch(observed['raw_response_sha256']),
         'missing_exact_response_fingerprint')
    need(type(observed['authentication_challenge_present']) is bool,'missing_selected_auth_challenge')
    known_failure=record['legacy_defect_needs_own_review'] and role in ('admin_jwt','admin_api_key')
    need(observed['authenticated_source_format_500_candidate'] is known_failure,'legacy_defect_classification_drift')
    matches=(observed['actual_status']==status and observed['scope_response_match'] is True
             and observed['owned_sql_rows_match'] is True)
    need(observed['selected_scope_passed'] is matches,'case_pass_flag_contradicts_actual_evidence')
    physical=matches
    if status==204:
        physical=physical and observed['raw_response_sha256']==EMPTY_SHA
    elif status in (401,403):
        physical=physical and observed['mime']=='application/json' and bool(
            isinstance(observed['denial_json_sha256'],str) and SHA.fullmatch(observed['denial_json_sha256']))
        if status==401:
            physical=physical and observed['authentication_challenge_present'] is True
    else:
        physical=physical and observed['mime']=='application/json' and observed['denial_json_sha256'] is None
    return bool(physical)


def compare_cells(required, report, receipt):
    checks=report['checks']
    indexed={row['name']:row for row in checks}
    need(len(checks)==len(indexed),'duplicate_captured_scope')
    need(all('connect-matrix:'+scope in indexed for scope in (*PHASES,'paired')),'missing_captured_scope')
    phases={phase:indexed['connect-matrix:'+phase]['details'] for phase in PHASES}
    for phase,result in phases.items():
        side,variant=phase.split('-',1)
        need(result['side']==side and result['variant']==variant and result['completed_cell_count']==60,'unexpected_phase_boundary')
        expected={record['case_key']+'-'+role for record in required['operations']
                  if record['variant']==variant for role in record['representative_role_cells']}
        need(set(result['cases'])==expected,'missing_or_extra_role_cells')
        need(result['preserved_unrelated_SQL_rows'] is True and result['zero_owned_delivery'] is True,
             'collateral_or_outbound_effect_in_phase')
    recomputed={'bare':{},'json':{}}
    reviewed=[]
    for record in required['operations']:
        variant=record['variant']
        source,native=(phases[side+'-'+variant] for side in ('source','native'))
        phase_ok=all(phase['complete_selected_table_transition'] is True and
                     phase['expected_sequence_transition'] is True for phase in (source,native))
        cells={}
        for role in record['representative_role_cells']:
            marker=record['case_key']+'-'+role
            a,b=(phase['cases'][marker] for phase in (source,native))
            source_ok=evidence_cell(a,record,role)
            native_ok=evidence_cell(b,record,role)
            keys=('actual_status','mime','authentication_challenge_present','denial_json_sha256')
            equal=all(a[k]==b[k] for k in keys)
            summary_match=a['selected_scope_passed'] and b['selected_scope_passed'] and equal
            recomputed[variant][marker]=summary_match
            cells[role]={'source_status':a['actual_status'],'native_status':b['actual_status'],
                         'source_response_sha256':a['raw_response_sha256'],'native_response_sha256':b['raw_response_sha256'],
                         'representative_pair_passed':source_ok and native_ok and equal and phase_ok}
        matched=all(row['representative_pair_passed'] for row in cells.values())
        identity={name:record[name] for name in ('contract_id','resolver_ordinal','resolver_name','pattern_exact',
                                                 'source_handler','method','source_action')}
        reviewed.append({**identity,'role_cells':cells,
                         'representative_scope_verified':matched,'operation_fully_verified':False,'role_matrix_complete':False,
                         'remaining_for_whole_operation':record['required_beyond_representative_capture'],
                         'legacy_result_requires_review':record['legacy_defect_needs_own_review']})
    for phase,result in phases.items():
        actual=sum(row['selected_scope_passed'] for row in result['cases'].values())
        scope_pass=actual==60 and result['complete_selected_table_transition'] and result['expected_sequence_transition']
        need(result['source_or_native_scope_matches']==actual and result['selected_scope_passed'] is bool(scope_pass) and
             indexed['connect-matrix:'+phase]['passed'] is bool(scope_pass),'scope_summary_contradicts_actual_or_SQL')
    aggregate=indexed['connect-matrix:paired']['details']
    count=sum(sum(values.values()) for values in recomputed.values())
    need(aggregate['pairs']==recomputed and aggregate['paired_cells_matching']==count and
         aggregate['physical_http_cells_attempted']==240,'aggregate_summary_contradicts_role_cases')
    reps={variant:{record['case_key']:all(recomputed[variant][record['case_key']+'-'+role]
                                       for role in record['representative_role_cells'])
                   for record in required['operations'] if record['variant']==variant} for variant in ('bare','json')}
    need(reps==aggregate['paired_representative_role_groups']==receipt['selected_matrix']['paired_representative_role_groups'],
         'representative_scope_summary_contradicts_actual_cases')
    return reviewed


def evaluate(matrix,approval,custody_hash,folder,build):
    required=verify_requirements(matrix)
    base={'base_register_sha256':required['base_register_sha256'],'whole_operation_policy_approved':False,
          'register_untouched':True,'fully_verified_operations_added':0,'full_milestone_accepted':False}
    try:
        custody,bundle=load_custody(approval,custody_hash,folder)
        report,receipt=common_identity(required,matrix,custody,bundle,build)
        reviewed=compare_cells(required,report,receipt)
    except (Unqualified,ValueError,TypeError,KeyError,OSError,IndexError) as error:
        reason=str(error) if isinstance(error,Unqualified) else type(error).__name__
        return {**base,'evidence_qualified':False,'verified_representative_operations':0,'reason':reason,
                'preserved_contract_ids':[row['contract_id'] for row in required['operations']]}
    return {**base,'evidence_qualified':True,'actual_candidate_revision':custody['candidate_revision'],
            'actual_packaged_image':custody['candidate_image'],'verified_representative_operations':
                sum(row['representative_scope_verified'] for row in reviewed),'reviewed_operations':reviewed}


if __name__=='__main__':
    argument=argparse.ArgumentParser(description=__doc__)
    argument.add_argument('--custody',required=True,type=Path)
    argument.add_argument('--trusted-custody-sha256',required=True)
    argument.add_argument('--evidence',required=True,type=Path)
    argument.add_argument('--build',required=True,type=Path)
    argument.add_argument('--fixture',required=True,type=Path)
    options=argument.parse_args()
    result=evaluate(options.fixture.resolve(),options.custody,options.trusted_custody_sha256,options.evidence,options.build)
    print(json.dumps(result,indent=2,sort_keys=True))
    raise SystemExit(0 if result['evidence_qualified'] else 1)
