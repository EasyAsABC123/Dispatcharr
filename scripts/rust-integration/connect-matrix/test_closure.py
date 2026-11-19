from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

from check import EMPTY_SHA, PHASES, Unqualified, compare_cells, evaluate, sha, verify_requirements
from requirements import compile_requirements
from materialize import ROOT, load_recipe, populate


RAW='a'*64
ERROR_JSON='b'*64


def raw_case(requirement, role):
    status=requirement['representative_role_cells'][role]
    return {'role':role,'path_template':requirement['path_template'],'method':requirement['method'],
            'resolver_ordinal':requirement['resolver_ordinal'],'expected_stock_status':status,'actual_status':status,
            'transport_error':None,'raw_response_sha256':EMPTY_SHA if status==204 else RAW,
            'scope_response_match':True,'owned_sql_rows_match':True,
            'selected_scope_passed':True,'mime':'' if status==204 else 'application/json',
            'authentication_challenge_present':status==401,
            'denial_json_sha256':ERROR_JSON if status in (401,403) else None,
            'authenticated_source_format_500_candidate':requirement['legacy_defect_needs_own_review']
                and role in ('admin_jwt','admin_api_key')}


def fake_report(required):
    checks=[]
    for phase in PHASES:
        side,variant=phase.split('-',1)
        cases={record['case_key']+'-'+role:raw_case(record,role) for record in required['operations']
               if record['variant']==variant for role in record['representative_role_cells']}
        checks.append({'name':'connect-matrix:'+phase,'passed':True,'details':{
            'side':side,'variant':variant,'completed_cell_count':60,'cases':cases,
            'source_or_native_scope_matches':60,'selected_scope_passed':True,
            'preserved_unrelated_SQL_rows':True,'zero_owned_delivery':True,
            'complete_selected_table_transition':True,'expected_sequence_transition':True}})
    paired={variant:{record['case_key']+'-'+role:True
                     for record in required['operations'] if record['variant']==variant
                     for role in record['representative_role_cells']}
            for variant in ('bare','json')}
    groups={variant:{record['case_key']:True for record in required['operations'] if record['variant']==variant}
            for variant in ('bare','json')}
    checks.append({'name':'connect-matrix:paired','passed':True,'details':{
        'pairs':paired,'paired_cells_matching':120,'physical_http_cells_attempted':240,
        'paired_representative_role_groups':groups}})
    return {'checks':checks},{'selected_matrix':{'paired_representative_role_groups':deepcopy(groups)}}


class ClosureContract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.scratch=tempfile.TemporaryDirectory(prefix='dispatcharr-connect-portable-test-')
        cls.addClassCleanup(cls.scratch.cleanup)
        cls.fixture=Path(cls.scratch.name)/'artifacts/rust/connect-matrix'
        populate(cls.fixture,ROOT,*load_recipe())
        cls.requirements=verify_requirements(cls.fixture)

    def test_byte_reproduced_source_derived_requirements_and_gaps(self):
        recreated=json.loads(json.dumps(compile_requirements(self.fixture)))
        self.assertEqual(recreated,self.requirements)
        self.assertEqual(len(recreated['operations']),20)
        self.assertEqual(sum(len(op['representative_role_cells']) for op in recreated['operations']),120)
        self.assertTrue(all(op['required_beyond_representative_capture'] and
                            op['whole_operation_policy_approved'] is False for op in recreated['operations']))

    def test_per_cell_comparator_never_promotes_even_perfect_in_memory_matches(self):
        raw,receipt=fake_report(self.requirements)
        rows=compare_cells(self.requirements,raw,receipt)
        self.assertEqual(sum(row['representative_scope_verified'] for row in rows),20)
        self.assertTrue(all(not row['operation_fully_verified'] and not row['role_matrix_complete'] for row in rows))

    def test_legacy_original_500_remains_one_unmatched_scope(self):
        report,receipt=fake_report(self.requirements)
        source=report['checks'][1]
        marker='nested-subscriptions-admin_jwt'
        source['details']['cases'][marker].update(actual_status=500,mime='text/html',
                                                  scope_response_match=False,selected_scope_passed=False)
        source['passed']=False
        source['details']['selected_scope_passed']=False
        source['details']['source_or_native_scope_matches']=59
        agg=report['checks'][-1]['details']
        agg['pairs']['json'][marker]=False
        agg['paired_cells_matching']=119
        agg['paired_representative_role_groups']['json']['nested-subscriptions']=False
        receipt['selected_matrix']['paired_representative_role_groups']['json']['nested-subscriptions']=False
        rows=compare_cells(self.requirements,report,receipt)
        self.assertEqual(sum(row['representative_scope_verified'] for row in rows),19)
        self.assertTrue(any(row['legacy_result_requires_review'] and not row['representative_scope_verified'] for row in rows))

    def test_refuse_forged_summary_alias_wrong_role_or_missing_cell(self):
        baseline,receipt=fake_report(self.requirements)
        marker='integration-get-admin_jwt'
        for mutate in (
            lambda v:v['checks'][0]['details']['cases'][marker].update(path_template='/api/connect/integrations/1.json'),
            lambda v:v['checks'][0]['details']['cases'][marker].update(role='standard'),
            lambda v:v['checks'][1]['details']['cases'].pop(marker),
            lambda v:v['checks'][1]['details']['cases'][marker].update(actual_status=500),
            lambda v:v['checks'][1]['details'].update(zero_owned_delivery=False),
            lambda v:v['checks'][-1]['details'].update(paired_cells_matching=119),
        ):
            altered=deepcopy(baseline)
            mutate(altered)
            with self.assertRaises(Unqualified):
                compare_cells(self.requirements,altered,receipt)

    def test_complete_run_verifier_rejects_unapproved_or_unbound_artifacts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            fake=root/'auth.json'
            fake.write_text('{"approved_actual_run":true}\n')
            approved=evaluate(self.fixture,fake,sha(fake.read_bytes()),root,root/'build.json')
            unpinned=evaluate(self.fixture,fake,'0'*64,root,root/'build.json')
            self.assertEqual(approved['reason'],'unexpected_custody_schema')
            self.assertEqual(unpinned['reason'],'externally_pinned_custody_digest_mismatch')
            for result in (approved,unpinned):
                self.assertFalse(result['evidence_qualified'])
                self.assertEqual(result['verified_representative_operations'],0)
                self.assertEqual(result['fully_verified_operations_added'],0)
                self.assertEqual(len(result['preserved_contract_ids']),20)
                self.assertTrue(result['register_untouched'])
