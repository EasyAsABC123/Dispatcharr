"""Offline retained Source only. No Source app, Main, network or Docker requests."""
import copy
import hashlib
import json
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'tools'))
from source_reference import validate,validate_frozen_reference
R=ROOT/'retained-source'
SOURCE=json.loads((R/'worker-header-source-private.json').read_text());SUMMARY=json.loads((R/'summary-private.json').read_text());AUTH=json.loads((R/'auth-identity-projection.json').read_text());STDERR=(R/'source-web.stderr').read_text();POLICY=json.loads((ROOT/'policy.json').read_text())

class SourceReference(unittest.TestCase):
    def reject(self,change):
        s,m,a,e,p=copy.deepcopy(SOURCE),copy.deepcopy(SUMMARY),copy.deepcopy(AUTH),STDERR,copy.deepcopy(POLICY)
        change(s,m,a)
        with self.assertRaises((ValueError,KeyError,TypeError)):validate(s,m,a,e,p)
    def test_exact_retained_six_valid_without_rewriting_failure(self):
        before=copy.deepcopy(SOURCE);result=validate_frozen_reference(ROOT)
        self.assertTrue(result['Source_routed_six_header_subscope_offline_valid']);self.assertEqual(result['actual_routed_statuses'],[405,401,503,200,206,500]);self.assertFalse(result['original_Source7_run_passed']);self.assertFalse(result['excluded_unrouted_SPA_diagnostic']['VOD_handler_authority']);self.assertEqual(SOURCE,before)
    def test_old_derived_failure_cannot_be_rewritten(self):self.reject(lambda s,m,a:s['qualification'].update(capture_valid=True))
    def test_original_exit_cannot_be_promoted(self):self.reject(lambda s,m,a:m.update(passed=True))
    def test_missing_source_row(self):self.reject(lambda s,m,a:s['cases'].pop())
    def test_wrong_auth_ID(self):self.reject(lambda s,m,a:a.update(auth_response_user_id=-1))
    def test_wrong_source_identity(self):self.reject(lambda s,m,a:s['cases'][4].update(headers=[['X-Worker-ID','owned-provider-spoof']]))
    def test_missing_provider_canary(self):self.reject(lambda s,m,a:next(row for row in s['cases'][4]['origin_after']['requests']if row['kind']=='stream_GET'and row['id']not in{x['id']for x in s['cases'][4]['origin_before']['requests']}).update(response_headers={}))
    def test_body_prefix(self):self.reject(lambda s,m,a:s['cases'][4].update(prefix_sha256='0'*64))
    def test_other_status_must_not_be_waived(self):self.reject(lambda s,m,a:s['cases'][2].update(status=500))
    def test_success_header_on_error(self):self.reject(lambda s,m,a:s['cases'][6].update(headers=[['X-Worker-ID','provider-spoof']]))
    def test_source_cleanup_must_be_complete(self):self.reject(lambda s,m,a:m['remaining'].update(containers='owned-leak'))
    def test_source_default_audit_required(self):self.reject(lambda s,m,a:m.update(default_named_tables_sequences_unchanged=False))
    def test_excluded_diagnostic_does_not_waive_physical_ownership(self):self.reject(lambda s,m,a:s['cases'][3]['physical_close'].update(fd_after=3))
    def test_template_evidence_required(self):
        with self.assertRaisesRegex(ValueError,'MissingCompiledTemplate'):validate(SOURCE,SUMMARY,AUTH,'no template exception',POLICY)
    def test_original_private_raw_bindings_still_exact(self):
        inputs=json.loads((ROOT/'retained-inputs.json').read_text());base=Path(inputs['private_original_output'])
        for name,sha in inputs['exact_private_raw_SHA256'].items():self.assertEqual(hashlib.sha256((base/name).read_bytes()).hexdigest(),sha)
    def test_native_excludes_unrouted_branch_and_never_dispatches_source(self):
        self.assertNotIn('bad_uuid404',POLICY['Native_cases']);self.assertEqual(len(POLICY['Native_cases']),7);self.assertFalse(POLICY['Source_VOD_requests_rerun'])

if __name__=='__main__':unittest.main()
