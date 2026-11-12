"""Offline corruption and reconstruction controls; no app, network, Docker or media."""
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest
import verify as V
import reconstruct as R
HERE=Path(__file__).resolve().parent

class Main53(unittest.TestCase):
    def header(self):
        p=HERE/'worker-header'
        return[V.read(p/x)for x in ['source-phase.json','source-failed-summary.json','native-phase.json','native-summary.json','auth-identity-projection.json','native-auth-identity-projection.json']]+[(p/'source-web.stderr').read_text(),V.read(p/'Root-independent-review.json')]
    def reject_header(self,change):
        values=self.header();change(*values)
        with self.assertRaises((ValueError,KeyError,TypeError)):V.header(*values)
    def notification(self,attempt=2):
        p=HERE/'notifications';return[V.read(p/('attempt'+str(attempt)+suffix))for suffix in ['-phase.json','-summary.json','-Root-review.json']]
    def reject_notice(self,change,attempt=2):
        values=self.notification(attempt);change(*values)
        with self.assertRaises((ValueError,KeyError,TypeError)):V.notification(*values,negative=attempt==1)
    def main(self):
        m=V.load(HERE/'main4s/validate.py','test_Main4s53');return m,V.read(HERE/'main4s/retained.json'),V.read(HERE/'main4s/Root-independent-cleanup-review.json'),V.read(HERE/'main4s/coverage-authority.json')
    def reject_main(self,change):
        m,d,c,a=self.main();change(d,c,a)
        with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):m.validate(d,c,a)
    def test_actual_complete_packet(self):self.assertTrue(V.verify()['passed'])
    def test_main_missing_cases(self):self.reject_main(lambda d,c,a:d['summary'].update(cases=[]))
    def test_main_missing_source_phase(self):self.reject_main(lambda d,c,a:d['summary']['cases'][0].pop('source'))
    def test_main_missing_SQL_or_follower(self):
        for key in ['actual_SQL_Bearer_user_verified','early_follower_valid']:self.reject_main(lambda d,c,a,k=key:d['summary']['cases'][0]['source'].update({k:False}))
    def test_main_missing_checkpoints(self):self.reject_main(lambda d,c,a:d['summary']['cases'][0]['native'].update(checkpoints=[]))
    def test_main_physical_delay_above_max(self):self.reject_main(lambda d,c,a:d['summary']['cases'][0]['native']['hold_observation'].update(actual_seconds=4.4))
    def test_main_missing_signal_coverage(self):self.reject_main(lambda d,c,a:d['Source_signals'].update(success=[]))
    def test_main_missing_PID_coverage(self):self.reject_main(lambda d,c,a:d['owned_process_record_projection'].update(success=[]))
    def test_main_both_default_sequences_removed(self):
        def change(d,c,a):
            for key in ['default_database_before','default_database_after']:d['summary']['cases'][0][key]['sequences']={}
        self.reject_main(change)
    def test_main_wrong_other_fence(self):self.reject_main(lambda d,c,a:d['summary']['cases'][1]['native']['other_private_after']['ledger'].update(fence=None))
    def test_main_clientloss_fake_status(self):self.reject_main(lambda d,c,a:d['summary']['cases'][2]['native']['target_outcome'].update(status=503))
    def test_main_server_cancel_not_inferred(self):self.reject_main(lambda d,c,a:d['summary']['cases'][2]['source']['target_client_loss'].update(server_cancellation_inferred=True))
    def test_main_negative_exit_not_promoted(self):self.reject_main(lambda d,c,a:d.update(actual_exit_code=1))
    def test_header_recorded_broad_claim_rejected(self):self.reject_header(lambda s,f,n,m,a,b,e,r:n['qualification'].update(full_header_or_M4_parity=True))
    def test_header_missing_routed_case(self):self.reject_header(lambda s,f,n,m,a,b,e,r:n['cases'].pop(0))
    def test_header_wrong_own_identity(self):self.reject_header(lambda s,f,n,m,a,b,e,r:n['cases'][3].update(headers=[['X-Worker-ID','owned-provider-spoof']]))
    def test_header_missing_origin_coverage(self):self.reject_header(lambda s,f,n,m,a,b,e,r:n['cases'][0]['origin_before'].update(requests=[]))
    def test_header_missing_socket(self):self.reject_header(lambda s,f,n,m,a,b,e,r:n['cases'][0]['physical_close'].update(fd_after=3))
    def test_header_missing_auth(self):self.reject_header(lambda s,f,n,m,a,b,e,r:b.update(status=401))
    def test_header_source_failure_preserved(self):self.reject_header(lambda s,f,n,m,a,b,e,r:f.update(passed=True))
    def test_header_Source500_not_VOD_authority(self):
        result=V.header(*self.header());self.assertFalse(result['Source6_offline']['excluded_unrouted_SPA_diagnostic']['VOD_handler_authority']);self.assertFalse(result['Source_invalid_HEAD_measured'])
    def test_header_cleanup_nonempty(self):self.reject_header(lambda s,f,n,m,a,b,e,r:m['remaining'].update(volumes='leak'))
    def test_notification_recorded_broad_claim_rejected(self):self.reject_notice(lambda p,s,r:p['qualification'].update(full_endpoint_or_Source48_parity=True))
    def test_notification_missing_case(self):self.reject_notice(lambda p,s,r:p['cases'].pop())
    def test_notification_missing_auth_role(self):self.reject_notice(lambda p,s,r:p['actual_auth_requests'].pop('standard'))
    def test_notification_missing_SQL_user(self):self.reject_notice(lambda p,s,r:p['cases'][0]['before']['tables'].update(accounts_user=[]))
    def test_notification_secret_value_not_allowed(self):self.reject_notice(lambda p,s,r:p['cases'][0]['before']['tables']['accounts_user'][0].update(api_key='plain'))
    def test_notification_repeat_sequence(self):self.reject_notice(lambda p,s,r:p['cases'][2]['after']['sequences']['core_notificationdismissal'].update(last_value=99))
    def test_notification_duplicate_sequence(self):self.reject_notice(lambda p,s,r:p['cases'][4]['after']['sequences']['core_systemnotification'].update(last_value=99))
    def test_notification_clock_outside_request(self):self.reject_notice(lambda p,s,r:p['cases'][0]['after']['tables']['core_systemnotification'][-1].update(created_at='2030-01-01T00:00:00Z'))
    def test_notification_final_default_must_equal(self):self.reject_notice(lambda p,s,r:s['default_after']['sequences']['core_systemnotification'].update(last_value=99))
    def test_notification_first_failure_not_promoted(self):self.reject_notice(lambda p,s,r:s.update(passed=True),attempt=1)
    def test_notification_first_failure_not_masked(self):self.reject_notice(lambda p,s,r:s.update(default_after=copy.deepcopy(s['default_before'])),attempt=1)
    def test_all_five_blocked_tool_restorations(self):
        for name in V.read(HERE/'tool-closures.json'):
            with self.subTest(name=name),tempfile.TemporaryDirectory()as d:
                result=R.tools(name,Path(d)/'out');self.assertFalse(result['application_executed']);self.assertFalse(result['new_authorization_granted'])
    def test_historical_flag_restoration_is_not_authorization(self):
        with tempfile.TemporaryDirectory()as d:self.assertFalse(R.tools('main4s',Path(d)/'out',True)['new_authorization_granted'])
    def test_existing_output_rejected(self):
        with tempfile.TemporaryDirectory()as d:
            with self.assertRaises(ValueError):R.tools('main4s',Path(d))
    def test_corrupt_tool_rejected(self):
        with tempfile.TemporaryDirectory()as d:
            source=Path(d)/'packet';shutil.copytree(HERE,source);p=source/'inputs/main4s/blocked/tools/phase.py';p.write_bytes(p.read_bytes()+b'changed')
            with self.assertRaises(ValueError):R.tools('main4s',Path(d)/'out',source=source)
    def test_startup_only_change_verified(self):self.assertTrue(V.startup_fix()['request_phase_qualifier_and_policy_unchanged'])
    def test_wrong_historical_action(self):
        d=V.read(HERE/'historical-once-receipts.json');d['header-native']['cue']['projection']['action']='other'
        with self.assertRaises(ValueError):V.once_receipts(d,V.read(HERE/'tool-closures.json'))
    def test_corrupt_receipt_binding(self):
        d=V.read(HERE/'historical-once-receipts.json');d['header-native']['claim']['projection']['cue_sha256']='0'*64
        with self.assertRaises(ValueError):V.once_receipts(d,V.read(HERE/'tool-closures.json'))

if __name__=='__main__':unittest.main()
