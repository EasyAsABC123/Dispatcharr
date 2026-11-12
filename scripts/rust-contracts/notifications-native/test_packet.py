"""Read-only retained component/union corruption controls; no app/process runs."""
import copy,json,tempfile,shutil,unittest
from pathlib import Path
import validate
class NotificationPacketControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.values=validate.load();cls.controller=validate.executed_controller(validate.HERE)
    def rejected(self,mutate,union=False):
        values=copy.deepcopy(validate.union_values(validate.HERE,self.values)if union else self.values);mutate(values)
        with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):validate.derive(values,self.controller,validate.UNION if union else None)
    def test_component_and_union_validate_read_only(self):
        paths=[p for p in validate.HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts];before={str(p):validate.sha(p)for p in paths};result=validate.validate();self.assertTrue(result['component_Native2']['passed']);self.assertTrue(result['union791_same8plus8']['passed']);self.assertEqual(before,{str(p):validate.sha(p)for p in paths})
    def test_all8_component_status_corruptions(self):
        for index in range(8):
            with self.subTest(index=index):self.rejected(lambda v:v['native2-observations']['cases'][index].update(status=500))
    def test_all8_union_status_corruptions(self):
        for index in range(8):
            with self.subTest(index=index):self.rejected(lambda v:v['native2-observations']['cases'][index].update(status=500),True)
    def test_raw_body_bytes(self):self.rejected(lambda v:v['native2-observations']['cases'][1].update(body_hex='7b7d'))
    def test_body_SHA(self):self.rejected(lambda v:v['native2-observations']['cases'][1].update(body_sha256='0'*64))
    def test_Source_controller_failure_not_relabelled(self):self.rejected(lambda v:v['source-negative-controller'].update(passed=True))
    def test_Native1_failure_not_relabelled(self):self.rejected(lambda v:v['native1-negative-execution'].update(passed=True))
    def test_controller_exit_not_hidden(self):self.rejected(lambda v:v.update({'native2-controller-exit.txt':'1\n'}))
    def test_process_exit(self):self.rejected(lambda v:v['native2-process-exit'].update(returncode=101))
    def test_initial_builtin_cannot_be_omitted(self):self.rejected(lambda v:v['native2-observations']['native_initialized_notification_rows_before_isolation'].update(notifications=[]))
    def test_owned_clear_cannot_target_default(self):self.rejected(lambda v:v['native2-observations'].update(isolation_database='dispatcharr'))
    def test_Source_seed_clock_not_normalized(self):self.rejected(lambda v:v['native2-observations']['native_seed_notification_rows_before_requests']['notifications'][0].update(created_at='2030-01-01T00:00:00Z'))
    def test_repeat_clock_not_normalized(self):self.rejected(lambda v:v['native2-observations']['cases'][4]['after']['tables']['core_notificationdismissal'][0].update(dismissed_at='2030-01-01T00:00:00Z'))
    def test_create_own_clock(self):self.rejected(lambda v:v['native2-observations']['cases'][7]['body'].update(created_at='2030-01-01T00:00:00Z'))
    def test_repeat_sequence_bump(self):self.rejected(lambda v:v['native2-observations']['cases'][4]['after']['sequences']['core_notificationdismissal'].update(last_value=2))
    def test_created_notice_sequence(self):self.rejected(lambda v:v['native2-observations']['cases'][7]['after']['sequences']['core_systemnotification'].update(last_value=3))
    def test_duplicate_rejection_no_sequence_use(self):self.rejected(lambda v:v['native2-observations']['extra_cases'][0]['after']['sequences']['core_systemnotification'].update(last_value=3))
    def test_truthy_update_cannot_change_timestamp(self):self.rejected(lambda v:v['native2-observations']['extra_cases'][3]['after']['tables']['core_notificationdismissal'][0].update(dismissed_at='2030-01-01T00:00:00Z'))
    def test_credential_user_row_effect_not_masked(self):self.rejected(lambda v:v['native2-observations']['cases'][1]['after']['tables']['accounts_user'][0].update(last_login='2030-01-01T00:00:00Z'))
    def test_credential_event_not_masked(self):self.rejected(lambda v:v['native-credential-rows-after']['value']['tables']['core_systemevent'].update(rows=1))
    def test_other9table_mutation(self):self.rejected(lambda v:v['native2-observations']['cases'][1]['after']['tables']['core_coresettings'].pop())
    def test_default11_changed(self):self.rejected(lambda v:v['default-after']['value']['sequences']['public.core_systemnotification_id_seq'].update(last_value=2))
    def test_Source_credentials_claim(self):self.rejected(lambda v:v['native2-execution'].update(Source_credentials_or_HTTP_TCP_acceptance=True))
    def test_Main_claim(self):self.rejected(lambda v:v['native2-execution'].update(Main_binary_runs=1))
    def test_registration_claim(self):self.rejected(lambda v:v['native2-execution'].update(operation_registration_changes=3))
    def test_wrong_exact_cleanup_ID(self):self.rejected(lambda v:v['cleanup-exact-ID-command']['argv'].__setitem__(6,'id='+'0'*64))
    def test_nonempty_cleanup(self):self.rejected(lambda v:v['cleanup-label-inventories'].update(container=['retained']))
    def test_PID_still_present(self):
        def mutate(v):
            r=v['cleanup-PID-absence'][0];r['original_process_absent']=False;r['after_starttime']=r['starttime']
        self.rejected(mutate)
    def test_cache_raw_bytes(self):self.rejected(lambda v:v['native-cache-after']['value']['rows'][0].update(dump_hex='bad-hex'))
    def test_retained_regression_environment_failure(self):self.rejected(lambda v:v.update({'legacy-store-lib-sandbox.log':'silently removed'}))
    def test_legacy_regression_failure(self):self.rejected(lambda v:v.update({'legacy-store-lib-host-loopback.exit':'101'}))
    def test_projection_or_raw_hash_drift(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'packet';shutil.copytree(validate.HERE,root,ignore=shutil.ignore_patterns('__pycache__'));f=root/'raw/native2-observations.json';f.write_bytes(f.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'RawHashBinding'):validate.validate(root)
if __name__=='__main__':unittest.main()
