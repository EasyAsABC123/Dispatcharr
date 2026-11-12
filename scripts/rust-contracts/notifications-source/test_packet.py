"""Focused portable corruption controls: no Docker, Django or Source execution."""
import copy,hashlib,json,shutil,tempfile,unittest
from pathlib import Path
import validate
from fixture_guards import require

def rehash_audit(a):a['dataset_sha256']=hashlib.sha256(json.dumps({'tables':a['tables'],'sequences':a['sequences']},sort_keys=True).encode()).hexdigest()
class PublicPacketControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.original=validate.load()
    def rejected(self,change):
        values=copy.deepcopy(self.original);change(values)
        with self.assertRaises((ValueError,KeyError,TypeError,IndexError)):validate.derive(values)
    def test_exact_packet_validates_213_effects_and_retains_exit1(self):
        result=validate.validate();self.assertTrue(result['passed']);self.assertEqual(result['effect_checks'],213);self.assertEqual(result['controller_exit_preserved'],1);self.assertEqual(result['registered_operations'],0);self.assertFalse(result['Source_or_Docker_executed_by_validator'])
    def test_read_only_validation(self):
        files=[p for p in validate.HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts];before={str(p):validate.sha(p)for p in files};validate.validate();self.assertEqual(before,{str(p):validate.sha(p)for p in files})
    def test_status_corruption_each_of_eight_cases(self):
        for index in range(8):
            with self.subTest(index=index):self.rejected(lambda v:v['source-observations']['requests'][index].update(actual_status=500))
    def test_response_body_changed(self):self.rejected(lambda v:v['source-observations']['requests'][1].update(body_hex='7b7d'))
    def test_JSON_disagrees_with_raw_body(self):self.rejected(lambda v:v['source-observations']['requests'][1]['json'].update(count=99))
    def test_body_capture_incomplete(self):self.rejected(lambda v:v['source-observations']['requests'][7].update(body_capture_complete=False))
    def test_wrong_notification_route(self):self.rejected(lambda v:v['source-observations']['requests'][0].update(path='/api/system/notifications/'))
    def test_wrong_resolver_action(self):self.rejected(lambda v:v['source-observations']['requests'][0].update(resolver_action='retrieve'))
    def test_force_auth_role_widened(self):self.rejected(lambda v:v['source-observations']['seed'].update(standard_user_level=10))
    def test_actual_user_identity_inactive(self):self.rejected(lambda v:v['source-observations']['seed_users'][0].update(is_active=False))
    def test_policy_mutation_claim(self):self.rejected(lambda v:v['source-observations'].update(no_network_setting_mutation=False))
    def test_owned_database_replaced(self):self.rejected(lambda v:v['source-observations'].update(database='dispatcharr'))
    def test_denied_create_mutation(self):self.rejected(lambda v:v['source-observations']['requests'][2]['after']['notifications'][0].update(title='Phish'))
    def test_other_users_dismissal(self):self.rejected(lambda v:v['source-observations']['requests'][3]['after']['dismissals'][0].update(user_id=999))
    def test_repeat_dismiss_timestamp_changed(self):self.rejected(lambda v:v['source-observations']['requests'][4]['after']['dismissals'][0].update(dismissed_at='2030-01-01T00:00:00Z'))
    def test_admin_create_default_changed(self):self.rejected(lambda v:v['source-observations']['requests'][7]['after']['notifications'][1].update(source='developer'))
    def test_admin_create_clock_outside_interval(self):self.rejected(lambda v:v['source-observations']['requests'][7]['after']['notifications'][1].update(created_at='2030-01-01T00:00:00Z'))
    def test_cache_capture_bound_exceeded(self):self.rejected(lambda v:v['source-observations']['requests'][0]['after_owned_cache'].update(capture_bytes=1048577))
    def test_cache_raw_bytes_corrupted(self):self.rejected(lambda v:v['source-observations']['requests'][0]['after_owned_cache']['rows'][0].update(dump_hex='not-hex'))
    def test_executed_effect_check_removed(self):self.rejected(lambda v:v['executed-effect-validation']['checks'].pop())
    def test_negative_controller_changed_to_pass(self):self.rejected(lambda v:v['controller-execution'].update(passed=True))
    def test_controller_exit_zeroed(self):self.rejected(lambda v:v.update({'controller-exit':'0\n'}))
    def test_cleanup_error_hidden(self):self.rejected(lambda v:v['controller-execution'].pop('cleanup_error'))
    def test_recovery_relabels_controller_success(self):self.rejected(lambda v:v['execution-recovery'].update(controller_passed=True))
    def test_retry_hidden(self):self.rejected(lambda v:v['execution-recovery'].update(Source_fixture_retries=1))
    def test_default_notification_sequence_changed(self):
        def change(v):
            a=v['default-after'];a['sequences']['public.core_notificationdismissal_id_seq']['last_value']+=1;rehash_audit(a)
        self.rejected(change)
    def test_default_notification_table_removed(self):self.rejected(lambda v:v['default-after']['tables'].pop('core_systemnotification'))
    def test_audit_report_substituted(self):self.rejected(lambda v:v['audit-after-report'].update(database='other'))
    def test_cleanup_wrong_exact_ID(self):self.rejected(lambda v:v['independent-cleanup'][0]['argv'].__setitem__(2,'0'*64))
    def test_cleanup_wrong_project(self):self.rejected(lambda v:v['independent-cleanup'][1]['argv'].__setitem__(6,'label=com.docker.compose.project=other'))
    def test_cleanup_stopped_containers_omitted(self):self.rejected(lambda v:v['independent-cleanup'][1]['argv'].remove('-a'))
    def test_cleanup_nonempty_inventory(self):self.rejected(lambda v:v['independent-cleanup'][2].update(stdout_text='retained-network\n',stdout_hex=b'retained-network\n'.hex()))
    def test_cleanup_inspect_success(self):self.rejected(lambda v:v['independent-cleanup'][0].update(returncode=0))
    def test_capitalization_not_silently_rewritten(self):
        def change(v):
            row=v['independent-cleanup'][0];row['stderr_text']=row['stderr_text'].replace('no such object','No such object');row['stderr_hex']=row['stderr_text'].encode().hex()
        self.rejected(change)
    def test_recorded_PID_present(self):self.rejected(lambda v:v['independent-process-absence'][0].update(proc_exists=True))
    def test_recorded_PID_omitted(self):self.rejected(lambda v:v['independent-process-absence'].pop())
    def test_published_port_added(self):self.rejected(lambda v:v['executed-compose']['services']['baseline'].update(ports=['5656:5656']))
    def test_external_network_enabled(self):self.rejected(lambda v:v['executed-compose']['networks']['default'].update(internal=False))
    def test_registration_claim_added(self):self.rejected(lambda v:v['controller-execution'].update(registered=True))
    def test_source_seal_replaced(self):self.rejected(lambda v:v['postexecution-seal-check'].update(seal_sha256='0'*64))
    def test_raw_manifest_and_projection_drift_fail_closed(self):
        with tempfile.TemporaryDirectory()as directory:
            packet=Path(directory)/'packet';shutil.copytree(validate.HERE,packet,ignore=shutil.ignore_patterns('__pycache__'))
            path=packet/'raw/source-observations.json';path.write_bytes(path.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'RawHashBinding'):validate.load(packet)
            shutil.copyfile(validate.HERE/'raw/source-observations.json',path)
            value=json.loads((packet/'projection.json').read_text());value['executed_controller_success']=True;(packet/'projection.json').write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'ExactReadOnlyProjection'):validate.validate(packet)
    def test_pinned_Original_source_drift(self):
        with tempfile.TemporaryDirectory()as directory:
            packet=Path(directory)/'packet';shutil.copytree(validate.HERE,packet,ignore=shutil.ignore_patterns('__pycache__'))
            path=packet/'executed/inputs/original/core/tests/test_system_notifications_permissions.py';path.write_bytes(path.read_bytes().replace(b'/api/system/',b'/api/core/'))
            with self.assertRaisesRegex(ValueError,'RawHashBinding'):validate.load(packet)
if __name__=='__main__':unittest.main()
