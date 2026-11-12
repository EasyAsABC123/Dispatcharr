"""Pure/host-local controller controls. No Docker, SQL or native ELF executes."""
import ast,copy,hashlib,json,os,subprocess,sys,tempfile,unittest
from pathlib import Path
import run

def default_audit():
    value={'database':'dispatcharr','tables':{name:{'rows':0,'sha256':'0'*64}for name in run.TABLES},'sequences':{'public.'+name+'_id_seq':{'last_value':1,'is_called':False}for name in run.TABLES}}
    value['dataset_sha256']=hashlib.sha256(json.dumps({'tables':value['tables'],'sequences':value['sequences']},sort_keys=True).encode()).hexdigest();return value
class NativeControllerControls(unittest.TestCase):
    def test_exact_owned_database_only(self):
        self.assertEqual(run.owned_database('rust_probe_notif_native_0123456789'),'rust_probe_notif_native_0123456789')
        for value in ['dispatcharr','postgres','rust_probe_notif_0123456789','rust_probe_notif_native_ABCDEF0123','rust_probe_notif_native_01234567890','rust_probe_notif_native_0123456789;DROP DATABASE dispatcharr']:
            with self.subTest(value=value),self.assertRaises(ValueError):run.owned_database(value)
    def test_eleven_default_audit_valid(self):self.assertEqual(run.audit(default_audit()),default_audit()['dataset_sha256'])
    def test_default_renamed_target_rejected(self):
        value=default_audit();value['database']='rust_probe_notif_native_0123456789'
        with self.assertRaises(ValueError):run.audit(value)
    def test_notification_table_missing_rejected(self):
        value=default_audit();value['tables'].pop('core_systemnotification')
        with self.assertRaises(ValueError):run.audit(value)
    def test_dismissal_sequence_missing_rejected(self):
        value=default_audit();value['sequences'].pop('public.core_notificationdismissal_id_seq')
        with self.assertRaises(ValueError):run.audit(value)
    def test_default_digest_tamper_rejected(self):
        value=default_audit();value['tables']['accounts_user']['rows']=1
        with self.assertRaises(ValueError):run.audit(value)
    def test_default_bad_sequence_type_rejected(self):
        value=default_audit();value['sequences']['public.accounts_user_id_seq']['is_called']=1
        with self.assertRaises(ValueError):run.audit(value)
    def test_psql_cache_helper_does_not_import_Django_on_import(self):
        tree=ast.parse(Path(__file__).with_name('audit.py').read_text())
        for node in tree.body:
            if isinstance(node,(ast.Import,ast.ImportFrom)):self.assertNotIn('django',ast.unparse(node).lower())
        self.assertIn("if __name__ == '__main__'",ast.unparse(tree))
    def test_local_default_check_does_not_invoke_Docker_or_ELF(self):
        result=subprocess.run([sys.executable,str(Path(__file__).with_name('run.py')),'--docker','/this/docker/must/not/exist'],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr);self.assertFalse(json.loads(result.stdout)['Native_SQL_or_Source_executed'])
    def test_wrong_review_hash_precedes_new_output_or_Docker(self):
        with tempfile.TemporaryDirectory()as directory:
            output=Path(directory)/'must-not-exist';result=subprocess.run([sys.executable,str(Path(__file__).with_name('run.py')),'--execute-reviewed-seal','0'*64,'--output',str(output),'--docker','/this/docker/must/not/exist'],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1'),capture_output=True,text=True,timeout=10)
            self.assertNotEqual(result.returncode,0);self.assertIn('ReviewedSealHashBinding',result.stderr);self.assertFalse(output.exists())
    def test_sealed_files_cannot_be_omitted(self):
        from unittest.mock import patch
        with tempfile.TemporaryDirectory()as directory:
            root=Path(directory);(root/'controller').mkdir();(root/'inputs').mkdir();(root/'PLAN.md').write_text('plan');(root/'controller/run.py').write_text('placeholder')
            value={'state':'SEALED_NATIVE_FIXTURE_REQUIRES_ROOT_REVIEW','image':run.IMAGE,'test_filter':run.TEST,'expected_Source8_cases':8,'expected_extra_Native_cases':8,'files':[]};path=root/'seal.json';path.write_text(json.dumps(value))
            with patch.object(run,'ROOT',root),self.assertRaisesRegex(ValueError,'ExactControllerInputInventory'):run.check_seal(path)
    def synthetic_isolation(self):
        notice={'id':1,'notification_key':'test.notif','source':'system','created_at':'2026-11-12T13:33:29.530188+00:00','updated_at':'2026-11-12T13:33:29.530193+00:00'}
        return {'isolation_database':'rust_probe_notif_native_0123456789','source_initial_notification_rows_before_isolation':{'notifications':[],'dismissals':[]},'owned_notification_rows_after_isolation':{'notifications':[],'dismissals':[]},'native_initialized_notification_rows_before_isolation':{'notifications':[{'id':1,'notification_key':'network_security_m3u_epg'}],'dismissals':[]},'source_seed_notification_rows':{'notifications':[copy.deepcopy(notice)],'dismissals':[]},'native_seed_notification_rows_before_requests':{'notifications':[copy.deepcopy(notice)],'dismissals':[]},'native_seed_notice_sequence':{'last_value':1,'is_called':True}}
    def test_synthetic_isolation_preserves_and_matches_Source(self):self.assertTrue(run.validate_isolation(self.synthetic_isolation(),'rust_probe_notif_native_0123456789'))
    def test_isolation_default_database_rejected(self):
        value=self.synthetic_isolation();value['isolation_database']='dispatcharr'
        with self.assertRaises(ValueError):run.validate_isolation(value,'rust_probe_notif_native_0123456789')
    def test_initialized_rows_cannot_be_hidden(self):
        value=self.synthetic_isolation();value['native_initialized_notification_rows_before_isolation']['notifications']=[]
        with self.assertRaises(ValueError):run.validate_isolation(value,'rust_probe_notif_native_0123456789')
    def test_uncleared_owned_notice_rejected(self):
        value=self.synthetic_isolation();value['owned_notification_rows_after_isolation']['notifications']=[{'id':1}]
        with self.assertRaises(ValueError):run.validate_isolation(value,'rust_probe_notif_native_0123456789')
    def test_Source_seed_clock_cannot_be_normalized(self):
        value=self.synthetic_isolation();value['native_seed_notification_rows_before_requests']['notifications'][0]['created_at']='2030-01-01T00:00:00Z'
        with self.assertRaises(ValueError):run.validate_isolation(value,'rust_probe_notif_native_0123456789')
    def test_process_lookup_missing_PID_is_not_an_existing_owner(self):self.assertIsNone(run.proc_start(2147483647))
if __name__=='__main__':unittest.main()
