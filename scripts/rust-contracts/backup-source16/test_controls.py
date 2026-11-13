"""Pure projected-evidence corruption controls; no apps/builds/Docker."""
import copy,json,tempfile,unittest
from pathlib import Path
import verify
from binding import validate_row,path_for
from projection import project,evidence
class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  cls.temp=tempfile.TemporaryDirectory(prefix='backup-portable-controls-',dir=verify.ROOT.parent);cls.folder=Path(cls.temp.name);verify.unpack(cls.folder);cls.source=verify.load_source_projection()
 @classmethod
 def tearDownClass(cls):cls.temp.cleanup()
 def reject(self,mutate):
  source=copy.deepcopy(self.source);mutate(source)
  path=self.folder/'backup-job-private.json';original=path.read_bytes();path.write_text(json.dumps(source))
  try:
   with self.assertRaises(ValueError):verify.validate_observations(source,self.folder)
  finally:path.write_bytes(original)
 def test_actual_projected_observations(self):self.assertTrue(verify.validate_observations(self.source,self.folder)['passed'])
 def test_no_forceauth(self):self.reject(lambda r:r.update(force_authenticate_used=True))
 def test_missing_request(self):self.reject(lambda r:r['requests'].pop())
 def test_changed_status(self):self.reject(lambda r:r['requests'][0].update(status=200))
 def test_swapped_auth_roles(self):self.reject(lambda r:r['requests'][1].update(auth_selectors={'x_api_key':'admin'}))
 def test_body_hash_corruption(self):self.reject(lambda r:r['requests'][0].update(body_sha256='0'*64))
 def test_wrong_task_target(self):self.reject(lambda r:r['requests'][4]['resolver']['kwargs'].update(task_id='wrong'))
 def test_api_key_projection_change(self):self.reject(lambda r:r['seed_users'][0]['api_key'].update(sha256='0'*64))
 def test_archive_raw_fabricated(self):self.reject(lambda r:r['requests'][15].update(body_hex='00'))
 def test_missing_literal_headers(self):self.reject(lambda r:r['requests'][9].update(headers={}))
 def test_drop_SQL_sequence(self):self.reject(lambda r:r['requests'][5]['after']['audit']['sequences'].popitem())
 def test_changed_SQL_fingerprint(self):self.reject(lambda r:r['requests'][5]['after']['audit'].update(dataset_sha256='0'*64))
 def test_user_effect_omitted(self):self.reject(lambda r:r['requests'][5]['after'].update(users=[]))
 def test_queue_rerouted(self):self.reject(lambda r:r['before_Celery_configuration'].update(owned_queue='wrong'))
 def test_credential_header_hash_changed(self):self.reject(lambda r:r['requests'][1]['raw_headers_private']['HTTP_X_API_KEY'].update(sha256='0'*64))
 def test_sensitive_projection_rules(self):
  raw={'api_key':'secret','password':'!','task_token':'token','raw_headers_private':{'HTTP_X_API_KEY':'736563726574'},'path':'/api/a?token=token','ordinal':4,'body_hex':'746f6b656e'};p,changes=project(raw);self.assertEqual(len(changes),6);self.assertEqual(p['api_key'],evidence(b'secret'));self.assertEqual(p['body_hex'],evidence(b'token'));self.assertNotIn('token=token',json.dumps(p))
 def test_projection_noop_safe_fields(self):self.assertEqual(project({'status':200,'value':[1,None]})[0],{'status':200,'value':[1,None]})
if __name__=='__main__':unittest.main()
