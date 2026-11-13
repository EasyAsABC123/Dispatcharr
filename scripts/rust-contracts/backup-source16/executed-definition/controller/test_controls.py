"""Pure static/seed/alias controls only; never imports Django/Celery or runs tools."""
import ast,copy,hashlib,json,sys,unittest
from pathlib import Path
from binding import *
from effects import check_seed,queue_frames,validate_envelope,validate_archive
C=Path(__file__).resolve().parent;ROOT=C.parent;PLAN=json.loads((ROOT/'inputs/plan.json').read_text())
class LocalControls(unittest.TestCase):
 def seed(self):
  n='123456abcd';return {'nonce':n,'seed_users':[dict(role,username='rust-probe-backupjob-'+n+'-'+role['alias'],api_key='backup-fixture-'+n+'-'+role['alias'],password='!')for role in PLAN['roles']],'seed_snapshot':{},'initial_owned_snapshot':{'users':[]},'locks_after_preclaim':[{'key':k,'raw_hex':'31','pttl':299999}for k in('dvr:recover_dispatch_lock','core:version_check_dispatch_lock')]}
 def valid_seed(self):
  r=self.seed();r['seed_snapshot']['users']=copy.deepcopy(r['seed_users']);return r
 def test_exact_seed(self):self.assertTrue(check_seed(self.valid_seed(),PLAN))
 def reject_seed(self,mutate):
  r=self.valid_seed();mutate(r)
  with self.assertRaises(ValueError):check_seed(r,PLAN)
 def test_swapped_role(self):self.reject_seed(lambda r:r['seed_users'].reverse())
 def test_wrong_admin_level(self):self.reject_seed(lambda r:r['seed_users'][0].update(user_level=1))
 def test_wrong_denied_network(self):self.reject_seed(lambda r:r['seed_users'][2].update(custom_properties={}))
 def test_wrong_api_key(self):self.reject_seed(lambda r:r['seed_users'][1].update(api_key='wrong'))
 def test_extra_seed_user(self):self.reject_seed(lambda r:r['seed_users'].append(r['seed_users'][0]))
 def test_missing_lock(self):self.reject_seed(lambda r:r['locks_after_preclaim'].pop())
 def test_expired_lock(self):self.reject_seed(lambda r:r['locks_after_preclaim'][0].update(pttl=0))
 def test_lock_wrong_value(self):self.reject_seed(lambda r:r['locks_after_preclaim'][0].update(raw_hex='32'))
 def test_fixed_16_order(self):self.assertEqual([x['ordinal']for x in PLAN['cases']],list(range(1,17)))
 def test_one_create_authority(self):self.assertEqual([x['ordinal']for x in PLAN['cases']if x['predicted_status_UNMEASURED']==202],[4])
 def test_invalid_body_literal(self):self.assertEqual(PLAN['cases'][3]['raw_body_ascii'],'{broken')
 def test_bad_jwt_literal(self):self.assertEqual(PLAN['cases'][9]['auth'],{'bearer_literal':'not-a-jwt'})
 def aliases(self):
  a=initial_aliases('123456abcd');task='12345678-1234-1234-1234-123456789abc';absorb_created(a,{'detail':'Backup started','task_id':task,'task_token':token(task)});return a
 def test_created_own_hmac(self):self.assertEqual(self.aliases()['created_task_token'],token(self.aliases()['created_task']))
 def test_mismatched_hmac(self):
  with self.assertRaises(ValueError):absorb_created(initial_aliases('123456abcd'),{'detail':'Backup started','task_id':'12345678-1234-1234-1234-123456789abc','task_token':'wrong'})
 def test_fixed_unknown_not_uuid(self):self.assertEqual(initial_aliases('123456abcd')['unknown_task'],'never-created-123456abcd')
 def test_wrong_task_alias(self):
  a=self.aliases();spec=PLAN['cases'][4];row={'ordinal':5,'name':spec['name'],'method':'GET','path':path_for(spec,a),'auth_selectors':{},'resolver':{'module':'apps.backups.api_views','function':'backup_status','kwargs':{'task_id':a['unknown_task']}}}
  with self.assertRaises(ValueError):validate_row(row,spec,a)
 def test_wrong_complete_task(self):
  with self.assertRaises(ValueError):absorb_completed(self.aliases(),'wrong',{'status':'completed','filename':'dispatcharr-backup-a.zip','size':1})
 def test_traversal_archive(self):
  a=self.aliases()
  with self.assertRaises(ValueError):absorb_completed(a,a['created_task'],{'status':'completed','filename':'dispatcharr-backup-../x.zip','size':1})
 def test_unknown_nonowned_nonce(self):
  with self.assertRaises(ValueError):initial_aliases('../unsafe')
 def test_shell_startup_order(self):
  t=(C/'source_fixture.py').read_text();self.assertLess(t.index("sys.argv.append('shell')"),t.index('django.setup()'));self.assertIn('should_skip_initialization()is True',t)
 def test_worker_startup_order(self):
  t=(C/'worker_fixture.py').read_text();self.assertLess(t.index("sys.argv.append('celery')"),t.index('django.setup()'));self.assertIn('should_skip_initialization()is True',t)
 def test_real_pinned_marker_semantics(self):
  # Execute only the independently parsed pure function with local inert globals.
  path=ROOT/'inputs/Original/dispatcharr/app_initialization.py';tree=ast.parse(path.read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='should_skip_initialization');code=compile(ast.Module(body=[fn],type_ignores=[]),str(path),'exec');import types,os
  scope={'sys':types.SimpleNamespace(argv=['fixture','shell']),'os':os,'_uwsgi_worker_id':lambda:None,'_is_worker_process':lambda:False,'logger':types.SimpleNamespace(debug=lambda *a:None)};exec(code,scope);self.assertTrue(scope['should_skip_initialization']());scope['sys'].argv=['fixture','celery'];self.assertTrue(scope['should_skip_initialization']());scope['sys'].argv=['fixture'];self.assertFalse(scope['should_skip_initialization']())
 def test_no_method_patches(self):
  for name in('source_fixture.py','worker_fixture.py'):
   t=(C/name).read_text();self.assertNotIn('mock.patch',t);self.assertNotIn('force_authenticate(',t);self.assertNotIn('.delay =',t)
 def test_worker_no_unknown_consumption(self):
  t=(C/'worker_fixture.py').read_text();self.assertIn("'--queues=rust_probe_backupjob_'+nonce()",t);self.assertNotIn('purge(',t);self.assertNotIn('consumer.add_queue',t)
 def test_producer_route_only(self):
  t=(C/'backup_common.py').read_text();self.assertIn("if producer:app.conf.task_routes=",t);self.assertIn("'apps.channels.tasks.run_recording':{'queue':'dvr'}",t)
 def test_no_extra_http_poll(self):self.assertNotIn('APIClient',(C/'run.py').read_text());self.assertIn("exec_args(db,'poll')",(C/'run.py').read_text())
 def test_ast_all(self):
  for path in C.glob('*.py'):ast.parse(path.read_text())
 def envelope(self):
  import base64
  return {'headers':{'id':self.aliases()['created_task'],'task':'apps.backups.tasks.create_backup_task'},'properties':{'delivery_info':{'routing_key':'own'}},'body':base64.b64encode(b'[[],{},{}]').decode()}
 def test_valid_envelope(self):self.assertTrue(validate_envelope(self.envelope(),self.aliases(),'own'))
 def test_wrong_envelope_task(self):
  x=self.envelope();x['headers']['id']='wrong'
  with self.assertRaises(ValueError):validate_envelope(x,self.aliases(),'own')
 def test_wrong_envelope_route(self):
  with self.assertRaises(ValueError):validate_envelope(self.envelope(),self.aliases(),'other')
 def test_extra_task_argument(self):
  import base64
  x=self.envelope();x['body']=base64.b64encode(b'[[1],{},{}]').decode()
  with self.assertRaises(ValueError):validate_envelope(x,self.aliases(),'own')
 def archive(self,db='postgresql',dump=b'PGDMPtest',name='database.dump',utc='2026-11-13T00:00:00+00:00'):
  import io,zipfile
  b=io.BytesIO()
  with zipfile.ZipFile(b,'w')as z:z.writestr(name,dump);z.writestr('metadata.json',json.dumps({'format':'dispatcharr-backup','version':2,'database_type':db,'database_file':'database.dump','created_at':utc}))
  return b.getvalue()
 def test_valid_archive(self):self.assertEqual(validate_archive(self.archive())['version'],2)
 def test_sqlite_not_pg(self):
  with self.assertRaises(ValueError):validate_archive(self.archive(db='sqlite'))
 def test_invalid_custom_dump(self):
  with self.assertRaises(ValueError):validate_archive(self.archive(dump=b'not-a-pg-dump'))
 def test_traversal_zip(self):
  with self.assertRaises(ValueError):validate_archive(self.archive(name='../database.dump'))
 def test_non_utc_metadata(self):
  with self.assertRaises(ValueError):validate_archive(self.archive(utc='2026-11-13T00:00:00'))
 def test_resolver_callable_identity_binding(self):self.assertIn("func is getattr(api_views,name)",(C/'source_fixture.py').read_text())
if __name__=='__main__':unittest.main()
