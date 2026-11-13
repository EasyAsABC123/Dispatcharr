"""Pure Original byte/AST/alias controls; no application imports."""
import ast,hashlib,json,sys,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;sys.path.insert(0,str(ROOT));import binding
PLAN=json.loads((ROOT/'plan.json').read_text())
class Controls(unittest.TestCase):
 def test_28_Original_bytes(self):
  self.assertEqual(len(PLAN['Original_inputs_sha256']),28)
  for name,sha in PLAN['Original_inputs_sha256'].items():self.assertEqual(hashlib.sha256((ROOT/'Original'/name).read_bytes()).hexdigest(),sha)
 def test_exact16sequence(self):
  self.assertEqual([s['ordinal']for s in PLAN['cases']],list(range(1,17)));self.assertTrue(all(s['phase']=='before_worker'for s in PLAN['cases'][:12]));self.assertTrue(all(s['phase']=='after_worker_completed'for s in PLAN['cases'][12:]))
 def test_unmeasured(self):
  self.assertEqual(PLAN['actual_Source_requests'],0);self.assertEqual(PLAN['actual_Native_requests'],0);self.assertEqual(PLAN['state'],'DEFINITION_ONLY_UNEXECUTED')
 def test_Source_create_never_reads_body(self):
  tree=ast.parse((ROOT/'Original/apps/backups/api_views.py').read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='create_backup');self.assertNotIn('request.data',ast.unparse(fn));self.assertIn('create_backup_task.delay()',ast.unparse(fn));self.assertEqual(PLAN['cases'][3]['raw_body_ascii'],'{broken')
 def test_no_mock_forceauth_or_unknown_consumption(self):
  self.assertFalse(PLAN['force_authenticate']);self.assertFalse(PLAN['worker']['producer_public_configuration_override']['Source_method_patch_or_mock']);self.assertTrue(PLAN['worker']['consume_only_owned_queue']);self.assertTrue(PLAN['worker']['never_consume_unknown_startup_tasks'])
 def test_startup_locks_match_Source(self):
  text=(ROOT/'Original/dispatcharr/celery.py').read_text();self.assertEqual([v['key']for v in PLAN['worker']['startup_dispatch_locks_preclaimed']],['dvr:recover_dispatch_lock','core:version_check_dispatch_lock']);self.assertTrue(all(v['ttl']==300 for v in PLAN['worker']['startup_dispatch_locks_preclaimed']));self.assertIn('_claim("dvr:recover_dispatch_lock")',text);self.assertIn('_claim("core:version_check_dispatch_lock")',text)
 def aliases(self):
  a=binding.initial_aliases('0123456789');task='01234567-89ab-cdef-0123-456789abcdef';binding.absorb_created(a,{'detail':'Backup started','task_id':task,'task_token':binding.token(task)});return a
 def test_created_token_own_ID(self):self.assertEqual(self.aliases()['created_task_token'],binding.token(self.aliases()['created_task']))
 def test_wrong_created_token(self):
  a=binding.initial_aliases('0123456789')
  with self.assertRaises(ValueError):binding.absorb_created(a,{'detail':'Backup started','task_id':'01234567-89ab-cdef-0123-456789abcdef','task_token':'0'*32})
 def test_second_create_alias(self):
  a=self.aliases()
  with self.assertRaises(ValueError):binding.absorb_created(a,{'detail':'Backup started','task_id':a['created_task'],'task_token':a['created_task_token']})
 def row(self,number):
  a=self.aliases();s=PLAN['cases'][number-1];target=a['unknown_task']if number==12 else a['created_task'];return a,s,{'ordinal':number,'name':s['name'],'method':s['method'],'path':binding.path_for(s,a),'auth_selectors':s['auth'],'resolver':{'module':'apps.backups.api_views','function':binding.FUNCTIONS[number-1],'kwargs':{'task_id':target}}}
 def test_task_target(self):a,s,r=self.row(8);self.assertTrue(binding.validate_row(r,s,a))
 def test_self_consistent_wrong_task(self):
  a,s,r=self.row(8);wrong='ffffffff-ffff-ffff-ffff-ffffffffffff';r['path']=r['path'].replace(a['created_task'],wrong);r['resolver']['kwargs']['task_id']=wrong
  with self.assertRaises(ValueError):binding.validate_row(r,s,a)
 def test_wrong_resolved_function(self):
  a,s,r=self.row(8);r['resolver']['function']='create_backup'
  with self.assertRaises(ValueError):binding.validate_row(r,s,a)
 def test_unknown_not_assumed_UUID(self):a,s,r=self.row(12);self.assertTrue(binding.validate_row(r,s,a));self.assertTrue(a['unknown_task'].startswith('never-created-'))
 def test_foreign_completed_task(self):
  a=self.aliases()
  with self.assertRaises(ValueError):binding.absorb_completed(a,'different-task',{'status':'completed','filename':'dispatcharr-backup-2026.11.12.00.00.00.zip','size':1})
 def test_filename_escape(self):
  a=self.aliases()
  with self.assertRaises(ValueError):binding.absorb_completed(a,a['created_task'],{'status':'completed','filename':'dispatcharr-backup-../bad.zip','size':1})
 def test_stream_and_body_caps_distinct(self):self.assertEqual(PLAN['caps']['response_bytes'],65536);self.assertEqual(PLAN['caps']['stream_archive_bytes'],8388608)
 def test_python_syntax(self):
  for p in ROOT.glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))
if __name__=='__main__':unittest.main()
