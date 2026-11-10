"""Guard regression tests; no database, Docker or Django import permitted."""
import json,os,subprocess,sys,unittest
from pathlib import Path
HELPER=Path(__file__).with_name('backup_fixture.py')
class BackupGuards(unittest.TestCase):
 def reject(self,db,action,guard=True):
  env=dict(os.environ,POSTGRES_DB=db,POSTGRES_PASSWORD='playback-fixture-only',BACKUP_DISPOSABLE_FIXTURE='playback-fixture-only'if guard else'')
  result=subprocess.run([sys.executable,str(HELPER)],input=json.dumps({'action':action}),capture_output=True,text=True,env=env,timeout=3)
  self.assertNotEqual(result.returncode,0);self.assertNotIn('No module named',result.stderr);return result.stderr
 def test_default_database_rejects_mutations(self):
  for action in ['seed','source_create','source_restore','manifest']:
   self.assertIn('Only exact owned copied databases',self.reject('dispatcharr',action))
 def test_guard_required(self):self.assertIn('Explicit disposable',self.reject('rust_probe_backup_test','seed',False))
 def test_names_fail_closed(self):
  for db in ['rust_probe_backup_test;drop database dispatcharr','rust_probe_backup_../data','RUST_PROBE_BACKUP_test','rust_probe_backup_'+'x'*64]:self.assertIn('Only exact owned copied databases',self.reject(db,'seed'))
if __name__=='__main__':unittest.main()
