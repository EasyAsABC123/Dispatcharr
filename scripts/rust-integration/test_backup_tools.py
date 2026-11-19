"""Guard regression tests; no database, Docker or Django import permitted."""
import json,os,subprocess,sys,unittest
from pathlib import Path
from unittest.mock import patch

from build_backup_component import inspect_postgres_runtime
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

class PostgresRuntimeChecks(unittest.TestCase):
    versions = {
        name: name + ' (PostgreSQL) 17.11 (fixture)'
        for name in ('pg_dump', 'pg_restore', 'psql')
    }
    image_id = 'sha256:' + '0' * 64

    def runtime_manifest(self):
        return {'binaries': {name: {'version': version} for name, version in self.versions.items()}}

    def test_inspects_canonical_manifest_and_all_wrappers_on_same_image(self):
        manifest = self.runtime_manifest()
        outputs = [json.dumps(manifest)] + [version + '\n' for version in self.versions.values()]
        with patch('build_backup_component.subprocess.check_output', side_effect=outputs) as run:
            self.assertEqual(inspect_postgres_runtime(self.image_id), (manifest, self.versions))
        commands = [call.args[0] for call in run.call_args_list]
        common = ['docker', 'run', '--rm', '--network', 'none', '--entrypoint']
        self.assertTrue(all(command[:len(common)] == common for command in commands))
        shell, image_id, switch, check = commands[0][len(common):]
        self.assertEqual((shell, image_id, switch), ('/bin/sh', self.image_id, '-c'))
        self.assertTrue(check.endswith('cat /usr/share/doc/dispatcharr/postgres-runtime.json'))
        self.assertIn('test ! -e /usr/bin/python3', check)
        self.assertEqual([command[len(common):] for command in commands[1:]], [
            ['/usr/local/bin/' + name, self.image_id, '--version'] for name in self.versions
        ])

    def test_rejects_missing_binary_or_mismatched_or_non_pg17_wrapper(self):
        cases = (
            ({'binaries': {'pg_dump': {}}}, '', 'manifest binaries'),
            (self.runtime_manifest(), 'pg_dump (PostgreSQL) 17.12', 'differs from manifest: pg_dump'),
        )
        obsolete = self.runtime_manifest()
        obsolete['binaries']['pg_dump']['version'] = 'pg_dump (PostgreSQL) 16.11'
        for manifest, output, message in (*cases, (obsolete, 'pg_dump (PostgreSQL) 16.11', cases[1][2])):
            with self.subTest(manifest=manifest, output=output):
                with patch('build_backup_component.subprocess.check_output',
                           side_effect=[json.dumps(manifest), output]):
                    with self.assertRaisesRegex(ValueError, message):
                        inspect_postgres_runtime(self.image_id)


if __name__=='__main__':unittest.main()
