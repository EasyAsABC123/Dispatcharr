"""Finite static input/authority/access checks; no Cargo/apps/Docker."""
from pathlib import Path
import hashlib,json,re,unittest
H=Path(__file__).resolve().parent;S=H/'source';B=S/'rust/crates';sha=lambda p:hashlib.sha256(p.read_bytes()).hexdigest()
class StaticControls(unittest.TestCase):
 def test_exact802_map(self):
  expected=json.loads((H/'full-source-map.json').read_text());actual={str(p.relative_to(S)):sha(p)for p in S.rglob('*')if p.is_file()};self.assertEqual(actual,expected);self.assertEqual(len(actual),802)
 def test_eight_paths_only(self):
  rows=json.loads((H/'selected-source.json').read_text())['files'];self.assertEqual(len(rows),8);self.assertFalse(any(x['path'].endswith(('Cargo.toml','Cargo.lock','main.rs'))or'/vendor/'in x['path']for x in rows))
 def test_coupled5_unchanged(self):
  base=json.loads((H/'base-source-map.json').read_text());self.assertEqual(json.loads((H/'coupled5-map.json').read_text()),{k:v for k,v in base.items()if not k.startswith('rust/')})
 def test_strict_single_projection(self):
  binding=json.loads((H/'inputs/Source-binding.json').read_text());self.assertEqual(binding['mode'],'projected-backup-source16-reference-v1');self.assertEqual(binding['Source16_projection_sha256'],sha(H/'inputs/Source16-projection.json'));self.assertTrue(binding['strict_single_mode_no_fallback']);self.assertTrue(binding['new_projected_reference_execution_has_not_occurred'])
 def test_source_original_hash(self):self.assertEqual(json.loads((H/'inputs/Source-binding.json').read_text())['Original_raw_sha256'],'daa2835e76df10e6dece88473d4b08d2ce1e4ad82680de3296de99604b54a288')
 def peer(self):return(B/'job-transport/src/backup_source16_peer.rs').read_text()
 def test_source_credential_seed_overwrites(self):
  p=self.peer();a=p.index('let mut user = source["seed_users"]');b=p.index('INSERT INTO accounts_user',a);section=p[a:b]
  for field in('username','api_key','password','date_joined'):self.assertIn('user["'+field+'"] =',section)
 def test_no_source_headers_tokens_archive_reads(self):
  p=self.peer();self.assertNotIn('source["aliases"]',p);self.assertNotIn('source["requests"]',p);self.assertNotIn('source["raw_headers_private"]',p);self.assertNotIn('source["tokens_private"]',p)
 def test_case_count_and_schema(self):self.assertEqual(len(json.loads((H/'inputs/plan.json').read_text())['cases']),16)
 def test_native_capture_false_explicit(self):self.assertIn('"capture_only_passed_field":true',self.peer());self.assertIn('"passed":false',self.peer())
 def test_no_source_imports_or_calls(self):
  self.assertNotIn('Command::new("python',self.peer());self.assertNotIn('manage.py',self.peer())
 def test_default_501_retained(self):
  p=(B/'store/src/backup_files.rs').read_text();self.assertIn('jobs: None',p);self.assertIn('let Some(jobs) = &self.jobs else',p);self.assertIn('Native durable backup jobs are not migrated',p)
 def test_lookup_before_opaque_uuid_decode(self):
  p=(B/'job-transport/src/transport.rs').read_text();p=p[p.index('pub async fn read_backup('):p.index('/// Reserve only captured manual index ETA children.')];self.assertLess(p.index('command_classified'),p.index('Uuid::parse_str'));self.assertIn('0 => Ok(None)',p)
 def test_backup_cannot_restore(self):
  p=(B/'job-transport/src/transport.rs').read_text();a=p.index('pub async fn publish(');b=p.index('pub async fn publish_refresh',a);self.assertIn('!matches!(task, Task::CreateBackup)',p[a:b])
 def test_known_create_worker_preserved(self):
  base=json.loads((H/'base-source-map.json').read_text());self.assertIn('rust/crates/job-transport/src/worker.rs',base);p=(B/'job-transport/src/worker.rs').read_text();self.assertIn('pub fn new_fixture',p);self.assertIn('pub fn new_backup',p);self.assertIn('spawn_blocking',p);self.assertIn('_retained = physical',p)
 def test_five_compile_commands_only(self):
  p=json.loads((H/'compile-plan.json').read_text());self.assertEqual(len(p['commands']),5);self.assertTrue(all('--offline'in c and'--locked'in c for c in p['commands'][1:]));self.assertFalse(any('--ignored'in c for c in p['commands']))
class ContextControls(unittest.TestCase):
 def source(self):return json.loads((H/'inputs/Source16-projection.json').read_text())
 def test_measured_context(self):
  from context import validate_source
  self.assertTrue(validate_source(self.source())['passed'])
 def reject(self,change):
  from context import validate_source
  source=self.source();change(source)
  with self.assertRaises(ValueError):validate_source(source)
 def test_prefilled_five_rejected(self):self.reject(lambda r:r['seed_snapshot'].update(redis=r['requests'][0]['after']['redis']))
 def test_persistent_version_not_finite(self):self.reject(lambda r:r['seed_snapshot']['redis'][0]['rows'][0].update(pttl=300000))
 def test_lock_cannot_be_persistent(self):self.reject(lambda r:r['seed_snapshot']['redis'][0]['rows'][1].update(pttl=-1))
 def test_wrong_initial_value(self):self.reject(lambda r:r['seed_snapshot']['redis'][0]['rows'][1].update(value_hex='32'))
 def test_added_cache_keys_cannot_be_dropped(self):self.reject(lambda r:r['requests'][0]['after']['redis'][0]['rows'].pop())
 def test_callback_before_auth(self):
  t=(B/'store/src/backup_files.rs').read_text();a=t.index('async fn serve_owned(');self.assertLess(t.index('callback_group("system_settings")',a),t.index('self.auth.authenticate',a))
 def test_network_before_anonymous_level(self):
  t=(B/'store/src/backup_files.rs').read_text();a=t.index('if !manual {');self.assertLess(t.index('callback_group("network_access")',a),t.index('user.as_ref().ok_or',a))
 def test_actual_sql_sync_close_before_error_return(self):
  t=(B/'store/src/backup_files.rs').read_text();a=t.index('async fn load_callback_group(');b=t.index('async fn callback_group(',a);t=t[a:b];self.assertLess(t.index('connection.ping().await'),t.index('let value = result?'));self.assertLess(t.index('connection.close().await'),t.index('let value = result?'))
 def test_same_scope_composed_into_job_commands(self):
  t=(B/'job-transport/src/backup_application.rs').read_text();self.assertEqual(t.count('Arc::new((self.lease()?, caller))'),2)
if __name__=='__main__':unittest.main()
