"""Finite private map/order/fixture binding controls; no app, SQL or Cargo."""
import hashlib,json,re,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parent;SOURCE=ROOT/'source';BASE=json.loads((ROOT/'prototype1-base-source-map.json').read_text())
class Binding(unittest.TestCase):
 def test_all_base806_and_coupled5_present(self):
  self.assertEqual(sum(p.startswith('rust/')for p in BASE),806);self.assertEqual(len(BASE),811);self.assertTrue(all((SOURCE/p).is_file()for p in BASE))
 def test_exact_finite_selected_paths(self):
  changed={p for p,h in BASE.items()if hashlib.sha256((SOURCE/p).read_bytes()).hexdigest()!=h};extra={str(p.relative_to(SOURCE))for p in SOURCE.rglob('*')if p.is_file()}-set(BASE)
  self.assertEqual(changed,{'rust/crates/store/src/notifications.rs'});self.assertEqual(extra,{'rust/crates/store/src/notification_forms.rs','rust/crates/store/src/notifications_forms_sql_tests.rs'})
 def test_no_cargo_lock_or_coupled_changes(self):
  names=[p for p in BASE if not p.startswith('rust/')or p.endswith(('Cargo.lock','Cargo.toml'))]
  self.assertTrue(all(hashlib.sha256((SOURCE/p).read_bytes()).hexdigest()==BASE[p]for p in names))
 def test_create_captures_body_rejection_after_auth(self):
  text=(SOURCE/'rust/crates/store/src/notifications.rs').read_text();text=text[text.index('async fn create('):text.index('/// Reuse the pinned serializer validators')]
  self.assertIn('Result<Bytes, axum::extract::rejection::BytesRejection>',text);self.assertLess(text.index('auth.account_user'),text.index('body.map_err'));self.assertLess(text.index('body.map_err'),text.index('forms::create_input'))
 def test_update_lookup_before_parse(self):
  text=(SOURCE/'rust/crates/store/src/notifications.rs').read_text();text=text[text.index('async fn update('):text.index('async fn destroy(')];self.assertLess(text.index('auth.account_user'),text.index('mutation_target'));self.assertLess(text.index('mutation_target'),text.index('write_input'))
 def test_delete_bytes_unchanged(self):
  import subprocess
  path='rust/crates/store/src/notifications.rs';original=subprocess.run(['git','show','e194c9e1d96db7e33857b93df85bfb9b0a97808f:'+path],capture_output=True,check=True).stdout.decode();current=(SOURCE/path).read_text()
  def function(t):return t[t.index('async fn destroy('):t.index('#[cfg(test)]',t.index('async fn destroy('))]
  self.assertEqual(function(original),function(current))
 def test_json_create_inner_extractor_retained(self):
  text=(SOURCE/'rust/crates/store/src/notification_forms.rs').read_text();self.assertIn('Json::<Value>::from_request',text);self.assertIn('crate::admin_writes::body_object(parsed)',text);self.assertIn('DefaultBodyLimit::disable().apply',text)
 def test_fixed_source7_byte_literal(self):
  text=(SOURCE/'rust/crates/store/src/notification_forms.rs').read_text();raw=json.loads(re.search(r'const BODY\s*:\s*&\[u8\]\s*=\s*b("(?:[^"\\]|\\.)*");',text).group(1)).encode();wanted=Path('artifacts/rust/m5-notification-forms-source-executor-5/inputs/bodies/07.bin').read_bytes();self.assertEqual(raw,wanted)
 def test_strict_projection_no_raw_fallback(self):
  text=(SOURCE/'rust/crates/store/src/notifications_forms_sql_tests.rs').read_text();self.assertIn('1c3f5dcce14ffe9a773cb29833c4caef52ed15c813cf9473f6c397dc34ca240f',text);self.assertIn('projected-notification-forms-source12-reference-v1',text);self.assertNotIn('Source12-private.json',text)
 def test_source_credentials_overwritten_before_insert(self):
  text=(SOURCE/'rust/crates/store/src/notifications_forms_sql_tests.rs').read_text();self.assertLess(text.index('user["api_key"] ='),text.index('jsonb_populate_record(NULL::accounts_user'));self.assertLess(text.index('user["date_joined"] ='),text.index('jsonb_populate_record(NULL::accounts_user'));self.assertNotIn('source["raw_headers_private"]',text)
 def test_no_cache_import_or_reset(self):
  text=(SOURCE/'rust/crates/store/src/notifications_forms_sql_tests.rs').read_text();self.assertIn('NaturalOwnedNativeEmptyCacheNoSourceWarmImport',text);self.assertNotIn('redis::cmd("RESTORE")',text);self.assertNotIn('redis::cmd("DEL")',text);self.assertIn('full_cache_parity_claimed',text)
 def test_case_counts_and_before_send_counter(self):
  text=(SOURCE/'rust/crates/store/src/notifications_forms_sql_tests.rs').read_text();self.assertIn('let diagnostics = [',text);self.assertLess(text.index('row["actual_request_invoked"]'),text.index('let response = request.send()'));self.assertIn('capture_only_passed_field',text)
if __name__=='__main__':unittest.main(verbosity=2)
