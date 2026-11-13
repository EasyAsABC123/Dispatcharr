"""Pure synthetic corruption controls; no Native/Source process or sockets."""
import ast,copy,json,sys,unittest,types
from pathlib import Path
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent;sys.path.insert(0,str(HERE));import validate_native as v
SOURCE=json.loads((ROOT/'inputs/Source20-private.json').read_text());PLAN=json.loads((ROOT/'inputs/plan.json').read_text())
def clone(x):return copy.deepcopy(x)
def sql(graph,audit):
 tables={n:[]for n in v.TABLES};tables['accounts_user']=clone(SOURCE['seed_users']);tables[v.N]=clone(graph['notifications']);tables[v.D]=clone(graph['dismissals']);return {'tables':tables,'sequences':{n:clone(audit['sequences']['public.'+n+'_id_seq'])for n in v.TABLES}}
def sample():
 nonce=SOURCE['database'].removeprefix('rust_probe_notifcred_');seed=sql(SOURCE['seed_notifications'],SOURCE['seed_audit']);n={'Native_only':True,'Source_application_runs':0,'capture_complete':True,'actual_request_count':20,'actual_auth_backend_observed':False,'Source20_sha256':v.hashlib.sha256((ROOT/'inputs/Source20-private.json').read_bytes()).hexdigest(),'plan_sha256':v.hashlib.sha256((ROOT/'inputs/plan.json').read_bytes()).hexdigest(),'nonce':nonce,'database':'rust_probe_notifcred_native_'+nonce,'seed_SQL':seed,'seed_users':clone(SOURCE['seed_users']),'seed_started_utc':SOURCE['seed_started_utc'],'seed_completed_utc':SOURCE['seed_completed_utc'],'tokens_private':clone(SOURCE['tokens_private']),'initialized_events':[],'cache_after_seed':clone(SOURCE['cache_after_seed']),'HTTP_listener_joined':True,'requests':[]};users={r['alias']:u for r,u in zip(PLAN['roles'],n['seed_users'])}
 for s,spec in zip(SOURCE['requests'],PLAN['cases']):
  row={k:clone(s[k])for k in('ordinal','name','method','path','auth_selectors','request_payload','before_events','after_events','began_utc','completed_utc','status','body_hex','body_bytes','body_sha256','json','before_cache','after_cache')};row['raw_headers_private']=v.expected_headers(spec,users,n['tokens_private']);row['headers_hex']={k.lower():value.encode().hex()for k,value in s['headers'].items()};row['before_SQL']=sql(s['before_notifications'],s['before_audit']);row['after_SQL']=sql(s['after_notifications'],s['after_audit']);n['requests'].append(row)
 return n
class Controls(unittest.TestCase):
 def test_complete_synthetic20(self):self.assertTrue(v.validate(sample())['passed'])
 def reject(self,mutate):
  n=sample();mutate(n)
  with self.assertRaises((ValueError,KeyError,AssertionError)):v.validate(n)
 def test_deniedUI_read_still403_rejected(self):self.reject(lambda n:n['requests'][0].update(status=403))
 def test_admin_denial_relaxed_rejected(self):self.reject(lambda n:n['requests'][5].update(status=201))
 def test_header_swapped_actor(self):self.reject(lambda n:n['requests'][0]['raw_headers_private'].update(authorization='00'))
 def test_invented_backend_observation(self):self.reject(lambda n:n.update(actual_auth_backend_observed=True))
 def test_wrong_role_seed(self):self.reject(lambda n:n['seed_users'][3].update(user_level=10))
 def test_user_row_sideeffect_not_normalized(self):self.reject(lambda n:n['requests'][14]['after_SQL']['tables']['accounts_user'][0].update(api_key='changed'))
 def test_event_sideeffect_not_normalized(self):self.reject(lambda n:n['requests'][14]['after_events'].append({'id':1}))
 def test_missing_failed_key_budget(self):self.reject(lambda n:n['requests'][14]['after_cache'].update(api_key_failed_auth_history=None))
 def test_budget_reset_between_requests(self):self.reject(lambda n:n['requests'][16]['before_cache'].update(api_key_failed_auth_history=None))
 def test_badjwt_goodkey_must401(self):self.reject(lambda n:n['requests'][11].update(status=200))
 def test_jwt_precedence_must_preserve_budget(self):self.reject(lambda n:n['requests'][12]['after_cache'].update(api_key_failed_auth_history=[1.0]))
 def test_sequence_advance_rejected(self):self.reject(lambda n:n['requests'][3]['after_SQL']['sequences'][v.D].update(last_value=10))
 def test_foreign_dismissal_rejected(self):self.reject(lambda n:n['requests'][3]['after_SQL']['tables'][v.D][0].update(user_id=1))
 def test_cross_row_notice_mutation(self):self.reject(lambda n:n['requests'][9]['after_SQL']['tables'][v.N][1].update(title='changed'))
 def test_unrelated_SQL_change(self):self.reject(lambda n:n['requests'][0]['after_SQL']['tables']['vod_movie'].append({'id':1}))
 def test_missing_SQLtable(self):self.reject(lambda n:n['requests'][0]['after_SQL']['tables'].pop('vod_movie'))
 def test_cross_request_SQL_rewind(self):self.reject(lambda n:n['requests'][4].update(before_SQL=n['seed_SQL']))
 def test_body_corruption(self):self.reject(lambda n:n['requests'][0].update(body_hex='7b7d'))
 def test_auth_challenge_missing(self):self.reject(lambda n:n['requests'][10]['headers_hex'].pop('www-authenticate'))
 def test_listener_unjoined(self):self.reject(lambda n:n.update(HTTP_listener_joined=False))
 def test_missing_request(self):self.reject(lambda n:n['requests'].pop())
 def test_mapped_clock_rewind_is_rejected_before_equality(self):
  c=v.Clocks(['2026-11-12T20:00:00Z']);row={'began_utc':'2026-11-12T21:00:00Z','completed_utc':'2026-11-12T21:00:01Z'};c.compare('2026-11-12T20:00:00Z','2026-11-12T21:00:00Z',row)
  with self.assertRaisesRegex(ValueError,'CrossRequestContinuity'):c.compare('2026-11-12T20:00:00Z','2026-11-12T20:00:00Z',row)
 def test_coincident_generated_clock_still_registered(self):
  c=v.Clocks(['2026-11-12T20:00:00Z']);row={'began_utc':'2026-11-12T20:00:00Z','completed_utc':'2026-11-12T20:00:01Z'};c.compare('2026-11-12T20:00:00Z','2026-11-12T20:00:00Z',row);self.assertEqual(len(c.mapping),1)
 def test_python_syntax(self):
  for p in HERE.glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))
 def test_wrong_review_hash_rejected_before_output_or_process(self):
  import run
  with self.assertRaisesRegex(ValueError,'ExactReviewedSeal'):run.execute(types.SimpleNamespace(execute_reviewed_seal='0'*64),{})
 def test_default_cli_has_readonly_branch(self):
  tree=ast.parse((HERE/'run.py').read_text());main=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='main');self.assertIn('if args.execute_reviewed_seal is None',ast.unparse(main))
 def test_storage_bootstrap_has_no_Source_startup(self):
  text=(HERE/'start.sh').read_text();self.assertNotIn('manage.py',text);self.assertIn('original-1241b3-schema.sql',text)
 def test_scope_only_five_actions(self):
  text=(Path('/home/justin/github/Dispatcharr/artifacts/rust/m5-notification-credential-native-qa-1/source')/'rust/crates/store/src/notifications.rs').read_text();self.assertEqual(text.count('authenticated_user(&auth, peer, headers'),5);self.assertNotIn('account_user(peer, headers, false)',text);self.assertEqual(text.count('account_user(peer, headers.clone(), true)'),1);self.assertEqual(text.count('account_user(peer, headers, true)'),2)
if __name__=='__main__':unittest.main()
