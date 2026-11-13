"""Finite corruptions/local parser controls, without Docker or app dispatch."""
import copy,hashlib,io,tarfile,unittest
import verify as V
class Public(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.n=V.load('native-phase.json');cls.s=V.load('native-summary.json');cls.a=V.load('native-auth.json');cls.r=V.load('agent-review.json')
 def bad(self,fn):
  values=copy.deepcopy([self.n,self.s,self.a,self.r]);fn(*values)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(*values)
 def test_actual_seven(self):self.assertEqual(V.validate(self.n,self.s,self.a,self.r)['Native_cases'],7)
 def test_missing_case(self):self.bad(lambda n,s,a,r:n['cases'].pop())
 def test_empty_cases(self):self.bad(lambda n,s,a,r:n.update(cases=[]))
 def test_old_HEAD405_not_relabelled(self):self.bad(lambda n,s,a,r:n['cases'][-1].update(status=405))
 def test_HEAD_wrong_length(self):self.bad(lambda n,s,a,r:next(x for x in n['cases'][-1]['headers']if x[0].lower()=='content-length').__setitem__(1,'0'))
 def test_HEAD_wrong_allow(self):self.bad(lambda n,s,a,r:next(x for x in n['cases'][-1]['headers']if x[0].lower()=='allow').__setitem__(1,'GET'))
 def test_HEAD_wrong_vary(self):self.bad(lambda n,s,a,r:next(x for x in n['cases'][-1]['headers']if x[0].lower()=='vary').__setitem__(1,'Accept'))
 def test_HEAD_body(self):self.bad(lambda n,s,a,r:n['cases'][-1].update(body_hex='00'))
 def test_HEAD_has_worker(self):self.bad(lambda n,s,a,r:n['cases'][-1]['headers'].append(['X-Worker-ID',n['kernel_witness']['expected_worker_header']]))
 def test_no_actual_SQL_Bearer(self):self.bad(lambda n,s,a,r:n.update(actual_SQL_Bearer_user_verified=False))
 def test_wrong_auth_id(self):self.bad(lambda n,s,a,r:a.update(auth_response_user_id=-1))
 def test_wrong_kernel_UID(self):self.bad(lambda n,s,a,r:n['kernel_witness'].update(uid=0))
 def test_provider_spoof(self):self.bad(lambda n,s,a,r:next(x for x in n['cases']if x['case']=='full200').update(headers=[['X-Worker-ID','owned-provider-spoof']]))
 def test_wrong_prefix(self):self.bad(lambda n,s,a,r:next(x for x in n['cases']if x['case']=='range206').update(prefix_sha256='0'*64))
 def test_empty_origin_requests(self):self.bad(lambda n,s,a,r:n['cases'][0]['origin_before'].update(requests=[]))
 def test_empty_origin_connections(self):self.bad(lambda n,s,a,r:n['cases'][0]['origin_before'].update(connections=[]))
 def test_origin_counter(self):self.bad(lambda n,s,a,r:n['cases'][0]['origin_before'].update(accepted_TCP_total=0))
 def test_socket_open(self):self.bad(lambda n,s,a,r:n['cases'][0]['physical_close'].update(fd_after=3))
 def test_bad_clock(self):self.bad(lambda n,s,a,r:n['cases'][0].update(request_started=-1))
 def test_cap_change(self):self.bad(lambda n,s,a,r:n['fixed_caps_after'].update(profile=1))
 def test_default_change(self):self.bad(lambda n,s,a,r:s['default_after']['tables'].clear())
 def test_Source_replay(self):self.bad(lambda n,s,a,r:s.update(Source_VOD_requests_rerun=True))
 def test_no_cleanup(self):self.bad(lambda n,s,a,r:s.update(remaining={}))
 def test_missing_fresh_queries(self):self.bad(lambda n,s,a,r:r['fresh_cleanup'].clear())
 def test_decoder_claim_rejected(self):self.bad(lambda n,s,a,r:r['actual_scope_limits'].update(decoder_run=True))
 def test_invented_full_audit_rejected(self):self.bad(lambda n,s,a,r:r['actual_scope_limits'].update(default_full6259=True))
class Build(unittest.TestCase):
 def bad(self,fn):
  values=[V.load('build.json'),V.load('Root-build-review.json'),V.load('Root-installed-probe.json')];fn(*values)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate_build(*values)
 def test_build(self):self.assertEqual(V.validate_build(V.load('build.json'),V.load('Root-build-review.json'),V.load('Root-installed-probe.json'))['application_inputs'],1351)
 def test_wrong_image(self):self.bad(lambda b,r,p:b.update(image_id='wrong'))
 def test_wrong_revision(self):self.bad(lambda b,r,p:b.update(git_revision='HEAD'))
 def test_wrong_app_input(self):self.bad(lambda b,r,p:b['source_sha256'].pop('LICENSE'))
 def test_changed_app_input(self):self.bad(lambda b,r,p:b['source_sha256'].update(LICENSE='0'*64))
 def test_wrong_UID(self):self.bad(lambda b,r,p:r.update(probe_actual_UID=0))
 def test_missing_frontend(self):self.bad(lambda b,r,p:p.update(installed_hashed_files=0))
class SourceHEAD(unittest.TestCase):
 def bad(self,fn):
  value=V.load('source-head-one.json');fn(value)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.head_source(value,V.load('source-head-observation.json'))
 def test_actual(self):self.assertEqual(V.head_source(V.load('source-head-one.json'),V.load('source-head-observation.json'))['Source_HEAD_status'],401)
 def test_missing(self):self.bad(lambda v:v['cases'].clear())
 def test_other_status(self):self.bad(lambda v:v['cases'][0].update(status=405))
 def test_Source_body(self):self.bad(lambda v:v['cases'][0].update(body_hex='00'))
class Reconstruct(unittest.TestCase):
 def archive(self,name,data=b'ok',kind=tarfile.REGTYPE):
  out=io.BytesIO()
  with tarfile.open(fileobj=out,mode='w')as tar:
   row=tarfile.TarInfo(name);row.type=kind;row.size=len(data);tar.addfile(row,io.BytesIO(data))
  return out.getvalue()
 def test_normalized_deterministic(self):
  import reconstruct_app as R
  self.assertEqual(R.normalized({'rust/b':b'2','rust/a':b'1'}),R.normalized({'rust/a':b'1','rust/b':b'2'}))
 def test_regular(self):
  import reconstruct_app as R
  self.assertEqual(R.readtar(self.archive('rust/a')),{'rust/a':b'ok'})
 def test_unsafe(self):
  import reconstruct_app as R
  with self.assertRaises(ValueError):R.readtar(self.archive('../a'))
 def test_link(self):
  import reconstruct_app as R
  with self.assertRaises(ValueError):R.readtar(self.archive('rust/a',b'',tarfile.SYMTYPE))
class OnceHistory(unittest.TestCase):
 def reject(self,name,fn,call):
  from unittest.mock import patch
  original=V.load;value=original(name);fn(value)
  with patch.object(V,'load',side_effect=lambda n:value if n==name else original(n)):
   with self.assertRaises((ValueError,KeyError,TypeError)):call()
 def test_actual_once(self):self.assertTrue(V.once()['once_projection_valid'])
 def test_actual_history(self):self.assertTrue(V.history()['old_Native405_and_Source7_failure_preserved'])
 def test_wrong_nonce_digest(self):self.reject('once.json',lambda v:v['claim'].update(nonce_sha256='0'*64),V.once)
 def test_wrong_cases(self):self.reject('once.json',lambda v:v['cue']['cases'].clear(),V.once)
 def test_wrong_selection(self):self.reject('once.json',lambda v:v['claim'].update(selection_sha256='0'*64),V.once)
 def test_complete_replay_cannot_be_claimed(self):self.reject('blocked-closure.json',lambda v:v.update(full_original_execution_closure_publicly_restored=True),V.once)
 def test_old405_cannot_upgrade(self):self.reject('history.json',lambda v:v['old_Native_HEAD405'].update(status=401),V.history)
 def test_oldSourceFailure_cannot_upgrade(self):self.reject('history.json',lambda v:v['old_Source7_failure'].update(passed=True),V.history)
if __name__=='__main__':unittest.main()

