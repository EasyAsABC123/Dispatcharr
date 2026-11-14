"""Focused semantic corruptions; authority hashing tested separately."""
import copy,json,unittest
import verify as V
class Public(unittest.TestCase):
 def setUp(self):self.args=[V.load(n)for n in ['native-execution.json','setup-negative.json','build.json','agent-fresh-cleanup.json','history.json']]
 def bad(self,fn):
  values=copy.deepcopy(self.args);fn(*values)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.qualify(*values)
 def test_actual(self):self.assertEqual(V.qualify(*self.args)['actual_settled_CLI_receipts'],45)
 def test_missing_request(self):self.bad(lambda v,*_:v['cases'].clear())
 def test_extra_request(self):self.bad(lambda v,*_:v['cases'].append(copy.deepcopy(v['cases'][0])))
 def test_old404(self):self.bad(lambda v,*_:v['cases'][0].update(status=404))
 def test_wrong_body(self):self.bad(lambda v,*_:v['cases'][0].update(body_hex='00'))
 def test_worker_header(self):self.bad(lambda v,*_:v['cases'][0]['headers'].append(['X-Worker-ID','provider-spoof']))
 def test_open_socket(self):self.bad(lambda v,*_:v['cases'][0]['physical_close'].update(fd_after=3))
 def test_SQL_missing_table(self):self.bad(lambda v,*_:v['owned_after']['tables'].pop('accounts_user'))
 def test_SQL_missing_sequence(self):self.bad(lambda v,*_:v['default_after']['sequences'].clear())
 def test_SQL_changed(self):self.bad(lambda v,*_:v['owned_after']['tables']['accounts_user'].update(rows=1))
 def test_origin_empty_coverage(self):self.bad(lambda v,*_:v['origin_before'].update(connections=[],accepted_TCP_total=0,control_requests=0))
 def test_origin_business(self):self.bad(lambda v,*_:v['origin_after']['rows'].append({'method':'GET'}))
 def test_origin_unjoined(self):self.bad(lambda v,*_:v['origin_terminal'].update(handler_threads_joined=False))
 def test_builtin_uncorrelated(self):self.bad(lambda v,*_:v['migration_builtin_context']['table_rows']['m3u_m3uaccount'][0][0].update(is_active=False))
 def test_missing_CLI(self):self.bad(lambda v,*_:v['commands'].pop())
 def test_unsettled_pipe(self):self.bad(lambda v,*_:v['commands'][0].update(pipe_threads_settled=False))
 def test_changed_stream_hash_shape(self):self.bad(lambda v,*_:v['commands'][0].update(stdout_sha256='bad'))
 def test_original_identity_alive(self):self.bad(lambda v,*_:v['original_host_identity_absence'][0].update(current_identity_sha256=v['original_host_identity_absence'][0]['process_identity_sha256']))
 def test_missing_cleanup(self):self.bad(lambda v,*_:v['cleanup_steps'].pop())
 def test_missing_CID(self):self.bad(lambda v,*_:v['fresh_exact_CID_cleanup'].pop())
 def test_backend_present(self):self.bad(lambda v,*_:v.update(owned_backends_after_stop=1))
 def test_wrong_image(self):self.bad(lambda v,*_:v.update(candidate_image='wrong'))
 def test_elapsed_infinite(self):self.bad(lambda v,*_:v.update(host_elapsed_seconds=float('inf')))
 def test_old_failure_relabel(self):self.bad(lambda v,n,*_:n.update(passed=True))
 def test_old_business_invented(self):self.bad(lambda v,n,*_:n.update(actual_business_request_invocations=1))
 def test_fresh_coverage_empty(self):self.bad(lambda v,n,b,f,*_:f['queries'].clear())
 def test_graph_transfer(self):self.bad(lambda v,*_:v.update(full_M4_acceptance=True))
 def test_Source_replay(self):self.bad(lambda v,*_:v.update(Source_replay=1))
 def test_history_relabel(self):self.bad(lambda v,n,b,f,h:h.update(run1_actual_host_exit=0))

class Reconstruction(unittest.TestCase):
 def test_unsafe_archive_path(self):
  import io,tarfile,reconstruct_app as R
  b=io.BytesIO()
  with tarfile.open(fileobj=b,mode='w')as t:
   x=tarfile.TarInfo('../escape');x.size=1;t.addfile(x,io.BytesIO(b'x'))
  with self.assertRaises(ValueError):R.readtar(b.getvalue())
 def test_symlink_archive_rejected(self):
  import io,tarfile,reconstruct_app as R
  b=io.BytesIO()
  with tarfile.open(fileobj=b,mode='w')as t:
   x=tarfile.TarInfo('link');x.type=tarfile.SYMTYPE;x.linkname='/private';t.addfile(x)
  with self.assertRaises(ValueError):R.readtar(b.getvalue())
 def test_duplicate_archive_rejected(self):
  import io,tarfile,reconstruct_app as R
  b=io.BytesIO()
  with tarfile.open(fileobj=b,mode='w')as t:
   for _ in range(2):
    x=tarfile.TarInfo('same');x.size=1;t.addfile(x,io.BytesIO(b'x'))
  with self.assertRaises(ValueError):R.readtar(b.getvalue())
 def test_existing_output_refused_before_git(self):
  import tempfile,reconstruct_app as R
  with tempfile.TemporaryDirectory()as p:
   with self.assertRaises(ValueError):R.reconstruct('/not-a-repo',p)


class Once(unittest.TestCase):
 def setUp(self):self.v=V.load('once.json');self.build=V.load('build.json');self.run=(V.HERE/'blocked-code/run.py').read_bytes()
 def bad(self,fn):
  v=copy.deepcopy(self.v);fn(v)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.check_once(v,self.build,self.run)
 def test_actual_flag_only_chain(self):V.check_once(self.v,self.build,self.run)
 def test_claim_rawcue_changed(self):self.bad(lambda v:v['claim'].update(cue_sha256='0'*64))
 def test_claim_run_changed(self):self.bad(lambda v:v['claim'].update(execution_run_sha256='0'*64))
 def test_claim_expired(self):self.bad(lambda v:v['claim'].update(claimed_epoch=v['cue']['expires_epoch']+1))
 def test_wrong_candidate(self):self.bad(lambda v:v['cue'].update(candidate_image='wrong'))
if __name__=='__main__':unittest.main()
