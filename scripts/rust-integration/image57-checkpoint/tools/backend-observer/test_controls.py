"""Pure controller/observer boundary controls; no application/socket execution."""
import copy,hashlib,json,unittest
from pathlib import Path
import run as R
ROOT=Path(__file__).resolve().parent
class Controls(unittest.TestCase):
 def value(self):
  schema=R.read(ROOT/'schema.json');db='rust_capture_'+'a'*16;snap={'database':db,'tables':{x:{'rows':0,'sha256':'0'*64}for x in schema['tables']},'sequences':{x:{'last_value':1,'is_called':False}for x in schema['sequences']}};raw=b'{"status":"ready","backend":"rust","parity_complete":false}';return {'scope':'SingleMain57SQLReadThenStopObserver','actual_request_count':1,'path':'/health/ready','method':'GET','Main_destination':'127.0.0.1:9192 shared baseline network namespace','database':db,'before':snap,'after':copy.deepcopy(snap),'observation':{'began_monotonic':1.,'completed_monotonic':1.2,'status':200,'headers':[['content-type','application/json']],'body_hex':raw.hex(),'body_sha256':hashlib.sha256(raw).hexdigest(),'absolute_observation':{'completed':1.1,'deadline':9.,'timer_joined':True},'physical_close':{'fd_after':-1,'timer_joined':True,'original_inode_absent':True}}}
 def reject(self,change):
  v=self.value();change(v)
  with self.assertRaises((ValueError,KeyError)):R.qualify_observer(v,v['database'])
 def test_positive_synthetic_only(self):v=self.value();self.assertTrue(R.qualify_observer(v,v['database']))
 def test_replayed_count(self):self.reject(lambda d:d.update(actual_request_count=26))
 def test_boolean_count(self):self.reject(lambda d:d.update(actual_request_count=True))
 def test_wrong_endpoint(self):self.reject(lambda d:d.update(path='/player_api.php'))
 def test_status_failure(self):self.reject(lambda d:d['observation'].update(status=503))
 def test_open_socket(self):self.reject(lambda d:d['observation']['physical_close'].update(fd_after=6))
 def test_unjoined_timer(self):self.reject(lambda d:d['observation']['absolute_observation'].update(timer_joined=False))
 def test_SQL_mutation(self):self.reject(lambda d:d['after']['tables'].pop(next(iter(d['after']['tables']))))
 def test_changed_body(self):self.reject(lambda d:d['observation'].update(body_hex=b'{}'.hex()))
 def test_missing_full_scope(self):
  def bad(d):d['before']['tables'].pop(next(iter(d['before']['tables'])));d['after']=copy.deepcopy(d['before'])
  self.reject(bad)
 def test_exact_owned_query(self):self.assertEqual(R.backend_query('rust_capture_'+'a'*16),"SELECT COALESCE(json_agg(t),'[]'::json) FROM (SELECT pid,state,backend_start FROM pg_stat_activity WHERE datname='rust_capture_aaaaaaaaaaaaaaaa') t")
 def test_malformed_database(self):
  for db in ['dispatcharr','postgres',"rust_capture_';DROP",None]:
   with self.assertRaises(ValueError):R.backend_query(db)
 def test_unix_socket_no_password_cli(self):
  text=(ROOT/'run.py').read_text();observer=text[text.index('until=time.monotonic()+10;observations=[]'):text.index("for action,label in[(stop_Main")];self.assertIn("'-h','/var/run/postgresql'",observer);self.assertNotIn('PGPASSWORD',observer);self.assertIn('backend_query(database)',observer)
 def test_no_catalogue_phase_or_info(self):
  text=(ROOT/'run.py').read_text();self.assertNotIn("'/capture/main_http.py'",text);self.assertIn("'/capture/observer_http.py'",text);self.assertIn('socket.create_connection',text)
 def test_existing_output_and_original_seal_immutable(self):
  old=ROOT.parents[1]/'image57-xc-catalogue-main-prep-1/controller/seal.json';self.assertEqual(R.sha(old),'e824c0e74d0769af34665811c31b1985f03ecd603b5331d6ca875f0d67b1064d');self.assertNotEqual(R.read(ROOT/'policy.json')['output'],'artifacts/rust/image57-xc-catalogue-main-1')
 def test_blocked(self):self.assertTrue(R.EXECUTION_BLOCKED)
if __name__=='__main__':unittest.main(verbosity=2)
