"""Offline guards only: no Docker, app/network fixture, SQL or resource sampler."""
import copy,hashlib,io,json,os,sys,tarfile,tempfile,threading,unittest
from pathlib import Path
import candidate_inputs as ci,fixture_guards as fg,resp_gate as rg,origin_guard as og
from unittest import mock
from bounded_process import Collected,CollectionFailure
class FakeSocket:
 def __init__(self,parts):self.parts=iter(parts)
 def recv(self,_):return next(self.parts,b'')
def frame(raw):return rg.Frame(FakeSocket([raw]),threading.Event()).read()
def resp(*args):return b'*'+str(len(args)).encode()+b'\r\n'+b''.join(b'$'+str(len(a)).encode()+b'\r\n'+a+b'\r\n'for a in args)
class Controls(unittest.TestCase):
 def test_candidate_positive_and_seven_negatives(self):
  with tempfile.TemporaryDirectory()as tmp:
   d=Path(tmp);runtime={'rust/a.rs':b'qualified'};script=b'const REAP: &str = r#"one"#;\nconst RESERVE_PROVIDER: &str = r#"two"#;'
   files={**runtime,'rust/crates/coordination/src/scripts.rs':script};archive=d/'source.tar'
   with tarfile.open(archive,'w')as t:
    for n,data in files.items():
     info=tarfile.TarInfo(n);info.size=len(data);t.addfile(info,io.BytesIO(data))
   expected={n:hashlib.sha256(v).hexdigest()for n,v in runtime.items()};ep=d/'expected.json';ep.write_text(json.dumps(expected));mp=d/'manifest.json'
   m={'passed':True,'exit_code':0,'tag':'qualified:1','image_id':'sha256:fixed','source_sha256':{n:hashlib.sha256(v).hexdigest()for n,v in files.items()},'source_archive_sha256':hashlib.sha256(archive.read_bytes()).hexdigest(),'source_tree_sha256':'tree','git_revision':'revision'}
   def invoke(value,image='qualified:1',path=archive):
    mp.write_text(json.dumps(value));return ci.candidate(mp,image,ep,path)
   self.assertEqual(invoke(m)['reserve_script_sha1'],hashlib.sha1(b'onetwo').hexdigest())
   for mutate in[lambda v:v.update(passed=False),lambda v:v.update(exit_code=1),lambda v:v.update(source_archive_sha256='bad'),lambda v:v['source_sha256'].update({'rust/a.rs':'bad'}),lambda v:v['source_sha256'].update({'rust/crates/coordination/src/scripts.rs':'bad'})]:
    bad=copy.deepcopy(m);mutate(bad)
    with self.assertRaises(ValueError):invoke(bad)
   with self.assertRaises(ValueError):invoke(m,'other:1')
   with self.assertRaises(FileNotFoundError):invoke(m,path=d/'missing.tar')
 def data(self,name):
  v={'database':name,'tables':{'Movie':{'rows':1,'sha256':'a'*64}},'sequences':{'movie_id_seq':{'last_value':1,'is_called':True}}};v['dataset_sha256']=hashlib.sha256(json.dumps({k:v[k]for k in['tables','sequences']},sort_keys=True).encode()).hexdigest();return v
 def test_clone_identity_and_forgery(self):
  names=['rust_pending_template','rust_pending_source','rust_pending_native'];v=[self.data(n)for n in names];self.assertTrue(fg.clones(*v,names)['named_tables_and_sequences_equal'])
  for kind in['database','rows','sequence','digest','reuse','bool']:
   bad=copy.deepcopy(v)
   if kind=='database':bad[1]['database']='dispatcharr'
   if kind=='rows':bad[1]['tables']['Movie']['rows']=2
   if kind=='sequence':bad[1]['sequences']['movie_id_seq']['last_value']=2
   if kind=='digest':bad[1]['dataset_sha256']='0'*64
   if kind=='reuse':bad[1]['database']=names[0]
   if kind=='bool':bad[1]['tables']['Movie']['rows']=True
   with self.assertRaises(ValueError):fg.clones(*bad,names)
 def test_recomputed_row_drift_rejected(self):
  names=['rust_pending_template','rust_pending_source','rust_pending_native'];v=[self.data(n)for n in names];v[1]['tables']['Movie']['rows']=2;v[1]['dataset_sha256']=hashlib.sha256(json.dumps({k:v[1][k]for k in['tables','sequences']},sort_keys=True).encode()).hexdigest()
  with self.assertRaises(ValueError):fg.clones(*v,names)
 def test_resp_split_binary_and_pipeline(self):
  raw=resp(b'EVALSHA',b'a'*40,b'1',b'profile_connections:1',b'\x00\r\n');f=rg.Frame(FakeSocket([raw[:5],raw[5:]+b'+OK\r\n']),threading.Event());self.assertEqual(f.read(),raw);self.assertEqual(f.read(),b'+OK\r\n');self.assertEqual(rg.arguments(raw)[-1],b'\x00\r\n')
 def test_resp_invalid_bounds(self):
  for raw in[b'$-2\r\n',b'*-2\r\n',b'$1048577\r\n',b'*1025\r\n',b'$1\r\nxZZ',b':no\r\n',b'_bad\r\n',b'+'+b'a'*8193+b'\r\n',b'*1\r\n'*18+b'+OK\r\n']:
   with self.subTest(raw=raw[:20]),self.assertRaises(ValueError):frame(raw)
  with self.assertRaises(ValueError):rg.arguments(resp(b'GET',b'key')+b'+OK\r\n')
 def test_gate_exact_target_noscript_and_single_hold(self):
  gate=rg.Gate('a'*40,None);args=[b'EVALSHA',b'a'*40,b'1',b'profile_connections:1'];self.assertFalse(gate.target(args));gate.arm('profile_connections:1');self.assertTrue(gate.target(args));self.assertFalse(gate.target(args[:-1]+[b'profile_connections:2']));self.assertFalse(gate.target([b'EVALSHA',b'b'*40]+args[2:]));gate.response(True,b'-NOSCRIPT absent\r\n');self.assertEqual(gate.seen,0)
  gate.release.set();gate.response(True,b':1\r\n');gate.response(True,b':1\r\n');self.assertEqual(gate.seen,2);self.assertEqual([v['kind']for v in gate.rows],['backend-target-reply-read-held','backend-target-reply-released'])
 def test_gate_invalid_profile_trace_cap(self):
  gate=rg.Gate('a'*40,None)
  for name in['anything','profile_connections:-1','profile_connections:1\n']:
   with self.assertRaises(ValueError):gate.arm(name)
  gate.rows=[{}]*1024
  with self.assertRaises(ValueError):gate.trace('overflow')
 def test_pid_identity_negative_no_side_effect(self):
  row={'role':'gate','nonce':'owned','pid':2345,'starttime':'123'};self.assertEqual(fg.stop_record(row,'gate','owned',lambda _: '123'),2345)
  for key,value in[('role','other'),('nonce','other'),('pid',0),('pid',1),('pid',True),('pid',2**31),('starttime','456')]:
   bad={**row,key:value}
   with self.assertRaises(ValueError):fg.stop_record(bad,'gate','owned',lambda _: '123')
 def local(self,code,limit=1024):
  # This callback owns precisely the child launched below; no fixture/container claim.
  jobs=[]
  def stop():
   if jobs and jobs[0].process.poll()is None:jobs[0].process.terminate()
  c=Collected([sys.executable,'-c',code],dict(os.environ),limit,stop);jobs.append(c);return c
 def test_output_positive(self):
  c=self.local('print("ok")');self.assertEqual(c.wait(2),b'ok\n');self.assertTrue(c.physically_settled)
 def test_stdout_stderr_inflight_cap(self):
  for stream in['stdout','stderr']:
   c=self.local('import os,time;\nwhile True: os.write('+str(1 if stream=='stdout'else 2)+',b"x"*8192)')
   with self.assertRaises(CollectionFailure):c.wait(3)
   self.assertEqual(c.used,1024);self.assertEqual(c.invalid,'CollectionCapacity');self.assertIsNotNone(c.process.poll());self.assertFalse(any(t.is_alive()for t in c.threads))
 def test_deadline_and_abort_physical_settlement(self):
  for abort in[False,True]:
   c=self.local('import time;time.sleep(10)')
   if abort:c.abort()
   with self.assertRaises(CollectionFailure):c.wait(.05)
   self.assertIsNotNone(c.process.poll());self.assertFalse(any(t.is_alive()for t in c.threads));self.assertEqual(c.invalid,'CollectionAborted'if abort else'CollectionDeadline')
 def test_exact_owned_pid_stop_real_child(self):
  import subprocess,signal
  child=subprocess.Popen([sys.executable,'-c','import time;time.sleep(10)'])
  def start(pid):
   raw=Path('/proc',str(pid),'stat').read_text();return raw[raw.rfind(')')+2:].split()[19]
  try:
   row={'role':'control-child','nonce':'offline-owned','pid':child.pid,'starttime':start(child.pid)}
   pid=fg.stop_record(row,'control-child','offline-owned',start);os.kill(pid,signal.SIGTERM);child.wait(timeout=2);self.assertIsNotNone(child.poll())
  finally:
   if child.poll()is None:child.kill();child.wait(timeout=2)
 def test_origin_guard_positive_and_binding_negatives(self):
  stats={'vod':{'fixture_sha256':og.FILE_SHA,'fixture_bytes':99999}};headers={'Content-Range':'bytes 0-8191/99999'};body=b'x'*8192
  with mock.patch.object(og.hashlib,'sha256')as digest:
   digest.return_value.hexdigest.return_value=og.PREFIX_SHA
   self.assertTrue(og.validate(stats,206,headers,body)['passed'])
   for status,bound,part in[(200,headers,body),(206,{'Content-Range':'bytes 0-8191/99998'},body),(206,headers,body+b'x')]:
    with self.assertRaises(ValueError):og.validate(stats,status,bound,part)
   digest.return_value.hexdigest.return_value='bad'
   with self.assertRaises(ValueError):og.validate(stats,206,headers,body)
  bad=copy.deepcopy(stats);bad['vod']['fixture_sha256']='bad'
  with self.assertRaises(ValueError):og.validate(bad,206,headers,body)
 def test_origin_starts_and_qualifies_before_Source(self):
  text=(Path(__file__).parent/'run.py').read_text();start=text.index("run('up','-d','--no-build','origin')");guard=text.index("'/pending-tools/origin_guard.py'");source=text.index("child('source-web'")
  self.assertLess(start,guard);self.assertLess(guard,source)
 def test_reserve_error_type_and_integer_never_held(self):
  for raw in[b'-ERR invalid reservation counter\r\n',b'-WRONGTYPE invalid\r\n',b'+OK\r\n',b'$1\r\n1\r\n',b'*1\r\n:1\r\n',b':0\r\n',b':2\r\n',b':-1\r\n',b':-2\r\n',b':01\r\n']:
   gate=rg.Gate('a'*40,None);gate.arm('profile_connections:1')
   with self.assertRaisesRegex(ValueError,'UnexpectedReserveReplySchema'):gate.response(True,raw)
   self.assertEqual(gate.seen,0);self.assertEqual(gate.rows,[]);self.assertEqual(gate.invalid,['UnexpectedReserveReplySchema'])
 def test_busy_reserve_requires_actual_minus_two(self):
  gate=rg.Gate('a'*40,None);gate.arm('profile_connections:1',-2);gate.release.set();gate.response(True,b':-2\r\n');self.assertEqual(gate.seen,1)
  for bad in[0,True,2,-1]:
   with self.assertRaises(ValueError):rg.Gate('a'*40,None).arm('profile_connections:1',bad)
 def test_stop_key_not_just_admin200(self):
  self.assertTrue(fg.early_stop({'stop':'true'}))
  with self.assertRaisesRegex(ValueError,'EarlyStopConsumed'):fg.early_stop({'stop':None})
 def test_hold_overshoot_and_control_work_bound(self):
  self.assertTrue(fg.hold_observation(10,11,12.4)['within_declared_bound']);self.assertFalse(fg.hold_observation(10,11,12.4)['exact_pause_identity_claimed'])
  for end,control in[(12.8,11),(12.3,11),(12.6,12.5),(13.5,13.4)]:
   with self.assertRaisesRegex(ValueError,'DeclaredHoldTimingBound'):fg.hold_observation(10,control,end)
 def test_other_movie_reservation_not_readability_only(self):
  row={'owner_key':'ns:owner','owner_token':'lease','owner_confirm':'lease','ledger':{'token':'lease','owner':'ns:owner','profile':'profile_connections:4','reserved':'1','fence':'42'},'metadata':{'content_uuid':'other','user_id':'5','active_streams':'1','rust_owner_token':'metadata-token','m3u_profile_id':'4'}}
  self.assertTrue(fg.other_reservation(row,copy.deepcopy(row),'profile_connections:4','other',5,1)['reserved'])
  for name in['token','owner','profile','reserved','fence']:
   bad=copy.deepcopy(row);bad['ledger'][name]='changed'
   with self.assertRaises(ValueError):fg.other_reservation(row,bad,'profile_connections:4','other',5,1)
  for name in['content_uuid','user_id','active_streams','m3u_profile_id','rust_owner_token']:
   bad=copy.deepcopy(row);bad['metadata'][name]=''
   with self.assertRaises(ValueError):fg.other_reservation(row,bad,'profile_connections:4','other',5,1)
  with self.assertRaisesRegex(ValueError,'GlobalReservation'):fg.other_reservation(row,row,'profile_connections:4','other',5,0)
 def test_private_fence_required_positive_bounded_in_both_snapshots(self):
  row={'owner_key':'ns:owner','owner_token':'lease','owner_confirm':'lease','ledger':{'token':'lease','owner':'ns:owner','profile':'profile_connections:4','reserved':'1','fence':'42'},'metadata':{'content_uuid':'other','user_id':'5','active_streams':'1','rust_owner_token':'metadata-token','m3u_profile_id':'4'}}
  for value in[None,'0','-1','+1','01','1.0','bad','9223372036854775808','9'*100,1,True]:
   for side in['before','after','both']:
    before=copy.deepcopy(row);after=copy.deepcopy(row)
    if side in['before','both']:before['ledger']['fence']=value
    if side in['after','both']:after['ledger']['fence']=value
    with self.subTest(value=value,side=side),self.assertRaisesRegex(ValueError,'PositiveFence'):fg.other_reservation(before,after,'profile_connections:4','other',5,1)
  before=copy.deepcopy(row);after=copy.deepcopy(row);before['ledger'].pop('fence');after['ledger'].pop('fence')
  with self.assertRaisesRegex(ValueError,'PositiveFence'):fg.other_reservation(before,after,'profile_connections:4','other',5,1)
  for value in['1','9223372036854775807']:
   before=copy.deepcopy(row);before['ledger']['fence']=value
   self.assertTrue(fg.other_reservation(before,copy.deepcopy(before),'profile_connections:4','other',5,1)['reserved'])
 def test_fixture_still_hardblocked(self):
  source=(Path(__file__).parent/'run.py').read_text();self.assertIn("raise RuntimeError('UNVALIDATED_MAIN_FIXTURE_DRAFT",source)
if __name__=='__main__':unittest.main(verbosity=2)
