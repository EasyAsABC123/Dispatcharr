"""Portable corruption/ownership controls; no app, socket or build."""
import copy,hashlib,importlib.util,json,tempfile,unittest
from pathlib import Path
P=Path(__file__).resolve().parent;s=importlib.util.spec_from_file_location('portable_materialize',P/'materialize.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m);M=json.loads((P/'evidence-manifest.json').read_text());one=next(iter(M.values()))
class Controls(unittest.TestCase):
 def test_all_actual_payloads(self):
  for row in M.values():self.assertEqual(len(m.payload(P,row)),row['projected_bytes'])
 def test_wrong_compressed_digest(self):
  r={**one,'gzip_sha256':'0'*64}
  with self.assertRaises(ValueError):m.payload(P,r)
 def test_wrong_projected_digest(self):
  with self.assertRaises(ValueError):m.payload(P,{**one,'projected_sha256':'0'*64})
 def test_raw_digest_cannot_substitute_projection(self):
  r=next(r for r in M.values()if r['raw_sha256']!=r['projected_sha256'])
  with self.assertRaises(ValueError):m.payload(P,{**r,'projected_sha256':r['raw_sha256']})
 def test_truncated_size(self):
  with self.assertRaises(ValueError):m.payload(P,{**one,'projected_bytes':one['projected_bytes']-1})
 def test_missing_payload(self):
  with self.assertRaises(FileNotFoundError):m.payload(P,{**one,'payload':'blobs/'+'0'*64+'.gz'})
 def test_existing_output_refused(self):
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):m.fresh(Path(d))
 def test_broken_symlink_output_refused(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d)/'link';p.symlink_to(Path(d)/'missing')
   with self.assertRaises(ValueError):m.fresh(p)
 def test_absolute_refused(self):
  with self.assertRaises(ValueError):m.destination(Path('/tmp'),'/x')
 def test_escape_refused(self):
  with self.assertRaises(ValueError):m.destination(Path('/tmp'),'../x')
 def test_fresh_restore_retains_literal_negative(self):
  r=M['runs/clocked-recordings-native-audio61-2-3/integration.json'];h=json.loads(m.payload(P,r));self.assertIs(type(h['actual_host_exit']),int);self.assertEqual(h['actual_host_exit'],1);self.assertEqual(h['actual_controller_exit'],1)
 def test_epochs_are_distinct(self):
  scope=json.loads((P/'scope.json').read_text());self.assertEqual(len({x['source_map']for x in scope['epochs'].values()}),3);self.assertFalse(scope['no_execution_transfer_between_epochs']is False)
 def test_actual_report_remains_bounded(self):
  label='runs/clocked-recordings-native-audio61-2-4/native-dvr-physical-private.json';r=json.loads(m.payload(P,M[label]));self.assertIn('stderr',json.dumps(r));self.assertLess(M[label]['projected_bytes'],2*1024*1024)
class SidecarControls(unittest.TestCase):
 def setUp(self):
  import stderr_sidecars
  self.loader=stderr_sidecars;self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
  prefix='runs/clocked-recordings-native-audio61-2-4/'
  self.encoded=json.loads(m.payload(P,M[prefix+'native-dvr-physical-private.json']))
  for name,row in M.items():
   if name.startswith(prefix+'native-dvr-stderr-by-sha256/'):
    out=self.root/name.removeprefix(prefix);out.parent.mkdir(exist_ok=True);out.write_bytes(m.payload(P,row))
 def tearDown(self):self.temp.cleanup()
 def ref(self):
  return next(row['stderr']['tail']for c in self.encoded['commands']for row in c.get('diagnostics',{}).get('processes',{}).get('rows',[])if row.get('stderr'))
 def test_actual_lossless_roundtrip_equals_all_raw_IPC(self):
  expanded,proof=self.loader.expand_report(self.encoded,self.root)
  raw=json.loads(m.payload(P,M['runs/clocked-recordings-native-audio61-2-4/native-clocked-recording-private.json']))
  self.assertEqual(expanded['commands'],[x['reply']for x in raw['physical']['samples']]);self.assertEqual(proof['unique_blobs'],2)
 def test_corrupt_raw_tail_rejected(self):
  p=next((self.root/'native-dvr-stderr-by-sha256').iterdir());p.write_bytes(b'corrupt')
  with self.assertRaises(ValueError):self.loader.expand_report(self.encoded,self.root)
 def test_extra_sidecar_rejected(self):
  (self.root/'native-dvr-stderr-by-sha256'/'unknown').write_bytes(b'')
  with self.assertRaises(ValueError):self.loader.expand_report(self.encoded,self.root)
 def test_noncanonical_path_rejected(self):
  self.ref()['$native_dvr_stderr_ref']['path']='../outside'
  with self.assertRaises(ValueError):self.loader.expand_report(self.encoded,self.root)
 def test_boolean_size_rejected(self):
  self.ref()['$native_dvr_stderr_ref']['bytes']=True
  with self.assertRaises(ValueError):self.loader.expand_report(self.encoded,self.root)
 def test_raw_tail_fallback_rejected(self):
  tail=self.ref();tail.clear();tail['raw']=[1,2]
  with self.assertRaises(ValueError):self.loader.expand_report(self.encoded,self.root)
if __name__=='__main__':unittest.main()
