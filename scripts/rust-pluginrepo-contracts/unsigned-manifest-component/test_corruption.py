"""Socket-free meaningful status/SQL/cache/clock/reference corruption controls."""
import copy,gzip,hashlib,json,math,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
import verify as V
class Controls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.source=V.load_projection('Source24');cls.native=V.load_projection('Native24')
 def reject(self,change):
  a,b=copy.deepcopy(self.source),copy.deepcopy(self.native);change(a,b)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.N.validate(a,b)
 def test_full25_baseline(self):self.assertTrue(V.validate()['passed'])
 def test_status(self):self.reject(lambda a,b:b['requests'][0].__setitem__('status',200))
 def test_response_body_hash(self):self.reject(lambda a,b:b['requests'][0].__setitem__('response_body_sha256','0'*64))
 def test_body_length(self):self.reject(lambda a,b:b['requests'][0].__setitem__('response_body_bytes',True))
 def test_no_capture_complete(self):self.reject(lambda a,b:b.__setitem__('capture_complete',False))
 def test_count_not_fake24(self):self.reject(lambda a,b:b.__setitem__('actual_request_count',1))
 def test_sequence(self):
  def change(a,b):b['requests'][8]['after']['public_manifest']['sequences']['plugins_pluginrepo_id_seq'][0]+=1
  self.reject(change)
 def test_foreign_table(self):
  def change(a,b):
   tables=b['requests'][0]['after']['public_manifest']['tables'];name=next(n for n in tables if n not in('plugins_pluginrepo','plugins_pluginconfig'));tables[name]['count']+=1
  self.reject(change)
 def test_api_key(self):self.reject(lambda a,b:b['requests'][1]['before']['users'][0].__setitem__('api_key','wrong'))
 def test_response_schema(self):self.reject(lambda a,b:b['requests'][0]['json'].__setitem__('unmeasured',True))
 def test_response_clock_outside_window(self):
  def change(a,b):b['requests'][8]['completed_utc']=b['requests'][8]['began_utc']='2000-01-01T00:00:00+00:00'
  self.reject(change)
 def test_actual_clock_map_no_rewind(self):
  c=V.N.Clocks(self.source);s='2026-01-01T00:00:00+00:00';n='2026-02-01T00:00:00+00:00';c.mapping[V.N.clock(s)]=V.N.clock(n)
  with self.assertRaises(ValueError):c.compare(s,s)
 def test_frontend_fd_scope(self):self.reject(lambda a,b:b['requests'][0]['physical_close'].__setitem__('fd_after',7))
 def test_no_main_transfer(self):self.reject(lambda a,b:b.__setitem__('Main_or_fullM8',True))
 def test_exact_public_reference_no_original_fallback(self):
  with tempfile.TemporaryDirectory()as tmp:
   root=Path(tmp);(root/'data').mkdir();policy=json.loads((V.ROOT/'projection-policy.json').read_bytes());row=policy['projections']['Source24'];raw=gzip.compress(b'{}',mtime=0);(root/row['file']).write_bytes(raw);row['original_raw_sha256']=hashlib.sha256(b'{}').hexdigest();(root/'projection-policy.json').write_text(json.dumps(policy))
   with patch.object(V,'ROOT',root),self.assertRaises(ValueError):V.load_projection('Source24')

class LimitedNetworkControls(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  import verify_lifecycle as L
  cls.L=L;cls.source=V.load_projection('Source24');cls.network=L.load('Source24','network/network-observer-terminal.json')
 def reject(self,fn):
  r=copy.deepcopy(self.network);fn(r)
  with self.assertRaises(ValueError):self.L.limited_packets(self.source,r)
 def test_actual_preAPI_unsupported_scope(self):self.assertFalse(self.L.limited_packets(self.source,self.network)['retrospective_MLD_or_noDNS_whole_lifecycle_claim'])
 def test_unsupported_after_API_refused(self):self.reject(lambda r:r['exceptions'][0].__setitem__('epoch',self.L.instant(self.source['phase_completed_utc'])+1))
 def test_unsupported_nan_clock_refused(self):self.reject(lambda r:r['exceptions'][0].__setitem__('epoch',float('nan')))
 def test_unsupported_bool_clock_refused(self):self.reject(lambda r:r['exceptions'][0].__setitem__('epoch',True))
 def test_kernel_drop_refused(self):self.reject(lambda r:r.__setitem__('kernel_drops',1))
 def test_kernel_count_refused(self):self.reject(lambda r:r.__setitem__('kernel_received_packets',0))
 def test_no_retrospective_payload_claim(self):self.reject(lambda r:r['exceptions'][0].__setitem__('raw_payload_retained',True))
 def test_literal_eight_exception_count(self):self.reject(lambda r:r.__setitem__('exceptions',[]))


class PhysicalControls(unittest.TestCase):
 def reject(self,name,fn,mode='headers-abort'):
  import verify_physical as P
  with tempfile.TemporaryDirectory()as tmp:
   out=Path(tmp);P.materialize(mode,out);p=out/name;r=json.loads(p.read_bytes());fn(r);p.write_text(json.dumps(r))
   with self.assertRaises((ValueError,KeyError,TypeError)):P.validate_physical(out,mode)
 def test_actual_two_physical_baselines(self):
  import verify_physical as P
  self.assertTrue(P.validate()['passed_limited_physical2'])
 def test_original_native_fd_generation_still_present(self):self.reject('native-http-event-1.json',lambda r:r.__setitem__('after_link',r['original_socket']['socket_link']))
 def test_owner_retired_before_fd_close(self):self.reject('native-http-event-1.json',lambda r:r.__setitem__('original_writer_weak_some_at_event',False))
 def test_extra_permit_allowance(self):self.reject('native-http-event-1.json',lambda r:r.__setitem__('same_four_slots_available_at_event',4))
 def test_short_owner_sample(self):self.reject('native-held-observations.json',lambda r:r['samples'][1].__setitem__('elapsed_seconds',r['samples'][0]['elapsed_seconds']+.1))
 def test_bool_held_clock(self):self.reject('origin-wire-held.json',lambda r:r.__setitem__('held_monotonic',True))
 def test_origin_socket_mismatch(self):self.reject('origin-client-eof.json',lambda r:r.__setitem__('actual_socket_inode',1))
 def test_origin_fate_not_observed(self):self.reject('origin-client-eof.json',lambda r:r.__setitem__('fate','inferred FIN'))
 def test_bool_idle_counter(self):self.reject('pool-closed-ready.json',lambda r:r.__setitem__('idle_diagnostic',True))
 def test_remaining_sql_backend(self):self.reject('owned-backends-while-ELF-after-close.json',lambda r:r.append({'pid':1}))
 def test_actual_abort_claim_requires_join(self):self.reject('native-response-and-fate.json',lambda r:r['actual_observer_fate'].__setitem__('actual_join_cancelled',False))
 def test_frame_clone_must_hold_original(self):self.reject('native-response-and-fate.json',lambda r:r['actual_response']['after_drop_original_clone_kept'].__setitem__('original_weak_some',False),'body-deadline')
 def test_deadline_is_actual_timeout(self):self.reject('native-response-and-fate.json',lambda r:r['actual_observer_fate'].__setitem__('actual_timeout',False),'body-deadline')
 def test_actual_body_hash(self):self.reject('native-response-and-fate.json',lambda r:r['actual_response'].__setitem__('body_sha256','0'*64),'body-deadline')

if __name__=='__main__':unittest.main()
