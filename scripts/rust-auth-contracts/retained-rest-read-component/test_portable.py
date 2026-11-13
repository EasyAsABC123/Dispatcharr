"""Retained raw corruption controls, no sockets/apps/builds."""
import copy,json,unittest
import verify as V
class Portable(unittest.TestCase):
 def test_both_actual_raw_qualifications(self):self.assertTrue(V.verify()['passed'])
 def bad(self,path,new):
  for case in(1,2):
   files=V.archived(case);raw=json.loads(files['rest-reads-private.json']);value=raw
   for k in path[:-1]:value=value[k]
   value[path[-1]]=new;files['rest-reads-private.json']=json.dumps(raw).encode()
   with self.assertRaises((ValueError,KeyError,TypeError)):V.check_case(case,files)
 def test_wrong_target(self):self.bad(['held','held','parameter_hex'],['bad'])
 def test_wrong_backend(self):self.bad(['held','held','backend_pid'],1)
 def test_missing_owner(self):self.bad(['post_loss_or_deadline_held_observations',1,'original_owner_weak_alive'],False)
 def test_short_sample(self):self.bad(['post_loss_or_deadline_held_observations',1,'elapsed_monotonic_seconds'],.2)
 def test_nonfinite_clock(self):
  for case in(1,2):
   files=V.archived(case);raw=json.loads(files['rest-reads-private.json']);row=next(e for e in raw['proxy_final']['events']if e['kind']=='ready-held');row['monotonic']=float('nan');files['rest-reads-private.json']=json.dumps(raw).encode()
   with self.assertRaises(ValueError):V.check_case(case,files)
 def test_live_after_close(self):self.bad(['closed_pool_size'],1)
 def test_bad_idle_bool(self):self.bad(['closed_pool_idle_diagnostic'],False)
 def test_prior_SQL_change(self):self.bad(['after','tables','accounts_user'],[])
 def test_short_hold(self):self.bad(['actual_target_hold_seconds'],0)
 def test_slot_return(self):self.bad(['writer_slots_after_settlement'],3)
 def test_cache_drop(self):self.bad(['raw_cache_after','raw_DUMP_PTTL'],{})
 def test_literal_default_audit_corruption(self):
  for case in(1,2):
   files=V.archived(case);files['default-after.json']+=b' '
   with self.assertRaises(ValueError):V.check_case(case,files)
 def test_host_identity_unknown(self):
  files=V.archived(1);r=json.loads(files['execution-report.json']);r['processes'][0]['starttime']=None;files['execution-report.json']=json.dumps(r).encode()
  with self.assertRaises(ValueError):V.check_case(1,files)
if __name__=='__main__':unittest.main()
