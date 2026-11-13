"""Pinned Redis8.10.3 wire-error discriminators; no Redis/socket operation."""
import base64,hashlib,io,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from acl_probes import setex_denied,denied_write_observation
from redis_reply_proxy import frame
SHA='fce9b39b58ad3c271e70b21db9c428b48e04b169';USER='owned_configured_failure_1'
class Wire(unittest.TestCase):
 def receipt(self):
  raw=("-ERR ACL failure in script: User "+USER+" has no permissions to run the 'setex' command script: "+SHA+", on @user_script:11.\r\n").encode()
  return {'target_replies':1,'receipts':[{'mode':'result-denied','script_cache_miss':False,'raw_base64':base64.b64encode(raw).decode(),'actual_server_reply_observed':True,'script_sha1':SHA,'reply_sha256':hashlib.sha256(raw).hexdigest(),'reply_bytes':len(raw)}]}
 def test_actual_pinned_raw_shape(self):self.assertTrue(denied_write_observation(self.receipt(),SHA,USER,11,'8.10.3'))
 def test_wrong_user(self):self.assertFalse(denied_write_observation(self.receipt(),SHA,'owned_configured_failure_2',11,'8.10.3'))
 def test_wrong_script(self):self.assertFalse(denied_write_observation(self.receipt(),'a'*40,USER,11,'8.10.3'))
 def test_wrong_version(self):self.assertFalse(denied_write_observation(self.receipt(),SHA,USER,11,'7.0.0'))
 def test_wrong_line(self):self.assertFalse(denied_write_observation(self.receipt(),SHA,USER,12,'8.10.3'))
 def test_noscript_is_not_targeted_denial(self):
  r=self.receipt();r['receipts'][0]['script_cache_miss']=True;self.assertFalse(denied_write_observation(r,SHA,USER,11,'8.10.3'))
 def test_dryrun_bulk_and_error_are_not_equivalent(self):
  expected=b"User "+USER.encode()+b" has no permissions to run the 'setex' command";raw=b'$'+str(len(expected)).encode()+b'\r\n'+expected+b'\r\n';wire,value=frame(io.BytesIO(raw));self.assertTrue(setex_denied(wire,value,USER));self.assertFalse(setex_denied(b'-'+expected+b'\r\n',expected,USER))
 def test_resp_truncation_is_not_null(self):
  with self.assertRaises(ValueError):frame(io.BytesIO(b'$4\r\nabc'))
if __name__=='__main__':unittest.main()
