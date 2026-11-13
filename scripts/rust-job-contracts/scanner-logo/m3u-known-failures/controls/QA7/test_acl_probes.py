"""Socket-free actual-wire-shape fault-target controls; no Redis/API execution."""
import base64,hashlib,io,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from acl_probes import profile_exact,setex_denied,denied_write_observation,encode
from owned_redis_reply_proxy import frame
class Controls(unittest.TestCase):
 def profile(self):return[b'flags',[b'on'],b'passwords',[b'fixture-hash'],b'commands',b'+@all -setex',b'keys',b'~*',b'channels',b'&*',b'selectors',[]]
 def test_exact_setex_and_channels_profile(self):self.assertTrue(profile_exact(self.profile()))
 def test_old_wrong_set_command_and_absent_channel_grant_rejected(self):
  value=self.profile();value[5]=b'+@all -set';self.assertFalse(profile_exact(value));value=self.profile();value[9]=b'';self.assertFalse(profile_exact(value))
 def test_other_error_is_not_dryrun_denial(self):
  message=b"User owned_m3u_failure_3 has no permissions to run the 'setex' command";wire=b'$'+str(len(message)).encode()+b'\r\n'+message+b'\r\n';raw,value=frame(io.BytesIO(wire))
  self.assertTrue(setex_denied(raw,value,'owned_m3u_failure_3'));self.assertFalse(setex_denied(b'-ERR unknown\r\n',b"unknown command 'ACL'",'owned_m3u_failure_3'));self.assertFalse(setex_denied(b'+OK\r\n',b'OK','owned_m3u_failure_3'))
 def test_dryrun_actual_error_is_not_the_expected_bulk_denial(self):
  message=b"User owned_m3u_failure_3 has no permissions to run the 'setex' command";raw,value=frame(io.BytesIO(b'-ERR '+message+b'\r\n'));self.assertFalse(setex_denied(raw,value,'owned_m3u_failure_3'))
 def test_fabricated_parsed_value_cannot_hide_wrong_raw_wire(self):
  message=b"User owned_m3u_failure_3 has no permissions to run the 'setex' command";self.assertFalse(setex_denied(b'-ERR permission\r\n',message,'owned_m3u_failure_3'));self.assertFalse(setex_denied(b'$3\r\nfoo\r\n',message,'owned_m3u_failure_3'))
 def test_retained_actual_bulk_reply_exact_owned_username(self):
  raw=base64.b64decode('JDcwDQpVc2VyIG93bmVkX20zdV9mYWlsdXJlXzMgaGFzIG5vIHBlcm1pc3Npb25zIHRvIHJ1biB0aGUgJ3NldGV4JyBjb21tYW5kDQo=');wire,value=frame(io.BytesIO(raw))
  self.assertTrue(setex_denied(wire,value,'owned_m3u_failure_3'));self.assertFalse(setex_denied(wire,value,'owned_m3u_failure_4'));self.assertFalse(setex_denied(wire,value,'unowned'))
 def test_wrong_command_and_old_unbound_pronoun_not_accepted(self):
  for message in (b"User owned_m3u_failure_3 has no permissions to run the 'publish' command",b"This user has no permissions to run the 'setex' command"):
   raw=b'$'+str(len(message)).encode()+b'\r\n'+message+b'\r\n';self.assertFalse(setex_denied(raw,message,'owned_m3u_failure_3'))
 def evidence(self):
  raw=base64.b64decode('LUVSUiBBQ0wgZmFpbHVyZSBpbiBzY3JpcHQ6IFVzZXIgb3duZWRfbTN1X2ZhaWx1cmVfMyBoYXMgbm8gcGVybWlzc2lvbnMgdG8gcnVuIHRoZSAnc2V0ZXgnIGNvbW1hbmQgc2NyaXB0OiBmY2U5YjM5YjU4YWQzYzI3MWU3MGIyMWRiOWM0MjhiNDhlMDRiMTY5LCBvbiBAdXNlcl9zY3JpcHQ6MTEuDQo=');row={'mode':'result-denied','script_cache_miss':False,'raw_base64':base64.b64encode(raw).decode(),'reply_sha256':hashlib.sha256(raw).hexdigest(),'reply_bytes':len(raw),'actual_server_reply_observed':True,'script_sha1':'fce9b39b58ad3c271e70b21db9c428b48e04b169'};return{'target_replies':1,'receipts':[row]}
 def test_actual_matching_single_permission_error(self):self.assertTrue(denied_write_observation(self.evidence(),'fce9b39b58ad3c271e70b21db9c428b48e04b169','owned_m3u_failure_3',11,'8.10.3'))
 def test_different_script_or_retried_write_cannot_pass(self):
  self.assertFalse(denied_write_observation(self.evidence(),'a'*40,'owned_m3u_failure_3',11,'8.10.3'));value=self.evidence();value['receipts']*=2;value['target_replies']=2;self.assertFalse(denied_write_observation(value,'fce9b39b58ad3c271e70b21db9c428b48e04b169','owned_m3u_failure_3',11,'8.10.3'))
 def test_noscript_is_not_permission_execution_proof(self):
  value=self.evidence();value['receipts'][0]['script_cache_miss']=True;self.assertFalse(denied_write_observation(value,'fce9b39b58ad3c271e70b21db9c428b48e04b169','owned_m3u_failure_3',11,'8.10.3'))
 def test_damaged_raw_reply_or_other_server_error_rejected(self):
  value=self.evidence();value['receipts'][0]['raw_base64']=base64.b64encode(b'-WRONGTYPE bad\r\n').decode();self.assertFalse(denied_write_observation(value,'fce9b39b58ad3c271e70b21db9c428b48e04b169','owned_m3u_failure_3',11,'8.10.3'))
 def test_exact_owned_user_line_version_and_command_required(self):
  sha='fce9b39b58ad3c271e70b21db9c428b48e04b169'
  for user,line,version in [('owned_m3u_failure_4',11,'8.10.3'),('unowned',11,'8.10.3'),('owned_m3u_failure_3',10,'8.10.3'),('owned_m3u_failure_3',11,'7.0.15')]:self.assertFalse(denied_write_observation(self.evidence(),sha,user,line,version))
 def test_other_command_or_truncated_raw_refused_even_with_recomputed_hash(self):
  sha='fce9b39b58ad3c271e70b21db9c428b48e04b169'
  for transform in (lambda raw:raw.replace(b"'setex'",b"'publish'"),lambda raw:raw[:-1]):
   value=self.evidence();row=value['receipts'][0];raw=transform(base64.b64decode(row['raw_base64']));row.update(raw_base64=base64.b64encode(raw).decode(),reply_sha256=hashlib.sha256(raw).hexdigest(),reply_bytes=len(raw));self.assertFalse(denied_write_observation(value,sha,'owned_m3u_failure_3',11,'8.10.3'))
 def test_resp_encoder_preserves_binary_key_and_nonexecuted_probe(self):self.assertEqual(encode([b'GET',b'a\x00b']),b'*2\r\n$3\r\nGET\r\n$3\r\na\x00b\r\n')
if __name__=='__main__':unittest.main()
