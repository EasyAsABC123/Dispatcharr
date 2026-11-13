"""Actual owned-Docker spelling; strict wrong-object/error negatives, no Docker."""
import sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from run_pair import exact_absent_cid
CID='3c76c78631fb4e17759796169e889538d3a2e4fe9842b811949bf27916e8d60f'
class Controls(unittest.TestCase):
 def test_actual_lowercase_absence_accepted(self):self.assertTrue(exact_absent_cid(1,b'[]\n',('error: no such object: '+CID+'\n').encode(),CID))
 def test_older_case_variant_exact_message_accepted(self):self.assertTrue(exact_absent_cid(1,b'',('Error: No such object: '+CID+'\n').encode(),CID))
 def test_wrong_cid_never_empty(self):self.assertFalse(exact_absent_cid(1,b'[]\n',('error: no such object: '+'a'*64+'\n').encode(),CID))
 def test_other_errors_never_empty(self):
  for text in ('permission denied','Cannot connect to the Docker daemon','error: no such object: '+CID+'; permission denied','error: no such object: '+CID+'\nother error'):
   self.assertFalse(exact_absent_cid(1,b'[]\n',text.encode(),CID))
 def test_success_or_present_output_not_missing(self):
  self.assertFalse(exact_absent_cid(0,b'[]\n',('error: no such object: '+CID).encode(),CID));self.assertFalse(exact_absent_cid(1,b'[{"Id":"present"}]',('error: no such object: '+CID).encode(),CID));self.assertFalse(exact_absent_cid(True,b'[]',('error: no such object: '+CID).encode(),CID))
 def test_nonutf8_unknown_or_invalid_cid_refused(self):
  self.assertFalse(exact_absent_cid(1,b'[]',b'\xff',CID));self.assertFalse(exact_absent_cid(1,b'[]',('error: no such object: '+CID).encode(),'short'));self.assertFalse(exact_absent_cid(1,b'[]',b'error',None))
if __name__=='__main__':unittest.main()
