"""Actual runtime-prefix contract; observer bytes must remain untouched."""
import base64,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from compare_full import native_owner_token
class Controls(unittest.TestCase):
 def test_actual_frozen_runtime_prefix_and_raw_input_preserved(self):
  raw=base64.b64encode(b'rust:ad36a450-4260-4b02-a8d9-2e0ee833c8cf').decode();before=raw
  self.assertTrue(native_owner_token(raw));self.assertEqual(raw,before)
 def test_wrong_or_missing_prefix_refused(self):
  for value in (b'ad36a450-4260-4b02-a8d9-2e0ee833c8cf',b'other:ad36a450-4260-4b02-a8d9-2e0ee833c8cf',b'Rust:ad36a450-4260-4b02-a8d9-2e0ee833c8cf'):
   with self.subTest(value=value):self.assertFalse(native_owner_token(base64.b64encode(value).decode()))
 def test_noncanonical_uuid_refused(self):
  for value in (b'rust:AD36A450-4260-4B02-A8D9-2E0EE833C8CF',b'rust:ad36a45042604b02a8d92e0ee833c8cf',b'rust:{ad36a450-4260-4b02-a8d9-2e0ee833c8cf}',b'rust:ad36a450-4260-4b02-a8d9-2e0ee833c8cf\n'):
   with self.subTest(value=value):self.assertFalse(native_owner_token(base64.b64encode(value).decode()))
 def test_wrong_version_variant_and_malformed_bytes_refused(self):
  for value in (b'rust:ad36a450-4260-1b02-a8d9-2e0ee833c8cf',b'rust:ad36a450-4260-4b02-08d9-2e0ee833c8cf',b'rust:not-a-uuid',b'\xff'):
   with self.subTest(value=value):self.assertFalse(native_owner_token(base64.b64encode(value).decode()))
  self.assertFalse(native_owner_token('not-base64'))
if __name__=='__main__':unittest.main()
