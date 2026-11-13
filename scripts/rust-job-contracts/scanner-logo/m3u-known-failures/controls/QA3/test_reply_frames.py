"""UNEXECUTED real RESP2 parser corruption controls; no socket/application work."""
import io,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from owned_redis_reply_proxy import frame
class Controls(unittest.TestCase):
 def test_exact_nested_reply_bytes_preserved(self):
  raw=b'*2\r\n:1\r\n$3\r\nabc\r\n';self.assertEqual(frame(io.BytesIO(raw)),(raw,[1,b'abc']))
 def test_server_error_is_not_a_success_reply(self):
  raw=b'-NOPERM owned failure\r\n';self.assertEqual(frame(io.BytesIO(raw)),(raw,b'NOPERM owned failure'))
 def test_null_different_from_empty_bulk(self):
  self.assertIsNone(frame(io.BytesIO(b'$-1\r\n'))[1]);self.assertEqual(frame(io.BytesIO(b'$0\r\n\r\n'))[1],b'')
 def test_truncated_bulk_rejected(self):
  with self.assertRaisesRegex(ValueError,'RESPShortBulk'):frame(io.BytesIO(b'$4\r\nabc'))
 def test_oversize_declared_bulk_rejected_before_body_read(self):
  with self.assertRaisesRegex(ValueError,'RESPBulkCapacity'):frame(io.BytesIO(b'$1048576\r\n'))
 def test_excess_array_count_rejected(self):
  with self.assertRaisesRegex(ValueError,'RESPArrayCapacity'):frame(io.BytesIO(b'*257\r\n'))
 def test_unknown_protocol_rejected(self):
  with self.assertRaisesRegex(ValueError,'RESP2Expected'):frame(io.BytesIO(b'%0\r\n'))
if __name__=='__main__':unittest.main()
