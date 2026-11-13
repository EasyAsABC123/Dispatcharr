"""Socket-free parser bounds only; does not qualify proxy threads/network."""
import io,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'tools'))
from pg_observer import exact
class ObserverBounds(unittest.TestCase):
 def test_literal_bytes_are_not_reencoded(self):self.assertEqual(exact(io.BytesIO(b'\x00\xffZ'),3),b'\x00\xffZ')
 def test_short_frame_is_not_zero_padding(self):
  with self.assertRaises(EOFError):exact(io.BytesIO(b'abc'),4)
 def test_negative_length_refused_before_read(self):
  with self.assertRaises(ValueError):exact(io.BytesIO(b'abc'),-1)
 def test_overcap_refused_before_allocation(self):
  with self.assertRaises(ValueError):exact(io.BytesIO(b''),2097153)
if __name__=='__main__':unittest.main()
