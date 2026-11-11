import io,tarfile,tempfile,unittest
from pathlib import Path
from build_native import extract,freeze
class BuildGuards(unittest.TestCase):
 def test_traversal_archive_refused(self):
  data=io.BytesIO()
  with tarfile.open(fileobj=data,mode='w')as t:
   info=tarfile.TarInfo('../outside');info.size=1;t.addfile(info,io.BytesIO(b'x'))
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):extract(data.getvalue(),Path(d))
 def test_symlink_archive_refused(self):
  data=io.BytesIO()
  with tarfile.open(fileobj=data,mode='w')as t:
   info=tarfile.TarInfo('rust/link');info.type=tarfile.SYMTYPE;info.linkname='/etc';t.addfile(info)
  with tempfile.TemporaryDirectory()as d:
   with self.assertRaises(ValueError):extract(data.getvalue(),Path(d))
 def test_nonempty_output_preserved(self):
  with tempfile.TemporaryDirectory()as d:
   p=Path(d);(p/'keep').write_text('retained')
   with self.assertRaises(ValueError):freeze(Path.cwd(),'HEAD',p)
   self.assertEqual((p/'keep').read_text(),'retained')
if __name__=='__main__':unittest.main()
