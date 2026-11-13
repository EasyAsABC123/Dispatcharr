"""Pure reconstruction refusal controls; no Cargo, Docker or private fallback."""
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest import mock
import reconstruct

class ReconstructionControls(unittest.TestCase):
    def tar(self,names):
        out=io.BytesIO()
        with tarfile.open(fileobj=out,mode='w') as archive:
            for name,kind in names:
                item=tarfile.TarInfo(name)
                if kind=='link':item.type=tarfile.SYMTYPE;item.linkname='/tmp/escape';archive.addfile(item)
                else:item.size=1;archive.addfile(item,io.BytesIO(b'x'))
        return out.getvalue()
    def test_archive_rejects_traversal_absolute_links_and_duplicates(self):
        for names in [[('../escape','file')],[('/escape','file')],[('link','link')],[('same','file'),('same','file')]]:
            with self.subTest(names=names):
                with self.assertRaises(ValueError):reconstruct.contents(self.tar(names))
    def test_fresh_output_and_relative_path_checks(self):
        with tempfile.TemporaryDirectory() as name:
            root=Path(name)
            with self.assertRaises(ValueError):reconstruct.write_tree(root,{'safe':b'x'})
            with self.assertRaises(ValueError):reconstruct.write_tree(root/'fresh',{'../escape':b'x'})
    def test_incomplete_base_is_refused_before_materialization(self):
        with tempfile.TemporaryDirectory() as name:
            output=Path(name)/'fresh'
            with mock.patch.object(reconstruct,'git',return_value=self.tar([('only-one','file')])):
                with self.assertRaises(ValueError):reconstruct.reconstruct(Path('.'),output,'prototype')
            self.assertFalse(output.exists())
    def test_wrong_original_and_support_archives_do_not_weaken_pins(self):
        for mode in ['original','support']:
            with tempfile.TemporaryDirectory() as name:
                output=Path(name)/'fresh'
                with mock.patch.object(reconstruct,'git',return_value=b'wrong archive'):
                    with self.assertRaises(ValueError):reconstruct.reconstruct(Path('.'),output,mode)
                self.assertFalse(output.exists())
    def test_manifest_corruption_is_rejected_before_git(self):
        original=Path.read_bytes
        def changed(path):
            return b'{}' if path==reconstruct.HERE/'prototype-inputs.json' else original(path)
        with mock.patch.object(Path,'read_bytes',changed),mock.patch.object(reconstruct,'git') as git:
            with self.assertRaises(ValueError):reconstruct.reconstruct(Path('.'),Path('unused'),'prototype')
            git.assert_not_called()

if __name__=='__main__':unittest.main(verbosity=2)
