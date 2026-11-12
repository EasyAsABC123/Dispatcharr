"""Finite component publication integrity checks; no application or private data."""
from pathlib import Path
import json,shutil,tempfile,unittest
import verify

class Controls(unittest.TestCase):
    def test_positive(self):self.assertTrue(verify.verify(Path(__file__).parent)['passed'])
    def test_reject(self):
        for mode in ['missing-batch','bad-count','bool-count','false-build','Main','image','performance','cleanup']:
            with self.subTest(mode=mode),tempfile.TemporaryDirectory()as directory:
                root=Path(directory)/'component';shutil.copytree(Path(__file__).parent,root);p=root/'verified.json';d=json.loads(p.read_text())
                if mode=='missing-batch':d['real_batches'].pop()
                if mode=='bad-count':d['real_batches'][0]['passed_count']=0
                if mode=='bool-count':d['real_batches'][2]['passed_count']=True
                if mode=='false-build':d['non_test_build_passed']=False
                if mode=='Main':d['Main_validated']=True
                if mode=='image':d['new_application_image_validated']=True
                if mode=='performance':d['performance_accepted']=True
                if mode=='cleanup':d['independent_cleanup_sha256']='0'*64
                p.write_text(json.dumps(d))
                with self.assertRaises((AssertionError,ValueError,KeyError)):verify.verify(root)

if __name__=='__main__':unittest.main(verbosity=2)
