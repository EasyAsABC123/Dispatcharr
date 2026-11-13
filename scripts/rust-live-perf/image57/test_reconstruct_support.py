"""Finite Git-only reconstruction negatives; never launches an application."""
import copy,json,os,shutil,tempfile,unittest
from pathlib import Path
import reconstruct_support as r
HERE=Path(__file__).resolve().parent
class Reconstruct(unittest.TestCase):
    def test_corruptions(self):
        repo=Path(os.environ.get('RECONSTRUCTION_REPO',Path.cwd())).resolve()
        original=json.loads((HERE/'execution-inputs.json').read_text())
        cases=['revision','archive','support-missing','unknown-overlay','overlay-digest','existing-output']
        for label in cases:
            with self.subTest(label=label),tempfile.TemporaryDirectory()as tmp:
                root=Path(tmp);data=copy.deepcopy(original)
                if label=='revision':data['support_revision']='wrong'
                if label=='archive':data['support_archive_sha256']='wrong'
                if label=='support-missing':data['support_sha256'].pop(next(iter(data['support_sha256'])))
                if label=='unknown-overlay':data['actual_executed_tools_sha256']['unowned/file']='0'*64
                if label=='overlay-digest':data['actual_executed_tools_sha256']['scripts/rust-live-perf/run.py']='0'*64
                (root/'execution-inputs.json').write_text(json.dumps(data))
                for name in ['run.py','resources.py','acceptance.py','controller_contract.py','test_resources.py','test_acceptance.py','test_controller_contract.py','test_verify.py','verify.py']:
                    shutil.copyfile(HERE/name,root/name)
                if label=='existing-output':(root/'out').mkdir()
                previous=r.HERE;r.HERE=root
                try:
                    with self.assertRaises(ValueError):r.reconstruct(repo,root/'out')
                finally:r.HERE=previous
                if label!='existing-output':self.assertFalse((root/'out').exists())
if __name__=='__main__':unittest.main(verbosity=2)
