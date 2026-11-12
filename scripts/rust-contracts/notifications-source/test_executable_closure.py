"""Pure/local input reconstruction controls; no Docker or Source app runs."""
import json,os,shutil,subprocess,sys,tempfile,unittest
from pathlib import Path
import materialize_inputs as closure
class ExecutableInputControls(unittest.TestCase):
    def test_all28_public_inputs_are_exact(self):self.assertEqual(len(closure.validate()['files']),29)
    def test_existing_destination_never_overwritten(self):
        with tempfile.TemporaryDirectory()as folder:
            with self.assertRaisesRegex(ValueError,'NewOwnedMaterializationRequired'):closure.materialize(Path(folder),Path(folder))
    def test_missing_proposal_blob_fails_closed(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'packet';shutil.copytree(closure.HERE,root,ignore=shutil.ignore_patterns('__pycache__'));(root/'executed/inputs/proposal-plan.json').unlink()
            with self.assertRaisesRegex(ValueError,'ExactPublicExecutedInput'):closure.validate(root)
    def test_changed_nonexecuted_proposal_fails_closed(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'packet';shutil.copytree(closure.HERE,root,ignore=shutil.ignore_patterns('__pycache__'));file=root/'executed/inputs/proposal-PLAN.md';file.write_bytes(file.read_bytes()+b' ')
            with self.assertRaisesRegex(ValueError,'ExactPublicExecutedInput'):closure.validate(root)
    def test_original_controller_input_bytes_cannot_be_corrected(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'packet';shutil.copytree(closure.HERE,root,ignore=shutil.ignore_patterns('__pycache__'));file=root/'executed/controller/run.py';file.write_bytes(file.read_bytes().replace(b"b'No such object'",b"b'no such object'"))
            with self.assertRaisesRegex(ValueError,'ExactPublicExecutedInput'):closure.validate(root)
    def test_negative_scope_cannot_be_relabelled(self):
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'packet';shutil.copytree(closure.HERE,root,ignore=shutil.ignore_patterns('__pycache__'));file=root/'executable-closure.json';value=json.loads(file.read_text());value['Source_controller_exit_preserved']=0;file.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError,'ClosureScope'):closure.validate(root)
    def test_exact_archive_materialization_and_original_default_readonly_check(self):
        repo=Path(subprocess.check_output(['git','rev-parse','--show-toplevel'],cwd=closure.HERE,text=True).strip())
        with tempfile.TemporaryDirectory()as folder:
            root=Path(folder)/'exact-inputs';result=closure.materialize(repo,root);self.assertEqual(result['exact_materialized_inputs'],29);self.assertEqual(result['archive_sha256'],closure.ARCHIVE);self.assertFalse(result['Source_application_or_request_executed'])
            output=subprocess.check_output([sys.executable,str(root/'controller/run.py')],env=dict(os.environ,PYTHONDONTWRITEBYTECODE='1',DOCKER_HOST='unix:///must-not-be-used'),text=True,timeout=10);actual=json.loads(output);self.assertEqual(actual['seal_sha256'],closure.SEAL);self.assertFalse(actual['Source_executed']);self.assertEqual(actual['requests_executed'],0)
if __name__=='__main__':unittest.main()
