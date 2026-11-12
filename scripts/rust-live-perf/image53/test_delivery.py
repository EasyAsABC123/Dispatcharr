"""Meaningful publication input/privacy controls on retained public observations."""
import copy,json,unittest
from pathlib import Path
from verify_delivery import verify,privacy
HERE=Path(__file__).parent
class Delivery(unittest.TestCase):
    def test_retained(self):self.assertTrue(verify()['passed'])
    def test_corruptions(self):
        cases=[('application-inputs.json',lambda d:d.update(candidate_image='wrong')),('application-inputs.json',lambda d:d.update(git_revision='wrong')),('application-inputs.json',lambda d:d.update(Rust_input_count=792)),('application-inputs.json',lambda d:d['source_sha256'].pop('LICENSE')),('execution-inputs.json',lambda d:d.update(support_revision='wrong')),('execution-inputs.json',lambda d:d['actual_executed_tools_sha256'].pop('scripts/rust-live-perf/run.py')),('parent-review.json',lambda d:d.update(resource_samples_raw=1062)),('parent-review.json',lambda d:d.update(gain_transfer_to_image54_or_792_input_runtime=True)),('cue-claim-chain.json',lambda d:d['stages'].pop()),('plan.json',lambda d:d.update(worker_graph_activation=True)),('plan.json',lambda d:d.update(resource_phase_seconds=[15,61]))]
        for name,change in cases:
            with self.subTest(name=name,change=change):
                d=json.loads((HERE/name).read_text());change(d)
                with self.assertRaises((AssertionError,KeyError,ValueError)):verify(overrides={name:d})
    def test_privacy(self):
        for d in [{'boot_id':'private'},{'nested':{'password':'secret'}},{'nested':'Bearer secret'},{'nested':('eyJ'+'abcdefghijk')+'.abcdefghijk.abcdefghijk'},{'argv':['private command']}]:
            with self.subTest(d=d),self.assertRaises(AssertionError):privacy(d)
if __name__=='__main__':unittest.main(verbosity=2)
