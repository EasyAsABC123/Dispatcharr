"""Public wrapper integrity controls against real published data; no private inputs."""
import copy,json,tempfile,unittest
from pathlib import Path
from verify_projection import verify

class Projection(unittest.TestCase):
    def test_retained(self):self.assertTrue(verify(Path(__file__).with_name('projection.json'))['passed'])
    def test_changes_reject(self):
        original=json.loads(Path(__file__).with_name('projection.json').read_text())
        for label in ['summary','phase','ratio','candidate','protocol','decoder']:
            with self.subTest(label=label),tempfile.TemporaryDirectory()as directory:
                data=copy.deepcopy(original)
                if label=='summary':data['verified']['summaries'][0]['median_paired_CPU_reduction_percent']=0
                if label=='phase':data['report']['phases'].pop()
                if label=='ratio':data['report']['pairs'][0]['ratios']['memory']=0
                if label=='candidate':data['report']['candidate_image']='wrong'
                if label=='protocol':data['protocol']['budgets']['max_CPU_ratio']=1
                if label=='decoder':data['report']['phases'][0]['viewers'][0]['decoder_exit_code']=1
                p=Path(directory)/'projection.json';p.write_text(json.dumps(data))
                with self.assertRaises((ValueError,KeyError,IndexError)):verify(p)

if __name__=='__main__':unittest.main(verbosity=2)
