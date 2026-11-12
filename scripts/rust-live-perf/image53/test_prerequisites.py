"""Controls on real public prerequisite projections; no private fallback."""
import copy,json,unittest
from pathlib import Path
from verify_prerequisites import verify

class Controls(unittest.TestCase):
    def test_retained(self):self.assertTrue(verify(Path(__file__).parent)['passed'])
    def test_corruptions(self):
        root=Path(__file__).parent;original=[json.loads((root/n).read_text())for n in ['functional.json','qualification.json']]
        for name in ['functional-missing','qualification-missing','identity','decoder','sampler','Source-native','wrong-image','wrong-protocol','OOM','bad-samples','root-binding','cleanup']:
            with self.subTest(name=name):
                f,q=copy.deepcopy(original)
                if name=='functional-missing':f['report']['phases'].pop()
                if name=='qualification-missing':q['report']['phases'].pop()
                if name=='identity':f['report']['phases'][0]['authenticated_identity']['all_owned_user_id']=False
                if name=='decoder':f['report']['phases'][0]['viewers'][0]['decoder_frames']=0
                if name=='sampler':f['report']['phases'][0]['resources']={}
                if name=='Source-native':q['report']['phases'][0]['backend']='candidate'
                if name=='wrong-image':q['report']['candidate_image']='wrong'
                if name=='wrong-protocol':q['report']['protocol_sha256']='wrong'
                if name=='OOM':q['report']['phases'][0]['resources']['raw']['counters']['before']['backend']['memory_events']['oom']=1
                if name=='bad-samples':q['report']['phases'][0]['resources']['raw']['samples']=[]
                if name=='root-binding':q['raw_report_sha256']='wrong'
                if name=='cleanup':q['Root_review']['remaining']['containers']='not empty'
                with self.assertRaises((AssertionError,ValueError,KeyError,IndexError)):verify(root,f,q)

if __name__=='__main__':unittest.main(verbosity=2)
