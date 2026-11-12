"""Publication-structure corruption controls; no application or Docker execution."""
import copy
import json
import unittest
from pathlib import Path
from verify import verify

class Controls(unittest.TestCase):
    def test_retained_packet(self):
        self.assertTrue(verify(Path(__file__).parent)['verified'])

    def test_corruptions(self):
        root = Path(__file__).parent
        original = json.loads((root/'verified-checkpoint.json').read_text())
        for change in ('omit', 'duplicate', 'guide-pass', 'delete-failure', 'candidate', 'inflate-required', 'performance', 'review-hash', 'clock', 'controller-hash', 'worker-activation', 'Original', 'required-swap', 'times-rewrite'):
            with self.subTest(change=change):
                d = copy.deepcopy(original)
                if change == 'omit': d['attempts'].pop()
                if change == 'duplicate': d['attempts'][-1] = d['attempts'][0]
                if change == 'guide-pass': next(x for x in d['attempts'] if x['name']=='guide-grid-packaged')['actual_returncode']=0
                if change == 'delete-failure': d['attempts']=[x for x in d['attempts'] if x['name']!='guide-grid-packaged']
                if change == 'candidate': d['candidate']['image_id']='sha256:wrong'
                if change == 'inflate-required': d['required_nodes']=29
                if change == 'performance': d['performance_verified']=True
                if change == 'review-hash': d['attempts'][0]['review_sha256']='0'*64
                if change == 'clock': d['attempts'][0]['completed_epoch']=0
                if change == 'controller-hash': d['attempts'][0]['controller_result']['sha256']='0'*64
                if change == 'worker-activation': d['worker_graph_activation']=True
                if change == 'Original': d['Original_revision']='wrong'
                if change == 'required-swap': d['attempts'][0]['required_node'],d['attempts'][1]['required_node']=d['attempts'][1]['required_node'],d['attempts'][0]['required_node']
                if change == 'times-rewrite': d['attempts'][0]['started_epoch']=1;d['attempts'][0]['completed_epoch']=2
                with self.assertRaises((AssertionError,KeyError,ValueError)): verify(root,d)

if __name__=='__main__': unittest.main()
