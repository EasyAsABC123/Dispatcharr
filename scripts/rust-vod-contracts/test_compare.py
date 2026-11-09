import unittest
from compare import compare
class Comparison(unittest.TestCase):
    def test_allow_is_a_set_but_media_and_redirect_bytes_are_exact(self):
        source={'events':[{'case':'range','status':206,'bytes':3,'body_sha256':'same','headers':{'allow':'OPTIONS, GET'},'mint':{'headers':{'allow':'OPTIONS, GET','location':'/source'}}}]}
        import copy
        candidate=copy.deepcopy(source)
        candidate['events'][0]['headers']['allow']='GET, OPTIONS'
        candidate['events'][0]['mint']['headers']['allow']='GET, OPTIONS'
        self.assertTrue(compare(source,candidate)['captured_wire_matches'])
        candidate['events'][0]['mint']['headers']['location']='/wrong'
        self.assertFalse(compare(source,candidate)['captured_wire_matches'])
        candidate=copy.deepcopy(source)
        candidate['events'][0]['body_sha256']='different'
        self.assertFalse(compare(source,candidate)['captured_wire_matches'])
if __name__=='__main__':unittest.main()
