"""Pure verifier controls from preserved public actual Source dataset.
No Source/native execution or acceptance follows synthetic candidate payloads.
"""
import copy,hashlib,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unified_packaged_probe import probe

class PackagedVerificationControls(unittest.TestCase):
    def fixture(self):
        source=json.loads(Path(__file__).with_name('unified-source-3.json').read_text())['modes']['http']
        source['database_witness']={'actual_database':'dispatcharr','configured_database':'dispatcharr'}
        # Explicit synthetic HEAD projections for verifier controls only; the
        # actual189-observation Source/native network proof is separate.
        for case in list(source['records']):
            if case['path'].endswith('format=api'):
                head=copy.deepcopy(case);head['method']='HEAD';head['label']='Source format-api JSON HEAD negotiation';head['expected']['body_bytes']=0;head['expected']['body_sha256']=hashlib.sha256(b'').hexdigest();head['expected'].pop('json',None);source['records'].append(head)
        candidate=copy.deepcopy(source);candidate['target']='candidate'
        for case in candidate['records']:
            if case['headers']['Accept']=='text/html':
                case['expected']['status']=501
            if case['label']=='cross-kind LOWER(name),id tie':
                raw=json.dumps(case['expected']['json'],ensure_ascii=False,separators=(',',':'))
                assert hashlib.sha256(raw.encode()).hexdigest()==case['expected']['body_sha256']
                case['fresh_Original_HTTP']=copy.deepcopy(case['expected']);case['fresh_Original_raw_body']=raw
        return source,candidate
    def verify(self,source,candidate):
        def command(*args,**kwargs):return SimpleNamespace(stdout=json.dumps(candidate)+'\n',stderr='')
        with tempfile.TemporaryDirectory()as directory:return probe(command,[],Path(directory),source)
    def test_same_bodies_with_explicit_not_passed_HTML(self):
        source,candidate=self.fixture();result=self.verify(source,candidate);self.assertTrue(result['passed']);self.assertEqual(result['compared'],188);self.assertTrue(all(not r['parity_pass']for r in result['explicit_browsable_HTML_not_parity_pass']))
    def test_one_wrong_actual_raw_body_is_rejected(self):
        source,candidate=self.fixture();candidate['records'][1]['expected']['body_sha256']='0'*64;result=self.verify(source,candidate);self.assertFalse(result['passed']);self.assertEqual(len(result['differences']),1)
    def test_not_501_unsupported_HTML_is_not_hidden(self):
        source,candidate=self.fixture();row=next(r for r in candidate['records']if r['headers']['Accept']=='text/html');row['expected']['status']=200;self.assertFalse(self.verify(source,candidate)['passed'])
    def test_wrong_post_phase_metadata_fingerprint_is_rejected(self):
        source,candidate=self.fixture();candidate['fingerprints_after']['core_coresettings']['sha256']='0'*64;self.assertFalse(self.verify(source,candidate)['passed'])
    def test_fresh_Source_authority_cannot_change_nonorder_fields(self):
        source,candidate=self.fixture();case=next(r for r in candidate['records']if r['label']=='cross-kind LOWER(name),id tie');case['fresh_Original_HTTP']['json']['results'][0]['year']=1234;raw=json.dumps(case['fresh_Original_HTTP']['json'],ensure_ascii=False,separators=(',',':'));case['fresh_Original_raw_body']=raw;case['fresh_Original_HTTP']['body_sha256']=hashlib.sha256(raw.encode()).hexdigest()
        with self.assertRaises(AssertionError):self.verify(source,candidate)
if __name__=='__main__':unittest.main()
