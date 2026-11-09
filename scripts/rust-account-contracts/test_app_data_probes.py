import copy
import json
import unittest

import app_data_probes
from test_probes import ACCOUNTS


class AppDataProbeTests(unittest.TestCase):
    def test_unsafe_fixture_is_rejected_without_mutation(self):
        accounts=copy.deepcopy(ACCOUNTS);accounts['viewer']['username']='real-person'
        calls=[];records=[]
        app_data_probes.run(lambda *args,**kwargs:calls.append((args,kwargs)),
                            'http://127.0.0.1:1','http://127.0.0.1:2',accounts,
                            lambda *args:records.append(args))
        self.assertEqual(calls,[]);self.assertFalse(records[0][1])

    def test_unavailable_reference_does_not_become_skipped_success_or_leak_secrets(self):
        records=[]
        def broken(*args,**kwargs):raise RuntimeError('secret-viewer-access secret-password')
        app_data_probes.run(broken,'http://127.0.0.1:1','http://127.0.0.1:2',ACCOUNTS,
                            lambda *args:records.append(args))
        self.assertTrue(any(not row[1] for row in records))
        self.assertNotIn('secret-viewer-access',json.dumps(records));self.assertNotIn('secret-password',json.dumps(records))


if __name__=='__main__':unittest.main()
