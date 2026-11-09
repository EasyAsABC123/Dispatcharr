import copy
import json
import unittest

import crud_probes
from test_probes import ACCOUNTS


class CrudProbeTests(unittest.TestCase):
    def test_only_created_root_id_is_normalized(self):
        value={'id':12,'username':'unchanged','channel_profiles':[12],
               'custom_properties':{'id':12},'permissions':[12]}
        before=copy.deepcopy(value)
        result=crud_probes.normalized_created(value,12)
        self.assertEqual(value,before)
        self.assertEqual(result['id'],'<created-object-id>')
        self.assertEqual(result['channel_profiles'],[12])
        self.assertEqual(result['custom_properties'],{'id':12})
        self.assertEqual(result['permissions'],[12])

    def test_unsafe_target_never_issues_http_requests(self):
        calls=[];records=[]
        accounts=copy.deepcopy(ACCOUNTS)
        accounts['viewer']['username']='actual-user'
        crud_probes.run(lambda *args,**kwargs:calls.append((args,kwargs)),
                        'http://127.0.0.1:1','http://127.0.0.1:2',accounts,
                        lambda *args:records.append(args))
        self.assertEqual(calls,[])
        self.assertFalse(records[0][1])

    def test_fixture_failure_records_exception_type_not_secret_text(self):
        records=[]
        def broken(*args,**kwargs):
            raise RuntimeError('secret-admin-access secret-viewer-access secret-password')
        crud_probes.run(broken,'http://127.0.0.1:1','http://127.0.0.1:2',ACCOUNTS,
                        lambda *args:records.append(args))
        rendered=json.dumps(records)
        for secret in ('secret-admin-access','secret-viewer-access','secret-password'):
            self.assertNotIn(secret,rendered)
        self.assertTrue(any(row[2].get('error_type')=='RuntimeError' for row in records))
        self.assertTrue(any(not row[1] for row in records))


if __name__=='__main__':
    unittest.main()
