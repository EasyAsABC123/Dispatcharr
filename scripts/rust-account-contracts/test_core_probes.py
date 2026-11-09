import json
import unittest
from urllib.parse import urlsplit
import core_probes


class CoreProbeTests(unittest.TestCase):
    def execute(self, mismatch=False, raises=False):
        records=[]
        accounts={role:{'access':role} for role in ('admin','standard','viewer','inactive')}
        def http(url,body=None,token=None,method=None,headers=None):
            parsed=urlsplit(url)
            if raises and parsed.port==2:
                raise RuntimeError('sensitive-payload-must-not-escape')
            if token is None or token=='inactive': return 401,{'detail':'Authentication failed'}
            if token=='viewer':return 403,{'detail':'Permission denied'}
            if parsed.path.rstrip('/').split('/')[-1] in ('2147483647','not-a-number'):
                return 404,{'detail':'Missing'}
            value={'id':7,'value':True if mismatch and parsed.port==2 else 1}
            return (200,[value]) if parsed.path.count('/')==4 else (200,value)
        core_probes.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',accounts,
                        lambda *record:records.append(record))
        return records

    def test_permission_matrix_and_details_are_exercised(self):
        records=self.execute()
        self.assertEqual(len(records),32)
        self.assertTrue(all(record[1] for record in records))

    def test_type_differences_cannot_pass_via_python_equality(self):
        self.assertTrue(any(not record[1] for record in self.execute(mismatch=True)))

    def test_failure_diagnostics_do_not_leak_exception_payload(self):
        records=self.execute(raises=True)
        self.assertTrue(all(not record[1] for record in records))
        self.assertNotIn('sensitive-payload',json.dumps(records))

if __name__=='__main__':unittest.main()
