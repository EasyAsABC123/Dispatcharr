"""Redaction and typed hash-witness checks; real HTTP proof is separate."""
import json
import unittest
import auth_hash_probes
from test_startup_probes import signed
class AuthHashProbeTests(unittest.TestCase):
    def execute(self,broken=False,mismatch=False):
        rows=[];removed=[];inspections={}
        def fixture(data):
            action=data['action']
            if action=='auth_package_prepare':return {'id':7}
            if action=='auth_package_reset':return {'reset':True}
            if action=='auth_package_cleanup':removed.append(data['kind']);return {'removed':True}
            inspections[data['kind']]=inspections.get(data['kind'],0)+1
            # Bool/int must not compare equal through Python's loose equality.
            return {'changed':1 if mismatch and inspections[data['kind']]==2 else True}
        def http(url,body=None,**kwargs):
            if broken:raise RuntimeError('must-not-emit-password-token')
            if body['username'].endswith(('-inactive','-wrong')):return 401,{'detail':'Fixture failure'}
            return 200,{'access':signed('access'),'refresh':signed('refresh')}
        auth_hash_probes.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',{},lambda *row:rows.append(row),fixture)
        return rows,removed
    def test_outcome_witness_type_difference_fails(self):
        rows,removed=self.execute(mismatch=True)
        self.assertEqual(len(removed),6)
        self.assertTrue(all(not passed for name,passed,_ in rows if ':cleanup:' not in name))
    def test_valid_signature_normalization_and_cleanup(self):
        rows,removed=self.execute()
        self.assertEqual(len(rows),12)
        self.assertEqual(len(removed),6)
        self.assertTrue(all(row[1] for row in rows))
    def test_failure_never_captures_credentials_or_exception_message(self):
        rows,removed=self.execute(broken=True)
        self.assertEqual(len(removed),6)
        rendered=json.dumps(rows)
        self.assertNotIn('must-not-emit',rendered)
        self.assertTrue(all(details=={'error_type':'RuntimeError'} for name,_,details in rows if ':cleanup:' not in name))
if __name__=='__main__':unittest.main()
