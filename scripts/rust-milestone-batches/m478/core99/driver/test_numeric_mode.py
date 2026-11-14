"""Offline explicit-mode refusal controls; synthetic metadata only."""
import datetime,json,unittest
from gpg_trace import validate

def fixture(finite):
    rows=[]
    for pid,role,code in ([(1,'import',0),(2,'verify',1)]if finite else[(1,'import',0)]):
        common={'pid':pid,'start_generation':str(100+pid),'pid_namespace':'pid:[123]','role':role,'home_id':'owned-home'}
        rows += [{'target':'dispatcharr::plugin_gpg','epoch_seconds':1.0,'fields':{**common,'event':'spawn'}},{'target':'dispatcharr::plugin_gpg','epoch_seconds':2.0,'fields':{**common,'event':'joined','stdin_complete':True,'stdout_eof':True,'stderr_eof':True,'wait_observed':True,'stdout_bytes':0,'stderr_bytes':1,'wait_exit_code':code,'wait_signal':None}}]
    rows += [{'target':'dispatcharr::plugin_gpg','epoch_seconds':3.0,'fields':{'event':'home_drop','home_id':'owned-home','remove_ok':True,'home_absent':True}}]
    native={'capture_complete':True,'actual_request_count':0,'completed_capture_count':0,'requests':[],'numeric_diagnostic':{'began_utc':'1970-01-01T00:00:00+00:00','completed_utc':'1970-01-01T00:00:04+00:00','status':200 if finite else 501,'json':{'signature_verified':False}}}
    return {'requests':[]},native,('\n'.join(json.dumps(r)for r in rows)+'\n').encode()

class ExplicitMode(unittest.TestCase):
    def test_legacy_only_accepts_guard(self):
        self.assertTrue(validate(*fixture(False),numeric_mode='legacyguard-v1')['passed'])
        with self.assertRaises(ValueError):validate(*fixture(True),numeric_mode='legacyguard-v1')
    def test_finite_only_accepts_real_stale_verify(self):
        self.assertTrue(validate(*fixture(True),numeric_mode='finite-v1')['passed'])
        with self.assertRaises(ValueError):validate(*fixture(False),numeric_mode='finite-v1')
    def test_finite_cannot_accept_stale_signature_true(self):
        source,native,raw=fixture(True);native['numeric_diagnostic']['json']['signature_verified']=True
        with self.assertRaises(ValueError):validate(source,native,raw,numeric_mode='finite-v1')
    def test_unknown_mode_refused(self):
        with self.assertRaises(ValueError):validate(*fixture(True),numeric_mode='anything')
if __name__=='__main__':unittest.main()
