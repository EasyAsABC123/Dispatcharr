"""Notification oracle redaction and contradictory-cache reset checks."""
import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('notification_probes',Path(__file__).with_name('notification_probes.py'))
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
NAMES=['public','admin','inactive','expired','future','developer-empty','developer-known','developer-unknown']
class NotificationProbeTests(unittest.TestCase):
    def accounts(self):return {role:{'access':'secret-token','id':index+1,'username':'rust-probe-'+role} for index,role in enumerate(['admin','standard','viewer','inactive'])}
    def test_transport_failures_are_visible_without_sensitive_exception_text(self):
        records=[]
        def fixture(data):return {'ids':dict(zip(NAMES,range(1,9)))} if data['action']=='notification_prepare' else {}
        def http(*args,**kwargs):raise RuntimeError('secret-token response contents')
        module.run(http,'http://127.0.0.1:1','http://127.0.0.1:2',self.accounts(),lambda *args:records.append(args),fixture)
        self.assertGreater(len(records),40);self.assertTrue(all(not row[1] for row in records));self.assertNotIn('secret-token',repr(records));self.assertNotIn('response contents',repr(records))
    def test_each_contradictory_cache_mode_is_reset_for_both_backends(self):
        modes=[]
        def fixture(data):
            if data['action']=='notification_prepare':return {'ids':dict(zip(NAMES,range(1,9)))}
            if data['action']=='notification_reset':modes.append((data.get('network_allow'),data.get('cached_condition')))
            if data['action']=='notification_state':return {'condition_cache_bool':False,'dismissal_count':0,'actions_sha256':'hash','timestamps_sha256':'hash'}
            return {}
        module.run(lambda *args,**kwargs:(200,{}),'http://127.0.0.1:1','http://127.0.0.1:2',self.accounts(),lambda *args:None,fixture)
        for mode in [(True,None),(False,True),(True,False),('malformed',None)]:self.assertEqual(modes.count(mode),2)
if __name__=='__main__':unittest.main()
