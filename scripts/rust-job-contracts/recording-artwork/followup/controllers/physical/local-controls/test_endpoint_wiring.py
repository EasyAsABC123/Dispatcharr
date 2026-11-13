import ast,os,sys,unittest,urllib.parse
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];tree=ast.parse((ROOT/'tools/source_configured.py').read_text());fn=next(x for x in tree.body if isinstance(x,ast.FunctionDef)and x.name=='physical_endpoint_config');ns={'os':os};exec(compile(ast.Module(body=[fn],type_ignores=[]),'actual_guest_endpoints','exec'),ns)
class Endpoints(unittest.TestCase):
 def setUp(self):self.base={'broker_url':'redis://:playback-fixture-only@127.0.0.1:6379/8','result_url':'redis://:playback-fixture-only@127.0.0.1:6379/9','events_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/rust_probe_backup_jobs_owned'}
 def apply(self,mode):
  with patch.dict(os.environ,POSTGRES_DB='rust_probe_backup_jobs_owned'):return ns['physical_endpoint_config'](mode,self.base)
 def test_result_proxy_same_actual_address_distinct_DBs(self):
  x=self.apply('result-reply');a,b=[urllib.parse.urlsplit(x[k])for k in ['broker_url','result_url']];self.assertEqual((a.hostname,a.port),(b.hostname,b.port));self.assertEqual(a.port,61603);self.assertEqual([a.path,b.path],['/8','/9']);self.assertEqual(x['events_url'],self.base['events_url'])
 def test_Channels_proxy_only_event_DB0(self):
  x=self.apply('Channels-reply');a=urllib.parse.urlsplit(x['events_url']);self.assertEqual((a.port,a.path),(61603,'/0'));self.assertEqual(x['broker_url'],self.base['broker_url']);self.assertEqual(x['result_url'],self.base['result_url'])
 def test_PG_proxies_preserve_owned_database_andRedis(self):
  for mode in ['recording-read-Ready','update-Ready','update-error-Ready','fresh-status-merge']:
   x=self.apply(mode);a=urllib.parse.urlsplit(x['database_url']);self.assertEqual((a.port,a.path),(61651,'/rust_probe_backup_jobs_owned'));self.assertEqual(x['broker_url'],self.base['broker_url'])
 def test_baseline_not_mutated(self):
  old=dict(self.base);self.apply('result-reply');self.assertEqual(self.base,old)
 def test_oldSUCCESS_has_no_proxy_and_preserves_all_addresses(self):self.assertEqual(self.apply('oldSUCCESS-preserved'),self.base)
if __name__=='__main__':unittest.main()
