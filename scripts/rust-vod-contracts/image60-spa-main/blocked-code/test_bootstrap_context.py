"""Source-backed static clauses, actual dynamic identities, exact SQL correlation."""
import copy,hashlib,json,unittest
import bootstrap_context as B
def fixture():
 a={'id':7,'name':'custom','locked':True,'is_active':True,'max_streams':0,'server_url':None,'file_path':None,'username':None,'password':None,'refresh_task_id':19,'created_at':'retained dynamic Source-created timestamp'}
 p={'id':11,'m3u_account_id':7,'name':'custom Default','is_default':True,'is_active':True,'max_streams':0,'search_pattern':'^(.*)$','replace_pattern':'$1'}
 t={'id':19,'name':'m3u_account-refresh-7','task':'apps.m3u.tasks.refresh_single_m3u_account','kwargs':'{"account_id": 7}','interval_id':23,'enabled':True,'date_changed':'retained actual timestamp'}
 i={'id':23,'every':24,'period':'hours'}
 context={'SQL_read_only':True,'database':'dispatcharr','table_rows':{name:[[row]]for name,row in zip(B.TABLES,[a,p,t,i])}};audit={'database':'dispatcharr','tables':{'accounts_user':{'rows':0,'sha256':hashlib.sha256(b'[]').hexdigest()},'vod_movie':{'rows':0,'sha256':hashlib.sha256(b'[]').hexdigest()}}};correlate(context,audit);return context,audit
def correlate(context,audit):
 for name,raw in context['table_rows'].items():audit['tables'][name]={'rows':len(raw),'sha256':hashlib.sha256(json.dumps(raw,sort_keys=True,default=str,ensure_ascii=False).encode()).hexdigest()}
class Builtin(unittest.TestCase):
 def setUp(self):self.context,self.audit=fixture()
 def bad(self,fn,rebind=True):
  c,a=copy.deepcopy(self.context),copy.deepcopy(self.audit);fn(c,a)
  if rebind:correlate(c,a)
  with self.assertRaises((ValueError,KeyError,TypeError)):B.validate(c,a)
 def test_positive_dynamic_IDs_not_forced1(self):self.assertTrue(B.validate(self.context,self.audit)['passed'])
 def test_account_inactive_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]][0][0].update(is_active=False))
 def test_profile_inactive_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[1]][0][0].update(is_active=False))
 def test_remoteURL_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]][0][0].update(server_url='https://provider.invalid'))
 def test_extra_account_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]].append(copy.deepcopy(c['table_rows'][B.TABLES[0]][0])))
 def test_not_locked_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]][0][0].update(locked=False))
 def test_wrong_profile_FK_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[1]][0][0].update(m3u_account_id=99))
 def test_wrong_task_FK_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]][0][0].update(refresh_task_id=99))
 def test_wrong_task_payload_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[2]][0][0].update(kwargs='{"account_id":99}'))
 def test_wrong_interval_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[3]][0][0].update(every=1))
 def test_nonempty_user_rejected(self):self.bad(lambda c,a:a['tables']['accounts_user'].update(rows=1))
 def test_nonempty_movie_rejected(self):self.bad(lambda c,a:a['tables']['vod_movie'].update(rows=1))
 def test_raw_SQL_hash_correlation_rejected(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[0]][0][0].update(created_at='changed'),rebind=False)
 def test_task_enabled_literal_not_assumed_disabled(self):
  self.context['table_rows'][B.TABLES[2]][0][0]['enabled']=False;correlate(self.context,self.audit);B.validate(self.context,self.audit)
 def test_no_arbitrary_default_profile(self):self.bad(lambda c,a:c['table_rows'][B.TABLES[1]][0][0].update(search_pattern='different'))
if __name__=='__main__':unittest.main()
