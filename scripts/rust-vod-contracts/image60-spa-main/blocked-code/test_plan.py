"""Synthetic plan/coverage corruptions only; no app or sockets."""
import copy,json,unittest
from pathlib import Path
import validate as V
HERE=Path(__file__).resolve().parent
class Plan(unittest.TestCase):
 def setUp(self):
  plan=json.loads((HERE/'plan.json').read_text());ref=json.loads((HERE/'source-reference.json').read_text());schema=json.loads((HERE/'schema62.json').read_text());self.plan=plan;self.ref=ref;fp={'tables':{k:{'rows':0,'sha256':'a'*64}for k in schema['tables']},'sequences':{k:{'last_value':1,'is_called':False}for k in schema['sequences']}};self.v={'Native_only':True,'Source_requests':0,'resource_sampling':False,'full_M4_acceptance':False,'candidate_image':plan['candidate_binding']['image_id'],'candidate_Git':plan['candidate_binding']['Git'],'candidate_tree':plan['candidate_binding']['tree'],'Main_identity':{'PID':1,'UID':65532,'comm':'dispatcharr-rs'},'cases':[{**{k:plan['Native_case'][k]for k in ['id','name','method','path']},'request_headers':plan['Native_case']['headers'],'request_body_hex':'','status':200,'body_hex':ref['case']['body_hex'],'body_bytes':858,'body_sha256':ref['case']['body_sha256'],'headers':[['Content-Type','text/html; charset=utf-8'],['Content-Length','858']],'began_monotonic':1.,'completed_monotonic':1.1,'absolute_observation':{'timer_joined':True},'physical_close':{'fd_after':-1,'original_inode_absent':True,'timer_joined':True}}],'owned_before':copy.deepcopy(fp),'owned_after':copy.deepcopy(fp),'default_before':copy.deepcopy(fp),'default_after':copy.deepcopy(fp),'origin_identity':{'listener_ready_actual':True,'PID':55,'starttime':'123','nonce_sha256':'b'*64},'origin_before':{'rows':[],'invalid':[],'nonce_sha256':'b'*64,'accepted_TCP_total':1,'unclassified_connections':0,'connections':[{'requests':1,'controls':1,'socket_closed':True}]},'origin_after':{'rows':[],'invalid':[],'nonce_sha256':'b'*64,'accepted_TCP_total':1,'unclassified_connections':0,'connections':[{'requests':1,'controls':1,'socket_closed':True}]},'origin_terminal':{'rows':[],'invalid':[],'nonce_sha256':'b'*64,'shutdown_acknowledged':True,'socket_listener_closed':True,'handler_threads_joined':True,'accepted_TCP_total':1,'unclassified_connections':0,'connections':[{'requests':1,'controls':1,'socket_closed':True}]},'actual_business_request_invocations':1,'Source_replay':0,'no_adaptive_retry':True}

  for k in ['origin_before','origin_after','origin_terminal']:self.v[k].update(PID=55,starttime='123',control_requests=1)
 def bad(self,f):
  v=copy.deepcopy(self.v);f(v)
  with self.assertRaises((ValueError,KeyError,TypeError)):V.validate(v)
 def test_finite_synthetic_not_actual(self):self.assertEqual(V.validate(self.v)['qualified_Native_Main_SPA_GET'],1)
 def test_missing(self):self.bad(lambda v:v['cases'].clear())
 def test_extra(self):self.bad(lambda v:v['cases'].append(copy.deepcopy(v['cases'][0])))
 def test_old_image(self):self.bad(lambda v:v.update(candidate_image='old'))
 def test_wrong_method(self):self.bad(lambda v:v['cases'][0].update(method='HEAD'))
 def test_wrong_path(self):self.bad(lambda v:v['cases'][0].update(path='/proxy/vod/other'))
 def test_authorization_extra(self):self.bad(lambda v:v['cases'][0]['request_headers'].update(Authorization='Bearer invalid'))
 def test_old404(self):self.bad(lambda v:v['cases'][0].update(status=404))
 def test_wrong_body(self):self.bad(lambda v:v['cases'][0].update(body_hex='00'))
 def test_wrong_type(self):self.bad(lambda v:v['cases'][0]['headers'].__setitem__(0,['Content-Type','application/json']))
 def test_worker_header(self):self.bad(lambda v:v['cases'][0]['headers'].append(['X-Worker-ID','fake']))
 def test_bad_clock(self):self.bad(lambda v:v['cases'][0].update(completed_monotonic=20))
 def test_socket_open(self):self.bad(lambda v:v['cases'][0]['physical_close'].update(fd_after=1))
 def test_missing_SQL(self):self.bad(lambda v:v['owned_after']['tables'].clear())
 def test_no_sequence(self):self.bad(lambda v:v['default_after']['sequences'].clear())
 def test_SQLchanged(self):self.bad(lambda v:v['owned_after']['tables'].update(accounts_user={'changed':True}))
 def test_providercontact(self):self.bad(lambda v:v['origin_after']['rows'].append({'method':'GET'}))
 def test_retry(self):self.bad(lambda v:v.update(actual_business_request_invocations=2))
 def test_Source_replay(self):self.bad(lambda v:v.update(Source_replay=1))
 def test_no_live_origin(self):self.bad(lambda v:v['origin_identity'].update(listener_ready_actual=False))
 def test_wrong_origin_nonce(self):self.bad(lambda v:v['origin_after'].update(nonce_sha256='a'*64))
 def test_origin_not_settled(self):self.bad(lambda v:v['origin_terminal'].update(handler_threads_joined=False))
 def test_untyped_SQL(self):self.bad(lambda v:v['owned_before']['tables'].update(accounts_user={}))
 def test_empty_origin_coverage(self):self.bad(lambda v:v['origin_before'].update(accepted_TCP_total=0,connections=[],control_requests=0))
 def test_wrong_origin_process(self):self.bad(lambda v:v['origin_after'].update(PID=999))
 def test_inconsistent_control_count(self):self.bad(lambda v:v['origin_terminal'].update(control_requests=0))
 def test_blocked_entry(self):
  import run,sys
  from unittest.mock import patch
  with patch.object(run,'check_seal',return_value={}),patch.object(run,'candidate_validate',return_value={}),patch.object(sys,'argv',['run','--execute-reviewed-seal','none']):
   with self.assertRaisesRegex(ValueError,'RootFlagOnlyActivation'):run.main()
if __name__=='__main__':unittest.main()
