"""Dispatcher outer-exit and review-local controls; no child/SDK/Docker execution."""
import contextlib,importlib.util,io,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
HERE=Path(__file__).parent;s=importlib.util.spec_from_file_location('finite_dispatch',HERE/'scheduler.py');m=importlib.util.module_from_spec(s);s.loader.exec_module(m)
class Controls(unittest.TestCase):
 def outer(self,child,expected):
  argv=['scheduler.py','execute','--name','baseline-all']
  with patch.object(sys,'argv',argv),patch.object(m,'execute',return_value={'returncode':child}),contextlib.redirect_stdout(io.StringIO()):
   with self.assertRaises(SystemExit)as v:m.main()
   self.assertEqual(v.exception.code,expected)
 def test_outer_zero(self):self.outer(0,0)
 def test_outer_nonzero_not_hidden(self):self.outer(1,1)
 def test_outer_unknown_not_success(self):self.outer(None,1)
 def review_fixture(self,root,code):
  logs=root/'logs';logs.mkdir();output=root/'output';output.mkdir();report=output/'integration.json';report.write_text('{}');cleanup=root/'cleanup.json';cleanup.write_text('synthetic empty inventory for local control only');result=logs/'baseline-all.result.json';result.write_text(json.dumps({'returncode':code,'process_settled':True}));state=m.fresh_state(m.PLAN);state['started']=True;state['nodes']['baseline-all']={'status':'awaiting-review','returncode':code,'lane':0,'process_settled':True,'controller_result_sha256':m.sha(result)};state['lanes']['0']['node']='baseline-all';(root/'state.json').write_text(json.dumps(state));node=dict(m.NODES['baseline-all']);node['argv']=['python3','unexecuted','--output',str(output)];nodes={**m.NODES,'baseline-all':node};v={'name':'baseline-all','controller_result_sha256':m.sha(result),'actual_process_exit':code,'actual_process_settled':True,'passed':code==0,'semantic_review_complete':True,'observed_default_audits':[],'actual_reports':[{'path':str(report),'sha256':m.sha(report)}],'cleanup':{'scope':'independent-owned-empty','verified':True,'inventories':{'containers':[],'networks':[],'volumes':[]},'query_receipt':{'path':str(cleanup),'sha256':m.sha(cleanup)}}};return logs,nodes,v
 def with_review(self,code,change,expect):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);logs,nodes,v=self.review_fixture(root,code);change(v);p=root/'review.json';p.write_text(json.dumps(v))
   with patch.object(m,'HERE',root),patch.object(m,'STATE',root/'state.json'),patch.object(m,'LOGS',logs),patch.object(m,'NODES',nodes):
    if expect:
     with self.assertRaises(m.Refusal):m.record_review(p,m.sha(p))
    else:self.assertIn(m.record_review(p,m.sha(p))['status'],['passed-clean','failed-clean'])
 def test_review_success_requires_actual0(self):self.with_review(1,lambda v:v.update(passed=True),True)
 def test_review_failed_clean_releases(self):self.with_review(1,lambda v:None,False)
 def test_review_unsettled_refused(self):self.with_review(0,lambda v:v.update(actual_process_settled=False),True)
 def test_review_cleanup_uncertainty_halts(self):self.with_review(0,lambda v:v['cleanup'].update(verified=False),True)
 def actual_unsettled(self,where,value):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);logs,nodes,v=self.review_fixture(root,0)
   if where=='state':
    state=json.loads((root/'state.json').read_text());state['nodes']['baseline-all']['process_settled']=value;(root/'state.json').write_text(json.dumps(state))
   else:
    result=logs/'baseline-all.result.json';data=json.loads(result.read_text());data['process_settled']=value;result.write_text(json.dumps(data));v['controller_result_sha256']=m.sha(result);state=json.loads((root/'state.json').read_text());state['nodes']['baseline-all']['controller_result_sha256']=m.sha(result);(root/'state.json').write_text(json.dumps(state))
   # Even a passed review explicitly saying settled may not override reality.
   v.update(passed=True,actual_process_settled=True);p=root/'review.json';p.write_text(json.dumps(v))
   with patch.object(m,'HERE',root),patch.object(m,'STATE',root/'state.json'),patch.object(m,'LOGS',logs),patch.object(m,'NODES',nodes):
    with self.assertRaises(m.Refusal):m.record_review(p,m.sha(p))
   after=json.loads((root/'state.json').read_text());self.assertTrue(after['halted']);self.assertEqual(after['nodes']['baseline-all']['status'],'settlement-uncertain');self.assertEqual(after['lanes']['0']['node'],'baseline-all')
 def test_false_actual_state_not_overridden(self):self.actual_unsettled('state',False)
 def test_unknown_actual_state_not_overridden(self):self.actual_unsettled('state',None)
 def test_false_sealed_result_not_overridden(self):self.actual_unsettled('result',False)
 def test_unknown_sealed_result_not_overridden(self):self.actual_unsettled('result',None)
 def test_mutated_actual_result_not_resealed_by_review(self):
  with tempfile.TemporaryDirectory()as t:
   root=Path(t);logs,nodes,v=self.review_fixture(root,0);result=logs/'baseline-all.result.json';data=json.loads(result.read_text());data['new_field']='tampered';result.write_text(json.dumps(data));v['controller_result_sha256']=m.sha(result);p=root/'review.json';p.write_text(json.dumps(v))
   with patch.object(m,'HERE',root),patch.object(m,'STATE',root/'state.json'),patch.object(m,'LOGS',logs),patch.object(m,'NODES',nodes):
    with self.assertRaises(m.Refusal):m.record_review(p,m.sha(p))
   after=json.loads((root/'state.json').read_text());self.assertTrue(after['halted']);self.assertEqual(after['lanes']['0']['node'],'baseline-all')
if __name__=='__main__':unittest.main()
