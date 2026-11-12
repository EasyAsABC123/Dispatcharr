import copy,unittest
from controller_contract import complete
PLAN={'viewers':[1,10,100],'orders':[['source','candidate'],['candidate','source'],['source','candidate']]}
def example():
 return {'phases':[{'count':v,'trial':t,'backend':b,'passed':True,'qualification':{'passed':True}}for v in PLAN['viewers']for t,order in enumerate(PLAN['orders'],1)for b in order],'pairs':[{'viewers':v,'trial':t,'order':o,'passed':True,'failed':[]}for v in PLAN['viewers']for t,o in enumerate(PLAN['orders'],1)]}
class Matrix(unittest.TestCase):
 def setUp(self):self.report=example()
 def reject(self):
  with self.assertRaises(ValueError):complete(self.report,PLAN)
 def test_all9(self):self.assertTrue(complete(self.report,PLAN))
 def test_no_mutation(self):
  old=copy.deepcopy(self.report);complete(self.report,PLAN);self.assertEqual(old,self.report)
 def test_missing_phase(self):self.report['phases'].pop();self.reject()
 def test_duplicate_phase(self):self.report['phases'][1]=self.report['phases'][0];self.reject()
 def test_missing_pair(self):self.report['pairs'].pop();self.reject()
 def test_duplicate_pair(self):self.report['pairs'][1]=self.report['pairs'][0];self.reject()
 def test_pair_fail(self):self.report['pairs'][0]['passed']=False;self.reject()
 def test_hidden_failure(self):self.report['pairs'][0]['failed']=['memory_budget'];self.reject()
 def test_phase_fail(self):self.report['phases'][0]['passed']=False;self.reject()
 def test_qualification_fail(self):self.report['phases'][0]['qualification']['passed']=False;self.reject()
 def test_wrong_declared_order(self):self.report['pairs'][0]['order']=['candidate','source'];self.reject()
 def test_wrong_actual_order(self):self.report['phases'][0],self.report['phases'][1]=self.report['phases'][1],self.report['phases'][0];self.reject()
if __name__=='__main__':unittest.main(verbosity=2)
