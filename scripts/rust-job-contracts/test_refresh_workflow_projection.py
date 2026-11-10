import copy,json,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).parent))
from refresh_workflow_projection import project
class ProjectionTests(unittest.TestCase):
 def rows(self):
  row={'iteration':1,'http':{'status':202,'body':{'success':True}},'completed':[{'task':'owned','state':'SUCCESS','result':None,'id':'private'}],'index':{'channels':{'owned':[12]}},'current':[{'id':1,'title':'owned'}],'programmes':[{'id':1,'title':'owned'}],'file_sha256':'abc','events':[{'type':'update','data':{'updated_at':'2026-11-10T12:00:00+00:00','status':'success'}}]}
  second=copy.deepcopy(row);second['iteration']=2;return {'imports':[row,second]}
 def test_only_runtime_ids_and_event_clock_excluded(self):
  a=self.rows();b=copy.deepcopy(a);b['imports'][0]['completed'][0]['id']='other';b['imports'][0]['events'][0]['data']['updated_at']='2026-11-10T13:00:00+00:00';self.assertEqual(project(a),project(b))
 def test_event_result_index_and_guide_differences_retained(self):
  a=self.rows()
  for field in ['events','index','current','programmes','completed','http','file_sha256']:
   b=copy.deepcopy(a);b['imports'][0][field]=[]if isinstance(b['imports'][0][field],list)else 'changed';self.assertNotEqual(project(a),project(b))
 def test_missing_second_import_rejected(self):
  a=self.rows();a['imports'].pop()
  with self.assertRaises(ValueError):project(a)
 def test_invalid_event_clock_rejected(self):
  a=self.rows();a['imports'][0]['events'][0]['data']['updated_at']='2026-11-10T12:00:00-05:00'
  with self.assertRaises(ValueError):project(a)
if __name__=='__main__':unittest.main()
