import ast,hashlib,json,os,subprocess,sys,unittest
from pathlib import Path

class Envelope(unittest.TestCase):
 def typed(self):
  source=Path(__file__).with_name('source_envelope.py').read_text();parsed=ast.parse(source)
  definitions=[n for n in parsed.body if isinstance(n,ast.FunctionDef)and n.name in ['digest','typed']]
  scope={'hashlib':hashlib};exec(compile(ast.Module(body=definitions,type_ignores=[]),'<projection>','exec'),scope)
  return scope['typed']
 def test_dynamic_values_hashed_deterministic_types_preserved(self):
  typed=self.typed();value=typed({'headers':{'id':'PRIVATE_ID','root_id':'PRIVATE_ID','origin':'PRIVATE_HOST','parent_id':None,'retries':0,'ignore_result':False,'timelimit':[None,None]},'properties':{'correlation_id':'PRIVATE_ID','reply_to':'PRIVATE_REPLY','delivery_tag':'PRIVATE_TAG','delivery_mode':2}})
  raw=json.dumps(value)
  for private in ['PRIVATE_ID','PRIVATE_HOST','PRIVATE_REPLY','PRIVATE_TAG']:self.assertNotIn(private,raw)
  self.assertEqual(value['fields']['headers']['fields']['id']['sha256'],hashlib.sha256(b'PRIVATE_ID').hexdigest())
  self.assertEqual(value['fields']['headers']['fields']['ignore_result'],{'type':'bool','value':False})
  self.assertEqual(value['fields']['headers']['fields']['retries'],{'type':'int','value':0})
 def test_projection_bounds_and_guards_precede_imports(self):
  typed=self.typed()
  for value in ['x'*4097,list(range(33)),{str(i):i for i in range(65)}]:
   with self.assertRaises(RuntimeError):typed(value)
  value=None
  for _ in range(18):value=[value]
  with self.assertRaises(RuntimeError):typed(value)
  env=dict(os.environ);env.pop('JOB_DISPOSABLE_FIXTURE',None)
  result=subprocess.run([sys.executable,str(Path(__file__).with_name('source_envelope.py'))],env=env,capture_output=True,text=True,timeout=5)
  self.assertNotEqual(result.returncode,0);self.assertIn('Owned fixture guard required',result.stderr);self.assertNotIn('ModuleNotFoundError',result.stderr)
if __name__=='__main__':unittest.main()
