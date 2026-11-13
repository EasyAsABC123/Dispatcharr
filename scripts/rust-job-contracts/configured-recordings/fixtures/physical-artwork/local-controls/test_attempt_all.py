"""Actual collector cleanup function with synthetic held-handle failures only."""
import ast,sys,unittest
from pathlib import Path
SOURCE=Path(__file__).resolve().parents[1]/'tools/source_physical.py'
class Close(unittest.TestCase):
 def function(self,first_fail):
  calls=[]
  class Handle:
   def __init__(self,name):self.name=name
   def close(self):
    calls.append(self.name)
    if first_fail and self.name=='PG':raise OSError('owned synthetic close failure')
    return {'physical_close_verified':True,'errors':[]}
  module=ast.parse(SOURCE.read_text());node=next(n for n in module.body if isinstance(n,ast.FunctionDef)and n.name=='finish_proxies');ns={'pg':Handle('PG'),'reply':Handle('Redis'),'case':{},'save':lambda:calls.append('save')};exec(compile(ast.Module(body=[node],type_ignores=[]),str(SOURCE),'exec'),ns);return ns,calls
 def test_first_failed_handle_still_closes_second_and_retains_error(self):
  ns,calls=self.function(True)
  with self.assertRaises(RuntimeError):ns['finish_proxies']()
  self.assertEqual(calls,['PG','Redis','save']);self.assertFalse(ns['case']['PG']['physical_close_verified']);self.assertTrue(ns['case']['Redis']['physical_close_verified']);self.assertIsNone(ns['pg']);self.assertIsNone(ns['reply'])
 def test_both_close_normally(self):
  ns,calls=self.function(False);ns['finish_proxies']();self.assertEqual(calls,['PG','Redis','save']);self.assertEqual(ns['case']['proxy_close_errors'],[])
if __name__=='__main__':unittest.main()
