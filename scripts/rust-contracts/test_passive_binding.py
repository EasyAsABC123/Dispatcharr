import ast, contextlib, inspect, json, socket, types, unittest
from pathlib import Path
from unittest.mock import patch
from django.conf import settings
settings.configure(SECRET_KEY='fixture-only', ROOT_URLCONF=__name__, REST_FRAMEWORK={})
import django
django.setup()
from django.views.generic.base import View
from rest_framework.viewsets import ViewSet, ViewSetMixin
from rest_framework.decorators import api_view
source=Path('/probe/passive_source.py').read_text()
tree=ast.parse(source)
keep=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))]
ns=dict(contextlib=contextlib,inspect=inspect,json=json,socket=socket,types=types,patch=patch,
        current={'stage':'test','ordinal':0},violations=[])
exec(compile(ast.Module(body=keep,type_ignores=[]),'<probe-functions>','exec'),ns)
route={'operations':[{'contract_id':'controlled','method':None}]}
codes=ns['factory_codes'](View.as_view)|ns['factory_codes'](ViewSetMixin.as_view)
class Binding(unittest.TestCase):
 def bind(self,cls,callback=None):
  return ns['bind_one'](route, callback or cls.as_view(), cls, View, codes)
 def test_get_head_options_no_handler_dispatch(self):
  class V(View):
   def get(self,*a,**k):raise AssertionError('handler invoked')
   def dispatch(self,*a,**k):raise AssertionError('dispatch invoked')
  r=self.bind(V);self.assertEqual(r['state'],'metadata_observed');self.assertEqual(r['binding']['allowed_methods'],['GET','HEAD','OPTIONS']);self.assertTrue(r['implicit_HEAD_separate'])
 def test_post(self):
  class V(View):
   def post(self,*a,**k):raise AssertionError()
  self.assertEqual(self.bind(V)['binding']['allowed_methods'],['POST','OPTIONS'])
 def test_drf_api_view(self):
  @api_view(['POST'])
  def f(request):raise AssertionError('handler invoked')
  r=self.bind(f.cls,f);self.assertEqual(r['state'],'metadata_observed');self.assertEqual(set(r['binding']['allowed_methods']),{'POST','OPTIONS'})
 def test_viewset_action_copy(self):
  class V(ViewSet):
   def list(self,*a,**k):raise AssertionError('handler invoked')
   def get_permissions(self):raise AssertionError('permissions invoked')
  callback=V.as_view({'get':'list'});before=dict(callback.actions)
  r=self.bind(V,callback);self.assertEqual(r['state'],'metadata_observed');self.assertEqual(callback.actions,before);self.assertEqual(r['binding']['action_map'],{'get':'list','head':'list'})
 def test_custom_init_not_run(self):
  class V(View):
   def __init__(self,**k):raise AssertionError('constructor invoked')
  self.assertEqual(self.bind(V)['reason'],'custom_constructor_not_invoked')
 def test_custom_setup_not_run(self):
  class V(View):
   def setup(self,*a,**k):raise AssertionError('setup invoked')
  self.assertEqual(self.bind(V)['reason'],'custom_setup_not_invoked')
 def test_property_not_run(self):
  class V(View):
   @property
   def get(self):raise AssertionError('property invoked')
  self.assertEqual(self.bind(V)['reason'],'unsupported_method_descriptor')
 def test_custom_allowed_not_run(self):
  class V(View):
   def _allowed_methods(self):raise AssertionError('allowed invoked')
  self.assertEqual(self.bind(V)['reason'],'custom_allowed_methods_not_invoked')
 def test_spoof_factory_refused(self):
  class V(View):pass
  def view(request):raise AssertionError('spoof invoked')
  view.view_class=V
  self.assertEqual(self.bind(V,view)['reason'],'not_exact_framework_factory_closure')
 def test_custom_getattribute_not_run(self):
  class V(View):
   def __getattribute__(self,name):raise AssertionError('lookup invoked')
  self.assertEqual(self.bind(V)['reason'],'custom_attribute_lookup_not_invoked')
 def test_custom_new_not_run(self):
  class V(View):
   def __new__(cls,*a,**k):raise AssertionError('new invoked')
  self.assertEqual(self.bind(V)['reason'],'custom_attribute_lookup_not_invoked')
 def test_violation_survives_caught_exception(self):
  ns['violations'].clear();ns['current'].update(stage='test',ordinal=0)
  try:ns['no_connect']('unused')
  except ns['Blocked']:pass
  self.assertEqual(ns['violations'],[{'kind':'network_connect_attempt','stage':'test','resolver_ordinal':0}])
class Inventory(unittest.TestCase):
 def setUp(self):
  self.rows=json.loads(Path('/inventory/rust-contract-register.json').read_text())['runtime_routes']
  self.observed=[({'pattern':r['pattern_exact'],'name':r['name'],'view':r['source_handler']},None,None) for r in self.rows]
 def test_exact_inventory(self):ns['validate_inventory'](self.observed,self.rows)
 def test_dropped(self):
  with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed[:-1],self.rows)
 def test_reordered(self):
  self.observed[0],self.observed[1]=self.observed[1],self.observed[0]
  with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed,self.rows)
 def test_duplicate_id(self):
  self.rows[1]['route_id']=self.rows[0]['route_id']
  with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed,self.rows)
 def test_ordinal_changed(self):
  self.rows[1]['resolver_ordinal']=900
  with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed,self.rows)
 def test_unresolved_changed(self):
  self.rows[0]['declared_methods_resolved']=False
  with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed,self.rows)
 def test_each_identity_changed(self):
  for key in ('pattern','name','view'):
   old=self.observed[0][0][key];self.observed[0][0][key]='invalid'
   with self.assertRaises(ns['Blocked']):ns['validate_inventory'](self.observed,self.rows)
   self.observed[0][0][key]=old
if __name__=='__main__':unittest.main(verbosity=2)
