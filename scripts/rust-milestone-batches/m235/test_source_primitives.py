"""LOCAL primitive controls, NOT Source application observations.
Compile exact pinned Base/TS class AST; synthetic clock/loader are explicit.
No Django import, DB, Redis, network, Source function replacement or app start.
"""
import ast,sys,types,unittest
from pathlib import Path
from unittest.mock import patch
SOURCE=Path(__file__).resolve().parent/'source_workflows/inputs/Original/apps/proxy/config.py'
class Clock:
 def __init__(self):self.now=100.;self.loads=0;self.next_value={'stream_startup_timeout':8};self.delay=0.;self.error=False;self.closes=0
 def time(self):return self.now
 def load(self):
  self.loads+=1;self.now+=self.delay
  if self.error:raise RuntimeError('synthetic loader error')
  return dict(self.next_value)
 def close(self):self.closes+=1
class SourceCachePrimitive(unittest.TestCase):
 def setUp(self):
  tree=ast.parse(SOURCE.read_bytes());classes=[n for n in tree.body if isinstance(n,ast.ClassDef)and n.name in ('BaseConfig','TSConfig')];self.assertEqual([n.name for n in classes],['BaseConfig','TSConfig'])
  self.clock=Clock();self.scope={'time':self.clock,'connection':self.clock};exec(compile(ast.Module(body=classes,type_ignores=[]),str(SOURCE),'exec'),self.scope)
  module=types.ModuleType('core.models');module.CoreSettings=types.SimpleNamespace(get_proxy_settings=self.clock.load);self.imports=patch.dict(sys.modules,{'core.models':module});self.imports.start();self.addCleanup(self.imports.stop)
  self.base=self.scope['BaseConfig'];self.ts=self.scope['TSConfig']
 def test_base_clear_does_not_clear_primed_subclass(self):
  self.assertEqual(self.ts.get_proxy_settings()['stream_startup_timeout'],8)
  self.clock.next_value={'stream_startup_timeout':3};self.base.clear_proxy_settings_cache()
  self.assertEqual(self.base.get_proxy_settings()['stream_startup_timeout'],3)
  self.assertEqual(self.ts.get_proxy_settings()['stream_startup_timeout'],8)
  self.assertEqual(self.clock.loads,2)
  self.assertTrue('_proxy_settings_cache'in self.ts.__dict__)
 def test_Base_first_hit_is_inherited_without_TS_slot(self):
  self.base.get_proxy_settings();self.ts.get_proxy_settings();self.assertNotIn('_proxy_settings_cache',self.ts.__dict__)
  self.clock.next_value={'stream_startup_timeout':3};self.base.clear_proxy_settings_cache()
  self.assertEqual(self.ts.get_proxy_settings()['stream_startup_timeout'],3);self.assertIn('_proxy_settings_cache',self.ts.__dict__)
 def test_slow_load_timestamps_before_not_after(self):
  self.clock.delay=2.4;self.ts.get_proxy_settings();self.assertEqual(self.ts._proxy_settings_cache_time,100)
  self.clock.delay=0;self.clock.now=110.;self.clock.next_value={'stream_startup_timeout':3}
  self.assertEqual(self.ts.get_proxy_settings()['stream_startup_timeout'],3);self.assertEqual(self.clock.loads,2)
 def test_over_ttl_load_returns_value_then_next_call_reloads(self):
  self.clock.delay=11.;self.ts.get_proxy_settings();self.assertEqual(self.clock.loads,1);self.assertEqual(self.ts._proxy_settings_cache_time,100)
  self.clock.delay=0.;self.ts.get_proxy_settings();self.assertEqual(self.clock.loads,2)
 def test_backwards_wall_clock_remains_source_hit(self):
  self.ts.get_proxy_settings();self.clock.now=99.;self.ts.get_proxy_settings();self.assertEqual(self.clock.loads,1)
 def test_error_returns_fallback_without_install_and_always_closes(self):
  self.clock.error=True;value=self.ts.get_proxy_settings();self.assertEqual(value['stream_startup_timeout'],8);self.assertEqual(value['connection_timeout'],10)
  self.assertNotIn('_proxy_settings_cache',self.ts.__dict__);self.assertEqual(self.clock.closes,1)
if __name__=='__main__':unittest.main()
