"""Actual helper import pollution, but no Docker, worker or sampler execution."""
from pathlib import Path
import subprocess
import sys
import unittest


class RealImportEnvironment(unittest.TestCase):
    def test_shared_guide_orchestrator_and_generic_cache_cannot_shadow_m1(self):
        tools=Path(__file__).resolve().parent
        root=tools.parents[3]
        script=r'''
import importlib.util, pathlib, sys, types
root=pathlib.Path(sys.argv[1]);tools=pathlib.Path(sys.argv[2])
support=tools.parent/'support'
sys.path.insert(0,str(support/'scripts/rust-integration'))
import run
sys.path.insert(0,str(support/'scripts/rust-guide-perf'))
import orchestrator as shared
assert pathlib.Path(shared.__file__).resolve()==support/'scripts/rust-guide-perf/orchestrator.py'
for name in ['runtime','policy','metrics','reporting']:
    sys.modules[name]=types.ModuleType('poisoned_'+name)
spec=importlib.util.spec_from_file_location('m1_controller_import_probe',tools/'controller.py')
controller=importlib.util.module_from_spec(spec);spec.loader.exec_module(controller)
for name in ['runtime','policy','metrics','orchestrator','reporting']:
    module=controller.LOCAL.load(name)
    assert pathlib.Path(module.__file__).resolve()==tools/(name+'.py'),(name,module.__file__)
    assert module.__name__=='dispatcharr_m1_core_private.'+name
assert callable(controller.LOCAL.load('orchestrator').experiment)
assert controller.CoreRuntime is controller.LOCAL.load('runtime').CoreRuntime
assert sys.modules['orchestrator'] is shared
print('isolated M1 package under real shared helper imports PASS')
'''
        result=subprocess.run([sys.executable,'-c',script,str(root),str(tools)],cwd=root,capture_output=True,text=True,timeout=20)
        self.assertEqual(result.returncode,0,result.stdout+result.stderr)


if __name__=='__main__':unittest.main()
