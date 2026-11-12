"""Narrow final-cgroup-only authority after normal command admission closes."""
class FinalCounterCommand:
 def __init__(self,registry,selected,script):self.registry=registry;self.identifiers={v['id']for v in selected.values()};self.script=script
 def __call__(self,argv,timeout=5):
  if len(argv)!=6 or argv[:2]!=['docker','exec']or argv[2]not in self.identifiers or argv[3:5]!=['/dispatcharrpy/bin/python','-c']or argv[5]!=self.script or timeout!=5:raise ValueError('CleanupCounterCommandScope')
  return self.registry.run(argv,timeout=5,cleanup=True)
