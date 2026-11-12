"""Capped direct-child controls/decoder helper. No shell or process on import."""
import os,subprocess
from bounded_process import Collected
class LocalCommand(Collected):
 def __init__(self,argv,limit=1048576,**kwargs):super().__init__(argv,dict(os.environ),limit,self.stop_child,**kwargs)
 def stop_child(self):
  # Actual ffmpeg/python direct children only, never arbitrary process discovery.
  if self.process.poll()is None:
   self.process.terminate()
   try:self.process.wait(timeout=3)
   except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=3)
  if self.process.poll()is None:raise RuntimeError('OwnedDirectChildNotReaped')
