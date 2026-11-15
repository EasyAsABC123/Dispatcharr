"""Explicit guarded process scope for the first owned plugin-runtime slice.
The native owner actually waits/reaps the host and verifies its process group.
User subprocess/fork/exec remains required compatibility work and is rejected
BEFORE effect; it is never reported supported or silently detached.
"""
import sys
from .wire import current,CompatibilityUnavailable
_installed=False
# Python audit events run before these stdlib spawn/exec operations. This is a
# compatibility guard for normal library calls, not an adversarial-code sandbox.
_SPAWN={'subprocess.Popen','os.fork','os.forkpty','os.posix_spawn','os.exec','os.system'}
def install():
 global _installed
 if _installed:raise CompatibilityUnavailable('Native process policy already installed')
 actual=current().call('process','policy',{})
 if actual!={'mode':'guarded-no-spawn','actual_host_group_owned':True}:raise CompatibilityUnavailable('Actual native process scope unavailable')
 def audit(event,args):
  if event in _SPAWN:raise CompatibilityUnavailable('Plugin subprocess/fork/exec requires the remaining owned-child adapter')
 sys.addaudithook(audit);_installed=True
