"""Thin frozen Driver32 adapter; Original behavior and factory remain unchanged.
Original Dispatcharr and dependency authors retain full credit.
"""
import fixture as base

class Fixture(base.Fixture):
 def start_native(self):
  self._plugin_environment={**getattr(self,'_plugin_environment',{}),'RUST_SELECTED_SHUTDOWN_DIAGNOSTICS':'1','RUST_PLUGIN_HOST_DIAGNOSTICS':'1','RUST_EPG_HTTP_DIAGNOSTICS':'1'}
  return super().start_native()

# Reuse the original context manager/bootstrap/finally/close implementation.
# Its global constructor resolves this subclass; no copied Factory lifecycle.
base.Fixture=Fixture
open_fixture=base.open_fixture
