"""Fence direct libpq connections before imports can capture a real connector.
Django uses the actual native-backed DBAPI wrapper instead. Direct user-created
libpq/async/pool connections remain an explicit compatibility gap; no silent
second database pool or redirected credential target is allowed.
"""
from .wire import CompatibilityUnavailable
_installed=False
def install():
 global _installed
 if _installed:raise CompatibilityUnavailable('Native SQL connection fence already installed')
 import psycopg
 def unavailable(*args,**kwargs):
  raise CompatibilityUnavailable('Direct psycopg connection requires an owned native session adapter')
 async def async_unavailable(*args,**kwargs):
  raise CompatibilityUnavailable('Direct asynchronous psycopg connection requires an owned native session adapter')
 psycopg.Connection.connect=classmethod(unavailable)
 psycopg.AsyncConnection.connect=classmethod(async_unavailable)
 psycopg.connect=psycopg.Connection.connect
 _installed=True
