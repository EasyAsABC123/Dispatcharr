"""Use Django's ACTUAL PostgreSQL ORM/compiler/operations over the native DBAPI.
Original Django/psycopg/Dispatcharr authors retain their implementation credit.
No alternate ORM or DB pool and no real psycopg.connect call is installed here.
"""
from django.db.backends.postgresql.base import DatabaseWrapper as PostgreSQLWrapper,CursorMixin
from django.db.backends.postgresql.psycopg_any import IsolationLevel,register_tzloader
from django.utils.asyncio import async_unsafe
from psycopg import pq
from .. import dbapi
from ..wire import CompatibilityUnavailable
from .operations import DatabaseOperations

class Cursor(CursorMixin,dbapi.Cursor):pass
class DatabaseWrapper(PostgreSQLWrapper):
 ops_class=DatabaseOperations
 @property
 def pool(self):return None
 def close_pool(self):pass
 @async_unsafe
 def get_new_connection(self,conn_params):
  from ..wire import current
  target=current().call('sql','settings',{})
  if self.alias!='default'or any(str(conn_params.get(key,'')or'')!=str(target[field])for key,field in(('dbname','NAME'),('user','USER'),('host','HOST'),('port','PORT'))):
   raise CompatibilityUnavailable('Django connection target differs from actual owned Main pool')
  if conn_params.get('server_side_binding')or self.settings_dict['OPTIONS'].get('server_side_binding'):
   raise CompatibilityUnavailable('Binary/server-side psycopg binding is not implemented')
  self.isolation_level=IsolationLevel(self.settings_dict['OPTIONS'].get('isolation_level',IsolationLevel.READ_COMMITTED))
  connection=dbapi.Connection(context=conn_params.get('context'))
  connection.cursor_factory=Cursor
  if'isolation_level'in self.settings_dict['OPTIONS']:connection.isolation_level=self.isolation_level
  return connection
 @async_unsafe
 def create_cursor(self,name=None):
  cursor=Cursor(self.connection,name=name,scrollable=False,withhold=self.connection.autocommit)
  loader=self.connection.adapters.get_loader(1184,pq.Format.TEXT)
  if self.timezone!=loader.timezone:register_tzloader(self.timezone,cursor)
  return cursor
