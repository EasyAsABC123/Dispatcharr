"""Actual psycopg adaptation over the Rust owner's existing SQL connection.
No psycopg.connect/libpq connection/pool, replacement ORM, or SQL evaluator.
"""
import base64,collections,contextlib,uuid
import psycopg
from psycopg import adapt,pq,sql
from psycopg._queries import PostgresClientQuery
from psycopg._py_transformer import Transformer
from psycopg._column import Column
from .wire import current,CompatibilityUnavailable

def unbase(value):return None if value is None else base64.b64decode(value,validate=True)
class Context:
 # The actual pinned pure-Python Transformer supports connection-free quoting.
 # Its own libpq Escaping implements safe SQL-file literals. Adapter overrides
 # (Django JSON/date loaders, user registered types) remain the actual map.
 connection=None
 def __init__(self,adapters):self.adapters=adapters
class Info:
 def __init__(self,value):self.update(value)
 def update(self,value):
  self.transaction_status={73:pq.TransactionStatus.IDLE,84:pq.TransactionStatus.INTRANS,69:pq.TransactionStatus.INERROR}[value['transaction_status']]
  self.server_version=value['server_version'];self.parameters=dict(value['parameters'])
 def parameter_status(self,name):return self.parameters.get(name)
class Result:
 """DBAPI view of ACTUAL PostgreSQL RowDescription/DataRow/CommandComplete."""
 def __init__(self,value):
  self.columns=value['columns'];self.rows=[[unbase(v)for v in row]for row in value['rows']];self.command_status=unbase(value['command_tag'])
  self.nfields=len(self.columns);self.ntuples=len(self.rows)
  final=self.command_status.rsplit(b' ',1)[-1]
  self.command_tuples=int(final)if final.isdigit()else None
 def fname(self,index):return self.columns[index]['name'].encode('utf8')
 def ftype(self,index):return self.columns[index]['oid']
 def fmod(self,index):return self.columns[index]['type_modifier']
 def fsize(self,index):return self.columns[index]['type_size']
 def fformat(self,index):return self.columns[index]['format']
 def ftable(self,index):return self.columns[index]['table_oid']or 0
 def ftablecol(self,index):return self.columns[index]['table_column']or 0
 def get_value(self,row,column):return self.rows[row][column]

def server_error(value):
 info={ord(key):unbase(raw)for key,raw in value['diagnostics'].items()}
 code=info.get(ord('C'),b'').decode('ascii')
 # Actual Source9 authority covers primary/detail/hint/context errors without
 # positions. Do not invent a client SQL/caret rendering for other failures.
 if ord('P')in info or ord('p')in info:raise CompatibilityUnavailable('Native SQL positioned error rendering is not qualified')
 message=info[ord('M')].decode('utf8')
 for key,label in [('D','DETAIL'),('H','HINT'),('W','CONTEXT')]:
  if ord(key)in info:message+='\n'+label+':  '+info[ord(key)].decode('utf8')
 try:kind=psycopg.errors.lookup(code)
 except KeyError:kind=psycopg.DatabaseError
 return kind(message,info=info,encoding='utf8')
class Connection:
 def __init__(self,context=None):
  self.ident=uuid.uuid4().hex;self.closed=False;self._autocommit=False;self._closed_cursors=False
  self.adapters=adapt.AdaptersMap(context.adapters if context is not None else psycopg.adapters)
  self.info=Info(current().call('sql','open',{'connection':self.ident}))
  self._isolation_level=None;self.cursor_factory=Cursor;self.read_only=None;self.deferrable=None
 @property
 def autocommit(self):return self._autocommit
 @autocommit.setter
 def autocommit(self,value):
  if self.info.transaction_status!=pq.TransactionStatus.IDLE:raise psycopg.ProgrammingError('autocommit cannot change inside a transaction')
  self._autocommit=bool(value)
 @property
 def isolation_level(self):return self._isolation_level
 @isolation_level.setter
 def isolation_level(self,value):
  if self.info.transaction_status!=pq.TransactionStatus.IDLE:raise psycopg.ProgrammingError('isolation level cannot change inside a transaction')
  self._isolation_level=None if value is None else psycopg.IsolationLevel(value)
 def _query(self,query,*,begin=True):
  if self.closed:raise psycopg.InterfaceError('the connection is closed')
  if begin and not self.autocommit and self.info.transaction_status==pq.TransactionStatus.IDLE:
   statement='BEGIN'
   if self.isolation_level is not None:statement+=' ISOLATION LEVEL '+self.isolation_level.name.replace('_',' ')
   if self.read_only is not None:statement+=' READ ONLY'if self.read_only else' READ WRITE'
   if self.deferrable is not None:statement+=' DEFERRABLE'if self.deferrable else' NOT DEFERRABLE'
   self._query(statement,begin=False)
  if isinstance(query,str):query=query.encode('utf8')
  value=current().call('sql','query',{'connection':self.ident,'query_base64':base64.b64encode(query).decode('ascii')});self.info.update(value)
  # Connection-free psycopg adapters are valid for the actual normal Django
  # UTF8/UTC/ISO session. Other connection-sensitive formats need a real
  # context-aware adapter before any values are mis-decoded.
  if value['error']is not None:raise server_error(value['error'])
  if self.info.parameter_status('client_encoding')not in(None,'UTF8')or self.info.parameter_status('TimeZone')not in(None,'UTC')or not (self.info.parameter_status('DateStyle')or'').startswith('ISO'):
   raise CompatibilityUnavailable('Native psycopg session codec needs UTF8/UTC/ISO')
  return[Result(row)for row in value['statements']]
 def cursor(self,name=None,*,scrollable=None,withhold=False,row_factory=None,binary=False):
  if binary:raise CompatibilityUnavailable('Native Python cursor binary protocol is not implemented')
  return self.cursor_factory(self,name,scrollable,withhold,row_factory)
 def commit(self):self._query(b'COMMIT',begin=False)
 def rollback(self):self._query(b'ROLLBACK',begin=False)
 def close(self):
  if not self.closed:current().call('sql','close',{'connection':self.ident});self.closed=True
 def __enter__(self):return self
 def __exit__(self,kind,*_):
  try:self.rollback()if kind else self.commit()
  finally:self.close()
class Cursor:
 def __init__(self,connection,name=None,scrollable=None,withhold=False,row_factory=None):
  self.connection=connection;self.name=name;self.scrollable=scrollable;self.withhold=withhold;self.row_factory=row_factory;self.adapters=adapt.AdaptersMap(connection.adapters);self.closed=False;self.arraysize=1;self.pgresult=None;self._results=[];self._set=0;self._row=0;self._encoding='utf8';self._declared=False
 @property
 def description(self):return None if self.pgresult is None or not self.pgresult.columns else[Column(self,index)for index in range(self.pgresult.nfields)]
 @property
 def rowcount(self):return self._many_rowcount if hasattr(self,'_many_rowcount')else(-1 if self.pgresult is None or self.pgresult.command_tuples is None else self.pgresult.command_tuples)
 @property
 def statusmessage(self):return None if self.pgresult is None or not self.pgresult.command_status else self.pgresult.command_status.decode('ascii')
 def mogrify(self,query,params=None):
  value=PostgresClientQuery(Transformer(Context(self.adapters)));value.convert(query,params);return value.query.decode('utf8')
 def _select(self,index):
  self._set=index;self._row=0;self.pgresult=self._results[index]if index<len(self._results)else None
  self._tx=Transformer(Context(self.adapters))
  if self.pgresult is not None:self._tx.set_loader_types([v['oid']for v in self.pgresult.columns],pq.Format.TEXT)
  self._make_row=self.row_factory(self)if self.row_factory is not None else tuple
 def execute(self,query,params=None,*,prepare=None,binary=None):
  if self.closed:raise psycopg.InterfaceError('the cursor is closed')
  self.__dict__.pop('_many_rowcount',None)
  if binary:raise CompatibilityUnavailable('Native Python cursor binary protocol is not implemented')
  rendered=self.mogrify(query,params)
  if self.name is not None:
   if self._declared:raise psycopg.ProgrammingError('cursor already declared')
   name=sql.Identifier(self.name).as_string(None)
   scroll=''if self.scrollable is None else'SCROLL'if self.scrollable else'NO SCROLL';hold='WITH HOLD'if self.withhold else''
   self.connection._query(f'DECLARE {name} {scroll} CURSOR {hold} FOR '+rendered);self._declared=True
   self._results=self.connection._query(f'FETCH FORWARD 0 FROM {name}')
  else:self._results=self.connection._query(rendered)
  self._select(0);return self
 def executemany(self,query,params_seq,*,returning=False):
  if returning:raise CompatibilityUnavailable('Native executemany returning sets are not implemented')
  total=0
  for params in params_seq:self.execute(query,params);total+=max(0,self.rowcount)
  self._many_rowcount=total;return self
 def _take(self,limit):
  if self.closed:raise psycopg.InterfaceError('the cursor is closed')
  if self.name is not None:
   number='ALL'if limit is None else str(int(limit));name=sql.Identifier(self.name).as_string(None);self._results=self.connection._query(f'FETCH FORWARD {number} FROM {name}');self._select(0)
  if self.pgresult is None or not self.pgresult.columns:raise psycopg.ProgrammingError('the last operation did not produce records')
  end=len(self.pgresult.rows)if limit is None else min(self._row+max(0,limit),len(self.pgresult.rows));rows=[self._make_row(self._tx.load_sequence(v))for v in self.pgresult.rows[self._row:end]];self._row=end;return rows
 def fetchone(self):
  rows=self._take(1);return rows[0]if rows else None
 def fetchmany(self,size=0):return self._take(size or self.arraysize)
 def fetchall(self):return self._take(None)
 def nextset(self):
  if self._set+1<len(self._results):self._select(self._set+1);return True
  return None
 def close(self):
  if not self.closed:
   if self.name is not None and self._declared and not self.connection.closed:self.connection._query('CLOSE '+sql.Identifier(self.name).as_string(None),begin=False)
   self.closed=True
 def __iter__(self):return self
 def __next__(self):
  value=self.fetchone()
  if value is None:raise StopIteration
  return value
 def __enter__(self):return self
 def __exit__(self,*_):self.close()
