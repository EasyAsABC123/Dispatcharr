"""Bounded evidence journal; durability at the real terminal Redis ECHO barrier.
This observes owned fixtures only and changes no application/Source function.
"""
import json,os,threading,time,uuid
from pathlib import Path
ROOT=Path(os.environ['CHANNEL_OBSERVER_ROOT'])
class Journal:
 def __init__(self,client,filename='redis-monitor.jsonl',label='m6-monitor-terminal',*,database=None):
  if database is not None and(type(database)is not int or database!=1):raise ValueError('OnlyExplicitBrokerDatabase1Supported')
  self.database=database;self.raw_rows=0;self.raw_bytes=0;self.excluded_rows_by_db={};self.excluded_bytes_by_db={}
  self.client=client;self.path=ROOT/filename;self.label=label;self.monitor=client.monitor();self.monitor.__enter__();self.rows=[];self.invalid=None;self.closing=False;self.used=0;self.written=0;self.persisted=0;self.marker=None;self.marker_read=False;self.durable=threading.Event();self.writer_closed=False;self.flush_calls=0;self.fsync_calls=0;self.thread=threading.Thread(target=self.run);self.thread.start()
 def run(self):
  try:
   with self.path.open('ab',buffering=262144)as file:
    pending=0
    try:
     for row in self.monitor.listen():
      data=(json.dumps(row,default=str)+'\n').encode()
      self.raw_rows+=1;self.raw_bytes+=len(data)
      if self.database is not None and row.get('db')!=self.database:
       key=str(row.get('db'));self.excluded_rows_by_db[key]=self.excluded_rows_by_db.get(key,0)+1;self.excluded_bytes_by_db[key]=self.excluded_bytes_by_db.get(key,0)+len(data);continue
      if len(self.rows)>=20000:raise ValueError('Monitor20000')
      if self.used+len(data)>16777216:raise ValueError('Monitor16MiB')
      self.used+=len(data);file.write(data);self.rows.append(row);self.written+=1;pending+=len(data)
      if self.marker is not None and row.get('command')=='ECHO '+self.marker:
       self.marker_read=True;file.flush();self.flush_calls+=1;os.fsync(file.fileno());self.fsync_calls+=1;self.persisted=self.written;self.durable.set();break
      if pending>=262144:
       file.flush();self.flush_calls+=1;pending=0
    finally:
     file.flush();self.flush_calls+=1
     if not self.durable.is_set():os.fsync(file.fileno());self.fsync_calls+=1;self.persisted=self.written
   self.writer_closed=True
  except BaseException as error:self.invalid=type(error).__name__+':'+str(error)
 def close(self):
  marker=self.label+'-'+uuid.uuid4().hex;self.marker=marker;close_error=None
  try:self.client.echo(marker);self.durable.wait(5)
  except BaseException as error:close_error=type(error).__name__+':'+str(error)
  finally:self.closing=True;self.monitor.connection.disconnect();self.thread.join(5)
  return {'thread_settled':not self.thread.is_alive(),'actual_rows':len(self.rows),'written_rows':self.written,'persisted_rows':self.persisted,'captured_bytes':self.used,'row_limit':20000,'byte_limit':16777216,'terminal_reader_error':self.invalid or close_error,'close_after_cases':True,'terminal_marker':marker,'terminal_marker_read':self.marker_read,'terminal_barrier_observed':self.marker_read and self.durable.is_set(),'terminal_barrier_fsynced':self.durable.is_set(),'writer_closed':self.writer_closed,'flush_calls':self.flush_calls,'fsync_calls':self.fsync_calls,'database_filter':self.database,'received_byte_unit':'serialized_parsed_monitor_JSON_bytes','raw_rows_received':self.raw_rows,'raw_bytes_received':self.raw_bytes,'excluded_rows_by_database':self.excluded_rows_by_db,'excluded_bytes_by_database':self.excluded_bytes_by_db,'complete_all_database_journal':self.database is None,'scope':'Only actual database1 commands through the actual database1 terminal ECHO are retained; other databases counted but excluded before retained byte/row budgets. Flush/fsync barrier is unchanged; no all-database coverage claimed.'if self.database is not None else'All commands through the actual terminal ECHO are buffered in bounded batches then flush/fsync completes before the durable marker event; no subsequent background polls are included.'}
