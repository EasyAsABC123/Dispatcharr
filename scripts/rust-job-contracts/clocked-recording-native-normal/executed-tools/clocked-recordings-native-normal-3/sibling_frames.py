"""Read-only Source Beat sibling provenance; never consume/move/mutate frames.
Only exact enabled static settings entries with empty args/kwargs are in this
reference context. Disabled fetchstats, built-in cleanup and unknowns refuse.
No execution/return/default graph/Native authority follows from queued siblings.
"""
import base64,datetime,hashlib,json,uuid
from pathlib import Path
def require(ok,label):
 if ok is not True:raise ValueError(label)
def utc(value):
 require(type(value)is str,'ActualSiblingUTC');value=datetime.datetime.fromisoformat(value);require(value.utcoffset()==datetime.timedelta(0),'SiblingUTCOffset');return value
def validate(frames,publications,periodic,actual_config,beat,finished_utc,authority=None):
 expected=json.loads((Path(__file__).parent/'beat-static-authority.json').read_text())if authority is None else authority
 require(json.dumps(actual_config,sort_keys=True,allow_nan=False)==json.dumps(expected,sort_keys=True,allow_nan=False),'ExactPinnedStaticConfiguration')
 require(type(frames)is list and len(frames)<=64 and all(type(raw)is str for raw in frames),'BoundedRawSiblingFIFO64')
 raw_values=[base64.b64decode(raw,validate=True)for raw in frames];require(sum(map(len,raw_values))<=1048576,'SiblingFIFO1MiB')
 require(type(beat['pid'])is int and beat['pid']>0 and type(beat['starttime'])is str and beat['starttime'].isdigit(),'ActualBeatIdentity')
 began=utc(beat['utc']);finished=utc(finished_utc);require(began<=finished,'ActualBeatCommandWindow')
 ids=[];rows=[]
 for raw,wire in zip(frames,raw_values):
  frame=json.loads(wire);headers=frame['headers'];properties=frame['properties'];ident=headers['id'];name=headers['periodic_task_name'];entry=expected.get(name)
  require(type(ident)is str and str(uuid.UUID(ident))==ident and uuid.UUID(ident).version==4,'ActualCanonicalSiblingID')
  require(type(entry)is dict and entry.get('enabled',True)is True,'KnownEnabledStaticSiblingOnly')
  require(headers['task']==entry['task']and headers['root_id']==ident and headers['parent_id']is None,'ExactStaticSiblingFamilyRoot')
  require(frame['content-type']=='application/json'and frame['content-encoding']=='utf-8','ActualSiblingEncoding')
  require(all(headers.get(key)is None for key in ('eta','expires','group','group_index','shadow'))and type(headers['retries'])is int and headers['retries']==0 and headers['ignore_result']is False and headers['timelimit']==[None,None],'ExactEmptySiblingOptions')
  require(properties['correlation_id']==ident and properties['delivery_info']=={'exchange':'','routing_key':'celery'}and type(properties['priority'])is int and properties['priority']==0,'NaturalDefaultSiblingQueue')
  body=json.loads(base64.b64decode(frame['body'],validate=True));require(body==[[],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}]and type(body[0])is list and type(body[1])is dict,'ActualEmptySiblingBody')
  emitted=[row for row in publications if row['headers'].get('id')==ident];require(len(emitted)==1,'ActualUniqueSiblingPublication');pub=emitted[0]
  require(pub['pid']==beat['pid']and type(pub['pid'])is int and pub['task']==entry['task']and pub['routing_key']=='celery'and pub['headers']==headers and pub['body']==body and began<=utc(pub['utc'])<=finished,'ActualBeatPublicationHeaderBodyAndWindow')
  model=[row for row in periodic if row['name']==name];require(len(model)==1 and model[0]['task']==entry['task']and model[0]['enabled']is True and json.loads(model[0]['args'])==[]and json.loads(model[0]['kwargs'])=={},'ActualStaticPeriodicRow')
  require(ident not in ids,'UniqueSiblingIDs');ids.append(ident);rows.append({'id':ident,'task':entry['task'],'periodic_task_name':name,'raw_sha256':hashlib.sha256(wire).hexdigest(),'publication_utc':pub['utc'],'source_execution_verified':False})
 return {'passed':True,'scope':'Queued unconsumed enabled static Beat siblings only; no execution/default graph/Native claim','raw_FIFO_base64':list(frames),'actual_FIFO_order_preserved':True,'rows_in_FIFO_order':rows,'ids':ids,'actual_Beat_pid':beat['pid'],'actual_Beat_starttime':beat['starttime'],'actual_Beat_began_utc':beat['utc'],'actual_Beat_finished_utc':finished_utc,'all_time_fields_retained':True}

def validate_intervals(periodic,intervals,siblings,actual_config):
 """Actual Source3 row binding; Source2 lacked an interval-row snapshot."""
 proof=[]
 for name in dict.fromkeys(row['periodic_task_name']for row in siblings['rows_in_FIFO_order']):
  model=[row for row in periodic if row['name']==name];require(len(model)==1,'UniqueSiblingPeriodicRow');model=model[0];interval=[row for row in intervals if row['id']==model['interval_id']];require(len(interval)==1,'ActualSiblingIntervalRow');interval=interval[0];seconds=actual_config[name]['schedule']
  require(type(interval['every'])is int and type(seconds)is float and seconds.is_integer()and interval['every']==int(seconds)and interval['period']=='seconds'and model['clocked_id']is None and model['crontab_id']is None and model['solar_id']is None,'ExactSourceStaticSecondsInterval')
  proof.append({'periodic_task_name':name,'periodic_id':model['id'],'interval':interval,'configured_seconds':seconds})
 return proof
