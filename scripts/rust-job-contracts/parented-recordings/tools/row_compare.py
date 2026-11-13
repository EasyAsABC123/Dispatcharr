"""Pure complete SQL-row comparison with declared input/identity/time bindings.
No row/timestamp keys are discarded; raw Source/Native fields remain retained.
Original Dispatcharr authors retain application/algorithm/license credit.
"""
import copy,json
from datetime import datetime,timezone,timedelta
from uuid import UUID
TABLES=('channels','sources','epg_data','programmes','source_indices','recordings','recurring_rules','periodic','clocked','sentinel','core_settings','VOD_movies','VOD_logos')
EXECUTION={'created_at','updated_at','date_changed','last_update'}
def require(v,label):
 if v is not True:raise ValueError(label)
def equal(a,b):
 if type(a)is not type(b):return False
 if isinstance(a,dict):return set(a)==set(b)and all(equal(v,b[k])for k,v in a.items())
 if isinstance(a,list):return len(a)==len(b)and all(equal(x,y)for x,y in zip(a,b))
 return a==b
def instant(s):
 require(type(s)is str,'AwareTimestampString');v=datetime.fromisoformat(s);require(v.tzinfo is not None,'ExplicitTimestampOffset');return v.astimezone(timezone.utc)
def key(table):return 'source_id'if table=='source_indices'else'ident'if table=='sentinel'else'id'
class Comparison:
 def __init__(self,reference,actual):
  self.reference=reference;self.actual=actual;self.ids={};self.opaque={};self.clocks={};self.observations=[]
  self.delta=instant(actual['seed']['anchor_utc'])-instant(reference['seed']['anchor_utc'])
  require(instant(actual['seed']['programme_start'])==instant(actual['seed']['anchor_utc'])+timedelta(hours=4),'OwnInputStartPlus4h')
  require(instant(actual['seed']['programme_end'])==instant(actual['seed']['programme_start'])+timedelta(hours=1),'OwnInputEndPlus1h')
 def bind(self,s,n):
  for table in TABLES:
   require(len(s[table])==len(n[table]),'CompleteRowCount:'+table);mapping=self.ids.setdefault(table,{})
   for a,b in zip(s[table],n[table]):
    k=key(table);require(k in a and k in b and type(a[k])is int and type(b[k])is int,'TypedRowKey:'+table)
    old=mapping.setdefault(a[k],b[k]);require(old==b[k],'StableRowKey:'+table)
   require(len(set(mapping.values()))==len(mapping),'OneToOneRowKey:'+table)
 def mapped(self,table,value):
  if value is None:return None
  require(type(value)is int and value in self.ids[table],'KnownForeignKey:'+table);return self.ids[table][value]
 def input(self,s):return instant(s)+self.delta
 def transform(self,table,row):
  x=copy.deepcopy(row);x[key(table)]=self.mapped(table,x[key(table)])
  for name,target in {'channel_id':'channels','epg_data_id':'epg_data','epg_id':'epg_data','epg_source_id':'sources','clocked_id':'clocked','refresh_task_id':'periodic'}.items():
   if name in x and x[name]is not None:x[name]=self.mapped(target,x[name])
  if table=='VOD_movies'and x.get('logo_id')is not None:x['logo_id']=self.mapped('VOD_logos',x['logo_id'])
  if table in('programmes','recordings'):
   for f in('start_time','end_time'):x[f]=self.input(x[f])
  if table=='clocked':x['clocked_time']=self.input(x['clocked_time'])
  if table=='recordings':
   if x['task_id']is not None:
    require(x['task_id']=='dvr-recording-'+str(row['id']),'ExactRecordingTaskName');x['task_id']='dvr-recording-'+str(x['id'])
   props=x['custom_properties'];program=props.get('program')if isinstance(props,dict)else None
   if isinstance(program,dict):
    for f,target in [('id','programmes'),('epg_source_id','sources')]:
     if f in program:program[f]=self.mapped(target,program[f])
    for f in('start_time','end_time'):
     if f in program:program[f]=self.input(program[f])
   if isinstance(props,dict)and'poster_logo_id'in props:props['poster_logo_id']=self.mapped('VOD_logos',props['poster_logo_id'])
  if table=='core_settings'and x['key']=='dvr_settings':
   for r in x['value']['series_rules']:
    r['channel_id']=self.mapped('channels',r['channel_id']);r['epg_source_id']=self.mapped('sources',r['epg_source_id'])
  if table=='periodic':
   args=json.loads(x['args'])
   if x['task']=='apps.channels.tasks.run_recording':
    require(len(args)==4,'FullRecordingTaskArgs');args[0]=self.mapped('recordings',args[0]);args[1]=self.mapped('channels',args[1]);args[2]=self.input(args[2]);args[3]=self.input(args[3]);x['name']='dvr-recording-'+str(args[0]);x['args']=args
   elif x['task']=='apps.epg.tasks.refresh_epg_data':
    require(args==[],'FullRefreshBeatArgs');kwargs=json.loads(x['kwargs']);require(set(kwargs)=={'source_id'},'FullRefreshBeatKwargs');x['args']=[];x['kwargs']={'source_id':self.mapped('sources',kwargs['source_id'])}
  return x
 def value(self,a,b,path):
  if isinstance(a,datetime):require(a==instant(b),'InputTimestamp:'+path);return
  require(type(a)is type(b),'FullValueType:'+path)
  if isinstance(a,dict):
   require(set(a)==set(b),'EveryFieldPresent:'+path)
   for k,v in a.items():self.value(v,b[k],path+'.'+k)
  elif isinstance(a,list):
   require(len(a)==len(b),'OrderedValueCount:'+path)
   for i,(x,y)in enumerate(zip(a,b)):self.value(x,y,path+'.'+str(i))
  else:require(a==b,'ExactBusinessValue:'+path)
 def rows(self,source,native,phase,source_case,native_case):
  self.bind(source,native)
  for table in TABLES:
   for a,b in zip(source[table],native[table]):
    require(set(a)==set(b),'FullRowFieldPresence:'+table);expected=self.transform(table,a);actual=copy.deepcopy(b);path=table+':'+str(a[key(table)])
    if table=='periodic'and actual['task']in('apps.channels.tasks.run_recording','apps.epg.tasks.refresh_epg_data'):actual['args']=json.loads(actual['args'])
    if table=='periodic'and actual['task']=='apps.epg.tasks.refresh_epg_data':actual['kwargs']=json.loads(actual['kwargs'])
    if 'uuid'in a:
     for value in (a['uuid'],b['uuid']):parsed=UUID(value);require(str(parsed)==value and parsed.version==4,'OpaqueModelUUID')
     previous=self.opaque.setdefault(path,(a['uuid'],b['uuid']));require(previous==(a['uuid'],b['uuid']),'StableOpaqueModelIdentity');expected['uuid']=a['uuid'];actual['uuid']=a['uuid']
    for field in EXECUTION:
     if field not in a:continue
     require(type(a[field])is type(b[field]),'ExecutionTimeType:'+path+'.'+field)
     if a[field]is None:continue
     previous_source=next((v for v in source_case['before'][table]if v[key(table)]==a[key(table)]),None)
     changed=phase=='after'and(previous_source is None or previous_source[field]!=a[field])
     sw=source_case['task_window']if changed else[self.reference['lifecycle_began_utc'],source_case['finished_utc']]
     nw=native_case['task_window']if changed else[self.actual['lifecycle_began_utc'],native_case['finished_utc']]
     for value,window,label in [(a[field],sw,'Source'),(b[field],nw,'Native')]:require(instant(window[0])<=instant(value)<=instant(window[1]),label+'ActualExecutionWindow:'+path+'.'+field)
     identifier=path+'.'+field;old=self.clocks.get(identifier)
     if old:require((old[0]==a[field])==(old[1]==b[field]),'SourceChangedUnchangedRelation:'+identifier)
     self.clocks[identifier]=(a[field],b[field]);self.observations.append({'path':identifier,'phase':phase,'Source_raw':a[field],'Native_raw':b[field],'Source_window':sw,'Native_window':nw,'changed_in_task':changed})
     # Classification is explicit and recorded above. Original raw fields stay
     # untouched, including source coincidences and nulls; compare other fields.
     expected[field]=a[field];actual[field]=a[field]
    self.value(expected,actual,phase+'.'+table)
