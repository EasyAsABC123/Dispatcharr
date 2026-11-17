"""Shared-account4 Source/Native observer; original authors retain all credit."""
import base64,copy,datetime,hashlib,json,re,uuid
from pathlib import Path
import vod_dense_list_compare as dense
from vod_dense_list_compare import exact,instant,uuid4,tables,state,CLOCKS,MUTABLE_CLOCKS
PHASES=('shared-A-initial','shared-B-update','shared-A-prune','shared-B-prune')
STAGES=(('shared-A-initial','A'),('shared-B-update','B'),('shared-A-prune','A'),('shared-B-prune','B'))
OWNED=(*dense.OWNED,'Owned VOD shared B')
ROOT_FILES=('SQL-before','SQL-after','HTTP','frame','backend','Origin','WS','catalogue','cache-before','cache-after')
SEED_FILES=('SQL-before','SQL-after','setup-result','seed-script','cache-before','cache-after','provider-before','provider-after','broker-before','broker-after')
SEED='declared-after-A-seed'
def load(p):return json.loads(Path(p).read_text())
def capture(directory,label):return {key:load(Path(directory)/label/(key+'.json'))for key in ROOT_FILES}
def namespace(source,native):
 def indexed(rows):
  result={}
  for name in OWNED:
   values=[r for r in rows if r['name']==name];assert len(values)==1;result[name]=values[0]
  return result
 s=indexed(source['m3u_m3uaccount']);n=indexed(native['m3u_m3uaccount'])
 for values in (s,n):
  A=values['Owned VOD ingest'];B=values['Owned VOD shared B'];skip={'id','name','created_at','updated_at'}
  exact({k:v for k,v in A.items()if k not in skip},{k:v for k,v in B.items()if k not in skip},'B initial transport/business clone equals A')
 mapping={s[name]['id']:n[name]['id']for name in OWNED};assert len(mapping)==len(set(mapping.values()))==4
 exact({r['id']for r in source['m3u_m3uaccount']if r['name']not in OWNED},{r['id']for r in native['m3u_m3uaccount']if r['name']not in OWNED},'Bootstrap account identities exact')
 return mapping
class SQLState(dense.SQLState):
 def row(self,table,raw):
  value=super().row(table,raw)
  if table=='m3u_m3uaccount'and raw['name']=='Owned VOD shared B':
   initial=next(r for r in self.first['m3u_m3uaccount']if r['id']==raw['id'])
   for key in ('username','password'):
    exact(raw[key],initial[key],('Shared B setup credential stable',key))
    if raw[key]is not None:
     assert isinstance(raw[key],str)and 1<=len(raw[key])<=255
     value[key]={'owned_fixture_credential':(raw['name'],key)}
  return value
 def advance(self,label,before,after,selected=None):
   old=tables(before);new=tables(after);lo=instant(before['observed_at']);hi=instant(after['observed_at']);assert lo<=hi
   effects={}
   for table,rows in new.items():
    prior={r['id']:r for r in old[table]}
    for row in rows:
     ident=row['id'];previous=prior.get(ident)
     if 'uuid'in row:
      if previous is None:
       uuid4(row['uuid']);self.remember(table,ident,'uuid',row['uuid'],('new-UUIDv4',table,ident))
      else:exact(row['uuid'],previous['uuid'],(label,table,ident,'stable UUID'))
     for key in CLOCKS&row.keys():
      value=row[key]
      if value is None:
       if previous is not None:exact(value,previous[key],(label,table,ident,key,'null state'))
       continue
      stamp=instant(value)
      if previous is None:
       assert lo<=stamp<=hi,(label,table,ident,key,'causal creation')
       self.remember(table,ident,key,value,('causal-new-clock',label,table,ident,key))
       effect='created'
      elif value==previous[key]:effect='unchanged'
      else:
       assert (table,key)in MUTABLE_CLOCKS,(label,table,ident,key,'unexpected clock mutation')
       assert lo<=stamp<=hi,(label,table,ident,key,'causal update')
       self.remember(table,ident,key,value,('causal-updated-clock',label,table,ident,key));effect='changed'
      if key=='last_seen'and table in ('vod_m3umovierelation','vod_m3useriesrelation'):
       if row['m3u_account_id']==selected:assert lo<=stamp<=hi,(label,table,ident,'selected owner causal last_seen')
       elif previous is not None:exact(value,previous[key],(label,table,ident,'unrelated owner last_seen stable'))
      effects[(table,self.ident(table,ident),key)]=effect
   return effects

def task(frame,backend,account,account_name,before,after):
 headers=frame['headers'];ident=headers['id'];uuid4(ident)
 exact(headers['task'],'apps.vod.tasks.refresh_vod_content','Actual task');exact(headers['root_id'],ident,'Actual root');assert headers['parent_id']is None
 exact(json.loads(base64.b64decode(frame['body'])),[[account],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}],'Selected account task args')
 result=copy.deepcopy(backend);exact(result['task_id'],ident,'Actual result task');result['task_id']='<bound-task-UUIDv4>'
 assert instant(before['observed_at'])<=instant(result['date_done'])<=instant(after['observed_at'])
 result['date_done']='<causal-task-completion>';result['result']=dense.duration(result['result'],'Batch VOD refresh completed for account '+account_name+' in ')
 exact(result['status'],'SUCCESS','Actual success');exact(result['children'],[],'No helper children');return ident,result

def seed_check(directory,previous,sqlstate):
 p=Path(directory)/SEED;values={name:load(p/(name+'.json'))for name in SEED_FILES};before=values['SQL-before'];after=values['SQL-after']
 exact(state(before),state(previous),'Root A after equals seed before')
 left=state(before);right=state(after)
 for value in (left,right):
  for table in ('vod_episode','vod_m3uepisoderelation'):
   value.pop(table);value['sequences'].pop('public.'+table+'_id_seq')
 exact(left,right,'Only two seed tables/two sequence states change')
 for table in ('vod_episode','vod_m3uepisoderelation'):
  assert not before[table]and len(after[table])==1
  key='public.'+table+'_id_seq';old=before['sequences'][key];exact(after['sequences'][key],{'last_value':old['last_value']+(1 if old['is_called']else 0),'is_called':True},'Exactly one seed nextval')
 exact(values['cache-before'],values['cache-after'],'Seed cache unchanged');exact(values['provider-before'],values['provider-after'],'Seed provider unchanged');exact(values['broker-before'],values['broker-after'],'Seed broker unchanged')
 episode=after['vod_episode'][0];stream=after['vod_m3uepisoderelation'][0];A=next(r['id']for r in after['m3u_m3uaccount']if r['name']=='Owned VOD ingest');relation=next(r for r in after['vod_m3useriesrelation']if r['m3u_account_id']==A)
 exact(episode['series_id'],relation['series_id'],'Seed global Series');exact(stream['episode_id'],episode['id'],'Seed Episode FK');exact(stream['series_relation_id'],relation['id'],'Seed SeriesRelation FK');exact(stream['m3u_account_id'],A,'Seed owner')
 exact([episode[k]for k in ('name','description','air_date','rating','duration_secs','season_number','episode_number','tmdb_id','imdb_id','custom_properties')],['Owned Shared Episode A','Explicit shared-owner fixture descendant',None,'7.0',60,1,1,None,None,{'fixture':'shared-owner-A'}],'Exact declared Episode values')
 exact([stream[k]for k in ('stream_id','container_extension','custom_properties')],['owned-shared-episode-A','mkv',{'fixture':'shared-owner-A'}],'Exact declared Episode relation values')
 canonical_before=sqlstate.canonical(before);effects=sqlstate.advance(SEED,before,after,None);canonical_after=sqlstate.canonical(after)
 return after,{'before':canonical_before,'after':canonical_after,'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'setup_result':values['setup-result'],'seed_script':values['seed-script']}

def safe_sql(value):
 value=copy.deepcopy(value)
 for row in value['m3u_m3uaccount']:
  for key in ('username','password'):
   if isinstance(row[key],str):row[key]={'fixture_login_sha256':hashlib.sha256(row[key].encode()).hexdigest()}
 return value

def project_actual_source(directory):
 directory=Path(directory);initial=capture(directory,PHASES[0])['SQL-before'];namespace(initial,initial);sqlstate=SQLState(initial,{})
 owners={key:next(r for r in initial['m3u_m3uaccount']if r['name']==name)for key,name in [('A','Owned VOD ingest'),('B','Owned VOD shared B')]};previous=None;phases={};seed=None
 for label,role in STAGES:
  values=capture(directory,label);before=values['SQL-before'];after=values['SQL-after'];account=owners[role]
  if previous is not None:exact(state(before),state(previous),'Exact raw root chain around declared seed')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after,account['id']);ca=sqlstate.canonical(after);ident,backend=task(values['frame'],values['backend'],account['id'],account['name'],before,after)
  exact(values['cache-before'],values['cache-after'],'Local cache stable')
  phases[label]={'before':safe_sql(cb),'after':safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],account,before,after),'ordered_WS':dense.websocket(values['WS'],account['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate)}
  previous=after
  if label=='shared-A-initial':
   previous,seed=seed_check(directory,after,sqlstate);seed['before']=safe_sql(seed['before']);seed['after']=safe_sql(seed['after'])
 return {'status':'actual_Source_capture_qualified','actual_Source_cases':4,'actual_Native_cases':0,'full_rows_support_and11sequences_preserved':True,'raw_chain':'Exact except declared two-table/two-sequence seed','phases':phases,'declared_seed':seed,'full_milestone_passed':False,'performance_verified':False}

def compare(source_directory,native_directory,source_origin,native_origin):
 dense.http_origin(source_origin);dense.http_origin(native_origin)
 directories=(Path(source_directory),Path(native_directory));initials=tuple(capture(d,PHASES[0])['SQL-before']for d in directories);mapping=namespace(*initials);states=(SQLState(initials[0],mapping),SQLState(initials[1],{}));previous=(None,None);results={}
 owners=tuple({key:next(r for r in initial['m3u_m3uaccount']if r['name']==name)for key,name in [('A','Owned VOD ingest'),('B','Owned VOD shared B')]}for initial in initials)
 for label,role in STAGES:
  captures=tuple(capture(d,label)for d in directories);projected=[];tasks=[]
  for index,(values,sqlstate,owner)in enumerate(zip(captures,states,(owners[0][role],owners[1][role]))):
   before=values['SQL-before'];after=values['SQL-after']
   if previous[index]is not None:exact(state(before),state(previous[index]),'Exact raw phase chain around seed')
   cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after,owner['id']);ca=sqlstate.canonical(after);ident,backend=task(values['frame'],values['backend'],owner['id'],owner['name'],before,after);tasks.append(ident)
   exact(values['cache-before'],values['cache-after'],'Local cache stable')
   projected.append({'before':cb,'after':ca,'effects':effects,'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate,source_origin if index==0 else native_origin,source_origin if index==1 else None)})
  exact(projected[0],projected[1],(label,'Full selected shared-account observations'))
  for table in ('vod_movie','vod_series'):
   existing={r['id']for r in captures[0]['SQL-before'][table]}
   for sr,nr in zip(captures[0]['SQL-after'][table],captures[1]['SQL-after'][table]):
    if sr['id']not in existing:assert sr['uuid']!=nr['uuid'],'Independent new global UUIDv4 values'
  assert tasks[0]!=tasks[1],'Independent actual task UUIDv4'
  results[label]={'status':'passed','full_business_support_SQL_11seq_typed_entropy_and_selected_wire_equal':True};previous=tuple(v['SQL-after']for v in captures)
  if label=='shared-A-initial':
   pairs=tuple(seed_check(d,p,s)for d,p,s in zip(directories,previous,states));exact(pairs[0][1],pairs[1][1],'Exact declared fixture seed and typed entropy');assert pairs[0][0]['vod_episode'][0]['uuid']!=pairs[1][0]['vod_episode'][0]['uuid'],'Independent actual seed UUIDv4 values';previous=tuple(p[0]for p in pairs)
 return {'status':'passed','phases':results,'Source_cases':4,'Native_cases':4,'declared_seed_equal':True,'owned_account_mapping':mapping,'all11sequence_values_exact':True,'full_milestone_passed':False,'performance_verified':False}
