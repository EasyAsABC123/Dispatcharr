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

def source_http_origin(refs,identity,cleanup):
 assert set(refs)=={'proof','receipt','stdout'}
 values={}
 for key,ref in refs.items():
  path=Path(ref['path']);exact(dense.file_sha(path),ref['sha256'],'Source shared origin bytes '+key);values[key]=path
 proof=load(values['proof']);receipt=load(values['receipt']);argv=proof['actual_port_query_argv']
 exact(proof['actual_Source_image_id'],identity['actual_Source_image_id'],'Actual Source image')
 exact(proof['actual_binding_sha256'],identity['actual_binding_sha256'],'Qualified Source binding')
 exact(proof['actual_project'],cleanup['project'],'Actual Source project')
 exact(argv[1:4],['compose','-p',cleanup['project']],'Actual Source port namespace')
 exact(argv[-3:],['port','baseline','5656'],'Actual Source service port')
 exact(proof['baseline_service'],'baseline','Actual Source baseline');exact(proof['container_port'],5656,'Actual Source container port')
 exact(proof['actual_port_query_receipt']['sha256'],refs['receipt']['sha256'],'Bound actual Source receipt')
 exact(proof['actual_port_query_stdout']['sha256'],refs['stdout']['sha256'],'Bound actual Source stdout')
 exact(receipt['argv_sha256'],hashlib.sha256(json.dumps(argv).encode()).hexdigest(),'Actual Source port invocation')
 assert receipt['actual_exit']==0 and receipt['failure']is None and receipt['stdout_EOF']and receipt['stderr_EOF']and receipt['process_group_settled']
 exact(receipt['stdout_sha256'],refs['stdout']['sha256'],'Source receipt stdout')
 origin=dense.http_origin('http://'+values['stdout'].read_text().strip());exact(origin,proof['actual_origin'],'Actual Source origin')
 return origin

def compare_public(source_projection,source_report,source_sql,native_directory,source_origin,native_origin):
 """Only public credential-safe Source evidence; never raw50/private binding."""
 exact(source_projection['status'],'actual_Source_capture_qualified','Actual Source shared projection authority')
 directory=Path(native_directory);initial_n=capture(directory,PHASES[0])['SQL-before'];initial_s=source_sql[PHASES[0]]['before']
 source_to_native=namespace(initial_s,initial_n);mapping={v:k for k,v in source_to_native.items()};sqlstate=SQLState(initial_n,mapping);previous=None;results={}
 owners={key:next(r for r in initial_n['m3u_m3uaccount']if r['name']==name)for key,name in [('A','Owned VOD ingest'),('B','Owned VOD shared B')]}
 source_A=next(r['id']for r in initial_s['m3u_m3uaccount']if r['name']=='Owned VOD ingest')
 exact(source_A,owners['A']['id'],'Declared seed script SHA requires same observed actual A ID; broader allocator namespace unqualified')
 source_tasks={row['name']:row['actual_root']for row in source_report['cases']}
 for label,role in STAGES:
  values=capture(directory,label);before=values['SQL-before'];after=values['SQL-after'];owner=owners[role]
  if previous is not None:exact(state(before),state(previous),'Exact raw phase chain around declared seed')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after,owner['id']);ca=sqlstate.canonical(after);ident,backend=task(values['frame'],values['backend'],owner['id'],owner['name'],before,after)
  uuid4(source_tasks[label]);assert ident!=source_tasks[label],'Independent actual task UUIDv4'
  exact(values['cache-before'],values['cache-after'],'Local cache stable')
  projection={'before':safe_sql(cb),'after':safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate,native_origin,source_origin)}
  exact(json.loads(json.dumps(projection,allow_nan=False)),source_projection['phases'][label],(label,'Complete public Source projection'))
  for table in ('vod_movie','vod_series'):
   existing={r['id']for r in source_sql[label]['before'][table]};source_after={r['id']:r for r in source_sql[label]['after'][table]}
   for row in after[table]:
    if row['id']not in existing:
     uuid4(source_after[row['id']]['uuid']);assert row['uuid']!=source_after[row['id']]['uuid'],'Independent actual global UUIDv4'
  results[label]={'status':'passed','full_business_support_SQL_11seq_typed_entropy_and_selected_wire_equal':True};previous=after
  if label=='shared-A-initial':
   previous,seed=seed_check(directory,after,sqlstate);seed['before']=safe_sql(seed['before']);seed['after']=safe_sql(seed['after'])
   exact(json.loads(json.dumps(seed,allow_nan=False)),source_projection['declared_seed'],'Exact declared two-row/two-sequence seed and typed entropy')
   source_episode=source_sql[SEED]['after']['vod_episode'][0];native_episode=previous['vod_episode'][0]
   uuid4(source_episode['uuid']);assert source_episode['uuid']!=native_episode['uuid'],'Independent actual seed UUIDv4'
 return {'status':'passed','phases':results,'Source_cases':4,'Native_cases':4,'declared_seed_equal':True,'owned_account_mapping_Native_to_Source':mapping,'all11sequence_values_exact':True,'retained_public_Source_authority':True,'private_Source_capture_files_opened':False,'same_actual_A_ID_for_seed_script_required':True,'four_owned_credentials_raw_initial_stable':True,'unrelated_owner_last_seen_raw_stable':True,'typed_fixture_HTTP_origin_binding_applied':True,'raw_encrypted_WS_frame_byte_equivalence_claimed':False,'internal_Source_cache_equivalence_claimed':False,'full_milestone_passed':False,'performance_verified':False}
