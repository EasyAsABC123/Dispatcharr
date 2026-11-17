"""External-ID9 typed Source/Native observer; original authors retain credit."""
import copy,datetime,hashlib,json,uuid
from pathlib import Path
import vod_dense_list_compare as dense
from vod_dense_list_compare import exact,instant,uuid4,tables,state,CLOCKS,MUTABLE_CLOCKS
PHASES=('fresh-priority','duplicate-ID-occurrences','same-provider-ID-repeat','external-ID-name-year-change','alias-and-zero-precedence','changed-primary-existing-target','sparse-ID-omission','secondary-ID-recheck','duplicate-secondary-conflict')
ROOT_FILES=('SQL-before','SQL-after','HTTP','frame','backend','Origin','WS','catalogue','cache-before','cache-after')
SETUP_FILES=('SQL-before','script','SQL-after','setup-result','cache-before','cache-after','provider-before','provider-after','broker-before','broker-after')
VOD=('vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation')
def load(p):return json.loads(Path(p).read_text())
def capture(directory,label):return {key:load(Path(directory)/label/(key+'.json'))for key in ROOT_FILES}
class SQLState(dense.SQLState):
 def advance(self,label,before,after,fixture=False):
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
       if fixture and key in ('last_seen','last_advanced_refresh','last_episode_refresh'):
        expected='2020-01-01T00:00:00+00:00'if key=='last_seen'else'2020-01-02T00:00:00+00:00'
        exact(stamp,instant(expected),'Exact declared setup refresh/seen marker');effect='declared-fixed'
       else:
        assert lo<=stamp<=hi,(label,table,ident,key,'causal creation')
        self.remember(table,ident,key,value,('causal-new-clock',label,table,ident,key));effect='created'
      elif value==previous[key]:effect='unchanged'
      else:
       assert (table,key)in MUTABLE_CLOCKS,(label,table,ident,key,'unexpected clock mutation')
       assert lo<=stamp<=hi,(label,table,ident,key,'causal update')
       self.remember(table,ident,key,value,('causal-updated-clock',label,table,ident,key));effect='changed'
      if not fixture and key=='last_seen'and table in ('vod_m3umovierelation','vod_m3useriesrelation'):assert lo<=stamp<=hi,(label,table,ident,'causal last_seen')
      effects[(table,self.ident(table,ident),key)]=effect
   return effects

def setup_check(directory,label,previous,sqlstate):
 p=Path(directory)/(label+'-setup');values={key:load(p/(key+'.json'))for key in SETUP_FILES};before=values['SQL-before'];after=values['SQL-after']
 if previous is not None:exact(state(before),state(previous),'Prior root to explicit setup before exact')
 left=state(before);right=state(after)
 for value in (left,right):
  for table in VOD:value.pop(table);value['sequences'].pop('public.'+table+'_id_seq')
 exact(left,right,'Only six VOD entity/relation tables and sequences change during setup')
 exact(values['cache-before'],values['cache-after'],'Setup cache stable');exact(values['provider-before'],values['provider-after'],'Setup provider stable');exact(values['broker-before'],values['broker-after'],'Setup broker stable')
 spec=values['script']['seed_spec'];assert set(spec)=={'movie','series'}
 for kind,table,relation in [('movie','vod_movie','vod_m3umovierelation'),('series','vod_series','vod_m3useriesrelation')]:
  exact(len(after[table]),len(spec[kind]),'Declared model seed count');exact(len(after[relation]),sum(r['stream']is not None for r in spec[kind]),'Declared relation seed count')
  for item,row in zip(spec[kind],after[table]):
   exact({k:row[k]for k in ['name','year','tmdb_id','imdb_id']},{k:item[k]for k in ['name','year','tmdb_id','imdb_id']},'Exact seed identity');exact(row['language'],'','Language-empty seed');exact(row['description'],'seed metadata','Exact seed metadata');exact(row['custom_properties'],{'fixture_marker':'external-id-seed'},'Exact seed CP')
  for name,count in [(table,len(spec[kind])),(relation,sum(r['stream']is not None for r in spec[kind]))]:
   key='public.'+name+'_id_seq';old=before['sequences'][key];expected=old if count==0 else{'last_value':old['last_value']+count-(0 if old['is_called']else 1),'is_called':True};exact(after['sequences'][key],expected,'Automatic setup nextvals only')
 assert not after['vod_episode']and not after['vod_m3uepisoderelation']
 for name in ['vod_episode','vod_m3uepisoderelation']:exact(before['sequences']['public.'+name+'_id_seq'],after['sequences']['public.'+name+'_id_seq'],'No Episode setup allocation')
 cb=sqlstate.canonical(before);effects=sqlstate.advance(label+'-setup',before,after,True);ca=sqlstate.canonical(after)
 return after,{'before':cb,'after':ca,'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'script':values['script'],'setup_result':values['setup-result']}

def safe_sql(value):
 value=copy.deepcopy(value)
 for row in value['m3u_m3uaccount']:
  for key in ('username','password'):
   if isinstance(row[key],str):row[key]={'fixture_login_sha256':hashlib.sha256(row[key].encode()).hexdigest()}
 return value

def project_actual_source(directory):
 directory=Path(directory);initial=load(directory/(PHASES[0]+'-setup')/'SQL-before.json');sqlstate=SQLState(initial,{})
 owner=next(r for r in initial['m3u_m3uaccount']if r['name']=='Owned VOD ingest');previous=None;cells={}
 for label in PHASES:
  prepared,setup=setup_check(directory,label,previous,sqlstate);values=capture(directory,label);before=values['SQL-before'];after=values['SQL-after'];exact(state(before),state(prepared),'Setup after equals actual root before')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);ca=sqlstate.canonical(after);ident,backend=dense.task(values['frame'],values['backend'],owner['id'],before,after)
  exact(values['cache-before'],values['cache-after'],'Local cache stable');setup['before']=safe_sql(setup['before']);setup['after']=safe_sql(setup['after'])
  cells[label]={'declared_setup':setup,'before':safe_sql(cb),'after':safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate)};previous=after
 return {'status':'actual_Source_capture_qualified','actual_Source_cases':9,'actual_Native_cases':0,'full_business_support_rows_all11sequences_preserved':True,'explicit_setup_boundaries':'Nine six-table/six-sequence reset/seed deltas with no manual sequence reset, unrelated state raw-exact','cells':cells,'full_milestone_passed':False,'performance_verified':False}

def compare(source_directory,native_directory,source_origin,native_origin):
 dense.http_origin(source_origin);dense.http_origin(native_origin);directories=(Path(source_directory),Path(native_directory));initials=tuple(load(d/(PHASES[0]+'-setup')/'SQL-before.json')for d in directories);mapping=dense.account_namespace(*initials);states=(SQLState(initials[0],mapping),SQLState(initials[1],{}));previous=(None,None);owners=tuple(next(r for r in initial['m3u_m3uaccount']if r['name']=='Owned VOD ingest')for initial in initials)
 exact(owners[0]['id'],owners[1]['id'],'Exact setup script SHA scope requires same observed account allocation');results={}
 for label in PHASES:
  setups=tuple(setup_check(d,label,p,s)for d,p,s in zip(directories,previous,states));exact(setups[0][1],setups[1][1],(label,'Full declared setup and typed UUID/clocks'))
  for table in ['vod_movie','vod_series']:
   for sr,nr in zip(setups[0][0][table],setups[1][0][table]):assert sr['uuid']!=nr['uuid'],'Independent actual setup UUIDv4'
  captures=tuple(capture(d,label)for d in directories);projected=[];tasks=[]
  for index,(values,prepared,sqlstate,owner)in enumerate(zip(captures,(x[0]for x in setups),states,owners)):
   before=values['SQL-before'];after=values['SQL-after'];exact(state(before),state(prepared),'Explicit setup after to root before raw continuity')
   cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);ca=sqlstate.canonical(after);ident,backend=dense.task(values['frame'],values['backend'],owner['id'],before,after);tasks.append(ident)
   exact(values['cache-before'],values['cache-after'],'Local cache stability')
   projected.append({'before':cb,'after':ca,'effects':effects,'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate,source_origin if index==0 else native_origin,source_origin if index==1 else None)})
  exact(projected[0],projected[1],(label,'Full external-ID observations and eleven exact sequences'));assert tasks[0]!=tasks[1]
  for table in ['vod_movie','vod_series']:
   old={r['id']for r in captures[0]['SQL-before'][table]}
   for sr,nr in zip(captures[0]['SQL-after'][table],captures[1]['SQL-after'][table]):
    if sr['id']not in old:assert sr['uuid']!=nr['uuid'],'Independent new model UUIDv4'
  results[label]={'status':'passed','full_setup_root_rows_support_11seq_and_selected_wire_equal':True};previous=tuple(v['SQL-after']for v in captures)
 return {'status':'passed','actual_Source_cases':9,'actual_Native_cases':9,'cells':results,'owned_account_mapping':mapping,'all11sequence_values_exact':True,'full_milestone_passed':False,'performance_verified':False}
