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

from vod_shared_metadata_compare import source_http_origin

def compare_public(source_projection,source_report,source_sql,native_directory,source_origin,native_origin):
 """Strict Native projection against published safe Source only; no raw180 opens."""
 exact(source_projection['status'],'actual_Source_capture_qualified','Actual Source external-ID projection authority')
 exact(source_projection['actual_Source_cases'],9,'Actual Source roots');assert source_projection['full_business_support_rows_all11sequences_preserved']
 dense.http_origin(source_origin);dense.http_origin(native_origin)
 directory=Path(native_directory);initial_n=load(directory/(PHASES[0]+'-setup')/'SQL-before.json');initial_s=source_sql[PHASES[0]+'-setup']['before']
 source_to_native=dense.account_namespace(initial_s,initial_n);mapping={v:k for k,v in source_to_native.items()};sqlstate=SQLState(initial_n,mapping)
 owner=next(r for r in initial_n['m3u_m3uaccount']if r['name']=='Owned VOD ingest');source_owner=next(r for r in initial_s['m3u_m3uaccount']if r['name']=='Owned VOD ingest')
 exact(source_owner['id'],owner['id'],'Exact setup script SHA scope requires same observed A account allocation')
 source_tasks={row['name']:row['actual_root']for row in source_report['cases']};exact(set(source_tasks),set(PHASES),'All actual Source task IDs');previous=None;results={}
 for label in PHASES:
  prepared,setup=setup_check(directory,label,previous,sqlstate);setup['before']=safe_sql(setup['before']);setup['after']=safe_sql(setup['after'])
  values=capture(directory,label);before=values['SQL-before'];after=values['SQL-after'];exact(state(before),state(prepared),'Explicit setup after to root before raw continuity')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);ca=sqlstate.canonical(after);ident,backend=dense.task(values['frame'],values['backend'],owner['id'],before,after)
  uuid4(source_tasks[label]);assert ident!=source_tasks[label],'Independent actual task UUIDv4'
  exact(values['cache-before'],values['cache-after'],'Local cache stable')
  projection={'declared_setup':setup,'before':safe_sql(cb),'after':safe_sql(ca),'clock_effects':{'/'.join(map(str,k)):v for k,v in effects.items()},'HTTP':values['HTTP'],'backend':backend,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':dense.catalogue(values['catalogue'],after,sqlstate,native_origin,source_origin)}
  exact(json.loads(json.dumps(projection,allow_nan=False)),source_projection['cells'][label],(label,'Complete public Source external-ID projection'))
  for table in ('vod_movie','vod_series'):
   source_setup=source_sql[label+'-setup']['after'][table];assert len(source_setup)==len(prepared[table])
   for sr,nr in zip(source_setup,prepared[table]):
    uuid4(sr['uuid']);assert sr['uuid']!=nr['uuid'],'Independent actual setup UUIDv4'
   old={r['id']for r in source_sql[label]['before'][table]};source_after={r['id']:r for r in source_sql[label]['after'][table]}
   for row in after[table]:
    if row['id']not in old:
     uuid4(source_after[row['id']]['uuid']);assert row['uuid']!=source_after[row['id']]['uuid'],'Independent actual new model UUIDv4'
  results[label]={'status':'passed','full_setup_root_rows_support_11seq_and_selected_wire_equal':True};previous=after
 return {'status':'passed','actual_Source_cases':9,'actual_Native_cases':9,'cells':results,'owned_account_mapping_Native_to_Source':mapping,'all11sequence_values_exact':True,'retained_public_Source_authority':True,'private_Source_capture_files_opened':False,'same_actual_A_ID_for_seed_script_required':True,'three_owned_credentials_raw_initial_stable':True,'typed_fixture_HTTP_origin_binding_applied':True,'full_milestone_passed':False,'performance_verified':False}
