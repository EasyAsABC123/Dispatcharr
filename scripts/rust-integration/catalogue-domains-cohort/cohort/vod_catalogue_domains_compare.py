"""Exact Source17/Native domain observer; no generic PK/business scrubbing.
Original Dispatcharr and dependency authors retain all application credit.
"""
import copy,gzip,hashlib,json,uuid
from pathlib import Path
import vod_dense_list_compare as dense
import vod_external_identity_compare as ext
import vod_grouped_catalogue_cases as grouped
import vod_catalogue_scale_cases as scale
from vod_dense_list_compare import exact,instant,uuid4,CLOCKS,MUTABLE_CLOCKS

def load(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def safe_sql(value):return ext.safe_sql(value)
def bounded_tables(value,large):
 exact(set(value),set(dense.BUSINESS)|{'support','sequences','observed_at'},'Exact SQL snapshot keys')
 exact(set(value['support']),set(dense.SUPPORT),'Exact support keys')
 # Preserve frozen shape/sequence validation using empty row lists; only this
 # new context-specific observer admits the larger captured row budget.
 shell={k:[]for k in dense.BUSINESS};shell.update(support={k:[]for k in dense.SUPPORT},sequences=value['sequences'],observed_at=value['observed_at']);dense.tables(shell)
 result={**{k:value[k]for k in dense.BUSINESS},**value['support']}
 for name,rows in result.items():
  maximum=(8192 if name in dense.BUSINESS else 512)if large else(128 if name in dense.BUSINESS else 256)
  assert isinstance(rows,list)and len(rows)<=maximum,(name,'Context row budget')
  ids=[row['id']for row in rows];assert all(type(i)is int for i in ids)and len(ids)==len(set(ids)),name
 return result
class SQLState(dense.SQLState):
 def __init__(self,first,mapping,large):
  self.large=large
  super().__init__(first,mapping)
 def canonical(self,snapshot):
  values=bounded_tables(snapshot,self.large)
  result={name:sorted((self.row(name,row)for row in rows),key=lambda r:r['id'])for name,rows in values.items()};result['sequences']=snapshot['sequences'];return result
 def advance(self,label,before,after,fixture=False):
   old=bounded_tables(before,self.large);new=bounded_tables(after,self.large);lo=instant(before['observed_at']);hi=instant(after['observed_at']);assert lo<=hi
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

def response_binding(values,expected):
 records=[r for r in values['Origin']if r.get('host')=='xc-vod-owned.invalid'and'path'in r];assert len(records)==5
 # Actual response bytes/hash are emitted by the unchanged live Origin, not
 # inferred from resulting SQL. Bind all four list documents to frozen payloads.
 for row,action in zip(records[1:],['get_vod_categories','get_series_categories','get_vod_streams','get_series']):
  body=json.dumps(expected[action],separators=(',',':')).encode();assert row['response_bytes']==len(body)and row['response_sha256']==hashlib.sha256(body).hexdigest(),action
 return {action:{'bytes':records[index+1]['response_bytes'],'sha256':records[index+1]['response_sha256']}for index,action in enumerate(['get_vod_categories','get_series_categories','get_vod_streams','get_series'])}
def staged_snapshot(directory,label,side,value):
 path=directory/label/('SQL-'+side+'.json');receipt=load(path.with_suffix('.staging.json'));assert 0<path.stat().st_size<=24*1024*1024 and receipt['bytes']==path.stat().st_size and receipt['sha256']==sha(path)
 assert receipt['all11sequences']and receipt['SQL_executor_UID']==0 and receipt['root_guest_private_file_only']
 counts={name:len(rows)for name,rows in bounded_tables(value,True).items()};dense.exact(counts,receipt['full_table_counts'],'Actual SQL staging complete table counts')
 return {'bytes':receipt['bytes'],'sha256':receipt['sha256'],'full_table_counts':counts,'SQL_executor_UID':0,'raw_bytes_retained':True}
def paginated(raw,after,sqlstate,origin):
 result={}
 for kind,table in [('movies','vod_movie'),('series','vod_series')]:
  current=raw[kind];assert current['actual_base_url']==origin and current['all_pages_collected']and current['count']==len(after[table]);pages=[];ids=[]
  for index,page in enumerate(current['pages']):
   assert page['page_number']==index+1;assert scale.page_number(origin+page['request_path'],origin,kind)[0]==index+1
   reply=page['response'];body=reply['body'];assert reply['status']==200 and body['count']==len(after[table])
   if body['next']is not None:assert scale.page_number(body['next'],origin,kind)[0]==index+2
   else:assert index==len(current['pages'])-1
   if body['previous']is not None:assert scale.page_number(body['previous'],origin,kind)[0]==index
   else:assert index==0
   # Reuse the existing exact SQL-bound API UUID/clock/field projection. It
   # validates full catalogue count while allowing each bounded result page.
   other='series'if kind=='movies'else'movies';pair={kind:reply,other:raw[other]['pages'][0]['response']};projected=dense.catalogue(pair,after,sqlstate,origin)[kind]
   pages.append({'request_path':page['request_path'],'page_number':index+1,'response':projected});ids.extend(row['id']for row in body['results'])
  assert len(ids)==len(set(ids))==len(after[table])and set(ids)=={row['id']for row in after[table]}
  result[kind]={'actual_base_url':origin,'count':len(ids),'pages':pages,'all_pages_collected':True}
 return result

def project(directory,case,origin,backend="Source"):
 phases=scale.PHASES if case==scale.CASE else grouped.STAGES[case];expected_payloads=scale.payloads()if case==scale.CASE else grouped.payloads(case)
 first=directory/phases[0]/'SQL-before.json';setup_first=directory/(phases[0]+'-setup')/'SQL-before.json';initial=load(setup_first if setup_first.exists()else first);sqlstate=SQLState(initial,{},case==scale.CASE)
 owner=next(row for row in initial['m3u_m3uaccount']if row['name']=='Owned VOD ingest');previous=None;cells={};manifest={}
 def record(path):
  path=Path(path);manifest[str(path.relative_to(directory))]=sha(path)
 for label in phases:
  setup_path=directory/(label+'-setup');setup=None
  if setup_path.exists():
   prepared,setup=ext.setup_check(directory,label,previous,sqlstate)
   for key in ext.SETUP_FILES:record(setup_path/(key+'.json'))
   setup['before']=safe_sql(setup['before']);setup['after']=safe_sql(setup['after'])
  else:prepared=previous
  values=ext.capture(directory,label);before=values['SQL-before'];after=values['SQL-after']
  for key in ext.ROOT_FILES:record(directory/label/(key+'.json'))
  if prepared is not None:dense.exact(dense.state(before),dense.state(prepared),'Raw phase/setup chain')
  cb=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);ca=sqlstate.canonical(after)
  ident,backend_result=dense.task(values['frame'],values['backend'],owner['id'],before,after);dense.exact(values['cache-before'],values['cache-after'],'Local cache stable')
  response_hashes=response_binding(values,expected_payloads[label]);custody=None
  if case==scale.CASE:
   custody={side:staged_snapshot(directory,label,side,values['SQL-'+side])for side in ['before','after']}
   for side in ['before','after']:record(directory/label/('SQL-'+side+'.staging.json'))
   catalogue=paginated(values['catalogue'],after,sqlstate,origin)
   expected_pages={}
   for kind in ['movies','series']:
    for page in values['catalogue'][kind]['pages']:
     path=directory/label/'catalogue-pages'/(kind+'-page-'+str(page['page_number']).zfill(3)+'.json');record(path);dense.exact(load(path),{'request_path':page['request_path'],'response':page['response']},'Durable page equals complete catalogue')
  else:catalogue=dense.catalogue(values['catalogue'],after,sqlstate,origin)
  cells[label]={'declared_setup':setup,'before':safe_sql(cb),'after':safe_sql(ca),'clock_effects':{'/'.join(map(str,key)):value for key,value in effects.items()},'HTTP':values['HTTP'],'backend':backend_result,'provider':dense.provider(values['Origin'],owner,before,after),'ordered_WS':dense.websocket(values['WS'],owner['id']),'catalogue':catalogue,'actual_provider_response_binding':response_hashes,'private_SQL_file_custody':custody};previous=after
 return {'status':'actual_Source_capture_qualified'if backend=='Source'else'actual_Native_capture_projected','Source_context':case,'actual_Source_cases':len(phases)if backend=='Source'else 0,'actual_Native_cases':len(phases)if backend=='Native'else 0,'full_business_support_rows_all11sequences_preserved':True,'cells':cells,'performance_verified':False,'full_milestone_accepted':False},manifest


def gzip_json(path,maximum=64*1024*1024):
 path=Path(path)
 with gzip.open(path,'rb')as stream:raw=stream.read(maximum+1);assert len(raw)<=maximum and not stream.read(1),'Bounded gzip oracle'
 return json.loads(raw),raw

def source_origin(public,case,identity):
 controller='scale-only'if case==scale.CASE else'grouped-attempt1';folder=public/'controllers'/controller;proof=load(folder/'HTTP-origin-proof.json');receipt_path=folder/proof['actual_port_query_receipt']['path'];stdout=folder/proof['actual_port_query_stdout']['path'];receipt=load(receipt_path)
 assert sha(receipt_path)==proof['actual_port_query_receipt']['sha256']and sha(stdout)==proof['actual_port_query_stdout']['sha256']==receipt['stdout_sha256']
 assert proof['actual_Source_image_id']==identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
 scope=next(row for row in identity['controllers']if row['name']==controller);assert scope['actual_binding_sha256']==proof['actual_binding_sha256'];assert scope['actual_exit']==(0 if case==scale.CASE else 1)
 argv=proof['actual_port_query_argv'];assert argv[-3:]==['port','baseline','5656'];assert argv[argv.index('-p')+1]==proof['actual_project']
 relocated=copy.deepcopy(receipt);relocated['stdout']=str(stdout);origin=dense.port_receipt(argv,relocated);assert origin==proof['actual_origin'];return origin

def project_pagination_origin(cells,native_origin,Source_origin):
 # Only the actual request origin changes. Every path, query byte/order,
 # page number, fragment rule and response field remains exact.
 for cell in cells.values():
  for kind in['movies','series']:
   value=cell['catalogue'][kind];assert value['actual_base_url']==native_origin;value['actual_base_url']=Source_origin
   for page in value['pages']:
    body=page['response']['body']
    for name in['next','previous']:
     if body[name]is not None:
      scale.page_number(body[name],native_origin,kind);assert body[name].startswith(native_origin+'/');body[name]=Source_origin+body[name][len(native_origin):]

def uuid_independence(public,case,directory,phases):
 for label in phases:
  for stage in[label]+([label+'-setup']if(Path(directory)/(label+'-setup')).exists()else[]):
   source_before,_=gzip_json(public/case/'SQL'/(stage+'-before.json.gz'));source_after,_=gzip_json(public/case/'SQL'/(stage+'-after.json.gz'));native_before=load(Path(directory)/stage/'SQL-before.json');native_after=load(Path(directory)/stage/'SQL-after.json')
   for table in['vod_movie','vod_series']:
    previous={r['id']for r in source_before[table]};native={r['id']:r for r in native_after[table]}
    for row in source_after[table]:
     if row['id']not in previous:uuid4(row['uuid']);uuid4(native[row['id']]['uuid']);assert row['uuid']!=native[row['id']]['uuid'],'Independent real model UUIDv4'
   for name in dense.OWNED:
    source=next(r for r in source_before['m3u_m3uaccount']if r['name']==name);native=next(r for r in native_before['m3u_m3uaccount']if r['name']==name);exact(source['id'],native['id'],'Observed fixture namespace required for exact setup-script SHA')

def require_fixture_namespace(public,case,directory,phases):
 first=phases[0];label=first+'-setup'if(Path(directory)/(first+'-setup')).exists()else first
 expected,_=gzip_json(public/case/'SQL'/(label+'-before.json.gz'));actual=load(Path(directory)/label/'SQL-before.json')
 for name in dense.OWNED:
  left=next(row for row in expected['m3u_m3uaccount']if row['name']==name);right=next(row for row in actual['m3u_m3uaccount']if row['name']==name)
  if left['id']!=right['id']:raise AssertionError(('UnsupportedFixtureAccountNamespace',name,left['id'],right['id']))

def compare_retained_Source(f,directory):
 refs=f.binding['retained_source_vod_catalogue_domains'];assert set(refs)=={'qualification','identity','index','provenance'}
 for ref in refs.values():assert sha(ref['path'])==ref['sha256']
 public=Path(refs['index']['path']).parent;index=load(refs['index']['path']);identity=load(refs['identity']['path']);qualification=load(refs['qualification']['path'])
 assert qualification['Source_three_domains_component_capture_qualified']and qualification['aggregate_actual_Source_cases_passed']==17 and qualification['single_controller_all17_passed']is False
 assert qualification['literal_grouped_controller_exit']==1 and qualification['literal_scale_controller_exit']==0 and qualification['known12_Source_roots_replayed']is False
 assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and identity['actual_captured_Source_roots']==17 and identity['actual_Native_roots']==0
 for row in index['files']:
  path=public/row['path'];assert path.is_relative_to(public)and sha(path)==row['sha256']and path.stat().st_size==row['bytes'],row['path']
 case=f.case;scope=qualification['contexts'][case];Source_origin=source_origin(public,case,identity);dense.exact(scope['actual_Source_origin'],Source_origin,'Qualified Source origin')
 oracle_path=public/scope['semantic_projection']['path'];assert sha(oracle_path)==scope['semantic_projection']['sha256'];oracle,canonical=gzip_json(oracle_path)
 assert hashlib.sha256(canonical).hexdigest()==scope['semantic_projection']['uncompressed_canonical_sha256']and len(canonical)==scope['semantic_projection']['uncompressed_bytes']
 phases=scale.PHASES if case==scale.CASE else grouped.STAGES[case];require_fixture_namespace(public,case,directory,phases)
 native_origin,proof=dense.native_http_origin(f);native,manifest=project(Path(directory),case,native_origin,backend='Native');native=json.loads(json.dumps(native,allow_nan=False))
 Source_cells=copy.deepcopy(oracle['cells']);native_cells=native['cells'];custody={}
 for label in Source_cells:
  left=Source_cells[label].pop('private_SQL_file_custody');right=native_cells[label].pop('private_SQL_file_custody')
  if case==scale.CASE:
   for side in['before','after']:
    exact(left[side]['full_table_counts'],right[side]['full_table_counts'],'Complete staged row counts');assert left[side]['SQL_executor_UID']==right[side]['SQL_executor_UID']==0 and left[side]['raw_bytes_retained']and right[side]['raw_bytes_retained']
   custody[label]={'Source':left,'Native':right,'each_SHA_verified_against_own_actual_bytes':True}
  else:assert left is None and right is None
 if case==scale.CASE:project_pagination_origin(native_cells,native_origin,Source_origin)
 exact(Source_cells,native_cells,(case,'Full Source-derived rows/PK/11seq/wire/API comparison'))
 phases=scale.PHASES if case==scale.CASE else grouped.STAGES[case];uuid_independence(public,case,directory,phases)
 Source_report=load(public/case/'report.json')
 seen_tasks=set()
 for row,label in zip(Source_report['cases'],phases):
  frame=load(Path(directory)/label/'frame.json');ident=frame['headers']['id'];assert ident!=row['actual_root']and ident not in seen_tasks,'Independent unique actual task UUIDv4';seen_tasks.add(ident)
 result={'status':'passed','actual_Source_cases':len(phases),'actual_Native_cases':len(phases),'case':case,'Source_controller_exit':scope['controller_exit'],'Source17_is_single_controller0':False,'Source_origin':Source_origin,'Native_origin':native_origin,'actual_Native_origin_binding':proof,'scale_independent_private_SQL_custody':custody,'actual_Native_capture_SHA':manifest,'all_business_support_rows_all11sequence_values_exact':True,'no_generic_business_or_PK_scrubbing':True,'full_milestone_accepted':False,'performance_verified':False}
 from workflow import save
 save(Path(directory).parent/'Source-comparison.json',result)
