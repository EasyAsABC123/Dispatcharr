"""Explicit Source/Native dense-list6 comparison; actual Source authority required.

No generic JSON/string/date scrubbing. Original Dispatcharr authors retain credit.
This module consumes captures; it neither invokes Source bodies nor mutates SQL.
"""
import base64,copy,datetime,hashlib,json,math,re,uuid
from pathlib import Path

PHASES=('fresh-dense','identical-repeat','nonblank-metadata-update','sparse-blank-and-logo-clear','duplicate-representative-and-reuse','last-owner-prune')
BUSINESS=('m3u_m3uaccount','vod_movie','vod_series','vod_episode','vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation')
SUPPORT=('vod_vodlogo','vod_vodcategory','vod_m3uvodcategoryrelation','core_systemevent')
SEQUENCES={'public.'+table+'_id_seq'for table in (*BUSINESS,*SUPPORT)}
OWNED=('Owned VOD ingest','Owned VOD disabled','Owned VOD non-XC')
CLOCKS={'created_at','updated_at','last_seen','last_advanced_refresh','last_episode_refresh'}
MUTABLE_CLOCKS={('vod_vodcategory','updated_at'),('vod_m3uvodcategoryrelation','updated_at'),('vod_m3umovierelation','last_seen'),('vod_m3useriesrelation','last_seen')}
ACCOUNT_FKS={'vod_m3umovierelation','vod_m3useriesrelation','vod_m3uepisoderelation','vod_m3uvodcategoryrelation'}

def load(path):return json.loads(Path(path).read_text())
def exact(left,right,label):
 # Python considers True==1 and 1==1.0; JSON field types remain contractual.
 if type(left)is not type(right):raise AssertionError((label,'value type differs'))
 if isinstance(left,dict):
  if set(left)!=set(right):raise AssertionError((label,'field keys differ'))
  for key in left:exact(left[key],right[key],(label,key))
 elif isinstance(left,(list,tuple)):
  if len(left)!=len(right):raise AssertionError((label,'value count differs'))
  for index,(a,b)in enumerate(zip(left,right)):exact(a,b,(label,index))
 elif left!=right:raise AssertionError(label)
def instant(value):
 assert isinstance(value,str),'Typed datetime string required'
 result=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
 assert result.utcoffset()is not None,'Timezone-aware datetime required'
 return result
def uuid4(value):
 assert isinstance(value,str)and uuid.UUID(value).version==4,'Typed UUIDv4 required'
def file_sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def http_origin(value):
 assert isinstance(value,str)and re.fullmatch(r'http://127\.0\.0\.1:([0-9]{1,5})',value),'Owned loopback HTTP origin required'
 assert 1<=int(value.rsplit(':',1)[1])<=65535
 return value
def port_receipt(argv,receipt):
 exact(receipt['argv_sha256'],hashlib.sha256(json.dumps(argv).encode()).hexdigest(),'Actual compose-port invocation')
 assert receipt['actual_exit']==0 and receipt['failure']is None and receipt['stdout_EOF']and receipt['stderr_EOF']and receipt['process_group_settled']
 path=Path(receipt['stdout']);exact(file_sha(path),receipt['stdout_sha256'],'Port stdout bytes')
 return http_origin('http://'+path.read_text().strip())
def source_http_origin(proof_ref,identity,source_cleanup):
 # Portable, byte-bound exports; historical paths are provenance, never opened.
 path=Path(proof_ref['path']);exact(file_sha(path),proof_ref['sha256'],'Source origin export')
 proof=load(path);origin=load(path.parent/'Source-origin-original.json')
 exact(file_sha(path.parent/'Source-origin-original.json'),proof['original_proof_sha256'],'Original origin proof')
 exact(origin['actual_Source_binding']['sha256'],identity['actual_binding_sha256'],'Source binding authority')
 argv=origin['argv'];exact(argv[1:4],['compose','-p',source_cleanup['project']],'Source namespace')
 exact(argv[-3:],['port','baseline','5656'],'Source service port')
 receipt=load(path.parent/'Source-port-receipt.json');exact(file_sha(path.parent/'Source-port-receipt.json'),origin['receipt']['sha256'],'Original Source receipt')
 exact(receipt['argv_sha256'],hashlib.sha256(json.dumps(argv).encode()).hexdigest(),'Source port invocation')
 assert receipt['actual_exit']==0 and receipt['failure']is None and receipt['stdout_EOF']and receipt['stderr_EOF']and receipt['process_group_settled']
 stdout=path.parent/'Source-port.stdout';exact(file_sha(stdout),origin['stdout']['sha256'],'Source port stdout');exact(file_sha(stdout),receipt['stdout_sha256'],'Receipt stdout')
 actual=http_origin('http://'+stdout.read_text().strip());exact(actual,origin['actual_origin'],'Source origin')
 baseline=proof['baseline'];exact(baseline['image'],identity['actual_Source_image_id'],'Actual Source image')
 exact(baseline['name'],'/'+source_cleanup['project']+'-baseline-1','Actual Source baseline')
 exact(baseline['ports']['5656/tcp'],[{'HostIp':'127.0.0.1','HostPort':actual.rsplit(':',1)[1]}],'Actual published Source port')
 assert origin['fixture_API_client_no_Host_or_forwarded_header_overrides']is True
 return actual
def native_http_origin(f):
 # Reuse the current context's already executed endpoint() receipt. The one
 # bounded inspect verifies Main's existing container network, not a new app.
 argv=f.compose+['port','baseline','9192'];digest=hashlib.sha256(json.dumps(argv).encode()).hexdigest();matches=[r for r in f.cli if r.get('argv_sha256')==digest];assert matches
 receipt=matches[-1];origin=port_receipt(argv,receipt);exact(origin,f.base_url,'Actual current Native API base URL')
 main_path=Path(f.output)/(f.native+'-inspection.json');main=load(main_path)
 exact(main['Name'],'/'+f.native,'Current metadata Main identity');exact(main['Image'],f.binding['candidate_image_id'],'Current metadata Main image')
 assert 'RUST_BIND=0.0.0.0:9192'in main['Config']['Env']
 assert 'DISPATCHARR_TRUSTED_PROXIES=none'in main['Config']['Env']
 baseline_name=f.project+'-baseline-1';baseline=json.loads(f.command([f.docker,'inspect',baseline_name],seconds=15,secret=True))[0]
 exact(baseline['Name'],'/'+baseline_name,'Actual baseline service identity');assert baseline['State']['Running']
 mode=main['HostConfig']['NetworkMode'];assert mode in('container:'+baseline['Id'],'container:'+baseline_name),'Main shares actual baseline network namespace'
 proof={'actual_origin':origin,'context':f.case,'actual_Main_CID':main['Id'],'actual_Main_image':main['Image'],'actual_Main_RUST_BIND':'0.0.0.0:9192','actual_Main_trusted_proxies':'none','actual_baseline_CID':baseline['Id'],'actual_baseline_name':baseline_name,'actual_Main_network_mode':mode,'actual_port_receipt':{'path':str(Path(receipt['stdout']).with_suffix('.json')),'stdout_sha256':receipt['stdout_sha256'],'argv_sha256':receipt['argv_sha256']},'actual_Main_inspection_sha256':file_sha(main_path),'API_calls_use_base_URL_without_Host_or_forwarded_header_overrides':True,'private_environment_values_exported':False}
 return origin,proof
def tables(value):
 exact(set(value),set(BUSINESS)|{'support','sequences','observed_at'},'Exact SQL snapshot keys')
 exact(set(value['support']),set(SUPPORT),'Exact support table keys')
 exact(set(value['sequences']),SEQUENCES,'All eleven sequence names required')
 for state in value['sequences'].values():
  exact(set(state),{'last_value','is_called'},'Exact sequence state shape')
  assert type(state['last_value'])is int and type(state['is_called'])is bool
 result={**{k:value[k]for k in BUSINESS},**value['support']}
 for name,rows in result.items():
  assert isinstance(rows,list)and len(rows)<=256,(name,'bounded rows')
  ids=[row['id']for row in rows];assert all(type(i)is int for i in ids)and len(ids)==len(set(ids)),name
 return result
def state(value):
 result=copy.deepcopy(value);result.pop('observed_at');return result

def account_namespace(source,native):
 def owned(rows):
  result={}
  for name in OWNED:
   matches=[r for r in rows if r['name']==name];assert len(matches)==1,(name,'exact owned setup')
   result[name]=matches[0]
  return result
 s=owned(source['m3u_m3uaccount']);n=owned(native['m3u_m3uaccount'])
 mapping={s[name]['id']:n[name]['id']for name in OWNED}
 assert len(mapping)==len(set(mapping.values()))==3
 source_boot={r['id']for r in source['m3u_m3uaccount']if r['name']not in OWNED}
 native_boot={r['id']for r in native['m3u_m3uaccount']if r['name']not in OWNED}
 exact(source_boot,native_boot,'Bootstrap account IDs remain exact')
 assert not(source_boot&set(mapping.values()))
 return mapping

class SQLState:
 def __init__(self,initial,mapping):
  self.mapping=mapping;self.slots={};self.first=initial
  for row in initial['m3u_m3uaccount']:
   for key in ('created_at','updated_at'):
    if row[key]is not None:
     assert instant(row[key])<=instant(initial['observed_at']),'Setup account clock predates first root'
     self.remember('m3u_m3uaccount',row['id'],key,row[key],('fixture-account-clock',self.ident('m3u_m3uaccount',row['id']),key))
 def ident(self,table,ident):return self.mapping.get(ident,ident)if table=='m3u_m3uaccount'else ident
 def remember(self,table,ident,key,value,token):self.slots[(table,ident,key,value)]=list(token)
 def row(self,table,raw):
  result=copy.deepcopy(raw);ident=raw['id'];result['id']=self.ident(table,ident)
  if table in ACCOUNT_FKS:result['m3u_account_id']=self.mapping.get(raw['m3u_account_id'],raw['m3u_account_id'])
  for key in ('uuid',*CLOCKS):
   if key in result and result[key]is not None:
    token=self.slots.get((table,ident,key,result[key]))
    if token is not None:result[key]={'declared_entropy_slot':token}
  if table=='m3u_m3uaccount'and raw['name']in OWNED:
   initial=next(r for r in self.first['m3u_m3uaccount']if r['id']==ident)
   for key in ('username','password'):
    exact(raw[key],initial[key],('Owned credential remains exact initial setup',raw['name'],key))
    if result[key]is not None:
     assert isinstance(result[key],str)and 1<=len(result[key])<=255,'Owned credential shape'
     result[key]={'owned_fixture_credential':(raw['name'],key)}
  return result
 def canonical(self,snapshot):
  values=tables(snapshot)
  result={name:sorted((self.row(name,row)for row in rows),key=lambda r:r['id'])for name,rows in values.items()}
  result['sequences']=snapshot['sequences']
  return result
 def advance(self,label,before,after):
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
     if key=='last_seen'and table in ('vod_m3umovierelation','vod_m3useriesrelation'):assert lo<=stamp<=hi,(label,table,ident,'causal last_seen')
     effects[(table,self.ident(table,ident),key)]=effect
  return effects

def duration(message,prefix):
 assert isinstance(message,str)
 match=re.fullmatch(re.escape(prefix)+r'([0-9]+\.[0-9]{2}) seconds',message);assert match,'Exact root duration slot'
 assert math.isfinite(float(match[1]))
 return prefix+'<typed-two-decimal-duration> seconds'
def task(frame,backend,account,before,after):
 headers=frame['headers'];ident=headers['id'];uuid4(ident)
 exact(headers['task'],'apps.vod.tasks.refresh_vod_content','Actual task name');exact(headers['root_id'],ident,'Actual root binding');assert headers['parent_id']is None
 exact(json.loads(base64.b64decode(frame['body'])),[[account],{},{'callbacks':None,'errbacks':None,'chain':None,'chord':None}],'Actual task args/options')
 result=copy.deepcopy(backend);exact(result['task_id'],ident,'Backend task identity');result['task_id']='<bound-task-UUIDv4>'
 assert instant(before['observed_at'])<=instant(result['date_done'])<=instant(after['observed_at']),'Causal task completion'
 result['date_done']='<causal-task-completion>';result['result']=duration(result['result'],'Batch VOD refresh completed for account Owned VOD ingest in ')
 exact(result['status'],'SUCCESS','Actual task success');exact(result['children'],[],'No invented helper children')
 return ident,result
def provider(trace,account,before,after):
 result=[];lo=instant(before['observed_at']).timestamp();hi=instant(after['observed_at']).timestamp()
 for raw in trace:
  row=copy.deepcopy(raw);epoch=row.pop('epoch');assert type(epoch)in(int,float)and math.isfinite(epoch)and lo<=epoch<=hi,'Causal provider trace epoch'
  for key in ('username','password'):
   if key in row.get('query_sha256',{}):
    expected=hashlib.sha256(account[key].encode()).hexdigest();exact(row['query_sha256'][key],[expected],'Actual owned provider credential binding');row['query_sha256'][key]=['<bound-owned-'+key+'-sha256>']
  result.append(row)
 return result
def websocket(raw,account):
 receiver=raw['actual_receiver'];match=re.fullmatch(r'specific\.aggregate_([0-9a-f]{32})!receiver',receiver);assert match;uuid4(str(uuid.UUID(hex=match[1])))
 result=copy.deepcopy(raw['events'])
 for event in result:
  exact(event['__asgi_channel__'],[receiver],'Actual WS receiver');event['__asgi_channel__']=['<bound-owned-receiver>']
  exact(event['data']['account'],account,'Actual WS account');event['data']['account']='<bound-owned-ingest-account>'
  if event['data']['status']=='success':event['data']['message']=duration(event['data']['message'],'VOD refresh completed in ')
 return result
def catalogue(raw,after,sqlstate,actual_origin=None,projected_origin=None):
 result=copy.deepcopy(raw)
 for kind,table in [('movies','vod_movie'),('series','vod_series')]:
  assert result[kind]['status']==200
  values={r['id']:r for r in after[table]}
  exact(result[kind]['body']['count'],len(values),'Catalogue global count')
  for row in result[kind]['body']['results']:
   sql=values[row['id']];canonical=sqlstate.row(table,sql)
   exact(row['uuid'],sql['uuid'],'Catalogue UUID binds actual SQL')
   row['uuid']=canonical['uuid']
   for key in ('created_at','updated_at'):
    exact(instant(row[key]),instant(sql[key]),'Catalogue clock binds actual SQL')
    row[key]=canonical[key]
   if row.get('logo')is not None and actual_origin is not None:
    logo=row['logo'];exact(logo['id'],sql['logo_id'],'Catalogue logo ID binds actual object')
    support=next(r for r in after['support']['vod_vodlogo']if r['id']==logo['id'])
    exact(logo['url'],support['url'],'Catalogue provider URL binds actual SQL')
    suffix='/api/vod/vodlogos/'+str(logo['id'])+'/cache/?v='+hashlib.md5(support['url'].encode()).hexdigest()[:8]
    exact(logo['cache_url'],http_origin(actual_origin)+suffix,'Exact request-origin cache URL/path/query/hash; no fragment/userinfo')
    if projected_origin is not None:logo['cache_url']=http_origin(projected_origin)+suffix
 return result

def compare_public(source_projection,source_report,source_sql,native_directory,source_origin,native_origin):
 """Strict Native projection against public Source values; no raw Source files."""
 exact(source_projection['status'],'actual_Source_capture_qualified','Source authority projection')
 native_directory=Path(native_directory)
 initial_n=load(native_directory/PHASES[0]/'SQL-before.json')
 initial_s=source_sql[PHASES[0]]['before']
 source_to_native=account_namespace(initial_s,initial_n);mapping={v:k for k,v in source_to_native.items()}
 sqlstate=SQLState(initial_n,mapping);previous=None;phases={};projected={}
 ingest=next(r['id']for r in initial_n['m3u_m3uaccount']if r['name']==OWNED[0])
 source_tasks={r['name']:r['actual_root']for r in source_report['cases']}
 def public_accounts(value):
  value=copy.deepcopy(value)
  for row in value['m3u_m3uaccount']:
   for key in ('username','password'):
    if isinstance(row[key],str):row[key]={'fixture_login_sha256':hashlib.sha256(row[key].encode()).hexdigest()}
  return value
 for label in PHASES:
  directory=native_directory/label
  values={k:load(directory/(k+'.json'))for k in ('SQL-before','SQL-after','HTTP','frame','backend','Origin','WS','catalogue','cache-before','cache-after')}
  before=values['SQL-before'];after=values['SQL-after'];expected=source_projection['phases'][label]
  if previous is not None:exact(state(before),state(previous),(label,'Exact raw phase continuity'))
  exact(values['cache-before'],values['cache-after'],(label,'Local cache stability'))
  canonical_before=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);canonical_after=sqlstate.canonical(after)
  ident,backend=task(values['frame'],values['backend'],ingest,before,after)
  uuid4(source_tasks[label]);assert ident!=source_tasks[label],'Independent actual task UUIDv4'
  for table in ('vod_movie','vod_series'):
   source_before={r['id']for r in source_sql[label]['before'][table]}
   source_after={r['id']:r for r in source_sql[label]['after'][table]}
   for row in after[table]:
    if row['id']not in source_before:
     uuid4(source_after[row['id']]['uuid']);assert row['uuid']!=source_after[row['id']]['uuid'],'Independent actual model UUIDv4'
  account=next(r for r in before['m3u_m3uaccount']if r['id']==ingest)
  projection={'before':public_accounts(canonical_before),'after':public_accounts(canonical_after),'clock_effects':{'/'.join(map(str,key)):value for key,value in effects.items()},'HTTP':values['HTTP'],'backend':backend,'provider':provider(values['Origin'],account,before,after),'ordered_WS':websocket(values['WS'],ingest),'catalogue':catalogue(values['catalogue'],after,sqlstate,native_origin,source_origin)}
  # Tuple entropy tokens serialize as arrays in the published JSON contract.
  projection=json.loads(json.dumps(projection,allow_nan=False))
  exact(projection,expected,(label,'Exact public Source projection'))
  projected[label]=projection
  phases[label]={'full_SQL_before_after_and11sequences_equal':True,'typed_causal_entropy_and_exact_stability':True,'provider_trace_equal':True,'HTTP_backend_catalogue_ordered_WS_equal':True}
  previous=after
 return {'status':'passed','phases':phases,'actual_Source_cases':6,'actual_Native_cases':6,'retained_public_Source_authority':True,'private_Source_capture_files_opened':False,'owned_account_mapping_Native_to_Source':mapping,'generic_JSON_string_or_date_scrubbing':False,'typed_fixture_HTTP_origin_binding_applied':True,'URL_projection_scope':'Only Native catalogue nested logo.cache_url origin; SQL-bound ID/providerURL and full suffix exact','raw_encrypted_WS_frame_byte_equivalence_claimed':False,'internal_Source_cache_equivalence_claimed':False,'full_milestone_passed':False,'performance_verified':False}
