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
def source_http_origin(proof_ref,identity,source_cleanup,source_capture_root):
 path=Path(proof_ref['path']);exact(file_sha(path),proof_ref['sha256'],'Source HTTP origin proof bytes');proof=load(path)
 binding_ref=proof['actual_Source_binding'];exact(file_sha(binding_ref['path']),binding_ref['sha256'],'Actual Source binding bytes');exact(binding_ref['sha256'],identity['actual_binding_sha256'],'Qualified Source binding identity')
 binding=load(binding_ref['path']);fixture=Path(source_capture_root).parents[1]
 expected=[binding['docker'],'compose','-p',source_cleanup['project'],'-f',str(Path(binding['support_root'])/'docker/compose.rust-playback.yml'),'-f',str(fixture/'ports.json'),'-f',str(fixture/'compose.json'),'port','baseline','5656']
 exact(proof['argv'],expected,'Source actual request namespace/service port')
 receipt_path=Path(proof['receipt']['path']);exact(receipt_path.parent,fixture,'Source port receipt belongs to actual Source fixture');exact(file_sha(receipt_path),proof['receipt']['sha256'],'Source port receipt hash')
 origin=port_receipt(expected,load(receipt_path));exact(origin,proof['actual_origin'],'Source actual origin')
 return origin
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

def project_actual_source(directory):
 """Validate and describe one actual Source capture; no Native qualification."""
 directory=Path(directory);initial=load(directory/PHASES[0]/'SQL-before.json');sqlstate=SQLState(initial,{})
 ingest=next(r['id']for r in initial['m3u_m3uaccount']if r['name']==OWNED[0]);previous=None;result={}
 for label in PHASES:
  p=directory/label;before=load(p/'SQL-before.json');after=load(p/'SQL-after.json')
  if previous is not None:exact(state(before),state(previous),(label,'Exact Source phase chain'))
  canonical_before=sqlstate.canonical(before);effects=sqlstate.advance(label,before,after);canonical_after=sqlstate.canonical(after)
  ident,backend=task(load(p/'frame.json'),load(p/'backend.json'),ingest,before,after)
  account=next(r for r in before['m3u_m3uaccount']if r['id']==ingest)
  exact(load(p/'cache-before.json'),load(p/'cache-after.json'),(label,'Local Source cache stability'))
  def safe_accounts(value):
   # Public projection preserves exact bootstrap credential hashes while
   # omitting every plaintext login, even pre-existing synthetic fixtures.
   value=copy.deepcopy(value)
   for row in value['m3u_m3uaccount']:
    for key in ('username','password'):
     if isinstance(row[key],str):row[key]={'fixture_login_sha256':hashlib.sha256(row[key].encode()).hexdigest()}
   return value
  result[label]={'before':safe_accounts(canonical_before),'after':safe_accounts(canonical_after),'clock_effects':{'/'.join(map(str,key)):value for key,value in effects.items()},'HTTP':load(p/'HTTP.json'),'backend':backend,'provider':provider(load(p/'Origin.json'),account,before,after),'ordered_WS':websocket(load(p/'WS.json'),ingest),'catalogue':catalogue(load(p/'catalogue.json'),after,sqlstate)}
  previous=after
 return {'status':'actual_Source_capture_qualified','actual_Source_cases':6,'actual_Native_cases':0,'all_eleven_sequence_states_preserved':True,'raw_phase_chain':'all five transitions exact','declared_entropy':'Typed causal creation/update/task clocks, stable generated UUIDv4 and actual fixture account clocks/credentials; no generic scrubbing','phases':result,'full_milestone_passed':False,'performance_verified':False}

def compare(source_directory,native_directory,source_origin=None,native_origin=None):
 assert (source_origin is None)==(native_origin is None),'Both actual HTTP origin proofs required'
 if source_origin is not None:http_origin(source_origin);http_origin(native_origin)
 source_directory=Path(source_directory);native_directory=Path(native_directory)
 def capture(directory,label):
  p=directory/label
  return {key:load(p/(key+'.json'))for key in ('SQL-before','SQL-after','HTTP','frame','backend','Origin','WS','catalogue','cache-before','cache-after')}
 initial_s=capture(source_directory,PHASES[0])['SQL-before'];initial_n=capture(native_directory,PHASES[0])['SQL-before'];mapping=account_namespace(initial_s,initial_n)
 states=(SQLState(initial_s,mapping),SQLState(initial_n,{}));prior=(None,None);phases={}
 source_ingest=next(r['id']for r in initial_s['m3u_m3uaccount']if r['name']==OWNED[0]);native_ingest=mapping[source_ingest]
 for label in PHASES:
  captures=(capture(source_directory,label),capture(native_directory,label));ends=[];canonical=[];effects=[];tasks=[]
  for index,(values,sqlstate,account)in enumerate(zip(captures,states,(source_ingest,native_ingest))):
   before=values['SQL-before'];after=values['SQL-after']
   if prior[index]is not None:exact(state(before),state(prior[index]),(label,'Exact raw phase continuity'))
   exact(values['cache-before'],values['cache-after'],(label,'Local cache unchanged; no cross-backend internal cache parity'))
   canonical.append(sqlstate.canonical(before));effects.append(sqlstate.advance(label,before,after));ends.append(sqlstate.canonical(after))
   tasks.append(task(values['frame'],values['backend'],account,before,after))
  exact(canonical[0],canonical[1],(label,'Full SQL-before and eleven sequences'))
  exact(ends[0],ends[1],(label,'Full SQL-after and eleven sequences'));exact(effects[0],effects[1],(label,'Exact clock effects'))
  assert tasks[0][0]!=tasks[1][0],'Independent actual task UUIDv4 values'
  exact(tasks[0][1],tasks[1][1],(label,'Backend status/body/children and typed entropy'))
  for table in ('vod_movie','vod_series'):
   before_ids={r['id']for r in captures[0]['SQL-before'][table]}
   for srow,nrow in zip(captures[0]['SQL-after'][table],captures[1]['SQL-after'][table]):
    if srow['id']not in before_ids:assert srow['uuid']!=nrow['uuid'],'Independent actual model UUIDv4 values'
  exact(captures[0]['HTTP'],captures[1]['HTTP'],(label,'HTTP root response'))
  provider_rows=[];ws=[];catalogues=[]
  for index,(values,sqlstate,account)in enumerate(zip(captures,states,(source_ingest,native_ingest))):
   owner=next(r for r in values['SQL-before']['m3u_m3uaccount']if r['id']==account)
   provider_rows.append(provider(values['Origin'],owner,values['SQL-before'],values['SQL-after']))
   ws.append(websocket(values['WS'],account));catalogues.append(catalogue(values['catalogue'],values['SQL-after'],sqlstate,source_origin if index==0 else native_origin,source_origin if index==1 else None))
  exact(provider_rows[0],provider_rows[1],(label,'Complete provider trace except bound epoch/credentials'))
  exact(ws[0],ws[1],(label,'Ordered parsed WS events'));exact(catalogues[0],catalogues[1],(label,'Catalogue responses'))
  phases[label]={'full_SQL_before_after_and11sequences_equal':True,'typed_causal_entropy_and_exact_stability':True,'provider_trace_equal':True,'HTTP_backend_catalogue_ordered_WS_equal':True}
  prior=tuple(values['SQL-after']for values in captures)
 return {'status':'passed','phases':phases,'actual_Source_cases':6,'actual_Native_cases':6,'owned_account_mapping':mapping,'generic_JSON_string_or_date_scrubbing':False,'typed_fixture_HTTP_origin_binding_applied':source_origin is not None,'URL_projection_scope':'Only Native catalogue nested logo.cache_url origin; SQL-bound ID/providerURL and full suffix exact','raw_encrypted_WS_frame_byte_equivalence_claimed':False,'internal_Source_cache_equivalence_claimed':False,'full_milestone_passed':False,'performance_verified':False}
