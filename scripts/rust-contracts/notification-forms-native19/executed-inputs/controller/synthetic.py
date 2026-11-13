"""Synthetic Native19-shaped corruption specimens, never executed observations."""
import copy,datetime,hashlib,json
from validate_native import SOURCE,PLAN,Clocks,clock,GROUP,SYS,VERSION,N,D
NONCE='a1b2c3d4e5'
def encode(value):return json.dumps(value,separators=(',',':')).encode()
def shift(text):return(clock(text)+datetime.timedelta(hours=1)).isoformat()
def timed(value,generated):
 output=copy.deepcopy(value)
 if type(output)is dict:
  for key,item in list(output.items()):
   if key in('created_at','updated_at','dismissed_at','date_joined','last_login','expires_at')and type(item)is str and clock(item)in generated:output[key]=shift(item)
 return output
def sql(source):
 generated=Clocks(SOURCE).generated;tables={name:[timed(row,generated)for row in entry['raw_rows']]for name,entry in source['audit']['tables'].items()};seq={name.removeprefix('public.').removesuffix('_id_seq'):copy.deepcopy(value)for name,value in source['audit']['sequences'].items()}
 for row,role in zip(tables['accounts_user'],PLAN['roles']):row.update(api_key='native-forms-api-'+NONCE+'-'+role['alias'],username='native-forms-'+NONCE+'-'+role['alias'])
 return {'tables':tables,'sequences':seq}
NETWORK=next(r for r in SOURCE['requests'][0]['after']['cache']['rows']if bytes.fromhex(r['key_hex']).decode()==GROUP)
def cache(warm):
 rows=[];named={key:{'raw_GET_hex':None,'decoded':None}for key in(GROUP,SYS,VERSION)}
 if warm:
  row={k:copy.deepcopy(NETWORK[k])for k in('key_hex','dump_hex','pttl')};row['pttl']=300000;rows=[row];named[GROUP]={'raw_GET_hex':NETWORK['value_hex'],'decoded':{'UI':'127.0.0.0/8'}}
 return {'rows':rows,'named_decoded':named,'api_key_failed_auth_history':None,'api_key_failed_auth_raw_hex':None}
def redis(warm):
 scopes=[{'database':i,'rows':[],'capture_bytes':0}for i in range(4)]
 if warm:
  row={k:copy.deepcopy(NETWORK[k])for k in('key_hex','dump_hex','value_hex')};row.update(type='string',pttl=300000);scopes[1]['rows']=[row]
 return scopes
def body(row,value):
 raw=encode(value)if value is not None else b'';row.update(json=value,response_body_hex=raw.hex(),response_body_sha256=hashlib.sha256(raw).hexdigest(),response_body_bytes=len(raw))
def example():
 generated=Clocks(SOURCE).generated;seed=sql(SOURCE['seed']);native={'_synthetic_only':True,'passed':False,'capture_only_passed_field':True,'capture_complete':True,'actual_request_count':19,'database':'rust_probe_notifforms_native_'+NONCE,'Source_projection_sha256':'1c3f5dcce14ffe9a773cb29833c4caef52ed15c813cf9473f6c397dc34ca240f','Source_reference_mode':'projected-notification-forms-source12-reference-v1','Original_raw_sha256':'b72af1de2683fa9e2345281d24cdaa7418368e1af0404eecba9ba018555a8d84','full_cache_parity_claimed':False,'actual_auth_backend_introspection':False,'Source_application_runs':0,'Main_application_runs':0,'seed_SQL':seed,'seed_events':[],'seed_users':copy.deepcopy(seed['tables']['accounts_user']),'seed_started_utc':shift(SOURCE['seed_started_utc']),'seed_completed_utc':shift(SOURCE['seed_completed_utc']),'cache_after_seed':cache(False),'requests':[],'Native_transport_diagnostics':[],'server_graceful_wait0':True}
 for source,spec in zip(SOURCE['requests'],PLAN['cases']):
  index=spec['index'];row={k:copy.deepcopy(source[k])for k in('index','actor','method','path','target_alias','body_sha256','content_type','status')};row.update(actual_request_invoked=True,headers={k.lower():v for k,v in source['headers'].items()},raw_headers_private={'HTTP_X_API_KEY':('native-forms-api-'+NONCE+'-'+spec['actor']).encode().hex()},before_SQL=sql(source['before']),after_SQL=sql(source['after']),before_events=[],after_events=[],before_cache=cache(index!=1),after_cache=cache(True),before_raw_Redis=redis(index!=1),after_raw_Redis=redis(True),began_utc=shift(source['began_utc']),completed_utc=shift(source['completed_utc']))
  response=timed(source['json'],generated);body(row,response);native['requests'].append(row)
 previous=native['requests'][-1]['after_SQL'];last=native['requests'][-1]['completed_utc']
 specs=[('anonymous',False,'application/json',401),('anonymous',True,'application/json',401),('standard',False,'application/json',403),('standard',True,'application/json',403),('admin',False,'application/json',400),('admin',True,'application/json',400),('admin',False,'text/plain',400)]
 for index,(actor,large,media,status)in enumerate(specs,13):
  request=b'x'*2097153 if large else b'{';row={'index':index,'actor':actor,'oversize':large,'content_type':media,'predicted_status_predeclared':status,'status':status,'actual_request_invoked':True,'raw_headers_private':{}if actor=='anonymous'else{'HTTP_X_API_KEY':('native-forms-api-'+NONCE+'-'+actor).encode().hex()},'body_bytes':len(request),'body_sha256':hashlib.sha256(request).hexdigest(),'before_SQL':copy.deepcopy(previous),'after_SQL':copy.deepcopy(previous),'before_events':[],'after_events':[],'before_cache':cache(True),'after_cache':cache(True),'before_raw_Redis':redis(True),'after_raw_Redis':redis(True),'began_utc':last,'completed_utc':last};body(row,{'detail':'Authentication credentials were not provided.'if actor=='anonymous'else'You do not have permission to perform this action.'if actor=='standard'else'JSON parse error'});native['Native_transport_diagnostics'].append(row)
 native.update(final_SQL=copy.deepcopy(previous),final_events=[],final_cache=cache(True),final_raw_Redis=redis(True));return native
