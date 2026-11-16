"""Actual Main counterparts of retained Source98 TCP producer authority.
No Source task-body replay; Original Dispatcharr authors retain full credit.
"""
import base64,hashlib,json,time,uuid
from pathlib import Path
from urllib.parse import urlsplit
ROUTES={'names':'set-names-from-epg/','tvg':'set-tvg-ids-from-epg/','logos':'set-logos-from-epg/','bulk':'from-stream/bulk/'}

def rebind_body(value,old,new):
 value=json.loads(json.dumps(value))
 if not isinstance(value,dict):return value
 for field,key in [('channel_ids','channel'),('stream_ids','stream')]:
  if isinstance(value.get(field),list):value[field]=[new[key]if type(v)is int and v==old[key]else v for v in value[field]]
 if type(value.get('epg_source_id'))is int and value['epg_source_id']==old['source']:value['epg_source_id']=new['source']
 return value

def credentials(f,role):
 if role=='anonymous':return {}
 if role=='admin_x_api_key':return {'X-API-Key':f.accounts['admin']['api_key']}
 if role=='admin_authorization_api_key':return {'Authorization':'ApiKey '+f.accounts['admin']['api_key']}
 if role in ['bad_bearer','bad_bearer_with_valid_api_key']:headers={'Authorization':'Bearer invalid.owned.fixture'}
 else:headers={'Authorization':'Bearer '+f.accounts['admin'if role=='admin_spoof'else role]['access']}
 if role=='bad_bearer_with_valid_api_key':headers['X-API-Key']=f.accounts['admin']['api_key']
 if role=='admin_spoof':headers.update({'X-Forwarded-For':'203.0.113.1','X-Real-IP':'203.0.113.1'})
 return headers

def normalized_reply(v):
 if isinstance(v,dict):return {k:('ACTUAL_TASK_ID'if k=='task_id'else normalized_reply(x))for k,x in v.items()}
 if isinstance(v,list):return [normalized_reply(x)for x in v]
 return v

def expected_body(reference,old,new,route):
 body=json.loads(base64.b64decode(reference['frame']['body']))
 if route=='logos':
  body[1]=rebind_body(body[1],old,new)
 else:
  key='stream'if route=='bulk'else'channel'
  if body[0]and isinstance(body[0][0],list):body[0][0]=[new[key]if type(v)is int and v==old[key]else v for v in body[0][0]]
 return body

def frame_review(actual,reference,old,new,route,inspection,representations):
 f=actual['frame'];s=reference['frame'];ident=f['headers']['id'];body=json.loads(base64.b64decode(f['body']));wanted=expected_body(reference,old,new,route)
 h=dict(f['headers']);sh=dict(s['headers']);p=dict(f['properties']);sp=dict(s['properties'])
 dynamic_headers=['id','root_id','origin','argsrepr','kwargsrepr'];dynamic_properties=['correlation_id','reply_to','delivery_tag']
 for k in dynamic_headers:h.pop(k,None);sh.pop(k,None)
 for k in dynamic_properties:p.pop(k,None);sp.pop(k,None)
 checks={'Source_static_headers_exact':h==sh,'Source_static_properties_exact':p==sp,'actual_bound_args_kwargs_callback':body==wanted,'argsrepr':f['headers']['argsrepr']==representations['argsrepr'],'kwargsrepr':f['headers']['kwargsrepr']==representations['kwargsrepr'],'actual_Main_PID_hostname':f['headers']['origin']=='gen1@'+inspection['Config']['Hostname'],'root_correlation':f['headers']['root_id']==ident==f['properties']['correlation_id'],'UUID4_task':uuid.UUID(ident).version==4,'UUID3_reply':uuid.UUID(f['properties']['reply_to']).version==3,'UUID4_delivery':uuid.UUID(f['properties']['delivery_tag']).version==4,'content_type':f['content-type']==s['content-type'],'content_encoding':f['content-encoding']==s['content-encoding']}
 return {'passed':all(checks.values()),'checks':checks,'actual_task_id':ident,'source_raw_frame_sha256':reference['sha256'],'binding_scope':'Only actual fixture selector IDs and generative identities rebound. PID1/actual inspected hostname; UUIDv3 identity implementation separately Source-qualified, opaque internal OID not fabricated.'}

def run(f,op,control,source_path,out,inspection,module,save,receiver):
 raw=Path(source_path).read_bytes();source=json.loads(raw)
 if len(source['cases'])!=98 or sum(len(r['new_family_frames'])for r in source['cases'])!=29 or source['Source_task_body_calls']!=0:raise ValueError('ExactSource98ProducerAuthorityRequired')
 seed=op('seed_http');result={'passed':False,'source_sha256':hashlib.sha256(raw).hexdigest(),'Source_replays':0,'Original_Django_setup_management_processes':1,'pinned_Celery_pure_library_processes':0,'Source_API_calls':0,'Source_task_body_calls':0,'Native_actual_HTTP_attempts':0,'expected_cases':98,'expected_producer_frames':29,'seed':seed,'cases':[],'execution_order':'All non-bulk cells first, then bulk cells: retains Source producer-only source-ID count context while Native consumes every actual frame. No database resets/task mocking.','Source_task_body_authority':'Source9 is qualified separately. Source98 captured producers without task consumers; these29 terminal observations are Native functional/no-FIFO-block checks, not newly measured Source task-result parity.','full_parity':False}
 def persist():save(out/'http98.json',result)
 http=module('absolute_http');old=source['seed_ids'];ordered=sorted(enumerate(source['cases']),key=lambda item:item[1]['definition']['route']=='bulk')
 try:
  for source_index,reference in ordered:
   f.remaining(1);spec=reference['definition'];row={'source_index':source_index,'name':spec['name'],'definition':spec,'passed':False};result['cases'].append(row);persist()
   try:
    op('clear',receiver);result['Original_Django_setup_management_processes']+=1;row['network_control']=op('http_network',spec['control'],f.accounts['admin']['id']);mark=control('frame_mark')['index'];body=rebind_body(spec['body'],old,seed);headers={'Accept':'application/json','Content-Type':'application/json',**credentials(f,spec['credential_role'])}
    row['credential_role']=spec['credential_role'];row['credential_header_names']=sorted(headers);row['credential_value_hashes']={k:hashlib.sha256(v.encode()).hexdigest()for k,v in headers.items()if k.lower()in ['authorization','x-api-key']}
    payload=spec['raw_body'].encode()if spec['raw_body']is not None else(None if body is None else json.dumps(body).encode());url=urlsplit(f.base_url);base='http://127.0.0.1:'+str(url.port);observation=http.Observation(base,seconds=min(20,f.remaining(20)))
    result['Native_actual_HTTP_attempts']+=1;row['attempted_monotonic']=time.monotonic()
    try:
     response=observation.request('/api/channels/channels/'+ROUTES[spec['route']],headers,method=spec['method'],body=payload);data=response.raw.read(1048577);row.update(status=response.status_code,headers=response.raw.getheaders(),raw_body_base64=base64.b64encode(data).decode(),raw_body_sha256=hashlib.sha256(data).hexdigest());persist()
     if len(data)>1048576:raise ValueError('BoundedHTTPBody1MiB')
     row['absolute_HTTP']=observation.complete()
     try:row['JSON']=json.loads(data)
     except (ValueError,UnicodeDecodeError):row['JSON']=None
    finally:row['actual_physical_HTTP_close']=observation.cancel();persist()
    frames=control('frames_since',index=mark)['frames'];row['actual_new_family_frames']=frames;expected=reference['new_family_frames'];checks={'Source_status_exact':row['status']==reference['status'],'Source_family_frame_count':len(frames)==len(expected),'actual_HTTP_socket_retired':row['actual_physical_HTTP_close']['original_inode_absent']is True}
    if reference.get('JSON')is not None:checks['Source_JSON_body_exact_bound_identity']=normalized_reply(row['JSON'])==normalized_reply(reference['JSON'])
    else:checks['Source_nonJSON_body_exact']=row['raw_body_base64']==reference['raw_body_base64']
    source_headers={k.lower():v for k,v in reference['headers']};actual_headers={k.lower():v for k,v in row['headers']}
    checks['Source_semantic_headers_exact']={k:actual_headers.get(k)for k in ['content-type','allow','www-authenticate']}=={k:source_headers.get(k)for k in ['content-type','allow','www-authenticate']}
    row['frame_reviews']=[];row['actual_terminal_observations']=[]
    for index,actual in enumerate(frames):
     ident=actual['frame']['headers']['id']
     if index<len(expected):
      authority=expected[index];wanted=expected_body(authority,old,seed,spec['route']);result['pinned_Celery_pure_library_processes']+=1;representation=op('representations',wanted[0],wanted[1]);row.setdefault('pinned_Celery_pure_repr_evaluations',[]).append(representation);review=frame_review(actual,authority,old,seed,spec['route'],inspection,representation);row['frame_reviews'].append(review)
     terminal=f.wait(lambda:(lambda x:x if x['metadata']and x['metadata']['status']in ['SUCCESS','FAILURE','REVOKED']else None)(op('backend',ident)),45,'ActualProducerFrameConsumedTerminal:'+spec['name']);row['actual_terminal_observations'].append(terminal)
    checks['Source29_publication_semantics']=len(row['frame_reviews'])==len(expected)and all(v['passed']for v in row['frame_reviews']);checks['actual_all_published_frames_terminal_no_FIFO_block']=len(row['actual_terminal_observations'])==len(frames)
    row['Native_post_cell_SQL_progress']=op('http_effects',receiver)
    row['checks']=checks;row['passed']=all(checks.values());persist()
   except Exception as error:
    row['primary_error']={'type':type(error).__name__,'message':str(error)};row['passed']=False;persist()
    if getattr(error,'observer_infrastructure_failure',False):
     result['infrastructure_failure']=row['primary_error'];result['remaining_HTTP_cases_blocked']='Actual publication observer is dead/unsettled; do not issue further dependent HTTP'
     persist();break
 finally:
  if f.functional_budget_expired():f.begin_cleanup()
  try:result['Original_Django_setup_management_processes']+=1;result['network_restored']=op('http_network',None,f.accounts['admin']['id'])
  except Exception as error:result['network_restore_error']=str(error)
 result['actual_frame_count']=sum(len(r.get('actual_new_family_frames',[]))for r in result['cases']);result['passed']=len(result['cases'])==98 and result['Native_actual_HTTP_attempts']==98 and result['actual_frame_count']==29 and all(r['passed']for r in result['cases'])and'network_restore_error'not in result;persist();return result
