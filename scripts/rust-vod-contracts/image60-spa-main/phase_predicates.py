"""Finite standalone SPA request predicates; no Source/app execution."""
import hashlib,json,math,re
from pathlib import Path
HERE=Path(__file__).resolve().parent

def need(v,label):
 if not v:raise ValueError(label)
def validate(value,plan=None,reference=None,schema=None):
 plan=json.loads((HERE/'plan.json').read_text())if plan is None else plan;reference=json.loads((HERE/'source-reference.json').read_text())if reference is None else reference;schema=json.loads((HERE/'schema62.json').read_text())if schema is None else schema
 need(value['Native_only']is True and value['Source_requests']==0 and value['resource_sampling']is False and value['full_M4_acceptance']is False,'NativeSingleSliceOnly')
 need(value['candidate_image']==plan['candidate_binding']['image_id']and value['candidate_Git']==plan['candidate_binding']['Git']and value['candidate_tree']==plan['candidate_binding']['tree'],'ExactFreshCandidate')
 need(value['Main_identity']['PID']==1 and value['Main_identity']['UID']==65532 and value['Main_identity']['comm']=='dispatcharr-rs','RealPackagedMain')
 rows=value['cases'];need(type(rows)is list and len(rows)==1,'ExactlyOneNativeBusinessRequest')
 row=rows[0];spec=plan['Native_case'];need(all(row[k]==spec[k]for k in ['id','name','method','path','request_headers']if k in spec),'FixedNativeSourceCounterpart')
 need(row['request_headers']==spec['headers']and row['request_body_hex']==''and not any(k.lower()in ['authorization','cookie']for k in row['request_headers']),'LiteralUnauthenticatedRequest')
 need(row.get('transport_error')is None and row['status']==reference['case']['status']==200,'ActualSourceStatus')
 raw=bytes.fromhex(row['body_hex']);need(raw==bytes.fromhex(reference['case']['body_hex'])and len(raw)==row['body_bytes']==858 and hashlib.sha256(raw).hexdigest()==row['body_sha256']==reference['case']['body_sha256'],'ActualWhole858SourceBody')
 headers={}
 for k,v in row['headers']:headers.setdefault(k.lower(),[]).append(v)
 need(headers.get('content-type')==['text/html; charset=utf-8']and headers.get('content-length')==['858']and'x-worker-id'not in headers,'SelectedActualSourceFieldsNotFullHeaders')
 need(type(row['began_monotonic'])in(int,float)and type(row['completed_monotonic'])in(int,float)and math.isfinite(row['began_monotonic'])and math.isfinite(row['completed_monotonic'])and 0<=row['completed_monotonic']-row['began_monotonic']<=15,'AbsoluteRequest15')
 need(row['absolute_observation']['timer_joined']is True and row['physical_close']['fd_after']==-1 and row['physical_close']['original_inode_absent']is True and row['physical_close']['timer_joined']is True,'ActualSocketTimerSettlement')
 for name in ['owned_before','owned_after','default_before','default_after']:
  snapshot=value[name];need(set(snapshot['tables'])==set(schema['tables'])and set(snapshot['sequences'])==set(schema['sequences']),'MandatoryFull6259:'+name)
  need(all(type(r.get('rows'))is int and 0<=r['rows']<=10000 and type(r.get('sha256'))is str and re.fullmatch('[0-9a-f]{64}',r['sha256'])for r in snapshot['tables'].values()),'TypedTableFingerprints:'+name)
  need(all(type(r.get('last_value'))is int and r['last_value']>=1 and type(r.get('is_called'))is bool for r in snapshot['sequences'].values()),'TypedSequenceFingerprints:'+name)
 need(value['owned_before']==value['owned_after']and value['default_before']==value['default_after'],'DefaultAndRequestPhaseOwnedSQLUnchanged')
 origin=value['origin_identity'];need(origin['listener_ready_actual']is True and type(origin['PID'])is int and origin['PID']>=1 and type(origin['starttime'])is str and origin['starttime'].isdigit()and re.fullmatch('[0-9a-f]{64}',origin['nonce_sha256']),'ActualOwnedOriginTrapReady')
 need(all(value[k]['nonce_sha256']==origin['nonce_sha256']for k in ['origin_before','origin_after','origin_terminal'])and value['origin_terminal']['shutdown_acknowledged']is True and value['origin_terminal']['socket_listener_closed']is True and value['origin_terminal']['handler_threads_joined']is True,'OwnedOriginIdentityAndPhysicalTerminal')
 need(all(v['PID']==origin['PID']and v['starttime']==origin['starttime']and type(v['accepted_TCP_total'])is int and 1<=v['accepted_TCP_total']==len(v['connections'])and v['unclassified_connections']==0 and type(v['control_requests'])is int and v['control_requests']==sum(r['controls']for r in v['connections'])and all(type(r['requests'])is int and type(r['controls'])is int and r['requests']==r['controls']and r['requests']>=1 for r in v['connections'])for v in [value['origin_before'],value['origin_after'],value['origin_terminal']]),'EveryOriginTCPIsAnObservedControlNotProvider')
 need(all(r['socket_closed']is True for r in value['origin_terminal']['connections']),'AllOriginTCPPhysicallyClosed')
 need(value['origin_before']['rows']==value['origin_after']['rows']==value['origin_terminal']['rows']==[]and value['origin_before']['invalid']==value['origin_after']['invalid']==value['origin_terminal']['invalid']==[],'LiteralNoProviderContact')
 need(value['actual_business_request_invocations']==1 and value['Source_replay']==0 and value['no_adaptive_retry']is True,'NoBusinessRetryOrSourceReplay')
 return {'qualified_Native_Main_SPA_GET':1,'status':200,'body_bytes':858,'Source_context':'fresh declared compiled-image57 template; not Originalinstalledartifactequality','auth_provider_dispatch_absence':'code/router-test evidence; actual owned origin no-contact only','full_header_or_encoded_route_or_M4_or_performance':False}
