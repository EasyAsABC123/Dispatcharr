#!/usr/bin/env python3
"""Verify this historical public trace; opaque private cache bytes are not re-executed."""
import datetime,gzip,hashlib,io,json,math
from pathlib import Path
HERE=Path(__file__).resolve().parent
def need(ok,label):
 if not ok:raise ValueError(label)
def canon(x):return json.dumps(x,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def sha(b):return hashlib.sha256(b).hexdigest()
def load(name):return json.loads((HERE/name).read_bytes())
def epoch(s):
 need(type(s)is str and len(s)<=64,'TypedUTC');d=datetime.datetime.fromisoformat(s);need(d.tzinfo is not None,'AwareUTC');return d.timestamp()
def observations():
 m=load('projection-manifest.json');raw=(HERE/'observations.json.gz').read_bytes();need(len(raw)<=4194304 and sha(raw)==m['compressed']['sha256']and len(raw)==m['compressed']['bytes'],'BoundedExactCompressedProjection')
 with gzip.GzipFile(fileobj=io.BytesIO(raw))as stream:blob=stream.read(16777217)
 need(len(blob)<=16777216 and len(blob)==m['projection']['bytes']and sha(blob)==m['projection']['sha256'],'BoundedExactProjection');return json.loads(blob)
def validate(d,life):
 ref=load('reference.json');schema=load('schema62-authority.json');plan=load('definition/plan.json');source=load('source32-responses.json')['cases'];headers=load('source32-content-types.json')['rows'];authority=load('coverage-authority.json')
 need(ref['Original_revision']==d['Original_git']==plan['Original_git']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','PinnedOriginal')
 need(d['capture_valid']is True and d['actual_request_count']==d['completed_capture_count']==len(d['requests'])==len(source)==len(headers)==len(plan['cases'])==32,'Mandatory32Coverage')
 need(d['phase']=='complete'and d['passed']is False and d['recorder_is_capture_only']is True and d['connections_close_called']is True,'RawRecorderFalsePreserved')
 need(d['Native_component']is True and d['Source_business_requests']==0 and d['Source_TCP_or_Native']is True and d['force_authenticate_used']is False,'NativeOnlyRealAuthScope')
 need(d['Native_network_or_system_save_signal_claimed']is False,'ImportedContextNotNewSignal')
 need(type(d['Native_phase_seconds'])in(int,float)and math.isfinite(d['Native_phase_seconds'])and 0<d['Native_phase_seconds']<=180,'FixedPhase180')
 found={}
 def walk(x,path='$'):
  if isinstance(x,list):
   for i,v in enumerate(x):walk(v,path+'['+str(i)+']')
  elif isinstance(x,dict):
   if {'audit','public_manifest','settings','users','raw_Redis','Channels'}<=set(x):
    found[path]=sha(canon(x));need(set(x['public_manifest']['tables'])==set(schema['tables'])and set(x['public_manifest']['sequences'])==set(schema['sequences']),'Mandatory62Tables59Sequences');need(len(x['audit']['tables'])==len(x['audit']['sequences'])==11,'Mandatory11OwnedAudit')
   for k,v in x.items():walk(v,path+'.'+k)
 walk(d);need(len(found)==authority['snapshot_count']==69 and found==authority['snapshots'],'Actual69SnapshotIntegrity')
 begin=epoch(life['Native_process_result']['began_utc']);end=epoch(life['Native_process_result']['completed_utc']);prior=begin
 rows=[]
 for i,(r,spec,src,ct)in enumerate(zip(d['requests'],plan['cases'],source,headers),1):
  need(r['id']==spec['id']==src['id']==ct['id']==i and all(r[k]==spec[k]==src[k]for k in('name','role','method')),'ExactFixedRoleAndCaseOrder')
  rows.append({'id':r['id'],'role':r['role'],'method':r['method'],'sha256':sha(canon(r))})
  raw=bytes.fromhex(r['response_body_hex']);need(len(raw)==r['response_body_bytes']and sha(raw)==r['response_body_sha256']and (json.loads(raw)if raw else None)==r['json'],'ActualResponseByteBinding')
  need(r['status']==src['status']and r['response_body_hex']==src['response_body_hex']and next((v for k,v in r['headers'].items()if k.lower()=='content-type'),None)==ct['content_type'],'ActualRetainedSourceResponse32')
  need(r['before']['settings']==src['before_settings']and r['after']['settings']==src['after_settings'],'LiteralSourceSettingDeltas')
  for table,row in r['before']['public_manifest']['tables'].items():
   if table!='core_coresettings':need(row==r['after']['public_manifest']['tables'][table],'Other61TablesUnchanged')
  for name,value in r['before']['public_manifest']['sequences'].items():
   expected=[value[0]+1,True]if name=='core_coresettings_id_seq'and r['status']==201 else value
   need(r['after']['public_manifest']['sequences'][name]==expected,'ExactCreateOnlySequenceDelta')
  a=epoch(r['began_utc']);b=epoch(r['completed_utc']);need(prior<=a<=b<=end,'ActualOrderedRequestWindow');prior=b
  need(r['actual_request_invoked']is True and r['capture_complete']is True and r['transport']=='http_response'and r['framework_exception']is None,'ActualCompleteTCPResponse')
  elapsed=r['elapsed_seconds'];need(type(elapsed)in(int,float)and math.isfinite(elapsed)and 0<elapsed<=15,'FixedAbsoluteHTTP15')
  need(r['socket_settlement']=={'fd_after':-1,'original_inode_absent':True,'timer_joined':True,'request_worker':'calling thread; returned after actual I/O unwind'},'PhysicalSocketAndTimerSettled')
 need(rows==authority['rows'],'ExactActual32RowIntegrity')
 need(life['actual_controller_exit']==1 and life['actual_controller_passed']is False and life['raw_recorder_passed']is False,'HistoricalHost1Preserved')
 need(life['default_after_audit']==life['backend_after_audit']=='unmeasured','HistoricalUnmeasuredNotUpgraded')
 terminal=load('semantic-peer-terminal.json');need(terminal=={'shutdown_drained':True,'active':0,'available_writer_permits':4,'cache_physical_operations':0,'pool_closed':True},'SemanticPeerOwnersAndPoolActuallySettled')
 transcript=(HERE/'native-peer.stdout').read_text();need(transcript.count('core_settings_writes::physical_tests::actual_core32_http_peer ... ok')==1 and '1 passed; 0 failed'in transcript,'ActualSemanticPeerTestTranscript')
 n=life['Native_process_result'];need(n['exit']==0 and n['collection_invalid']is None and n['physically_settled']is True and n['pipe_threads_settled']is True,'ActualSemanticPeer0Settled')
 need(len(life['commands'])==23 and len(life['host_children'])==24 and all(type(x['exit'])is int and type(x['physically_settled'])is bool and x['pipes_settled']is True for x in life['host_children']),'LiteralHistoricalCLIChildCoverage')
 need(sum(x['physically_settled']for x in life['host_children'])==23 and [x['exit']for x in life['host_children']if not x['physically_settled']]==[137],'OriginalCollectionTimeoutNegativePreserved')
 need(life['Main_activated']is False and life['Source_business_requests']==0 and life['whole_M5_acceptance']is False,'NoPromotion')
 return {'public_response_SQL_and_selected_trace_checks':32,'full_private_binary_cache_effect_recomputation':False,'historical_host_exit':1,'default_after':'unmeasured','Main_fullM5_performance':False}
def verify_inventory():
 expected=load('packet-inventory.json');actual={str(p.relative_to(HERE)):sha(p.read_bytes())for p in HERE.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-inventory.json'};need(actual==expected,'ExactPacketInventory');return len(actual)
if __name__=='__main__':print(json.dumps({'public_files':verify_inventory(),**validate(observations(),load('lifecycle.json'))},indent=2))
