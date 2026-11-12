"""Offline-only bounded characterization derivation; no requests or runtime code."""
import hashlib,json,math,re
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROLES={'first','second'}
PREFIX='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'
RETAINED='b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097'
TABLES={'accounts_user','m3u_m3uaccount','m3u_m3uaccountprofile','vod_movie','vod_m3umovierelation','vod_series','vod_episode','vod_m3uepisoderelation','core_coresettings'}
def require(value,label):
 if not value:raise ValueError(label)
def number(value):return type(value)in [int,float]and math.isfinite(value)
def integer(value,low=0,high=8388608):return type(value)is int and low<=value<=high
def hex64(value):return type(value)is str and re.fullmatch('[0-9a-f]{64}',value)is not None

def origin(value,settled=False):
 require(type(value)is dict and value.get('invalid')==[],'OriginInvalid')
 require(type(value.get('epoch'))is str and re.fullmatch('[0-9a-f]{32}',value['epoch'])and hex64(value.get('nonce_sha256')),'OriginIdentity')
 connections=value.get('connections');requests=value.get('requests')
 require(type(connections)is list and 1<=len(connections)<=128 and type(requests)is list and 1<=len(requests)<=64,'MandatoryOriginCoverage')
 ids=[c.get('id')for c in connections];numbers=[r.get('id')for r in requests]
 require(all(integer(i,1,128)for i in ids)and set(ids)==set(range(1,len(ids)+1))and all(integer(i,1,64)for i in numbers)and set(numbers)==set(range(1,len(numbers)+1)),'OriginIDs')
 for key in ['accepted_TCP_total','actual_open_TCP','transient_TCP_without_request','control_total','media_HEAD_total','actual_stream_GET_inflight']:require(integer(value.get(key),0,128),'TypedOriginCounter')
 require(all(r.get('connection')in ids for r in requests),'OriginConnectionReferences')
 for c in connections:
  require(number(c.get('opened'))and type(c.get('socket_closed'))is bool and type(c.get('peer_eof_observed'))is bool,'ConnectionScalars')
  require(integer(c.get('requests'),0,64)and c['requests']==sum(r['connection']==c['id']for r in requests),'ConnectionRequestCounts')
  if c['socket_closed']:require(c.get('fd_after')==-1 and type(c['fd_after'])is int and number(c.get('closed'))and c['closed']>=c['opened'],'PhysicalSocketClose')
 require(value.get('accepted_TCP_total')==len(connections)and type(value['accepted_TCP_total'])is int,'AcceptedCount')
 require(value.get('actual_open_TCP')==sum(not c['socket_closed']for c in connections),'OpenCount')
 require(value.get('transient_TCP_without_request')==sum(c['requests']==0 for c in connections),'TransientCount')
 require(value.get('control_total')==sum(r.get('kind')=='control'for r in requests)and value.get('media_HEAD_total')==sum(r.get('kind')=='media_HEAD'for r in requests),'KindCounts')
 require(value.get('actual_stream_GET_inflight')==sum(r.get('kind')=='stream_GET'and r.get('in_flight')is True for r in requests),'InflightCount')
 media=[]
 for r in requests:
  require(r.get('method')in ['GET','HEAD','POST']and r.get('kind')in ['stream_GET','media_HEAD','control']and type(r.get('in_flight'))is bool and number(r.get('began'))and integer(r.get('bytes_written')),'RequestScalars')
  require(r['began']>=next(c['opened']for c in connections if c['id']==r['connection']),'RequestAfterAccept')
  if not r['in_flight']:require(number(r.get('returned'))and r['returned']>=r['began'],'RequestReturn')
  if r['kind']=='control':require(r.get('path_role')is None,'ControlNotViewer');continue
  require(r.get('path_role')=='shared_movie'and r['method']==('GET'if r['kind']=='stream_GET'else'HEAD'),'OwnedMediaOnly')
  request=r.get('request_headers');response=r.get('response_headers');require(type(request)is dict and set(request)=={'Range','If-Range'}and type(response)is dict and {'content-type','content-length','accept-ranges'}<=set(response)<= {'content-type','content-length','accept-ranges','content-range'},'MandatoryLiteralHeaders')
  for collection in [request,response]:
   require(len(json.dumps(collection).encode())<=4096 and all(type(v)is list and len(v)<=8 and all(type(s)is str and len(s.encode())<=2048 for s in v)for v in collection.values()),'LiteralHeaderBounds')
  require(integer(r.get('response_status'),100,599),'ResponseStatus')
  length=response['content-length'];require(len(length)==1 and re.fullmatch('[0-9]{1,8}',length[0])and int(length[0])<=8388608,'ContentLength')
  if r['bytes_written']:
   require(number(r.get('first_body_write'))and number(r.get('last_body_write'))and r['began']<=r['first_body_write']<=r['last_body_write'],'WriteTimes')
   if not r['in_flight']:require(r['last_body_write']<=r['returned'],'LastWriteBeforeReturn')
  errors=r.get('body_write_failures',[]);require(type(errors)is list and len(errors)<=4,'WriteErrorsBound')
  for error in errors:require(type(error.get('exception_class'))is str and len(error['exception_class'])<=64 and number(error.get('at'))and r['began']<=error['at']<=r['returned'],'WriteErrorScalar')
  if settled:
   returned=r.get('serve_file_return');require(type(returned)is dict and returned.get('status')==r['response_status']and returned.get('body_bytes')==r['bytes_written']and integer(returned['body_bytes'])and returned['body_bytes']<=int(length[0]),'BodyReturn')
  if r['method']=='GET':media.append(r)
 require(bool(media)and any(r['bytes_written']>0 for r in media),'BodyBearingMediaCoverage')
 if settled:require(value.get('server_thread_settled')is True and value['actual_open_TCP']==0 and all(not r['in_flight']for r in requests),'PhysicalOriginSettled')
 return {'requests':len(requests),'connections':len(connections),'controls':value['control_total'],'media_HEAD':value['media_HEAD_total'],'transient':value['transient_TCP_without_request'],'media_GET':media}

def audit_schema(audit):
 require(type(audit)is dict and set(audit)=={'database','tables','sequences','dataset_sha256'}and audit['database']=='dispatcharr'and hex64(audit['dataset_sha256']),'MandatoryDefaultAuditSchema')
 require(type(audit['tables'])is dict and set(audit['tables'])==TABLES,'MandatoryAuditTables')
 for value in audit['tables'].values():require(type(value)is dict and set(value)=={'rows','sha256'}and integer(value['rows'],0,9223372036854775807)and hex64(value['sha256']),'AuditTableSchema')
 sequences=audit['sequences'];require(type(sequences)is dict and set(sequences)=={'public.'+table+'_id_seq'for table in TABLES},'MandatoryAuditSequences')
 for value in sequences.values():require(type(value)is dict and set(value)=={'last_value','is_called'}and integer(value['last_value'],1,9223372036854775807)and type(value['is_called'])is bool,'AuditSequenceSchema')

def coverage_authority(data):
 path=HERE/'origin-coverage-authority.json'
 require(hashlib.sha256(path.read_bytes()).hexdigest()=='2a725a2e2d95f16b1092e975f47fbdc2f0a9bedbeba33cab0668062a1dbaa0c0','ImmutableOriginCoverageAuthority')
 authority=json.loads(path.read_text());require(data['raw_summary_sha256']==authority['raw_summary_sha256'],'CoverageRawSummaryBinding')
 canonical=lambda value:hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 for mode in ['source','native']:
  phase=data['summary']['backends'][mode];reference=authority['backends'][mode];terminal=data['summary'][mode+'_origin_terminal']
  require(canonical(terminal)==reference['terminal_sha256']and[r['id']for r in terminal['requests']]==reference['observed_request_ids']and[r['id']for r in terminal['connections']]==reference['observed_connection_ids'],'RetainedTerminalCoverage')
  require(canonical(phase['terminal_origin_before_service_stop'])==reference['phase_terminal_sha256'],'RetainedPhaseTerminalCoverage')
  current={r['label']:canonical(r['origin'])for r in phase['checkpoints']if'origin'in r}
  require(current==reference['checkpoint_sha256'],'RetainedSnapshotCoverage')
 return True

def phase(value,mode,seed,terminal,files):
 require(value.get('mode')==mode and value.get('actual_SQL_Bearer_verified')is True,'ModeAuthWitness')
 for name in ['caps_before','caps_after','caps_terminal']:require(value.get(name)=={'account':2,'profile':2},'FixedMax2')
 clients=value.get('clients');require(type(clients)is list and len(clients)==2 and [r.get('role')for r in clients]==['first','second'],'MandatoryClients')
 for c in clients:
  require(c.get('sid')=='fixture-sharing-same'and c.get('content_uuid')==seed['movie_uuid']and c.get('status')==200 and c.get('prefix_sha256')==PREFIX and not c.get('observation_error'),'ActualClientInputPrefix')
  require(all(number(c.get(k))for k in ['began','headers_at','first_observation_complete'])and c['began']<=c['headers_at']<=c['first_observation_complete'],'ClientTimes')
  absolute=c.get('absolute_observation');require(type(absolute)is dict and absolute.get('timer_joined')is True and number(absolute.get('deadline'))and number(absolute.get('completed'))and absolute['completed']<=absolute['deadline']and c['began']<=absolute['deadline']-24<=c['headers_at'],'AbsoluteObservation')
 first,second=clients;target=first['first_observation_complete']+1;require(abs(value.get('second_dispatch_target',-1)-target)<1e-6 and target<=value.get('second_dispatch_actual',-1)<=second['began']<=target+.35,'FixedSecondStart')
 end=second['first_observation_complete']+35;require(abs(value.get('fixed_teardown_target',-1)-end)<1e-6 and end<=value.get('teardown_began',-1)<=end+.35,'FixedTeardown')
 checkpoints=value.get('checkpoints');labels=['drain_5','drain_15','drain_30','fixed_teardown'];require(type(checkpoints)is list and [c.get('label')for c in checkpoints]==labels,'MandatoryCheckpoints')
 final_origin=origin(terminal,settled=True);requestmap={r['id']:r for r in terminal['requests']};previous=None
 for row,offset in zip(checkpoints,[5,15,30,35]):
  expected=second['first_observation_complete']+offset;require(abs(row.get('target',-1)-expected)<1e-6 and expected<=row.get('at',-1)<=expected+.35,'FixedSnapshotTime')
  readers=row.get('readers');require(type(readers)is dict and set(readers)==ROLES,'MandatoryReaders')
  for role,state in readers.items():
   require(state.get('role')==role and integer(state.get('received'),8192)and state.get('cap')==1048576 and state.get('wire_cap')==8388608 and state.get('retained')==min(state['received'],1048576),'ReaderBytes')
   require(state.get('invalid')is None and state.get('eof')is False and state.get('reader_done')is False and integer(state.get('socket_fd'),0,1048576),'LiveReader')
   require(state.get('prefix_ready')is(state['retained']==1048576)and state.get('retention_does_not_close_or_pause')is True,'RetentionNotClose')
   if previous:require(state['received']>previous[role]['received'],'ContinuousClientProgress')
  previous=readers
  if offset!=35:
   require(not row.get('observation_error')and number(row.get('origin_observation_began'))and number(row.get('origin_observation_completed'))and row['origin_observation_began']<=row['origin_observation_completed'],'OriginObservation')
   require('literal_profile_counter'in row and(row['literal_profile_counter']is None or type(row['literal_profile_counter'])is str and re.fullmatch('-?[0-9]{1,20}',row['literal_profile_counter'])and abs(int(row['literal_profile_counter']))<=9223372036854775807),'ProfileCounterLiteral')
   require(row['at']<=row['origin_observation_began']<=row['origin_observation_completed'],'PhaseRelativeObservationClock')
   current=row.get('origin');origin(current);require(current['epoch']==terminal['epoch']and current['nonce_sha256']==terminal['nonce_sha256'],'EpochBinding')
   for request in current['requests']:
    final=requestmap.get(request['id']);require(final is not None and all(final.get(k)==request.get(k)for k in ['connection','method','path_role','kind','began'])and final['bytes_written']>=request['bytes_written'],'PastOriginCoverage')
 closes=value.get('client_closes');require(type(closes)is dict and set(closes)==ROLES,'MandatoryCloseReceipts')
 decoders=value.get('decoders');require(type(decoders)is dict and set(decoders)==ROLES and type(files)is dict and set(files)==ROLES,'MandatoryDecoderFiles')
 for role,c in closes.items():
  require(number(c.get('close_begin'))and number(c.get('close_complete'))and end<=c['close_begin']<=end+.35 and c['close_complete']>=c['close_begin']and c.get('reader_joined')is True and c.get('fd_after')==-1 and type(c['fd_after'])is int and c.get('original_inode_absent')is True,'PhysicalClientClose')
  state=c.get('snapshot');require(type(state)is dict and state.get('invalid')is None and state.get('received',-1)>=previous[role]['received']and state.get('retained')==1048576,'FinalReaderSnapshot')
  d=decoders[role];require(type(d)is dict and d.get('passed')is True and integer(d.get('frames'),50,100000)and d.get('retained_bytes')==1048576 and d.get('retained_sha256')==RETAINED and d.get('strict_audio_video_seconds')==3 and d.get('minimum_frames')==50 and d.get('process_reaped')is True and d.get('pipes_settled')is True,'MandatoryDecode')
  require(files[role]=={'bytes':1048576,'sha256':RETAINED},'RetainedFileBinding')
 phase_times=[c[k]for c in clients for k in ['began','headers_at','first_observation_complete']]+[c[k]for c in closes.values()for k in ['close_begin','close_complete']]+[c['at']for c in checkpoints]+[c[k]for c in checkpoints for k in ['origin_observation_began','origin_observation_completed']if k in c]+[e['at']for e in value['terminal_origin_before_service_stop']['events']]
 require(all(number(t)for t in phase_times),'FinitePhaseClock')
 require(number(value.get('elapsed_seconds'))and 0<value['elapsed_seconds']<=120 and value['elapsed_seconds']>=max(phase_times)-first['began'],'ElapsedCoversObservedSpan')
 require(number(value.get('elapsed_seconds'))and value['elapsed_seconds']<=120 and not any(value.get(k)for k in ['error_type','terminal_error','close_errors']),'BoundedPhase')
 if mode=='source':
  signals=value.get('Source_signal_rows');require(type(signals)is list and len(signals)==4,'MandatorySourceSignals')
  require({r.get('role')for r in signals}==ROLES and all(type(r.get('request'))is int and 1<=r['request']<=16 and number(r.get('monotonic'))and r.get('sid_sha256')==hashlib.sha256(b'fixture-sharing-same').hexdigest()for r in signals),'TypedSourceSignalCoverage')
  require(len({r['request']for r in signals if r.get('kind')=='request_started'})==2,'UniqueSourceRequests')
  for role in ROLES:
   rows=[r for r in signals if r.get('role')==role];client=next(c for c in clients if c['role']==role)
   require(len(rows)==2 and client['began']<=rows[0]['monotonic']<=client['headers_at']and closes[role]['close_begin']<=rows[1]['monotonic']<=first['began']+value['elapsed_seconds'],'PhaseRelativeSignalClock')
   require(len(rows)==2 and rows[0].get('kind')=='request_started'and rows[1].get('kind')=='request_finalized'and rows[0]['request']==rows[1]['request']and rows[0]['sid_sha256']==rows[1]['sid_sha256']and rows[0]['monotonic']<=rows[1]['monotonic'],'ResponseFinalizationIdentity')
 return {'qualified':True,'media_GET_count':len(final_origin['media_GET']),'literal_nonrange_200':[r['request_headers']['Range']==[]and r['request_headers']['If-Range']==[]and r['response_status']==200 and 'content-range'not in r['response_headers']and r['response_headers']['content-length']==['4299455']for r in final_origin['media_GET']],'origin_body_bytes':[r['bytes_written']for r in final_origin['media_GET']],'write_errors':[[e['exception_class']for e in r.get('body_write_failures',[])]for r in final_origin['media_GET']],'full_delivery':[r['bytes_written']==int(r['response_headers']['content-length'][0])and not r.get('body_write_failures')for r in final_origin['media_GET']],'client_progress':[{r:s['received']for r,s in c['readers'].items()}for c in checkpoints],'literal_profile_counters':[{'checkpoint':r['label'],'value':r['literal_profile_counter'],'observation_begin':r['origin_observation_began'],'observation_end':r['origin_observation_completed']}for r in checkpoints if 'literal_profile_counter'in r],'origin_counts':{k:v for k,v in final_origin.items()if k!='media_GET'},'upstream_to_client_association':'unresolved','two_people_inferred':False}

def validate(data,cleanup):
 require(data.get('actual_exit_code')==0,'ActualExitEvidence');summary=data.get('summary');require(type(summary)is dict and set(summary.get('backends',{}))=={'source','native'},'MandatoryBackends')
 require(type(cleanup)is dict and cleanup.get('project')==summary.get('project')and cleanup.get('remaining')=={'containers':'','networks':'','volumes':''}and cleanup.get('matching_owned_tool_processes')==[],'IndependentCleanup')
 require(summary.get('remaining')==cleanup['remaining']and summary.get('cleanup_errors')==[],'OwnedCleanup')
 audit_schema(summary.get('default_before'));audit_schema(summary.get('default_after'))
 require(summary.get('default_before')==summary.get('default_after')and set(summary['default_before']['tables'])==TABLES,'DefaultAudit')
 require(summary['source_before']['dataset_sha256']==summary['native_before']['dataset_sha256']and summary.get('Source_status_and_main_listener_closed')is True,'IndependentColdDBs')
 seed=data['seed_non_auth_projection'];require(seed['provider_profile_id']==2 and seed['provider_account_id']==2,'IndependentSeedCaps')
 coverage_authority(data)
 results={mode:phase(summary['backends'][mode],mode,seed,summary.get(mode+'_origin_terminal'),data['retained_files'].get(mode))for mode in ['source','native']}
 require(summary.get('actual_Main_image_id')=='sha256:909d506f1c1deb29c60bd360da995eef9d5ff5c7ffa0ea3799bca11f7c93b5d0','ActualImage51')
 require(summary['source_origin_terminal']['epoch']!=summary['native_origin_terminal']['epoch'],'IndependentOriginEpochs')
 for mode in results:
  actual=summary['backends'][mode];require(all(actual.get(k)is True for k in ['HTTP_capture_valid','scenario_ready','continuous_client_read_progress','literal_snapshot_coverage','fixed_schedule_valid','literal_origin_coverage']),'ActualDerivedPhaseAgreement')
 require(all(summary.get(k)is True for k in ['HTTP_pair_complete','observation_complete','sharing_characterization_qualified','settled_literal_origin_coverage','owned_cleanup_valid','default_named_tables_sequences_unchanged']),'ActualDerivedPairAgreement')
 return {'scope':'offline derivation from retained exact field projection only','actual_exit_code':0,'characterization_qualified':True,'phase_results':results,'mandatory_coverage_checked':True,'retained_origin_authority_checked':True,'finite_phase_relative_clocks_checked':True,'mandatory_default_sequence_schema_checked':True,'cleanup_default_audit_derived':True,'response_finalization_not_generator_finally':True,'runtime_or_application_rerun':False,'full_M4_acceptance':False,'TS_sharing_transfer':False,'performance_claim':False}
if __name__=='__main__':
 data=json.loads((HERE/'retained.json').read_text());path=HERE/'independent-cleanup.json';require(hashlib.sha256(path.read_bytes()).hexdigest()==data['independent_cleanup_sha256'],'ExactCleanupInput');print(json.dumps(validate(data,json.loads(path.read_text())),indent=2))
