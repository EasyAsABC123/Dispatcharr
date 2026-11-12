"""Offline gates only, never scheduling based on counter/status."""
def literal_origin(origin,settled=False):
 if not isinstance(origin,dict)or origin.get('invalid'):return False
 requests=origin.get('requests');connections=origin.get('connections')
 if not isinstance(requests,list)or not requests or not isinstance(connections,list)or not connections:return False
 ids=[c.get('id')for c in connections]
 if len(set(ids))!=len(ids)or any(type(i)is not int or i<=0 for i in ids):return False
 if any(r.get('connection')not in ids for r in requests):return False
 media=[r for r in requests if r.get('path_role')=='shared_movie'and r.get('method')in ['GET','HEAD']]
 if not any(r.get('method')=='GET'and r.get('bytes_written',0)>0 for r in media):return False
 for r in media:
  headers=r.get('request_headers');response=r.get('response_headers')
  if not isinstance(headers,dict)or set(headers)!={'Range','If-Range'}or not isinstance(response,dict)or not {'content-length','content-type','accept-ranges'}<=set(response):return False
  if type(r.get('response_status'))is not int or not 100<=r['response_status']<=599:return False
  for collection in [headers,response]:
   if any(not isinstance(v,list)or len(v)>8 or any(type(x)is not str or len(x.encode())>2048 for x in v)for v in collection.values()):return False
  if settled:
   returned=r.get('serve_file_return')
   if r.get('in_flight')is not False or not isinstance(returned,dict)or returned.get('status')!=r['response_status']or returned.get('body_bytes')!=r.get('bytes_written'):return False
 if settled and(origin.get('actual_open_TCP')!=0 or origin.get('server_thread_settled')is not True or any(c.get('fd_after')!=-1 or c.get('socket_closed')is not True for c in connections)):return False
 return True
def phase(report):
 rows=report.get('clients',[]);capture=all(not c.get('snapshot',{}).get('invalid')for c in report.get('client_closes',{}).values())and len(rows)==2 and[ r.get('role')for r in rows]==['first','second']and not any(report.get(k)for k in ['error_type','close_errors','terminal_error'])and report.get('elapsed_seconds',999)<=120
 checkpoints=report.get('checkpoints',[]);coverage=all(any(r.get('label')==label and not r.get('observation_error')for r in checkpoints)for label in ['drain_5','drain_15','drain_30','fixed_teardown'])
 continuous=coverage and all(set(r.get('readers',{}))=={'first','second'}and all(v.get('invalid')is None and not v.get('eof')and not v.get('reader_done')and type(v.get('socket_fd'))is int and v['socket_fd']>=0 for v in r['readers'].values())for r in checkpoints)
 progress=continuous and all(all(b['readers'][role]['received']>a['readers'][role]['received']for a,b in zip(checkpoints,checkpoints[1:]))for role in ['first','second'])
 decoders=report.get('decoders',{});decode=set(decoders)=={'first','second'}and all(r.get('passed')and r.get('frames',0)>=50 and r.get('retained_sha256')=='b4a9992aa94b69eaaebf85025ec5b55f3ebf6360b143196b0d056fcb04c6b097'for r in decoders.values())
 closes=report.get('client_closes',{})
 close_valid=set(closes)=={'first','second'}and all(c.get('fd_after')==-1 and c.get('reader_joined')is True and c.get('original_inode_absent')is True for c in closes.values())
 timing=close_valid and all(0<=c.get('close_begin',-1)-report.get('fixed_teardown_target',0)<=.35 for c in closes.values())and 0<=report.get('second_dispatch_actual',-1)-report.get('second_dispatch_target',0)<=.35 and 0<=report.get('teardown_began',-1)-report.get('fixed_teardown_target',0)<=.35
 literal=literal_origin(report.get('terminal_origin_before_service_stop'))
 scenario=capture and literal and continuous and progress and decode and timing and all(r.get('status')==200 and r.get('prefix_sha256')=='02921f832147133cdb84ff2dfae8ac4868e95abefdd1cc98a73d54a5c6202a1f'for r in rows)
 return{'HTTP_capture_valid':capture,'scenario_ready':scenario,'continuous_client_read_progress':progress,'literal_snapshot_coverage':coverage,'literal_origin_coverage':literal,'fixed_schedule_valid':timing,'no_reproduction':capture and not scenario,'full_M4_acceptance':False}
def pair(report):
 values=report.get('backends',{});complete=set(values)=={'source','native'}and all(p.get('HTTP_capture_valid')for p in values.values())and report.get('HTTP_pair_complete')and report.get('owned_cleanup_valid')and report.get('default_named_tables_sequences_unchanged')
 physical=all(literal_origin(report.get(mode+'_origin_terminal'),settled=True)for mode in ['source','native'])
 qualified=bool(complete and physical and all(p.get('scenario_ready')for p in values.values()))
 return{'observation_complete':bool(complete),'sharing_characterization_qualified':qualified,'settled_literal_origin_coverage':physical,'no_reproduction':bool(complete and not qualified),'full_M4_acceptance':False}
