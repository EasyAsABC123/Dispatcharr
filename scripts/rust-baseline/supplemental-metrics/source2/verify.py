"""Portable Source2 retained-data verifier; no Source replay/private fallback."""
import argparse,hashlib,json
from pathlib import Path
import numeric
HERE=Path(__file__).resolve().parent
LABELS=['before-reader','after-warm-before-window','after-physical-close','retention']
ROLES={'backend','worker','origin','infrastructure'}
def verify(root=HERE,deep_repo=None):
 root=Path(root);load=lambda n:json.loads((root/n).read_text());s=load('verified-source2.json')
 for file,key in [('observations.json','observations_sha256'),('ts-header-projection.json','TS_projection_sha256'),('raw-authorities.json','raw_authorities_sha256')]:
  if hashlib.sha256((root/file).read_bytes()).hexdigest()!=s[key]:raise ValueError('ProjectionHash:'+file)
 o=load('observations.json');headers=load('ts-header-projection.json');points=o['supplemental_points'];assert s['passed']is True and s['process_exit']==0 and o['report_passed']and o['process_exit']==0 and [p['label']for p in points]==LABELS
 previous=o['Redis_observer_setup']['prime']['server'];rows=0;role_points=0;overhead=0
 for index,point in enumerate(points):
  assert point['coverage_complete']and not point['errors']and set(point['roles'])==ROLES and point['resource_snapshots']and not point['resource_window_measurement']
  assert numeric.close(point['end_monotonic']-point['begin_monotonic'],point['overhead_seconds'])and 0<=point['overhead_seconds']<=15;overhead+=point['overhead_seconds']
  for role,value in point['roles'].items():rows+=numeric.role(value)['processes_observed'];role_points+=1
  redis=point['Redis'];assert numeric.redis_delta(previous,redis['server'])==redis['delta_from_previous'];previous=redis['server'];assert redis['observer_commands_total']==index+3 and redis['observer_INFO_commands']==index+2
 assert role_points==s['role_points']==16 and rows==s['per_PID_observations']==75
 playback=o['playback'];assert playback['continuous_connections']and playback['requests']==1 and playback['warmup_seconds']==15 and playback['measure_seconds']==120 and len(playback['viewers'])==1
 row=playback['viewers'][0];begin=playback['resources']['window_begin_monotonic'];end=playback['resources']['window_end_monotonic'];closed=row['physical_close']['completed_monotonic'];assert numeric.close(end-begin,120)and row['status']==200 and row['requests']==1 and not row['errors']and not row['unexpected_eof']and row['post_window_barrier']and row['physical_close']['socket_closed']and row['physical_close']['reader_returning_after_close']
 assert points[0]['end_monotonic']<=row['reader_start_monotonic']and points[1]['end_monotonic']<=begin and end<=closed<=points[2]['begin_monotonic']and points[3]['begin_monotonic']>=closed+70
 assert numeric.close(begin-row['reader_start_monotonic'],o['actual_reader_open_to_window_seconds'])and o['supplemental_retention_seconds']>=70 and numeric.close(o['supplemental_retention_lateness_seconds'],o['supplemental_retention_seconds']-70)
 assert row['measured_bytes']>=120*100000 and row['first_byte_ms']<=15000 and row['prefix_bytes']==2097152
 before=o['tasks']['workflow-prerun.jsonl'];after=o['tasks']['workflow-postrun.jsonl'];assert len(before)==len(after)==6 and len({x['id']for x in before})==6 and {x['id']for x in before}=={x['id']for x in after};done={x['id']:x for x in after};trace=row['byte_trace']
 for task in before:
  finish=done[task['id']];assert finish['task']==task['task']and finish['state']=='SUCCESS'and begin<=task['monotonic']<=finish['monotonic']<=end
  left=[r for r in trace if r['monotonic']<=task['monotonic']];right=[r for r in trace if r['monotonic']>=finish['monotonic']];assert left and right and right[0]['bytes']>left[-1]['bytes']
 assert o['task_executions']==6 and o['durable_checks']==len(o['workflow_checks'])==44 and all(c['passed']is True for c in o['workflow_checks'])and o['real_worker_task_overlap']and o['normal_collector_witness']['physical_process_and_pipes_settled']
 assert set(headers)==set(o['sampled_TS'])=={'0.ts','0-import1.ts','0-import2.ts'}
 for name,h in headers.items():
  item=o['sampled_TS'][name];assert numeric.packet_fields(h)==item['snapshot']and h['raw_media_sha256']==item['sha256']and h['captured_bytes']==2097152 and numeric.finite(item['host_analysis_seconds'])>=0
 assert [d['frames']for d in o['decodes']]==s['AV_frames']==[124,111,118]and all(d['frames']>=25 for d in o['decodes'])
 endpoint=o['Redis_endpoint_witness'];allocated=next(r['host']for r in endpoint['allocator']if r['container']==6379);expected=[{'HostIp':'127.0.0.1','HostPort':str(allocated)}]
 assert endpoint['HostConfig_PortBindings']['6379/tcp']==expected and endpoint['NetworkSettings_Ports']['6379/tcp']==expected and bytes.fromhex(endpoint['compose_port_stdout_hex']).decode()==endpoint['compose_port_stdout_text']and endpoint['compose_port_stdout_text'].strip()=='127.0.0.1:'+str(allocated)
 assert o['default_database_unchanged']is True and len(o['terminal_actions'])==8 and all(x['ok']for x in o['terminal_actions'].values())and o['terminal_actions']['Redis-owned-socket-close']['value']['socket_fd']==-1
 assert len(o['host_command_settlements'])==157 and all(x['drained']and x['pipes_alive']==0 for x in o['host_command_settlements'])and o['independent_cleanup']['all_empty']and all(x['empty']for x in o['independent_cleanup']['inventories'].values())
 assert s['Source_budget_acceptance']is None and s['peak_PSS']is None and s['allstream_dropped_packets']is None and s['M0_complete']is False and s['Native_comparison']is False
 deep=0
 if deep_repo is not None:
  for entry in load('raw-authorities.json')['optional_deep_bindings']:
   p=Path(deep_repo)/entry['path']
   if not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=entry['sha256']:raise ValueError('DeepRawAbsentOrHash')
   deep+=1
  # Deep mode also compares public numeric projection against the hashed raw
  # authority, not merely the existence of those private hashes.
  raw_authority=load('raw-authorities.json')
  for name,digest in raw_authority['executed_controller28_sha256'].items():
   p=Path(deep_repo)/'artifacts/rust/m0-supplemental-metrics-source-2/controller'/name
   if not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=digest:raise ValueError('DeepControllerHash')
   deep+=1
  authorities=raw_authority['optional_deep_bindings'];raw_entry=next(e for e in authorities if e['path'].endswith('/m0-supplemental-metrics-source-2/integration.json'));raw=json.loads((Path(deep_repo)/raw_entry['path']).read_text())
  for key in ['supplemental_points','Redis_observer_setup','sampled_TS','decodes','terminal_actions','host_command_settlements','physical_process_cleanup','default_database_unchanged']:assert raw[key]==o[key]
  sanitized_playback=raw['playback'].copy();sanitized_playback.pop('authenticated_identity',None);assert sanitized_playback==o['playback']
  for name,header in headers.items():
   entry=next(e for e in authorities if e['path'].endswith('/media/'+name));media=(Path(deep_repo)/entry['path']).read_bytes();length=len(media)//188*188
   expected=b''.join(media[i:i+6]for i in range(0,length,188)).hex()
   if expected!=header['first_six_bytes_per_complete_packet_hex']or media[length:].hex()!=header['tail_hex']:raise ValueError('DeepHeaderProjection')
   for index,digest in header['payload_sha256_for_same_CC_candidates'].items():
    packet=media[int(index)*188:(int(index)+1)*188];control=(packet[3]>>4)&3;offset=5+packet[4]if control in[2,3]else 4
    if hashlib.sha256(packet[offset:]).hexdigest()!=digest:raise ValueError('DeepPayloadCandidateHash')
 return {'passed':True,'mode':'public retainednumeric/headerprojection, not Source replay','role_points':role_points,'per_PID_rows':rows,'tasks':6,'checks':44,'callback_total_seconds':overhead,'deep_verified':deep,'M0_complete':False,'Native_gain':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--deep-repo',type=Path);a=parser.parse_args();print(json.dumps(verify(deep_repo=a.deep_repo),indent=2))
