"""Portable setup/witness/proposal verification; never executes Source or Docker."""
import argparse,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def verify(root=HERE,deep_repo=None):
 root=Path(root);load=lambda n:json.loads((root/n).read_text());n=load('verified-setup-negative-1.json');r=load('verified-redis-info-1.json');e=load('verified-endpoint-reproduction-1.json');p=load('blocked-recipe5.json')
 assert n['process_exit']==1 and n['error_message']=='OwnedRedisEndpointRequired'and n['supplemental_points']==0 and n['playback_started']is False and n['declared_6task_44check_workflow_started']is False
 assert n['Source_metric_observation_validity']=='NOT_RUN, not zero and not Sourcebudgetfailure'and n['endpoint_diagnosis']['mapping_present']and n['endpoint_diagnosis']['actual_stdout']=='NOT_RETAINED'and n['default_database_unchanged']and n['owned_independent_cleanup_empty']
 assert r['passed']is True and r['actual_daemon_credentials']['uids']==[65532]*4 and r['container_identity']['ReadonlyRootfs']is True and r['container_identity']['Memory']==134217728 and r['container_identity']['NanoCpus']<=1000000000
 assert len(r['normal_INFO_snapshots'])==5 and r['normal_physical_close']['local_descriptor']==-1 and r['normal_physical_close']['exact_server_client_absent']and r['reset_witness']['same_run_id']and r['reset_witness']['adapter_rejected_exactly']=='RedisCounterReset'and r['reset_witness']['future_operation_rejected']and r['independent_cleanup']['all_empty']
 a,b=e['cases'];assert a['case']=='internal-only'and a['compose_port_stdout_hex']=='3a300a'and a['NetworkSettings_Ports']['6379/tcp']is None and a['parsed']['strict_guard_accepts']is False
 assert b['case']=='internal-plus-observer-ingress'and b['parsed']['host']=='127.0.0.1'and b['actual_effective_binding_matches_allocator']and b['INFO_succeeded']and b['INFO_local_socket_closed']and e['independent_cleanup']['all_empty']
 assert p['Source2_actual_observations']is None and p['Source_budget_acceptance']is None and p['Native_comparison']is False and p['M0_complete']is False and len(p['blocked_controller28_sha256'])==28
 for name,digest in p['blocked_controller28_sha256'].items():assert hashlib.sha256((root/'controller'/name).read_bytes()).hexdigest()==digest
 assert b'SUPPLEMENTAL_EXECUTION_BLOCKED=True'in(root/'controller/run.py').read_bytes()and b'RESOURCE_EXECUTION_BLOCKED=True'in(root/'controller/run.py').read_bytes()
 for name in ['proc_helper.py','redis_session.py']:assert b'EXECUTION_BLOCKED=True'in(root/'controller'/name).read_bytes()
 deep=0
 if deep_repo is not None:
  for x in load('raw-hashes.json')['optional_deep_bindings']:
   q=Path(deep_repo)/x['path']
   if not q.is_file()or hashlib.sha256(q.read_bytes()).hexdigest()!=x['sha256']:raise ValueError('DeepRawAbsentOrHash')
   deep+=1
 return {'passed':True,'mode':'publicretainedprojection/proposal, no Source replay','Source1_failed':True,'Source2':'NOT_RUN','realRedis_helper_qualified':True,'deep_raw_verified':deep,'M0_complete':False}
if __name__=='__main__':
 p=argparse.ArgumentParser();p.add_argument('--deep-repo',type=Path);a=p.parse_args();print(json.dumps(verify(deep_repo=a.deep_repo),indent=2))
