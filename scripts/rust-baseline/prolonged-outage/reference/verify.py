"""Portable retained-data verification, NOT Source replay or budget acceptance."""
import argparse,hashlib,importlib.util,json
from pathlib import Path
HERE=Path(__file__).resolve().parent

def verify(directory=HERE,deep_repo=None):
 directory=Path(directory);summary=json.loads((directory/'verified-source-once-1.json').read_text());raw=(directory/'observations.json').read_bytes()
 if hashlib.sha256(raw).hexdigest()!=summary['observations_sha256']:raise ValueError('ObservationHash')
 o=json.loads(raw);controller=directory.parent/'controller'
 for name,digest in summary['blocked_controller_sha256'].items():
  if hashlib.sha256((controller/name).read_bytes()).hexdigest()!=digest:raise ValueError('ControllerHash')
 spec=importlib.util.spec_from_file_location('prolonged_fixed_protocol',controller/'outage_protocol.py');protocol=importlib.util.module_from_spec(spec);spec.loader.exec_module(protocol)
 assert protocol.EXECUTION_BLOCKED and protocol.derive(protocol.EXPECTED)==o['schedule'];events={r['name']:r for r in o['events']};start=events['all-unavailable-start'];end=events['all-unavailable-end'];paths={'/live/0.ts','/live/100.ts'}
 assert end['monotonic']-start['monotonic']>=102 and abs(end['monotonic']-start['monotonic']-o['outage_elapsed_seconds'])<.000001
 for e in [start,end]:assert {r['path']for r in e['probes']}==paths and len(e['probes'])==2 and all(r['status']==503 for r in e['probes'])
 warm=o['before']['origin_socket_witness']['active_input_connections'];assert len(warm)==1 and warm[0]['peer'][0]in o['selected_services']['backend']['IPs']
 closed={i for ack in start['acks']for i in ack['physical_close']['closed_ids']};assert warm[0]['id']in closed and all(r['physical_close']['all_closed']for r in start['acks'])
 history=o['before_restore']['origin_socket_witness'];assert all(history['controls'][path]['fail']for path in paths);assert any(r['id']==warm[0]['id']and r['socket_fileno']==-1 for r in history['physical_closed_inputs'])
 assert o['primary_recover_receipt']['path']=='/live/0.ts'and o['primary_recover_receipt']['action']=='recover'
 restored=o['restored_monotonic'];observed_end=o['restore_observation_end_monotonic'];assert observed_end-restored>=88 and abs(observed_end-restored-o['restore_observation_elapsed_seconds'])<.000001
 assert events['primary-restored']['monotonic']==restored and events['restore-observation-end']['monotonic']==observed_end
 decodes={r['prefix']:r for r in o['decodes']}
 for name in ['original-prefix.ts','fresh-prefix.ts']:assert decodes[name]['decoder_exit']==0 and decodes[name]['frames']>=25 and decodes[name]['bytes']==2097152
 assert o['original_body_outcome']['before_controlled_close']['errors']==['TimeoutError'];assert o['original_body_outcome']['independent_of_fresh_request']and o['original_body_outcome']['physical_close']['physical_socket_closed'];assert o['fresh_body_outcome']['status']==200 and o['fresh_body_outcome']['physical_close']['physical_socket_closed']
 retention=o['retention_outcome'];assert all(retention[k]==0 for k in ['clients','provider_slots','metadata_fields','timestamps','origin_active'])and not retention['owner']and not retention['input_index']
 assert len(o['terminal_actions'])==7 and all(r['ok']for r in o['terminal_actions'].values());assert len(o['host_command_settlements'])==49 and all(r['drained']and not r['pipes_alive']for r in o['host_command_settlements'])
 assert o['independent_cleanup']['all_empty']and all(r['empty']for r in o['independent_cleanup']['inventories'].values())
 assert o['stored_report_passed']is True and o['controller_process_exit']==1 and not o['process_success']and o['reporting_failure']['type']=='TypeError'
 assert o['reporting_failure']['scope']=='fixture context final report JSON serialization'and o['reporting_failure']['location']=='tools/scripts/rust-integration/run.py:64 fixture_source __exit__'
 assert not summary['process_success']and summary['controller_process_exit']==1 and not summary['old_body_recovery_proven']and summary['Source_budget_acceptance']is None and not summary['Native_comparison']and not summary['M0_complete']
 deep=0
 if deep_repo is not None:
  for record in summary['captured_data_raw']:
   p=Path(deep_repo)/record['path']
   if not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=record['sha256']:raise ValueError('DeepRawAbsentOrHash')
   deep+=1
 return {'valid_retained_observation':True,'controller_exit':1,'Source_budget_acceptance':None,'default_mode':'portable projected-data verification, no Source replay','deep_raw_verified':deep,'Native_gain':False,'M0_complete':False}
if __name__=='__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--deep-repo',type=Path);a=parser.parse_args();print(json.dumps(verify(deep_repo=a.deep_repo),indent=2))
