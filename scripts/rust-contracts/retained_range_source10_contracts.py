"""One retained range Source10 capture supplement, no new operation or alias.
Original Dispatcharr and dependency authors retain all application credit.
No application/URL resolver/network/database is executed by this module.
"""
import copy,hashlib,json
from pathlib import Path
KIND='retained_original_range_Source10_resolver_HTTP'
BASE='scripts/rust-contracts/retained-range-source10/'
INPUT=BASE+'bindings.json'
INVENTORY_SHA='d9bc1db24e237525edcf6e705b6ce39e64fb59417655720e36483e7a3c040560'
sha=lambda b:hashlib.sha256(b).hexdigest()
def digest(v):return sha(json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())
def checked(root,name,expected):
 p=Path(root)/name;assert p.is_file()and not p.is_symlink();raw=p.read_bytes();assert sha(raw)==expected,'exact retained range authority bytes';return json.loads(raw)
def inventory(root):return checked(root,BASE+'inventory.json',INVENTORY_SHA)
def dependencies():
 root=Path(__file__).resolve().parents[2]
 return {INPUT,BASE+'inventory.json',*(BASE+n for n in inventory(root))}
def generate(root):
 inv=inventory(root);data={}
 for name,h in inv.items():
  raw=(Path(root)/BASE/name).read_bytes();assert sha(raw)==h
  if name.endswith('.json'):data[name]=json.loads(raw)
 c=data['candidate-input.json'];q=data['capture-qualification.json'];behavior=data['behavior-review.json'];t=data['Source-Root-terminal.json'];cleanup=data['Source-cleanup.json']
 assert inv['capture-qualification.json']==c['capture_qualification_sha256']=='21dad852b4a18bbb18f4646d0fd99c3ceb9f513def59c3cece436da2e16dce01'
 assert inv['behavior-review.json']==c['separate_scoped_behavior_review_sha256']=='5158472743e1950019bf5c277f7aa9ef89573c52142e549459d4bc5f9109ce8b'
 assert q['all_ten_capture_qualified']and q['new_range_captures_qualified']==10 and q['actual_controller_exit']==0 and q['status']=='RETAINED_SOURCE_RANGE10_CAPTURES_BEHAVIOR_REVIEW_PENDING'
 assert behavior['status']=='ACTUAL_SOURCE_RANGE10_BEHAVIOR_REVIEW_PASSED_SCOPED'and behavior['qualification_sha256']==inv['capture-qualification.json']and behavior['Source_controller_actual_exit']==0 and behavior['Source_HTTP_observed_and_reviewed']==10
 assert t['actual_exit']==0 and t['actual_terminal_consumed']and t['stdout_EOF']and t['stderr_EOF']and t['process_group_settled']and t['input_integrity_passed']and t['input_drift']==[]
 assert cleanup['passed']and cleanup['exact_CIDs']==3 and cleanup['host_generations']==2
 terminal=data['resolver/Root-terminal.json'];report=data['resolver/UID-report.json'];observed=data['resolver/observations.json'];assert terminal['passed']and terminal['actual_exit']==0 and terminal['failure']is None and terminal['stdout_EOF']and terminal['stderr_EOF']and terminal['process_group_settled']and terminal['remaining_group_members']==[]and terminal['output_bytes_discarded']==0
 assert terminal['UID_report_sha256']==inv['resolver/UID-report.json']=='5aff1714a7b9b0fcadf29f6cfe5f6614078ff75b3606614674d07e31558a0bfc'
 assert report['passed']and report['control_actual_exit']==report['actual_exit']==0 and report['exact_CID_absent']and report['Source_paths']==1 and report['resolver_routes']==592 and report['actual_source_files']==1211
 assert report['prepared_closure_sha256']==inv['resolver/executed-control/closure.json']and report['observation_sha256']==inv['resolver/observations.json']
 for name,h in data['resolver/executed-control/closure.json']['files'].items():assert inv['resolver/executed-control/'+name]==h
 assert len(data['resolver/executed-control/source-files.json'])==1211 and len(data['resolver/executed-control/input.json']['routes'])==592
 assert observed['passed']and observed['actual_UID']==65532 and observed['all592_resolver_identities_order_actions_exact']and observed['Source_HTTP_business_calls']==report['Source_HTTP_business_calls']==0
 assert observed['counts']==report['actual_guard_counts']=={'SQL_connection_or_cursor_attempts':0,'network_attempts':1,'thread_attempts':0,'child_attempts':0,'handler_invocations':0}
 assert observed['qualified_blocked_import_probe_count']==report['qualified_blocked_import_probe_count']==1
 denied,=observed['denied_operations'];assert denied['stage']=='django_setup'and denied['operation']=='socket.socket.bind'and denied['caller']['module']=='urllib3.util.connection'and denied['caller']['function']=='_has_ipv6'and denied['caller_file_sha256']=='2633bbdb69731e5ccb5cf4e4afd65605d86c7979cc5633126f50c92d5ad74a74'and denied['IPv6_loopback_ephemeral_bind']and denied['original_operation_executed']is False
 witness,=observed['observations'];assert witness['path']=='/api/channels/channels/numbers-in-range/'and witness['actual_resolver_ordinal']==89 and witness['actual_handler']=='apps.channels.api_views.ChannelViewSet'and witness['actual_action_map']=={'get':'numbers_in_range'}and witness['actual_kwargs']=={}
 assert data['first-failure/Root-terminal.json']['actual_exit']==1 and not data['first-failure/UID-report.json']['passed']and data['first-failure/UID-report.json']['error_type']=='RuntimeError'
 assert any('dispatcharr-docker.sock'in x['stderr']and 'no such file or directory'in x['stderr']for x in data['first-failure/commands.json'])
 leaves={row['label']:row for row in q['leaves']};reviews={row['label']:row for row in behavior['cases']};assert len(leaves)==len(reviews)==len(c['cases'])==10
 for row in c['cases']:
  leaf=leaves[row['label']];review=reviews[row['label']];actual=next(x for x in witness['retained_observations']if x['label']==row['label']);assert row['resolver_path_without_query']==witness['path']and row['path']==leaf['literal_request_path']and row['method']==actual['method']=='GET'
  assert row['status']==leaf['actual_HTTP_status']==review['actual_HTTP_status']==actual['HTTP_status']and row['body_sha256']==leaf['body_sha256']and row['raw_wire_sha256']==actual['private_wire_sha256']
  assert row['capture_qualified']and row['raw_capture_artifact_behavior_pending_flag_preserved']and review['Source_behavior_reviewed']and review['complete_raw_JSONRenderer_bytes_match_Source_algorithm']and review['all61_SQL_tables59_sequences_unchanged']
  assert not row['effective_role_matrix_qualified']and not row['Source_HTTP_replayed'];row=copy.deepcopy(row)
 fixture='retained-range-Source10:'+c['contract_id'];contract={k:c[k]for k in ['contract_id','method','source_action','source_handler','pattern_exact','resolver_ordinal','owning_milestone']};contract.update(fixture_id=fixture,cases=copy.deepcopy(c['cases']),observed_roles=[],source_role_specs={},assertions=['Exact retained Source2 capture status/body/privatewire/provenance and full61table59sequence custody','Separate scoped Source method/renderer/ACL adapter behavior review bound without rewriting original capture-pending artifact','Actual successful URLPattern chain/ordinal89/actions/kwargs with full592forest; no alias/role/method inference'],result={'state':'retained_Source10_capture_plus_separate_scoped_behavior_review','new_Source_cases_this_task':0,'capture_custody_qualified':10,'separate_scoped_Source_behavior_reviewed':10,'Native_coverage_increased':False,'fully_verified':False},remaining=['Full effective role matrix/format alias/HEAD/OPTIONS and Native wholewire parity remain unqualified','All full milestones/performance acceptance remain open'])
 fixtures={fixture:{'baseline_revision':'1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430','actual_Source_controller_exit':0,'actual_resolver_exit':0,'normal_model_seed_only':True,'Source_HTTP_replayed':False,'original_capture_behavior_pending_preserved':True,'separate_scoped_behavior_review_bound':True,'effective_roles_qualified':False}}
 return {'format_version':1,'binding_kind':KIND,'credit':'Original Dispatcharr and dependency authors retain all application/algorithm credit.','contracts':[contract],'fixtures':fixtures,'new_assignments':0,'supplement_existing':1,'Source_observations':10,'new_Source_HTTP':0,'fully_verified_added':0}
def validate(root,value,bindings=None):
 assert value==generate(root),'exact range Source10 supplement and resolver/capture/behavior custody'
 return {'new_assignments':0,'supplement_existing':1,'Source_observations':10,'fully_verified_added':0}
def restore_pre_range(rows):
 root=Path(__file__).resolve().parents[2];packet=generate(root);proof=checked(root,BASE+'preservation.json',inventory(root)['preservation.json']);contract,=packet['contracts']
 for route in rows:
  for op in route['operations']:
   if op['contract_id']!=proof['selected_id']:continue
   captures=op.pop('supplemental_executable_Source_captures');assert captures==[{'definition':contract,'setup_fixture':packet['fixtures'][contract['fixture_id']],'input':{'path':INPUT,'sha256':sha((root/INPUT).read_bytes()),'kind':KIND}}];assert digest(op)==proof['selected_original_whole_operation_sha256']
 return rows
