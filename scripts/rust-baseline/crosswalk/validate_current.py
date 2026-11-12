"""Validate the DRAFT current appendix without rewriting historical crosswalk."""
import argparse,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
def validate(root=HERE):
 root=Path(root);v=json.loads((root/'current-reference-appendix.json').read_text());raw=(root/'crosswalk.json').read_bytes();history=json.loads(raw)
 assert hashlib.sha256(raw).hexdigest()==v['historical_crosswalk_sha256']and len(history['scenarios'])==8
 assert v['registry_counts_and_IDs_UNCHANGED']==history['registry']['counts']and v['fully_verified_route_operations']==0
 assert v['registry_counts_and_IDs_UNCHANGED']['runtime_routes']==592 and v['registry_counts_and_IDs_UNCHANGED']['runtime_operation_work_items']==798 and v['registry_counts_and_IDs_UNCHANGED']['static_non_HTTP_candidates']==250
 assert v['task_candidates_unchanged']==[x['literal_candidate_id']for x in history['task_observations']]
 assert [r['scenario']for r in v['historical_8_budget_presence']]==[r['name']for r in history['scenarios']]
 records={r['name']:r for r in v['current_reference_records']};resource=records['Resource7_fixed3_Original'];outage=records['settings_derived_prolonged_outage_once'];pending=records['supplemental_Source2']
 assert resource['Source_budget_acceptance']is False and resource['observed_process_FD_socket_growth_each']==[1,144,18]and resource['declared_process_FD_socket_growth']==[0,16,16]
 assert outage['process_exit']==1 and outage['stored_report_passed']is True and outage['Source_budget_acceptance']is None
 for r in records.values():
  if 'registered_contract_id'in r:assert r['registered_contract_id'].endswith(':METHOD_UNRESOLVED')and r['registered_method']is None
 assert pending['actual_RSS_PSS_rows']==75 and pending['actual_complete_role_points']==16 and pending['state']=='OBSERVATION_VALID_SOURCE_METRIC_ATTACHMENT_PASS'
 assert pending['Source_budget_acceptance']is None and pending['actual_workload']['tasks']==6 and pending['actual_workload']['durable_checks']==44 and pending['physical_cleanup_and_DB']['default_database_unchanged']and pending['physical_cleanup_and_DB']['owned_containers_networks_volumes_empty']
 assert len(pending['observed_callback_overhead'])==4 and len(pending['actual_three_prefix_discontinuity_fields'])==3
 repository=root.parents[2];reference=repository/pending['public_reference']
 if not reference.is_file()or hashlib.sha256(reference.read_bytes()).hexdigest()!=pending['public_reference_sha256']:raise ValueError('PublicSource2AuthorityAbsentOrHash')
 actual=json.loads(reference.read_text());assert actual['passed']is True and actual['role_points']==16 and actual['per_PID_observations']==75 and actual['M0_complete']is False
 assert v['M0_complete']is False and v['M1_formal_pass']is False
 required={'minimum_media_throughput','maximum_error_count','startup_p95_absolute_ceiling_ms','CPU_ratio_ceiling','memory_ratio_ceiling','cleanup_deadline_seconds','post_cleanup_growth_ceiling'}
 assert all(set(r['required_budget_fields'])==required for r in v['historical_8_budget_presence'])
 assert resource['required_budget_fields']['CPU_ratio_ceiling']is None and resource['required_budget_fields']['memory_ratio_ceiling']is None
 return {'passed':True,'mode':'draft static reconciliation, no milestone acceptance','historical_scenarios_unchanged':8,'current_records':len(records),'Source2':'ACTUAL_SCOPED_PASS','M0_complete':False,'M1_formal_pass':False}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
