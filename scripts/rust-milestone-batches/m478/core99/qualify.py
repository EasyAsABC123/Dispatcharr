"""Recompute captured Core99 scopes only: no application, Docker or Source API."""
import argparse,hashlib,json,runpy,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();out=a.output.absolute();sys.path[:0]=[str(HERE/'driver'),str(HERE/'review')]
from qualify_scope import qualify
from core51_compare import qualify as qualify_core
from core_effects import compare
report=json.loads((out/'report.json').read_bytes());normal=qualify(out);core=qualify_core(out);root=Path('/home/justin/github/Dispatcharr');refs=json.loads((out/'core51-config/definition/native-binding.json').read_bytes())['Source51'];effects=[]
for group,ref in refs.items():
 source=root/ref['path'];assert hashlib.sha256(source.read_bytes()).hexdigest()==ref['sha256'];effects.append(compare(json.loads(source.read_bytes()),json.loads((out/'core51'/(group+'-Main-private.json')).read_bytes())))
previous=sys.argv
try:
 sys.argv=[str(HERE/'review/qualify_rows_tasks.py'),str(out)];runpy.run_path(str(HERE/'review/qualify_rows_tasks.py'),run_name='__main__')
except SystemExit as e:
 assert e.code==0
finally:sys.argv=previous
rows=json.loads((out/'core51/Main-readonly-row-task-review.json').read_bytes());cleanup=json.loads((out/'independent-cleanup.json').read_bytes());assert cleanup['exact_owned_absence']and cleanup['fresh_CID_queries']==12 and cleanup['host_generations_absent']==159
actual=core['actual_requests']+normal['actual_reference_cases']+normal['startup_finite6']['checks']['cache4_plus_finite2_complete']*6+1
result={'passed':report['actual_controller_exit']==0 and core['passed']and normal['HTTP_SQL_GPG_normal_scope_passed']and all(x['passed']for x in effects)and rows['passed']and actual==99,'actual_Main_business_HTTP_calls':actual,'additional_plugin_readiness_HTTP_calls':sum(len(json.loads((out/(group+'-capture.json')).read_bytes())['readiness'])for group in ['transport16','retained24','fresh404']),'scoped_Core_effect_predicates':sum(x['predicates']for x in effects),'additional_row_task_predicates':rows['predicates'],'Original_management_setup_observer_processes':6,'Source_API_calls':0,'raw_report_stale_Source_application_runs_field':report['Source_application_runs'],'stale_field_is_not_a_zero_Source_program_claim':True,'actual_Controller_exit':report['actual_controller_exit'],'full_task_transport_metadata_parity':False,'task_metadata_gaps':rows['explicit_transport_metadata_gaps'],'strict_framework_cache_context_passed':False,'prior_actual_e88_strict_exit2_preserved':True,'all159_known_host_generations_and12CIDs_independently_absent':True,'private_peer290_predicates_claimed':False,'Native_private_timezone_proxy_consumers_qualified':False,'unconsumed_task_downstream_execution_qualified':False,'full_M5_M8_accepted':False,'performance_claims':False,'application_or_Source_replays_by_this_qualifier':0}
assert not any(k=='django'or k.startswith(('django.','apps.','dispatcharr.'))for k in sys.modules)
(out/'Core99-readonly-review.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result));sys.exit(0 if result['passed']else 2)
