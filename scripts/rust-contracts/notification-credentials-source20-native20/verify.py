#!/usr/bin/env python3
"""Portable read-only Source20/Native20 observation checks. No app or Docker."""
import argparse,hashlib,json,sys,tarfile,re
from pathlib import Path
ROOT=Path(__file__).resolve().parent
if str(ROOT)not in sys.path:sys.path.insert(0,str(ROOT))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def verify_inventory():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text());actual={str(p.relative_to(ROOT)):sha(p)for p in ROOT.rglob('*')if p.is_file()and p.name!='packet-inventory.json'and '__pycache__'not in p.parts};require(actual==inventory,'CompleteExactPortablePacketInventory');return len(inventory)
def load_reference():
 value=json.loads((ROOT/'reference.json').read_text());require(value['schema']=='notification-credential-projection-v1'and value['reference_mode']=='projected-observations-only','ExplicitProjectionSchemaAndMode')
 for name in('Source20','Native20'):
  manifest=json.loads((ROOT/(name+'-projection-manifest.json')).read_text());require(sha(ROOT/(name+'-projection.json'))==value[name]['projection_sha256']==manifest['projection_sha256'],'ExactSelectedProjection:'+name);require(manifest['original_sha256']==value[name]['original_sha256']and manifest['projected_observation_executed']is False and manifest['raw_observation_executed']is True,'OriginalVersusProjectedIdentity:'+name)
 require(sha(ROOT/'plan.json')==value['plan_sha256'],'ExactTwentyPlan');return value
def load_source_projection():
 """Return the SHA-bound Source observation projection, never original bytes."""
 load_reference();return json.loads((ROOT/'Source20-projection.json').read_text())
def load_plan():load_reference();return json.loads((ROOT/'plan.json').read_text())
def validate_lifecycle(label):
 require(label in('source','native'),'NamedLifecycle');expected=json.loads((ROOT/(label+'-lifecycle-inventory.json')).read_text());values={};used=0
 with tarfile.open(ROOT/(label+'-lifecycle.tar.gz'))as archive:
  for member in archive.getmembers():
   name=member.name;require(member.isfile()and not Path(name).is_absolute()and'..'not in Path(name).parts and name not in values and member.size<=8388608,'BoundedRegularLifecycleMember');data=archive.extractfile(member).read();used+=len(data);require(used<=67108864,'LifecycleTotalCapacity');require(name in expected and expected[name]=={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()},'ExactLifecycleBytes:'+name);values[name]=data
 require(set(values)==set(expected),'CompleteLifecycleInventory')
 report=json.loads(values['execution.json'if label=='source'else'execution-report.json']);host=json.loads(values['host/host-process.json']);require(report['passed']is True and report['controller_exit'if label=='source'else'actual_controller_exit']==0 and host['state']=='SETTLED'and host['actual_wait']==host['actual_poll']==0 and host['after_starttime']is None,'ActualControllerHostSettlement0')
 before='default-before-private.json'if label=='source'else'default-before.json';after='default-after-private.json'if label=='source'else'default-after.json';require(values[before]==values[after],'BothDefault11BytesExact')
 commands=[json.loads(data)for name,data in values.items()if re.fullmatch(r'command-\d{3}\.json',name)]
 require(commands and all(r['exit'if label=='source'else'actual_exit']==0 and r['physically_settled']is True and r['pipe_threads_settled'if label=='source'else'pipes_settled']is True for r in commands),'AllActualCommandsAndPipes0')
 cleanup=json.loads(values['independent-cleanup-query-receipt.json']);require(cleanup['passed']is True and cleanup['all_raw_saved_before_assertions']is True and len(cleanup['queries'])==5 and all(r['original_process_absent']for r in cleanup['PID_observations']),'IndependentActualCleanup')
 require(all(r['exit']==0 and bytes.fromhex(r['stdout_hex']).strip()==b''for r in cleanup['queries'][:4]),'AllProjectAndExactCIDEmpty');missing=cleanup['queries'][4];require(missing['exit']!=0 and json.loads(bytes.fromhex(missing['stdout_hex']))==[],'ExactCIDMissingLiteralJSON')
 if label=='native':require(report['owned_backends_absent_after_process_exit']is True and json.loads(values['backend-after-ELF-exit.json'])[-1]['actual_backends']==[],'ActualSQLBackendAbsenceAfterELF')
 return {'passed':True,'actual_commands':len(commands),'default11_bytes_exact':True,'independent_cleanup_queries':5,'original_PID_identities_absent':len(cleanup['PID_observations']),'actual_controller_exit':0,'raw_receipt_files':len(values)}
def validate():
 inventory_count=verify_inventory();reference=load_reference();source=load_source_projection();native=json.loads((ROOT/'Native20-projection.json').read_text());plan=load_plan()
 from validate_source_projection import compare
 from validate_native_projection import validate as qualify_native
 result_source=compare(source,plan);result_native=qualify_native(native,source,plan,reference_mode='retained-original-observation');require(native['passed']is False and native['capture_complete']is True,'CaptureOnlyRecorderFalsePreserved')
 lifecycle={label:validate_lifecycle(label)for label in('source','native')};source_review=json.loads((ROOT/'Source20-Root-review.json').read_text());native_review=json.loads((ROOT/'Native20-Root-review.json').read_text());require(source_review['passed']is True and native_review['review_passed']is True and native_review['raw_recorder_passed_field_preserved']is False,'RetainedIndependentReviews')
 return {'passed':True,'exact_packet_input_files':inventory_count,'Source':result_source,'Native':result_native,'lifecycle':lifecycle,'Source_original_raw_sha256':reference['Source20']['original_sha256'],'Native_original_raw_sha256':reference['Native20']['original_sha256'],'public_projections_were_executed':False,'Native_capture_only_raw_passed':False,'Native_capture_complete':True,'Native_controller_and_effect_qualification_passed':True,'Source_TCP_or_all_credentials_or_full_endpoint_claim':False,'physical2_4_6_close_drain_negatives_waived':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--json',action='store_true');a=p.parse_args();result=validate();print(json.dumps(result,indent=2)if a.json else'Source20 and Native20 observations qualified; Native raw recorder passed:false preserved; no app executed');return 0
if __name__=='__main__':raise SystemExit(main())
