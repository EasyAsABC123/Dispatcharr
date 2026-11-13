#!/usr/bin/env python3
"""Offline actual dependency experiment checks; no builds/apps/Docker."""
import argparse,hashlib,json,sys,tarfile,tempfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
if str(ROOT)not in sys.path:sys.path.insert(0,str(ROOT))
from validate_dependency import validate_directory

def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def unpack(variant,output):
 inventory=json.loads((ROOT/(variant+'-raw-inventory.json')).read_text());seen=set();total=0
 with tarfile.open(ROOT/(variant+'-raw.tar.gz'))as archive:
  for member in archive.getmembers():
   name=member.name;require(member.isfile()and name in inventory and name not in seen and not Path(name).is_absolute()and'..'not in Path(name).parts and member.size<=33554432,'ExactBoundedRawMember');data=archive.extractfile(member).read();total+=len(data);require(total<=134217728 and inventory[name]=={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()},'ExactRawMemberBytes');target=output/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data);seen.add(name)
 require(seen==set(inventory),'CompleteRawArchiveInventory');return len(seen)
def check_executed_definition():
 p=ROOT/'executed-definition';provenance=json.loads((ROOT/'provenance.json').read_text());require(sha(p/'seal.json')==provenance['executed_seal_sha256']=='5c01f50688a714f1751751f737867e4e3563ce3291e70f6c9c6a1915d89b1def','ExactExecutedSeal');seal=json.loads((p/'seal.json').read_text());omitted={r['path']:r for r in json.loads((p/'omitted-inputs.json').read_text())};require(set(omitted)=={'inputs/baseline-sqlx-close-tests','inputs/candidate-sqlx-close-tests'},'OnlyHistoricalELFsOmitted')
 for name,digest in seal['local_inputs'].items():
  if name in omitted:require(omitted[name]['sha256']==digest,'ExactOmittedELFHash')
  else:require(sha(p/name)==digest,'ExactExecutedInput:'+name)
 old=json.loads((p/'build/candidate-original/build.json').read_text());new=json.loads((p/'build/candidate/recovery.json').read_text());require(old['passed']is False and old['commands'][3]['actual_wait']==101 and new['passed']is True and len(new['commands'])==2 and all(r['actual_wait']==r['actual_poll']==0 for r in new['commands']),'OriginalENOSPCNegativeDistinctFromTwoSuccesses');q1=json.loads((ROOT/'negative-provenance/QA1-compile-negative/build.json').read_text());require(q1['passed']is False and q1['commands'][2]['actual_wait']==101,'QA1CompilerNegativePreserved');preflight=json.loads((ROOT/'negative-provenance/QA2-runner-preflight-negative/preflight-negative.json').read_text());require(preflight['actual_wrapper_exit']==1 and preflight['actual_Cargo_commands_started']==0,'TargetPreflightNegativePreserved');require(sha(ROOT/'sqlx-core-0.8.6.crate')==provenance['package_sha256']=='ee6798b1838b6a0f69c007c133b8df5866302197e404e8b6ee8ed3e3a5e68dc6','ExactUpstreamCrate');return {'executed_input_inventory':len(seal['local_inputs']),'omitted_historical_ELFs':2,'QA1_compiler_negative_preserved':True,'QA2_target_preflight_negative_preserved':True,'candidate_ENOSPC_negative_preserved':True,'candidate_successful_recovery_commands':2}
def validate_run(directory,variant):
 result=validate_directory(directory/'rust',variant);execution=json.loads((directory/'execution-report.json').read_text());review=json.loads((directory/'Root'/(variant+'-Root-review.json')).read_text());host=json.loads((directory/'Root'/(variant+'-actual-exit.json')).read_text());expected_exit=1 if variant=='baseline'else 0;require(result['passed']is(variant=='candidate')and execution['passed']is(variant=='candidate')and execution['actual_controller_exit']==host['exit']==expected_exit and host['wait_completed']is True,'ActualVariantContractAndHostExit');require([r['passed']for r in result['cases']]==([True,True,False]if variant=='baseline'else[True,True,True]),'RecordedActualThreeCaseOutcomes')
 require((directory/'default-before.json').read_bytes()==(directory/'default-after.json').read_bytes()and execution['default11_exact']is True,'ActualDefault11BytesExact');require(execution['all_command_processes_settled']is True and execution['owned_backends_absent_after_process_exit']is True and json.loads((directory/'backend-after-ELF-exit.json').read_text())[-1]['actual_backends']==[],'ActualWaitPipesSQLBackendSettlement');require(review['review_passed']is True and review['actual_contract_passed']is(variant=='candidate')and review['independent_project_CID_host_container_PIDs_absent']is True and review['container_PIDs']==8 and len(review['queries'])==4 and all(r['exit']==0 and r['stdout']==''for r in review['queries']),'IndependentFourInventoriesAndNinePIDIdentities')
 exits=[r['actual_exit']for r in execution['processes']];require(len(exits)==26 and exits.count(101)==(1 if variant=='baseline'else 0)and all(x in(0,101)for x in exits),'ActualCommandExitsWithoutHidingBaseline101');return {**result,'actual_controller_host_exit':expected_exit,'actual_command_count':26,'default11_bytes_exact':True,'independent_cleanup_inventories':4,'container_PIDs_absent':8,'host_PID_absent':True}
def validate():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text());actual={str(p.relative_to(ROOT)):sha(p)for p in ROOT.rglob('*')if p.is_file()and p.name!='packet-inventory.json'and '__pycache__'not in p.parts};require(actual==inventory,'CompleteExactPortablePacket');definition=check_executed_definition();results={}
 with tempfile.TemporaryDirectory(prefix='dispatcharr-sqlx-proof-',dir=ROOT.parent)as temporary:
  for variant in('baseline','candidate'):
   path=Path(temporary)/variant;path.mkdir();count=unpack(variant,path);results[variant]=validate_run(path,variant);results[variant]['raw_archive_files']=count
 return {'portable_evidence_checks_passed':True,'baseline_contract_passed':False,'candidate_three_max1_contracts_passed':True,'runs':results,'definition':definition,'Main_API_or_general_shutdown_qualification':False,'canonical_dependency_promotion':False,'old_notification2_4_6_negatives_waived':False}
def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--json',action='store_true');a=p.parse_args();v=validate();print(json.dumps(v,indent=2)if a.json else'Raw experiment validated: baseline contract NEGATIVE; candidate3 max1 contracts positive; no Main/promotion claim.');return 0
if __name__=='__main__':raise SystemExit(main())
