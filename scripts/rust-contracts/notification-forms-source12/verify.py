#!/usr/bin/env python3
"""Read-only Source12 projected authority; executed controller remains exit1."""
import hashlib,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent
for directory in (ROOT,ROOT/'validation'):
 if str(directory)not in sys.path:sys.path.insert(0,str(directory))
def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def load_source_projection():
 reference=json.loads((ROOT/'reference.json').read_text());manifest=json.loads((ROOT/'Source12-projection-manifest.json').read_text())
 require(reference['schema']=='notification-forms-source12-projection-v1'and reference['reference_mode']=='projected-observations-only','ExactProjectionMode')
 require(sha(ROOT/'Source12-projection.json')==reference['projection_sha256']==manifest['projection_sha256'],'StrictSelectedProjectionSHA')
 require(reference['Original_raw_sha256']==manifest['original_sha256']=='b72af1de2683fa9e2345281d24cdaa7418368e1af0404eecba9ba018555a8d84','StrictOriginalRawSource3SHA')
 require(manifest['raw_observation_executed']is True and manifest['projected_observation_executed']is False and reference['executed_controller_exit']==manifest['executed_controller_exit']==1,'ExecutedNegativeAndNewProjectionDistinct')
 return json.loads((ROOT/'Source12-projection.json').read_text())
def load_plan():
 reference=json.loads((ROOT/'reference.json').read_text());path=ROOT/'executed-executor5/inputs/plan.json';require(sha(path)==reference['plan_sha256'],'Exact12BodyRoleOrderPlan');return json.loads(path.read_text())
def validate_observations():
 from forms_effects import compare
 result=compare(load_source_projection(),load_plan());result.update(executed_controller_exit=1,executed_effect_negative_preserved=True,qualification='offline original-input21-byte correction only',public_projection_executed=False,Source_application_runs=0,Original_raw_SHA='b72af1de2683fa9e2345281d24cdaa7418368e1af0404eecba9ba018555a8d84',projected_SQL_fingerprints_are_new_reference_values=True);return result

def validate_lifecycle():
 import tarfile,re
 expected=json.loads((ROOT/'lifecycle-inventory.json').read_text());values={};used=0
 with tarfile.open(ROOT/'lifecycle.tar.gz')as archive:
  for member in archive.getmembers():
   require(member.isfile()and not Path(member.name).is_absolute()and'..'not in Path(member.name).parts and member.name not in values and member.size<=8388608,'BoundedRegularLifecycleMembers')
   raw=archive.extractfile(member).read();used+=len(raw);require(used<=67108864,'Lifecycle64MiB');require(expected.get(member.name)=={'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()},'LiteralLifecycleMemberSHA');values[member.name]=raw
 require(set(values)==set(expected),'CompleteLifecycleInventory')
 report=json.loads(values['execution.json']);require(report['controller_exit']==1 and report['actual_request_count']==12 and report['error_message']=='ActualUnknownSmallMemoryFileMetadataNotStoredOrReadByObserver','ExecutedNegativeLiteralPreserved')
 commands=[json.loads(raw)for name,raw in values.items()if re.fullmatch(r'command-\d{3}\.json',name)]
 require(len(commands)==20 and all(c['exit']==0 and c['physically_settled']is True and c['pipe_threads_settled']is True for c in commands),'All20ChildCommandsActual0AndPipesSettled')
 require(values['default-before-private.json']==values['default-after-private.json'],'LiteralNamed11Full62DefaultEquality')
 audit=json.loads(values['default-before-private.json']);require(len(audit['named11']['tables'])==11 and len(audit['public62']['tables'])==62 and len(audit['public62']['sequences'])==59,'Exact11Plus62_59AuditScope')
 clean=json.loads(values['independent-cleanup.json']);require(clean['all_exit0_and_empty']is True and len(clean['queries'])==4 and all(q['actual_exit']==0 and bytes.fromhex(q['stdout_hex']).strip()==b''for q in clean['queries'])and clean['all_original_PIDs_absent']is True and all(r['original_absent']for r in clean['PID_identities']),'FourFreshActualProjectCIDInventoriesAndPIDStarts')
 require(json.loads(values['owned-backends-after-Source.json'])==[] and report['cleanup_within90']is True and report['cleanup_elapsed_seconds']<=90,'ActualBackendAbsenceAndCleanup90')
 host=json.loads(values['host-tool-settlement.json']);require(host['actual_exit']==1 and host['session_fully_consumed']is True and host['host_controller_PID_absence_claimed']is False,'ToolSettlementWithoutUnobservedPIDClaim')
 return {'passed':True,'executed_controller_exit':1,'actual_Source_child_exit':0,'actual_requests':12,'actual_command_children':20,'exact_audit_scope':{'named_tables':11,'all_public_tables':62,'all_sequences':59},'fresh_project_CID_queries':4,'recorded_PID_identities_absent':len(clean['PID_identities']),'host_controller_PID_not_observed':True,'lifecycle_members':len(values)}

def verify_inventory():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text());actual={str(p.relative_to(ROOT)):sha(p)for p in ROOT.rglob('*')if p.is_file()and p.name!='packet-inventory.json'and'__pycache__'not in p.parts};require(actual==inventory,'CompleteExactPacketInventory');return len(inventory)
def validate():
 return {'passed':True,'exact_packet_inputs':verify_inventory(),'observations':validate_observations(),'lifecycle':validate_lifecycle(),'Source_original_controller_exit_preserved':1,'projection_never_executed':True,'new_Source_or_Native_application_runs':0}
if __name__=='__main__':print(json.dumps(validate(),indent=2))
