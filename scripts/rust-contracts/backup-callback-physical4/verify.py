"""Read-only four historical physical predicates, preserving all raw negatives."""
import hashlib,io,json,tarfile
from pathlib import Path
from capture_predicates import validate_native_capture
ROOT=Path(__file__).resolve().parent
SELECTED=[(1,1),(2,2),(2,3),(3,4)]
EXPECTED_EXITS=[1,1,0]

def require(ok,label):
 if not ok:raise ValueError(label)
def load_members():
 inventory=json.loads((ROOT/'lifecycle-inventory.json').read_text());raw=(ROOT/'lifecycle.tar.gz').read_bytes();require(hashlib.sha256(raw).hexdigest()==inventory['archive_sha256'],'ExactArchiveSHA')
 expected={v['path']:v for v in inventory['members']};members={}
 with tarfile.open(fileobj=io.BytesIO(raw),mode='r:gz')as tar:
  items=tar.getmembers();require(len(items)==len(expected)and{m.name for m in items}==set(expected),'Exact626MemberSet')
  for item in items:
   require(item.isfile(),'OnlyRegularEvidenceFiles');data=tar.extractfile(item).read();spec=expected[item.name];require(len(data)==spec['bytes']and hashlib.sha256(data).hexdigest()==spec['sha256'],'ExactRawEvidenceBytes');members[item.name]=data
 return members

def wire_and_wait(members,attempt,case,value):
 prefix=f'attempt{attempt}/case-{case}/';rows=[json.loads(line)for line in members[prefix+'proxy-events.jsonl'].splitlines()]
 receipt=json.loads(members[prefix+'proxy-process.json']);require(receipt['exit']==receipt['wait']==0 and receipt['signal']is None and receipt['forced']is False and receipt['stdout_reader_joined']is True and receipt['proc_PID_absent_after_wait']is True and type(receipt['starttime'])is str and receipt['starttime'].isdigit()and int(receipt['starttime'])>0,'ActualPositiveProxyWaitPipesPID')
 if case in(1,2,3):
  held=[r for r in rows if r['kind']=='ready-held'];forwarded=[r for r in rows if r['kind']=='ready-forwarded'];require(len(held)==len(forwarded)==1,'ActualPGReadyHoldAndForwardOnce')
  snapshot=value['actual_gate_reached']['held'];require(held[0]['backend_pid']==snapshot['backend_pid']and held[0]['sql']==snapshot['sql']and held[0]['parameter_hex']==snapshot['parameter_hex']and held[0]['sync_seen']is True,'ActualExecuteBindSyncReadySameTarget')
  require(held[0]['monotonic']<=forwarded[0]['monotonic']and forwarded[0]['monotonic']-held[0]['monotonic']<=25,'ActualPGReadyHold25')
  frames=[r for r in rows if r['kind']=='actual-target-backend-frame'and r['backend_pid']==snapshot['backend_pid']]
  require(any(r['kind_hex']=='5a'for r in frames),'ActualTargetBackendReadyFrame')
  if case==2:require(any(r['kind_hex']=='45'and b'C57014\0'in bytes.fromhex(r['body_hex'])for r in frames),'ActualSameBackend57014NotStartupQuery')
 else:
  held=[r for r in rows if r['kind']=='backend-target-reply-read-held'];released=[r for r in rows if r['kind']=='backend-target-reply-released'];require(len(held)==len(released)==1,'ActualLuaReplyHoldReleaseOnce')
  require(hashlib.sha256(bytes.fromhex(held[0]['reply_hex'])).hexdigest()==held[0]['reply_sha256'],'ActualLuaReplyHash')
  require(any(r['kind']=='reply-forwarded'and r['target']is True and r['reply_sha256']==held[0]['reply_sha256']for r in rows),'ActualHeldLuaReplyForwarded')
  require(0<=released[0]['monotonic']-held[0]['monotonic']<=25,'ActualLuaHold25')
 return {'actual_wire_and_physical_helper_wait':True}

def observations(members=None):
 members=load_members()if members is None else members;aggregate=json.loads((ROOT/'Root-aggregate-review.json').read_text());require(aggregate['raw_host_run_exits']==EXPECTED_EXITS,'OriginalHostExits110')
 selected=[]
 for attempt in(1,2,3):
  prefix=f'attempt{attempt}/';run=json.loads(members[prefix+'integration.json']);require(run['actual_host_exit']==EXPECTED_EXITS[attempt-1],'LiteralActualHostExit')
  before=members[prefix+'default-before-private.json'];after=members[prefix+'default-after-private.json'];require(before==after and run['default_database_unchanged']is True,'LiteralDefaultManifestBytes')
  manifest=json.loads(before)['manifest'];require(len(manifest['tables'])==62 and len(manifest['sequences'])==59 and len(manifest['columns'])==501 and len(manifest['constraints'])==198 and len(manifest['indexes'])==255,'ActualDefault6259Scope')
  require(run['cleanup_budget']['within_budget']is True,'ActualCleanupBudget90')
  require([row['case']for row in run['cases']]==({1:[1],2:[2,3],3:[4]}[attempt]),'NoPassingPrefixReplay')
 for attempt,case in SELECTED:
  prefix=f'attempt{attempt}/case-{case}/';raw=members[prefix+'callback-case.json'];value=json.loads(raw)
  qualification=validate_native_capture(value,case,value['database']);wire_and_wait(members,attempt,case,value);require(qualification['capture_valid']is True,'ExactFinalTypedPredicate')
  expected=next(row for row in aggregate['actual_passed_cases']if row['case']==case);require(hashlib.sha256(raw).hexdigest()==expected['capture_sha256'],'BoundOriginalCaptureSHA')
  receipt=json.loads(members[prefix+'Root-actual-Rust-result.json']);require(receipt['actual_passed']==1,'ActualOneRustCase')
  selected.append({'case':case,'attempt':attempt,'raw_capture_passed':value['passed'],'capture_only':value['capture_only'],'capture_complete':True,'Root_typed_predicate_passed':True,'actual_Rust_exit':0})
 for attempt,case in[(1,2),(2,4)]:
  value=json.loads(members[f'attempt{attempt}/case-{case}/callback-case.json']);require(value['capture_complete']is False and value['passed']is False and'actual_gate_reached'not in value,'OriginalPartialGateNegativeNotRelabeled')
 fate=json.loads(members['attempt2/case-3/callback-case.json'])['actual_HTTP_response_write_fate'];require(fate['handler_cancelled']==1 and fate['response_statuses']==[]and fate['bytes_written']==0 and fate['write_errors']==[],'ActualCase3FateNotInferred')
 return {'passed':True,'qualification_kind':'four-historical-bounded-callback-controls','actual_selected_cases':selected,'original_host_run_exits':EXPECTED_EXITS,'raw_all_six_capture_passed_fields_false':True,'partial_negatives_preserved':[(1,2),(2,4)],'actual_HTTP_case3_fate':fate,'new_app_or_request_runs':0,'Source_business_Main_current829_fullM8_performance_acceptance':False}

def validate():
 inventory=json.loads((ROOT/'packet-inventory.json').read_text())
 actual={str(p.relative_to(ROOT))for p in ROOT.rglob('*')if p.is_file()and'__pycache__'not in p.parts and p.name!='packet-inventory.json'};require(actual==set(inventory['files']),'CompletePacketInventory')
 for name,row in inventory['files'].items():
  raw=(ROOT/name).read_bytes();require(len(raw)==row['bytes']and hashlib.sha256(raw).hexdigest()==row['sha256'],'ExactPacketFile:'+name)
 return observations()
if __name__=='__main__':print(json.dumps(validate(),sort_keys=True))
