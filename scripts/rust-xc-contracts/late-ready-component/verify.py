"""Portable selected-predicate verification. No private fallback or dispatch."""
import hashlib,json
from pathlib import Path
import old1_validator,new9_validator
HERE=Path(__file__).resolve().parent
def need(v,label):
 if not v:raise ValueError(label)
def read(name):return json.loads((HERE/name).read_text())
def validate(attempts,old,new,cleanup):
 need(len(attempts)==3 and [a['attempt']for a in attempts]==[1,2,3],'ExactThreeAttempts')
 for a in attempts:
  need(type(a['controller_exit'])is int and a['controller_exit']==1 and a['passed']is False,'OriginalThreeHostFailuresPreserved')
  need(a['default_before']==a['default_after']and a['backend_after']==[],'DefaultEqualityAndBackendAbsence')
  schema=read('support/schema.json')
  need(sorted(a['default_before']['public62']['tables'])==schema['tables']and sorted(a['default_before']['public62']['sequences'])==schema['sequences'],'MandatoryAuditTablesAndSequences')
 need([a['raw_capture_passed']for a in attempts]==[False,False,True],'LiteralRecorderFlags')
 a=old1_validator.validate(old,HERE/'retained/attempt-2');b=new9_validator.validate(new,HERE/'retained/attempt-3')
 rows=cleanup['queries'];need(len(rows)==12 and {(r['attempt'],r['kind'])for r in rows}=={(n,k)for n in(1,2,3)for k in('containers','networks','volumes','CID')},'AllTwelveCleanupReceipts')
 for r in rows:
  need(r['physically_settled']is True and r['pipes_settled']is True and type(r['actual_exit'])is int and r['actual_exit']==(1 if r['kind']=='CID'else 0),'PhysicalCleanupCommandSettlement')
  for k in('stdout_sha256','stderr_sha256'):need(type(r[k])is str and len(r[k])==64,'BoundedCleanupOutputDigest')
 ids=cleanup['host_identity_absence'];need(len(ids)==66 and all(x['original_process_absent']is True and x['after_starttime']!=x['starttime']for x in ids),'SixtySixOriginalHostIdentitiesAbsent')
 return {'selected_projected_physical_pairs':a['qualified_fault_cases']+b['qualified_fault_cases'],'old_owned_after':'unmeasured','all_original_host_exits':[1,1,1],'parameter_bytes_public':False,'actual_track_activity_query_size':'unmeasured','observed_pgstat_agreement_bytes':1023,'Source_Main_fullM7_performance':False}
def run():
 for name,expected in read('payloads.json').items():need(hashlib.sha256((HERE/name).read_bytes()).hexdigest()==expected,'ExactRetainedPayload:'+name)
 return validate(read('attempts.json'),read('retained/attempt-2/faults.json'),read('retained/attempt-3/faults.json'),read('cleanup.json'))
if __name__=='__main__':print(json.dumps(run(),indent=2))
