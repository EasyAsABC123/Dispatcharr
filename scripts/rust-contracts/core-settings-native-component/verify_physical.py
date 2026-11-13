"""Seven selected retained physical predicates; no dispatch/private fallback."""
import json
from pathlib import Path
import positive_validator as P
HERE=Path(__file__).resolve().parent
def need(v,label):
 if not v:raise ValueError(label)
def load(n):return json.loads((HERE/n).read_text())
def validate(positives,attempts,cleanup):
 need([(x['value']['cases'][0]['case'],x['attempt'])for x in positives]==[(1,2),(2,3),(3,4),(4,5),(5,5),(6,7),(7,7)],'ExactSevenPredeterminedHistoricalPositives')
 for item in positives:need(P.validate(item['value'],HERE/'physical')['qualified_physical_cases']==1,'FinitePositive')
 need(len(attempts)==7 and [x['attempt']for x in attempts]==list(range(1,8)),'AllSevenAttempts')
 for x in attempts:
  want=0 if x['attempt']==7 else 1;need(type(x['actual_exit'])is int and x['actual_exit']==want and x['actual_passed']is(want==0),'LiteralSixFailuresAndLatestSuccess')
  if x['attempt']==1:need(x['default_after']==x['backend_after']=='unmeasured','OriginalAfterUnmeasured')
  else:need(x['default_before']==x['default_after']and x['backend_after']==[],'LaterDefaultEqualityBackendAbsence')
  need(x['cleanup_within90']is True,'BoundedCleanup')
 need(len(cleanup['queries'])==28 and {(x['attempt'],x['kind'])for x in cleanup['queries']}=={('m5-core-settings-native-1'if n==1 else'm5-core-settings-native-physical-'+str(n),kind)for n in range(1,8)for kind in('containers','networks','volumes','exactCID')},'AllTwentyEightExactCleanupReceipts')
 for x in cleanup['queries']:need(x['actual_exit']==(1 if x['kind']=='exactCID'else 0)and x['physically_settled']is True and x['pipes_settled']is True,'ActualCleanupSettlement')
 need(len(cleanup['host_identity_absence'])==150 and all(x['original_process_absent']is True and x['starttime']!=x['after_starttime']for x in cleanup['host_identity_absence']),'All150OriginalHostIdentitiesAbsent')
 return {'finite_physical_positives':list(range(1,8)),'historical_controller_exits':[x['actual_exit']for x in attempts],'default_after_original':'unmeasured','Source_error_timing_Main_fullM5_performance':False}
if __name__=='__main__':print(json.dumps(validate(load('physical-positives.json'),load('physical-attempts.json'),load('physical-agent-review.json')),indent=2))
