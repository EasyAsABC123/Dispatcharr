"""Source batch capture validity; literal observations remain Source authority.
No expectation-driven request selection, replay or caller/server mutation.
"""
import hashlib,json
from pathlib import Path

def qualify(root):
 root=Path(root);execution=json.loads((root/'execution-private.json').read_text());cold=json.loads((root/'ts-cache-source-private.json').read_text());profiles=json.loads((root/'profile-arguments-owner-private.json').read_text());fail=[]
 def check(v,label):
  if not v:fail.append(label)
 check(execution.get('controller_exit')==0 and execution.get('capture_complete')is True,'CompleteHostCaptureAndCleanup')
 check(cold['passed']is False and cold['capture_valid']is True,'RawCaptureOnlyFalseRetained')
 check(cold['actual_API_requests']==1 and len(cold['records'])==1,'OneNewPATCHOnly')
 check([r['label']for r in cold['helper_calls']]==['TS_initial','Base_initial','Base_after_write','TS_after_write','TS_after_fixed_expiry'],'ColdTSFirstActualOrder')
 check(len(cold['accessor_calls'])==6 and all(r.get('returned')is True for r in cold['helper_calls']+cold['accessor_calls']),'AllElevenActualReturns')
 gate=cold.get('physical_cache_gate',{});rows=gate.get('rows',[]);held=[r for r in rows if r['kind']=='backend-target-reply-read-held'];released=[r for r in rows if r['kind']=='backend-target-reply-released']
 check(not gate.get('invalid')and gate.get('listener_joined')is True and gate.get('release_thread_joined')is True,'PhysicalRelayActuallyRetired')
 check(len(held)==len(released)==1,'ExactlyOnePhysicalConsumerGETReply')
 if len(held)==len(released)==1:check(2.4<=released[0]['monotonic']-held[0]['monotonic']<=2.75,'Actual2p4SecondReplyHold')
 first=cold['helper_calls'][0];last=cold['helper_calls'][-1];stamp=first['after']['TS_own_time'];age=None
 check(first['before']['TS_has_own_cache']is False and first['after']['TS_has_own_cache']is True,'GenuinelyColdOwnTSContext')
 if type(stamp)in(int,float):
  age=last['began_wall']-last['before']['TS_own_time'];check(first['began_wall']<=stamp<=first['completed_wall'],'ActualSourcePreloadStampWithinCall');check(age>=10,'ActuallyExpiredRelevantTSStamp');check(last['before']['TS_own_time']==stamp,'NoInterveningTSReloadReset')
  check(last['began_wall']-first['completed_wall']<10,'ExpiryDistinguishesPostloadTimestamp')
 else:check(False,'ActualTSStampCaptured')
 check(profiles['passed']is False and profiles['capture_valid']is True,'ProfileCaptureOnly')
 check(profiles['actual_API_requests']==0 and profiles['actual_argument_attempts']==len(profiles['argument_cases'])==18 and len(profiles['owner_calls'])==6,'NewArgument18Owner6CoverageNoOutput24Replay')
 for r in profiles['argument_cases']:check(r['before']==r['after'],'ArgumentReadOnlySQL:'+r['name'])
 check(profiles.get('owned_keys_absent_after_delete')is True and profiles.get('actual_Redis_connection_retired')is True and profiles.get('actual_Django_connections_closed')is True,'ProfileExactOwnedCleanup')
 check(all(s['passed']is True for s in execution.get('cleanup_steps',[]))and len(execution.get('cleanup_steps',[]))==8,'AllEightHostCleanupSteps')
 result={'capture_and_domain_valid':not fail,'failures':fail,'Source_original_recorder_false_preserved':True,'actual_PATCH_status':cold['records'][0].get('status'),'actual_helper_values':[{'label':r['label'],'value':r.get('value')}for r in cold['helper_calls']],'actual_relevant_TS_age_seconds':age,'profile_argument_observations':[{'name':r['name'],'observed':r['observed']}for r in profiles['argument_cases']],'actual_owner_returns':[r.get('returned')for r in profiles['owner_calls']],'Source_model_and_cache_authority_only':True,'Main_or_profile_delivery_or_media_or_performance':False,'raw_sha256':{name:hashlib.sha256((root/name).read_bytes()).hexdigest()for name in ['execution-private.json','ts-cache-source-private.json','profile-arguments-owner-private.json']}}
 return result
