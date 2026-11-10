"""Copied-only actual Source/Native DVR cache and typed global lock prerequisites.
Original Dispatcharr authors retain application/algorithm credit and AGPL.
No native SeriesRules task consumption/result writing is claimed.
"""
import json,os,subprocess
from pathlib import Path
import source_series_rules_worker as f
report=f.report;report['passed']=False
try:
 for label,group in [('absent',None),('empty',{}),('empty-rules',{'series_rules':[]}),('nonlist',{'series_rules':'owned-nonlist'}),('nonmap',{'series_rules':[0,None,[]]})]:
  f.CoreSettings.objects.filter(key='dvr_settings').delete()
  if group is not None:f.CoreSettings.objects.create(key='dvr_settings',name='Owned native DVR policy',value=group)
  f.django_cache.delete(f.CoreSettings.group_cache_key('dvr_settings'))
  expected=f.CoreSettings.get_dvr_settings()
  for mode in ['source-cache-hit','native-db-fill']:
   if mode=='native-db-fill':f.django_cache.delete(f.CoreSettings.group_cache_key('dvr_settings'))
   value={'database_url':'postgres://dispatch:playback-fixture-only@127.0.0.1:5432/'+f.database,'cache_url':'redis://:playback-fixture-only@127.0.0.1:6379/0','expected':expected}
   peer=subprocess.run(['/evidence/native-series-policy','--ignored','--exact','native_series_dvr_policy_peer','--nocapture'],input=json.dumps(value),capture_output=True,text=True,timeout=30)
   with Path('/evidence/native-series-policy.log').open('a')as log:log.write(peer.stdout+peer.stderr)
   peer.check_returncode();marker='NATIVE_SERIES_POLICY_JSON=';actual=json.loads(next(line.split(marker,1)[1]for line in peer.stdout.splitlines()if marker in line))
   f.check(label+'-'+mode+'-exactSourceValue-and-retention',actual['value']==expected and actual['caller_released']and actual['physical_operations']==0)
   f.check(label+'-'+mode+'-Original-loads-native-fill',f.django_cache.get(f.CoreSettings.group_cache_key('dvr_settings'))==expected)
   f.check(label+'-'+mode+'-TTL',285<=f.cache.ttl(':1:'+f.CoreSettings.group_cache_key('dvr_settings'))<=300)
 env=dict(os.environ,JOB_REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0')
 peer=subprocess.run(['/evidence/native-series-lock','--ignored','--exact','global_series_key_no_renewal_and_foreign_owner_fencing','--nocapture'],env=env,capture_output=True,text=True,timeout=20)
 Path('/evidence/native-series-lock.log').write_text(peer.stdout+peer.stderr);peer.check_returncode()
 f.check('actual-global-literal-key-no-renew-and-foreign-fence','1 passed; 0 failed'in peer.stdout)
 report['scope']='Actual Original SeriesRules worker79 plus native retained DVR cache/native↔Django interoperability and exact global-all lock/no-renew/foreignfence; no native Series consumer or configured recording rule scheduling'
 report['passed']=True
finally:
 f.cache.delete(f.lock);f.broker.flushdb();f.backend.flushdb()
 report['queue_and_results_after_cleanup']=[f.broker.dbsize(),f.backend.dbsize()]
 Path('/evidence/source-native-series-prereq-private.json').write_text(json.dumps(report,indent=2,default=str)+'\n')
