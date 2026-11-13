"""Offline retained-data-only denial CORE proof; never a whole-case/run pass."""
import base64,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3];OUT=Path(__file__).resolve().parent
CAP=ROOT/'artifacts/rust/m3u-failure-native-physical-remaining-2'
POLICY=ROOT/'artifacts/rust/m3u-failure-qa-private-6/tools/qa-policy.json'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
r=json.loads((CAP/'source-logo-scanner-private.json').read_text());integration=json.loads((CAP/'integration.json').read_text());policy=json.loads(POLICY.read_text());case=r['cases'][0];account=case['before']['accounts'][0]['id'];username='owned_m3u_failure_'+str(account);script=policy['result_write_lua_sha1'];line=next(i for i,v in enumerate(policy['result_write_lua'].splitlines(),1)if "redis.call('SETEX'"in v)
assert integration['actual_controller_exit']==1 and r['passed']is False
assert integration['Redis_before_Source']['redis_version']=='8.10.3'and integration['Redis_before_Source']['before_any_Source_fixture_task']is True
assert all(case['ACL_preflight'][v]is True for v in ('owned_profile_exact','setex_denied','publish_allowed','nonmutating'))
assert case['run']=={'run':'Err(Transport(Corrupt))','slots':1}
assert case['denied_backend_before_assertion']=={'raw_base64':None,'metadata':None,'ttl':-2}
rows=case['redis']['receipts'];assert len(rows)==2 and rows[0]['script_cache_miss']is True and rows[1]['script_cache_miss']is False and case['redis']['target_replies']==1
assert all(v['script_sha1']==script and v['actual_server_reply_observed']is True for v in rows)
raw=base64.b64decode(rows[1]['raw_base64'],validate=True);expected=("-ERR ACL failure in script: User "+username+" has no permissions to run the 'setex' command script: "+script+", on @user_script:"+str(line)+".\r\n").encode();assert raw==expected and len(raw)==rows[1]['reply_bytes']and hashlib.sha256(raw).hexdigest()==rows[1]['reply_sha256']
assert case['peer_close']['process_settled']is True and case['peer_close']['actual_exit']==0 and case['redis']['physical_close_verified']is True and case['redis']['errors']==[]
assert case.get('tail')is None and len(r['cases'])==1
proof={'scope':'ONLY retained denial CORE separately validated offline; original guest/host1/failed assertion preserved. Single-tail and oldSUCCESS UNEXECUTED, whole physical case NOT passed','core_qualified':True,'actual_redis_version':'8.10.3','username':username,'exact_writer_sha1':script,'exact_SETEX_line':line,'actual_error_raw_base64':rows[1]['raw_base64'],'actual_error_sha256':rows[1]['reply_sha256'],'executions_after_NOSCRIPT':1,'actual_backend_absent':True,'actual_operational_return':case['run'],'actual_peer_and_proxy_closed':True,'source_report_sha256':sha(CAP/'source-logo-scanner-private.json'),'integration_sha256':sha(CAP/'integration.json'),'qa_policy_sha256':sha(POLICY),'whole_case_or_run_qualified':False,'tail_qualified':False,'oldSUCCESS_qualified':False,'no_new_application_or_Redis_execution':True}
(OUT/'proof.json').write_text(json.dumps(proof,indent=2)+'\n');print(json.dumps(proof,indent=2))
