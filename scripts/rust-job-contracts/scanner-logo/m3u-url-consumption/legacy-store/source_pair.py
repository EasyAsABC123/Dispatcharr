"""Existing pinned Original ORM batch helper and unchanged legacy Store integration test.
Original Dispatcharr/dependency authors retain code/algorithms/licenses.
"""
import json,os,subprocess
from pathlib import Path
assert os.environ.get('JOB_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_jobs_')
report={'passed':False,'native_test':'original_batch_create_touch_change_dedup_and_transaction_rollback','native_test_exit':None}
try:
 value=subprocess.run(['/dispatcharrpy/bin/python','/pair-tools/sql_fixture.py'],capture_output=True,text=True,timeout=180)
 Path('/evidence/Original-ORM-helper.log').write_text(value.stdout+value.stderr);value.check_returncode();fixture=json.loads(value.stdout.splitlines()[-1]);p=Path('/evidence/ingest-batch-fixture.json');p.write_text(json.dumps(fixture,indent=2)+'\n');report['Source_batch_steps']=len(fixture['steps'])
 env=dict(os.environ,INGEST_DISPOSABLE_FIXTURE='playback-fixture-only',INGEST_BATCH_FIXTURE=str(p),DATABASE_TEST_URL=os.environ['DATABASE_URL'])
 result=subprocess.run(['/native-m3u-peer',report['native_test'],'--exact','--ignored','--nocapture','--test-threads=1'],env=env,capture_output=True,text=True,timeout=180)
 Path('/evidence/normal-Store-API.log').write_text(result.stdout+result.stderr);report['native_test_exit']=result.returncode;result.check_returncode();assert '1 passed; 0 failed'in result.stdout;report['passed']=True
finally:Path('/evidence/source-url-hash-private.json').write_text(json.dumps(report,indent=2)+'\n')
