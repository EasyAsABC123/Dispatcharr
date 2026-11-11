"""Native-only actual PG/Redis physical ownership controls in this owned fixture.
No Source HTTP parity, route or performance acceptance is inferred.
Original Dispatcharr authors retain application and algorithm credit.
"""
import os,runpy,json,subprocess
from pathlib import Path
os.environ['VOD_REFRESH_CASE_FILTER']='__physical_bootstrap__'
context=runpy.run_path(str(Path(__file__).with_name('provider_refresh_source.py')))
assert os.environ.get('VOD_API_DISPOSABLE_FIXTURE')=='playback-fixture-only'
binary=Path(os.environ['VOD_REFRESH_ACTOR_NATIVE']);assert binary.is_file()
root=Path('/evidence/fair-physical');root.mkdir()
before=context['fingerprints']();sequences=context['sequences']()
try:
 env=dict(os.environ,DATABASE_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/dispatcharr',REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0',VOD_NATIVE_EVIDENCE=str(root),VOD_API_NATIVE_FIXTURE='playback-fixture-only',VOD_FAIR_PHYSICAL_DISPOSABLE='playback-fixture-only')
 process=subprocess.run([str(binary),'vod_api_draft::vod_api_owned_read::fair::physical_tests::actual_','--ignored','--nocapture','--test-threads=1'],env=env,text=True,capture_output=True,timeout=90)
 for name,text in [('stdout',process.stdout),('stderr',process.stderr)]:
  path=root/('native-'+name+'-private.log');path.write_text(text);path.chmod(0o600)
 reports={p.name:json.loads(p.read_text())for p in root.glob('fair-actual-*.json')}
 untouched=before==context['fingerprints']()and sequences==context['sequences']()
 expected={'fair-actual-sql-owner.json','fair-actual-cache-quiescence.json','fair-actual-cache-abort.json','fair-actual-cache-header-quiescence.json'}
 passed=process.returncode==0 and set(reports)==expected and all(value.get('passed')is True for value in reports.values())and untouched
 result={'passed':passed,'scope':__doc__,'records':[{'native_test_reports':sorted(reports),'test_exit_code':process.returncode}],'native_reports':reports,'selected_models_and_sequences_unchanged':untouched,'restored18':untouched,'sequences_restored':untouched,'Source_HTTP_parity':False,'performance_claim':False}
finally:
 if before!=context['fingerprints']()or sequences!=context['sequences']():context['restore']()
print(json.dumps(result,cls=context['Encoder']))
