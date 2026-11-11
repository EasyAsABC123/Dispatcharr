import os,sys,json,subprocess
from pathlib import Path
assert os.environ.get('OPERATOR_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_operator_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
r=subprocess.run(['/evidence/native-operator','--ignored','--nocapture','--test-threads=1'],env=dict(os.environ,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0'),capture_output=True,timeout=45)
Path('/evidence/physical-tests.log').write_bytes(r.stdout+r.stderr)
expected=[b'test cancelled_observer_retains_sql_admission_caller_and_settles_single_insert ... ok',b'test error_response_held_ready_keeps_caller_admission_until_sync_and_close ... ok']
passed=r.returncode==0 and all(name in r.stdout for name in expected) and b'test result: ok. 2 passed; 0 failed;' in r.stdout
Path('/evidence/operator-cli-source.json').write_text(json.dumps({'passed':passed,'native_verified':True,'scope':'Actual PostgreSQL lock/caller cancellation/admission/single eventual insert/shutdown only','cases':[{'id':'cancelled-observer-retains-physical-SQL-until-single-insert-settles'},{'id':'actual-23505-ErrorResponse-held-ReadyForQuery-retains-owner-until-sync-and-close'}]},indent=2)+'\n')
assert passed,(r.stdout+r.stderr)[-3000:]
