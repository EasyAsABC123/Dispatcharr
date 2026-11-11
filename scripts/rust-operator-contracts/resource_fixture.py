import os,sys,json,subprocess
from pathlib import Path
assert os.environ.get('OPERATOR_DISPOSABLE_FIXTURE')=='playback-fixture-only'
assert os.environ['POSTGRES_DB'].startswith('rust_probe_backup_operator_')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.accounts.models import User
for name in ['a','b']:
 User.objects.create(username='rust-operator-owned-'+name,password='!',custom_properties={'keep':name})
result=subprocess.run(['/evidence/operator-resource-tests','--ignored','--nocapture','--test-threads=1'],env=dict(os.environ,DATABASE_TEST_URL='postgresql://dispatch:playback-fixture-only@127.0.0.1:5432/'+os.environ['POSTGRES_DB'],REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0'),capture_output=True,timeout=90)
Path('/evidence/resource-tests.log').write_bytes(result.stdout+result.stderr)
Path('/evidence/operator-cli-source.json').write_text(json.dumps({'passed':result.returncode==0,'scope':'Native actual owned SQL/Redis resources only; no Source byte parity claim','cases':[{'id':x} for x in ['blocked-SQL-cancel-physical-owner','oversized-row-no-write','applied-DEL-held-reply-over2s-cancel-owner','row-count-and-aggregate-byte-limits-no-write','oversized-condition-key-explicit-no-flushdb','retained-publication-logical-timeout-composite-owner']]},indent=2)+'\n')
assert result.returncode==0, (result.stdout+result.stderr)[-3000:]
