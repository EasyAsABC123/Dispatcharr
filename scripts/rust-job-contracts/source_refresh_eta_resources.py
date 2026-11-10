"""Native refresh ETA receipt physical controls only, owned broker8/result9.
Original Dispatcharr/Celery/Kombu authors retain algorithm/protocol credit.
No Source task, ingestion, API or default consumer is executed here.
"""
import json,os,subprocess
from pathlib import Path
if os.environ.get('JOB_DISPOSABLE_FIXTURE')!='playback-fixture-only' or not os.environ.get('POSTGRES_DB','').startswith('rust_probe_backup_jobs_'):raise RuntimeError('Copied owned fixture required')
import redis
broker=redis.Redis.from_url(os.environ['CELERY_BROKER_URL']);backend=redis.Redis.from_url(os.environ['CELERY_RESULT_BACKEND'])
if broker.connection_pool.connection_kwargs['db']!=8 or backend.connection_pool.connection_kwargs['db']!=9:raise RuntimeError('Owned namespaces required')
report={'passed':False,'checks':[],'scope':'Native actualRedis refresh receipt frame/count/bytes/budget/ownership/delayedreply controls only; no task parity or app authority'}
try:
 broker.flushdb();backend.flushdb()
 env=dict(os.environ,JOB_REDIS_TEST_URL='redis://:playback-fixture-only@127.0.0.1:6379/0')
 result=subprocess.run(['/evidence/native-refresh-worker','--ignored','refresh_eta::physical::','--nocapture','--test-threads=1'],env=env,capture_output=True,text=True,timeout=45)
 Path('/evidence/native-refresh-eta-physical.log').write_text(result.stdout+result.stderr);result.check_returncode()
 report['checks'].append({'name':'three-realRedis-receipt-resource-tests','passed':'3 passed; 0 failed'in result.stdout})
 report['passed']=all(row['passed']for row in report['checks'])
finally:
 broker.flushdb();backend.flushdb();report['queue_and_results_after_cleanup']=[broker.dbsize(),backend.dbsize()]
 Path('/evidence/source-refresh-eta-resources-private.json').write_text(json.dumps(report,indent=2)+'\n')
