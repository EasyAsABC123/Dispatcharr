"""No Django/model/task/Beat/media imports. One actual compiled schema-only peer."""
import json,os
from pathlib import Path
from native_job import NativeJob
from schema_contract import validate

def main():
 root=Path('/evidence');receipt={'passed':False,'scope':'prepare-only actual SQL on copied schema; no tasks/media'};job=None
 try:
  database=os.environ['POSTGRES_DB'];assert database.startswith('rust_probe_backup_jobs_')and len(database)<=64
  environment=dict(os.environ,OWNED_DVR_SCHEMA_URL='postgres://dispatch:playback-fixture-only@localhost:5432/'+database,OWNED_DVR_SCHEMA_DISPOSABLE='copied-schema-only-no-tasks')
  job=NativeJob({},root,environment)
  actual=job.wait(30);receipt['actual_peer']=actual
  assert actual['actual_wait_completed']is actual['pipes_settled']is actual['writer_settled']is actual['original_process_absent']is True and actual['actual_exit']==0 and actual['invalid']is None and actual['forced']is False
  raw=json.loads((root/'native-dvr-schema-private.json').read_text());expected=json.loads(Path('/job-tools/expected-statements.json').read_text());receipt['acceptance']=validate(raw,expected);receipt['passed']=True
 except BaseException as error:
  receipt.update(error_type=type(error).__name__,error=str(error)[:2048])
 finally:
  if job is not None:receipt['actual_peer_final']=job.close()
  (root/'schema-guest-private.json').write_text(json.dumps(receipt,indent=2)+'\n')
 return 0 if receipt['passed']else 1
if __name__=='__main__':raise SystemExit(main())
