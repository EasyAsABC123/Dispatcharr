"""One actual normal Main health read; no catalogue/Source requests or retry."""
import hashlib,json,time
from pathlib import Path
import fixture_db as DB
from absolute_http import Observation
from main_http import raw_redis,write

def main():
 DB.setup('audit');out=Path('/evidence');before=DB.fingerprint();cache_before=raw_redis();owner=Observation('http://127.0.0.1:9192',8,address='127.0.0.1');row={'began_monotonic':time.monotonic()};report={'scope':'SingleMain57SQLReadThenStopObserver','actual_request_count':0,'path':'/health/ready','method':'GET','Main_destination':'127.0.0.1:9192 shared baseline network namespace','database':DB.DB,'before':before,'raw_Redis_before':cache_before,'Source_business_requests':0,'catalogue26_replayed':False}
 try:
  report['actual_request_count']=1;r=owner.request('/health/ready',{'Host':'127.0.0.1:5657','User-Agent':'owned-Main57-stop-observer'},method='GET');raw=r.raw.read(65537)
  if len(raw)>65536:raise ValueError('ReadBody64KiB')
  row.update(status=r.status_code,headers=list(r.raw.getheaders()),body_hex=raw.hex(),body_sha256=hashlib.sha256(raw).hexdigest(),absolute_observation=owner.complete())
 finally:
  row['physical_close']=owner.cancel();row['completed_monotonic']=time.monotonic();report['observation']=row;report['after']=DB.fingerprint();report['raw_Redis_after']=raw_redis();write(out/'Main-read-private.json',report)
if __name__=='__main__':main()
