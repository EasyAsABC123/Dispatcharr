"""Exactly one fixed Native Main business GET; no work on import or retry."""
import hashlib,json,os,socket,time
from pathlib import Path
from absolute_http import Observation

def capture(root,plan,address,origin_address,nonce,candidate_port=9192,origin_port=8080):
 root=Path(root);spec=plan['Native_case'];began=time.monotonic();report={'cases':[],'actual_business_request_invocations':0,'Source_replay':0}
 def save():
  data=(json.dumps(report,indent=2)+'\n').encode()
  if len(data)>4194304:raise ValueError('CaptureEvidenceCapacity')
  (root/'native-capture-private.json').write_bytes(data)
 def origin():
  owner=Observation('http://127.0.0.1:'+str(origin_port),seconds=3,address=origin_address,io_seconds=3)
  try:
   response=owner.request('/__capture_state__',{'X-Fixture-Nonce':nonce});raw=response.raw.read(65537)
   if response.status_code!=200 or len(raw)>65536:raise RuntimeError('OwnedOriginObservation')
   value=json.loads(raw);owner.complete();return value
  finally:owner.cancel()
 owner=None;row={k:spec[k]for k in ['id','name','method','path']};row.update(request_headers=spec['headers'],request_body_hex='');report['cases'].append(row)
 try:
  report['origin_before']=origin();owner=Observation('http://candidate:'+str(candidate_port),seconds=15,address=address,io_seconds=15);row['began_monotonic']=time.monotonic();report['actual_business_request_invocations']=1;save()
  try:
   response=owner.request(spec['path'],spec['headers'],method='GET',body=b'');raw=response.raw.read(1048577)
   if len(raw)>1048576:raise ValueError('WholeResponse1MiBCapacity')
   row.update(status=response.status_code,headers=list(response.raw.getheaders()),body_hex=raw.hex(),body_bytes=len(raw),body_sha256=hashlib.sha256(raw).hexdigest(),absolute_observation=owner.complete())
  except BaseException as error:row['transport_error']={'type':type(error).__name__,'message':str(error)[:1024]}
  finally:
   row['physical_close']=owner.cancel();row['completed_monotonic']=time.monotonic();save()
  report['origin_after']=origin();report['capture_elapsed_seconds']=time.monotonic()-began
 finally:save()
 return report

def main():
 if os.environ.get('SPA_FIXTURE_AUTHORITY')!='owned-native-spa-get-only':raise RuntimeError('OwnedSPAOnly')
 plan=json.loads(Path('/spa-inputs/plan.json').read_text());capture('/evidence',plan,socket.gethostbyname('candidate'),socket.gethostbyname('origin'),os.environ['CAPTURE_NONCE'])
if __name__=='__main__':main()
