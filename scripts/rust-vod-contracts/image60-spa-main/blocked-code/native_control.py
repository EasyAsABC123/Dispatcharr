"""Only bounded readiness/origin control requests, never a business request."""
import json,os,socket,sys
from absolute_http import Observation
def main():
 mode=sys.argv[1]
 if mode not in ['ready','origin']:raise RuntimeError('ControlOnly')
 if mode=='ready':base='http://candidate:9192';address=socket.gethostbyname('candidate');path='/health/ready';headers={}
 else:base='http://127.0.0.1:8080';address=socket.gethostbyname('origin');path='/__capture_state__';headers={'X-Fixture-Nonce':os.environ['CAPTURE_NONCE']}
 owner=Observation(base,seconds=2,address=address,io_seconds=2)
 try:
  response=owner.request(path,headers);raw=response.raw.read(65537)
  if len(raw)>65536 or response.status_code!=200:raise RuntimeError('BoundedControlNotReady')
  value=json.loads(raw);clock=owner.complete();print(json.dumps({'mode':mode,'value':value,'clock':clock}))
 finally:owner.cancel()
if __name__=='__main__':main()
