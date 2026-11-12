"""Actual deterministic origin/prefix/decode qualification ONLY, no Django/Source app."""
import hashlib,json,os,threading,time,urllib.request
from pathlib import Path
from base_origin import mp4_command
from ledger_origin import Server,FILE_SHA
from local_command import LocalCommand
from decoder import decode

def main():
 if os.environ.get('NORMAL_CONTROL_ONLY')!='playback-fixture-only':raise RuntimeError('OwnedOriginControlOnly')
 root=Path('/evidence');media=Path('/tmp/fixture.mp4');job=LocalCommand(mp4_command(media),limit=1048576,stderr_path=root/'generate.stderr');job.wait(60)
 if hashlib.sha256(media.read_bytes()).hexdigest()!=FILE_SHA:raise ValueError('DeterministicActualFixtureHash')
 nonce='a'*32;server=Server(('127.0.0.1',0),media,nonce,32768);thread=threading.Thread(target=server.serve_forever);thread.start();result={'passed':False,'Source_application_executed':False,'Native_executed':False};prefix=root/'actual-prefix.mp4'
 try:
  url='http://127.0.0.1:'+str(server.server_port)+'/movie/fixture/fixture/fixture.mp4';request=urllib.request.Request(url,headers={'Range':'bytes=0-1048575','X-Fixture-Nonce':nonce,'X-Fixture-Preflight':'1'});began=time.monotonic()
  with urllib.request.urlopen(request,timeout=8)as response,prefix.open('xb')as stream:
   if response.status!=206:raise ValueError('ActualRangePrefixStatus')
   total=0
   while total<1048576:
    data=response.read(min(8192,1048576-total))
    if not data:raise ValueError('ActualPrefixShort')
    stream.write(data);total+=len(data)
    if time.monotonic()-began>40:raise ValueError('ActualPrefix40SecondDeadline')
  result['prefix_seconds']=time.monotonic()-began;result['decoder']=decode(prefix,root/'decode.log');result['passed']=True
 finally:
  server.stop_owned();thread.join(10);result['origin_terminal']=server.ledger.snapshot();result['origin_server_thread_settled']=not thread.is_alive();result['generator_progress_inferred']=False;result['Source_application_executed']=False
  if thread.is_alive()or result['origin_terminal']['invalid']or result['origin_terminal']['actual_open_TCP']:result['passed']=False
  (root/'fixture-preflight.json').write_text(json.dumps(result,indent=2)+'\n')
 if not result['passed']:raise SystemExit(1)
if __name__=='__main__':main()
