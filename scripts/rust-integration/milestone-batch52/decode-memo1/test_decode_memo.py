"""Tiny host-only admission controls; no app, media tooling or real capture."""
import hashlib,json,sys,tempfile,time
from pathlib import Path
sys.dont_write_bytecode=True
sys.path.insert(0,str(Path(__file__).parent/'overlay'))
from decode_memo import DecodeMemo,outcome

def main():
 start=time.process_time();checks=[]
 with tempfile.TemporaryDirectory() as folder:
  root=Path(folder);a=root/'a.ts';b=root/'b.ts';a.write_bytes(b'a'*64);b.write_bytes(b'a'*64)
  identity={'image_id':'sha256:'+'1'*64,'ffmpeg_sha256':'2'*64,'argv_environment':['owned','baseline']}
  memo=DecodeMemo(root,identity,8);calls=[]
  def run(p,options=(),exit=0,incomplete=False):
   argv=['owned','-i','/evidence/'+p.name,*options]
   def invoke():
    calls.append(p.name);text='tiny-test-framehash-no-real-media\n';transcript=p.with_suffix('.framehash');transcript.write_text(text)
    stdout=p.with_suffix('.command.stdout');stdout.write_text(text);stderr=p.with_suffix('.command.stderr');stderr.write_bytes(b'')
    r={'actual_exit':exit,'failure':None,'stderr_bytes':0,'stderr_sha256':hashlib.sha256(b'').hexdigest(),'stdout_path':str(transcript),'stdout_sha256':hashlib.sha256(text.encode()).hexdigest(),'media_bytes':p.stat().st_size,'media_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'command_receipt':None if incomplete else {'actual_exit':exit,'failure':None,'argv':argv,'stdout':str(stdout),'stderr':str(stderr),'stdout_bytes':len(text.encode()),'stderr_bytes':0}}
    p.with_suffix('.ts.decode.json').write_text(json.dumps(r))
    return r,text
   return memo.decode(p,argv,invoke)[0]
  first=run(a);second=run(b)
  assert len(calls)==1 and second['kind']=='reused_decode' and not second['new_physical_command_executed'] and outcome(second)==first
  assert second['authority_receipt_path'].endswith('a.ts.decode.json') and second['input_custody']['path']==str(b)
  checks.append('identical-bytes-one-physical-command-honest-authority')
  b.write_bytes(b'b'*64);assert run(b).get('kind')!='reused_decode';checks.append('same-length-different-bytes-misses')
  assert run(b,('-map','other')).get('kind')!='reused_decode';checks.append('changed-options-misses')
  memo.identity['ffmpeg_sha256']='3'*64;assert run(a).get('kind')!='reused_decode';checks.append('changed-tool-misses')
  memo.identity['image_id']='sha256:'+'4'*64;assert run(a).get('kind')!='reused_decode';checks.append('changed-image-misses')
  a.write_bytes(b'c'*64);count=len(calls);run(a,exit=1);run(a,exit=1);assert len(calls)==count+2;checks.append('failed-decode-never-seeds')
  a.write_bytes(b'd'*64);count=len(calls);run(a,incomplete=True);run(a,incomplete=True);assert len(calls)==count+2;checks.append('incomplete-proof-never-seeds')
  a.write_bytes(b'e'*64);run(a);a.with_suffix('.framehash').write_text('corrupted')
  try:run(a)
  except ValueError as e:assert str(e)=='PhysicalDecodeTranscriptChanged'
  else:raise AssertionError('corrupted authority accepted')
  checks.append('corrupted-transcript-refused')
  a.write_bytes(b'f'*64);run(a);a.with_suffix('.ts.decode.json').write_text('{}')
  try:run(a)
  except ValueError as e:assert str(e)=='PhysicalDecodeReceiptChanged'
  else:raise AssertionError('corrupted receipt accepted')
  checks.append('corrupted-physical-receipt-refused')
  a.write_bytes(b'g'*64);run(a);a.with_suffix('.command.stderr').write_text('actual stderr corruption')
  try:run(a)
  except ValueError as e:assert str(e)=='PhysicalCommandArtifactChanged:stderr'
  else:raise AssertionError('corrupted stderr accepted')
  checks.append('corrupted-physical-stderr-refused')

  link=root/'link.ts';link.symlink_to(a)
  try:run(link)
  except ValueError as e:assert str(e)=='OwnedRegularTSRequired'
  else:raise AssertionError('symlink accepted')
  checks.append('symlink-input-refused')
 elapsed=time.process_time()-start
 assert elapsed<1
 print(json.dumps({'passed':True,'checks':checks,'CPU_seconds':elapsed,'actual_media_or_Docker':False}))
if __name__=='__main__':main()
