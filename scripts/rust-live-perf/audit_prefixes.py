"""Decode retained synthetic TS prefixes, preserving successful probe diagnostics.
Original Dispatcharr authors retain application/algorithm credit and licenses.
"""
import argparse,hashlib,json,subprocess
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--captures',type=Path,nargs='+',required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
out=args.output;out.mkdir(parents=True,exist_ok=True);assert not(out/'audit.json').exists();phases=[]
for root in args.captures:
 report=json.loads((root/'live-functional.json').read_text());assert report['passed']and not report['performance_verified']
 for phase in report['phases']:
  results=[];backend=phase['backend'];count=phase['count']
  for index,row in enumerate(phase['viewers']):
   relative=backend+'-'+str(count)+'/'+str(index)+'.ts';path=root/relative;payload=path.read_bytes();assert len(payload)==2097152 and hashlib.sha256(payload).hexdigest()==row['prefix_sha256'];del payload
   result=subprocess.run(['ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',str(path),'-map','0:v:0','-map','0:a:0','-t','5','-f','null','-'],capture_output=True,timeout=20)
   assert len(result.stdout)<=65536 and len(result.stderr)<=65536
   label=backend+'-'+str(count)+'-'+str(index)
   (out/(label+'.stdout')).write_bytes(result.stdout);(out/(label+'.stderr')).write_bytes(result.stderr)
   frames=max([int(x.split(b'=')[1])for x in result.stdout.splitlines()if x.startswith(b'frame=')],default=0)
   assert result.returncode==0 and frames>=25,(backend,count,index,result.returncode,frames)
   assert frames==row['decoder_frames']
   results.append({'viewer':index,'prefix_sha256':row['prefix_sha256'],'prefix_bytes':len(path.read_bytes()),'decoder_exit_code':result.returncode,'decoded_video_frames':frames,'stdout_sha256':hashlib.sha256(result.stdout).hexdigest(),'stderr_sha256':hashlib.sha256(result.stderr).hexdigest(),'probe_diagnostic_bytes':len(result.stderr),'raw_entire_prefix_decoder_verified':False})
  phases.append({'backend':backend,'viewers':count,'results':results})
assert len(phases)==6 and {(p['backend'],p['viewers'])for p in phases}=={(b,v)for b in ['source','candidate']for v in [1,10,100]} and sum(len(p['results'])for p in phases)==222
(out/'audit.json').write_text(json.dumps({'passed':True,'scope':'Strict bounded five-second playback decoding of actual2MiB synthetic captures; successful probing diagnostics retained separately, not a clean-every-byte raw-prefix claim','credit':'Original Dispatcharr authors retain full application/algorithm credit and licensing.','decoded_captures':222,'performance_verified':False,'phases':phases},indent=2)+'\n')
print(json.dumps({'passed':True,'captures':222,'with_probe_diagnostics':sum(r['probe_diagnostic_bytes']>0 for p in phases for r in p['results'])}))
