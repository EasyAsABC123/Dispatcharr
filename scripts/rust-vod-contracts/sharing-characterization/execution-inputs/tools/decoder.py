"""Fixed strict decoder for a retained actual1MiB prefix; no data-dependent tuning."""
import hashlib,json
from pathlib import Path
from local_command import LocalCommand
CAP=1048576

def decode(path,log):
 path=Path(path)
 if path.stat().st_size!=CAP:raise ValueError('ExactRetainedPrefixRequired')
 job=LocalCommand(['ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',str(path),'-map','0:v:0','-map','0:a:0','-t','3','-f','null','-'],limit=1048576,stderr_path=Path(str(log)+'.stderr'))
 raw=job.wait(45);Path(log).write_bytes(raw);frames=max((int(line.split('=')[1])for line in raw.decode().splitlines()if line.startswith('frame=')),default=0)
 if frames<50:raise ValueError('StrictAudioVideoMinimum50Frames')
 return{'passed':True,'retained_bytes':CAP,'retained_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'frames':frames,'strict_audio_video_seconds':3,'minimum_frames':50,'process_reaped':job.process.poll()is not None,'pipes_settled':job.physically_settled}
