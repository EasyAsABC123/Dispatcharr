"""Review/once-cue bindings; no source/fixture dispatch from this module."""
import datetime,hashlib,json,math,os,time
from pathlib import Path
ACTION='normal-max2-fixed-pair-once'
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def validate(cue,root):
 if cue.get('authorized')is not True or cue.get('action')!=ACTION or cue.get('backends')!=['source','native']or (type(cue.get('repetitions'))is not int or cue.get('repetitions')!=1):raise ValueError('ReviewedNormalPairCue')
 if any(cue.get(key)is not False for key in['resource_sampling','performance_acceptance','full_M4_acceptance']):raise ValueError('FunctionalOnly')
 if cue.get('boot_id')!=Path('/proc/sys/kernel/random/boot_id').read_text().strip():raise ValueError('FreshBoot')
 for key in['issued_epoch','expires_epoch']:
  if type(cue.get(key))not in[int,float]or not math.isfinite(cue[key]):raise ValueError('FiniteCueTime')
 if not cue['issued_epoch']<=time.time()<cue['expires_epoch']or not 0<cue['expires_epoch']-cue['issued_epoch']<=1200:raise ValueError('Fresh1200sCue')
 for name in['issued','expires']:
  value=cue.get(name+'_utc')
  if type(value)is not str or not value.endswith('Z')or abs(datetime.datetime.fromisoformat(value[:-1]+'+00:00').timestamp()-cue[name+'_epoch'])>.001:raise ValueError('UTCBinding')
 if digest(Path(root)/'selection.json')!=cue.get('selection_sha256'):raise ValueError('ReviewedNormalSelection')
 rows=json.loads((Path(root)/'selection.json').read_text())['files'];seen=set()
 if not 1<=len(rows)<=128:raise ValueError('InputCountBound')
 for row in rows:
  name=Path(row['path'])
  if name.is_absolute()or'..'in name.parts or str(name)in seen or digest(Path(root)/name)!=row['sha256']:raise ValueError('ExactNormalInput')
  seen.add(str(name))
 if digest(Path(root)/'policy.json')!=cue.get('policy_sha256'):raise ValueError('NormalPolicy')
 return cue
def process_start(pid):
 raw=Path('/proc/'+str(pid)+'/stat').read_text()
 return raw[raw.rindex(')')+2:].split()[19]
def validate_claim(cue,claim,activation,nonce,cue_sha,claim_sha):
 import re
 if type(nonce)is not str or not re.fullmatch('[0-9a-f]{32}',nonce)or activation.get('nonce')!=nonce:raise ValueError('OwnedNormalNonce')
 if claim.get('phase')!='exclusive_claim_before_dispatch'or claim.get('cue_sha256')!=cue_sha:raise ValueError('ExactNormalParentClaim')
 for key in['boot_id','selection_sha256']:
  if claim.get(key)!=cue.get(key)or activation.get(key)!=cue.get(key):raise ValueError('ParentClaimBinding '+key)
 pid=claim.get('pid');start=claim.get('pid_starttime');at=claim.get('claimed_epoch')
 if type(pid)is not int or pid<=0 or type(start)is not str or not re.fullmatch('[1-9][0-9]{0,19}',start):raise ValueError('ParentProcessIdentity')
 if type(at)not in[int,float]or not math.isfinite(at)or not cue['issued_epoch']<=at<=time.time()<cue['expires_epoch']or time.time()-at>1200:raise ValueError('ParentClaimTime')
 output=claim.get('output')
 if type(output)is not str or not Path(output).is_absolute()or '..'in Path(output).parts:raise ValueError('ParentOutputIdentity')
 repo=claim.get('repo');relative=cue.get('new_output')
 if type(repo)is not str or not Path(repo).is_absolute()or type(relative)is not str or Path(relative).is_absolute()or'..'in Path(relative).parts or Path(output)!=Path(repo)/relative:raise ValueError('ExactCueOutput')
 for key in['pid','pid_starttime','claimed_epoch','output','repo']:
  if activation.get(key)!=claim[key]:raise ValueError('ActivationClaimIdentity '+key)
 if activation.get('claim_sha256')!=claim_sha or activation.get('cue_sha256')!=cue_sha:raise ValueError('ActivationExactOnceClaim')
 return True
def child_guard():
 cue_path=Path('/evidence/normal-cue.json');cue=validate(json.loads(cue_path.read_text()),'/normal-root');claim_path=Path('/evidence/normal-claim.json');claim=json.loads(claim_path.read_text())
 activation=json.loads(Path('/evidence/normal-activation.json').read_text())
 validate_claim(cue,claim,activation,os.environ.get('NORMAL_FIXTURE_NONCE'),digest(cue_path),digest(claim_path))
 return cue

def write_once_claim(path,claim):
 if claim['pid']!=os.getpid()or process_start(claim['pid'])!=claim['pid_starttime']:raise ValueError('LiveClaimProcessIdentity')
 with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'w')as stream:
  stream.write(json.dumps(claim,indent=2)+'\n');stream.flush();os.fsync(stream.fileno())
 directory=os.open(Path(path).parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(directory)
 finally:os.close(directory)
