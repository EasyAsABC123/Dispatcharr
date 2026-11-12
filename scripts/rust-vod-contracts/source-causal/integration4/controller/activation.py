"""Pure fail-closed reviewed Source-only activation; no dispatch on import."""
import datetime,hashlib,json,math,os,time
from pathlib import Path
ACTION='source-causal-one-fixed-busy-four-second-once'
OUTPUT='artifacts/rust/m4-vod-source-causal-busy-1'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def validate(cue,root,now=None,boot=None):
 root=Path(root);now=time.time()if now is None else now;boot=Path('/proc/sys/kernel/random/boot_id').read_text().strip()if boot is None else boot
 if cue.get('authorized')is not True or cue.get('Source_application_requests_authorized')is not True or cue.get('action')!=ACTION:raise ValueError('ExactRootSourceCueRequired')
 if any(cue.get(key)is not False for key in['Native_requests','resource_sampling','performance_acceptance','full_M4_acceptance']):raise ValueError('SourceDiagnosticOnly')
 if cue.get('new_output')!=OUTPUT:raise ValueError('ExactFixedOutputRequired')
 if cue.get('cases')!=['busy']or cue.get('repetitions')!=1 or type(cue.get('repetitions'))is not int or cue.get('Original_revision')!=ORIGINAL or cue.get('Original_archive_sha256')!=ARCHIVE:raise ValueError('FixedOriginalCohort')
 if cue.get('boot_id')!=boot:raise ValueError('FreshBootRequired')
 for key in['issued_epoch','expires_epoch']:
  if type(cue.get(key))not in[int,float]or not math.isfinite(cue[key]):raise ValueError('FiniteUTCEpochRequired')
 if not cue['issued_epoch']<=now<cue['expires_epoch']or not 0<cue['expires_epoch']-cue['issued_epoch']<=1200:raise ValueError('Fresh1200SecondCueRequired')
 for key in['issued','expires']:
  text=cue.get(key+'_utc')
  if type(text)is not str or not text.endswith('Z')or abs(datetime.datetime.fromisoformat(text[:-1]+'+00:00').timestamp()-cue[key+'_epoch'])>.001:raise ValueError('ExactUTCBinding')
 if digest(root/'selection.json')!=cue.get('reviewed_selection_sha256'):raise ValueError('ReviewedSelectionSeal')
 selection=json.loads((root/'selection.json').read_text());seen=set()
 if not 1<=len(selection['files'])<=128:raise ValueError('SelectionCapacity')
 for row in selection['files']:
  name=Path(row['path'])
  if name.is_absolute()or '..'in name.parts or str(name)in seen:raise ValueError('SafeUniqueSelectedPath')
  seen.add(str(name))
  if digest(root/name)!=row['sha256']:raise ValueError('SelectedInputModified '+str(name))
 if digest(root/'policy.json')!=cue.get('reviewed_policy_sha256'):raise ValueError('ReviewedPolicySeal')
 return{'cue_scope':ACTION,'selection_sha256':cue['reviewed_selection_sha256'],'policy_sha256':cue['reviewed_policy_sha256'],'boot_id':boot,'file_count':len(seen)}
def claim(cue_path,root,output,repo):
 cue_path=Path(cue_path);raw=cue_path.read_bytes();cue=json.loads(raw);proof=validate(cue,root)
 expected=(Path(repo)/cue['new_output']).resolve()
 if Path(cue['new_output']).is_absolute()or '..'in Path(cue['new_output']).parts or expected!=Path(output).resolve()or expected.exists():raise ValueError('FreshExactOwnedOutput')
 path=cue_path.with_name(cue_path.stem+'.claimed.json');value={**proof,'cue_sha256':hashlib.sha256(raw).hexdigest(),'pid':os.getpid(),'claimed_epoch':time.time(),'phase':'exclusive_claim_before_dispatch','new_output':cue['new_output']}
 validate(cue,root)
 with os.fdopen(os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600),'w')as stream:stream.write(json.dumps(value,indent=2)+'\n');stream.flush();os.fsync(stream.fileno())
 descriptor=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(descriptor)
 finally:os.close(descriptor)
 return cue,path,value

def child_guard(cue_path,tools,root='/causal-root'):
 root=Path(root);cue_path=Path(cue_path);cue=json.loads(cue_path.read_text());proof=validate(cue,root);claimed=json.loads(cue_path.with_name('execution-claim.json').read_text())
 if claimed.get('cue_sha256')!=digest(cue_path)or claimed.get('selection_sha256')!=proof['selection_sha256']or claimed.get('phase')!='exclusive_claim_before_dispatch':raise ValueError('ExactParentClaimRequired')
 for row in json.loads((root/'selection.json').read_text())['files']:
  name=Path(row['path'])
  if name.parts[0]=='controller'and digest(Path(tools)/Path(*name.parts[1:]))!=row['sha256']:raise ValueError('MountedControllerBinding')
 return proof
