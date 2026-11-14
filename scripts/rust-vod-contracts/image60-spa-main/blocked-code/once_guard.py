"""Exact fresh Root cue and fsynced exclusive claim; no environment override."""
import hashlib,json,math,os,re,time
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr')
CUE=ROOT/'artifacts/rust/m4-vod-spa-native-main-image60-execution-cue-2.json'
OUTPUT=ROOT/'artifacts/rust/m4-vod-spa-native-main-image60-2'
ACTION='image60-vod-converter-spa-GET-once-correction2'
def need(v,label):
 if not v:raise ValueError(label)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def validate(cue,seal_sha,run_sha,boot,now,binding):
 need(cue.get('authorized')is True and cue.get('action')==ACTION,'ExplicitExactRootAction')
 need(cue.get('Native_business_requests')==1 and cue.get('Source_requests')==0 and all(cue.get(k)is False for k in ['resource_sampling','full_M4_acceptance','default_worker_activation','Source_replay']),'ExactOneNativeNoPromotion')
 need(cue.get('seal_sha256')==seal_sha and cue.get('execution_run_sha256')==run_sha and cue.get('new_output')==str(OUTPUT),'ExactSealRunOutput')
 need(cue.get('candidate_manifest_sha256')==binding['manifest_sha256']and cue.get('candidate_image')==binding['image_id']and cue.get('source_archive_sha256')==binding['archive_sha256']and cue.get('qualified_map_sha256')==binding['qualified_full_input_map_sha256'],'ExactCandidateSourceClosure')
 need(cue.get('boot_id')==boot and type(cue.get('nonce'))is str and re.fullmatch('[a-f0-9]{32}',cue['nonce'])is not None,'SameBootNonemptyNonce')
 a,b=cue.get('issued_epoch'),cue.get('expires_epoch');need(type(a)in(int,float)and type(b)in(int,float)and math.isfinite(a)and math.isfinite(b)and 0<b-a<=1200 and a<=now<b,'FreshFiniteRootCue')
 return cue
def admit(cue_path,seal,run,output,binding):
 need(Path(cue_path).resolve()==CUE and Path(output).resolve()==OUTPUT,'ExactCueOutputPaths')
 need(CUE.is_file()and not CUE.is_symlink()and CUE.stat().st_size<=65536,'RegularBoundedCue');need(not OUTPUT.exists()and not OUTPUT.is_symlink(),'AbsentOutputOnly')
 cue=json.loads(CUE.read_text());validate(cue,sha(seal),sha(run),Path('/proc/sys/kernel/random/boot_id').read_text().strip(),time.time(),binding)
 claim=OUTPUT.with_name(OUTPUT.name+'.claim.json');body=json.dumps({'cue_sha256':sha(CUE),'seal_sha256':sha(seal),'execution_run_sha256':sha(run),'output':str(OUTPUT),'nonce':cue['nonce'],'PID':os.getpid(),'PID_starttime':Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19],'claimed_epoch':time.time()},indent=2).encode()+b'\n';fd=os.open(claim,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 try:
  view=memoryview(body)
  while view:
   n=os.write(fd,view);need(n>0,'ClaimWriteProgress');view=view[n:]
  os.fsync(fd)
 finally:os.close(fd)
 parent=os.open(claim.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(parent)
 finally:os.close(parent)
 return cue
