"""BLOCKED preparation: no dispatch until Root reviewed flag-only activation and fresh cue."""
import argparse,datetime,hashlib,importlib.util,json,math,os,shutil,time
from pathlib import Path
EXECUTION_BLOCKED=False
HERE=Path(__file__).resolve().parent
ACTION='image55-main-four-second-physical-once'
def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def guard_candidate(here,repo,binding):
 spec=importlib.util.spec_from_file_location('image55_inputs',here/'tools/candidate_inputs.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 return m.bound_candidate(repo/binding['build_manifest'],binding['candidate_image'],here/'tools/expected-runtime.json',repo/binding['source_archive'])
def validate(repo,here,cue,now):
 repo=Path(repo).resolve();here=Path(here).resolve();plan=json.loads((here/'execution-plan.json').read_text());binding=json.loads((here/'candidate-binding.json').read_text());selection=here/'selection.json';rows=json.loads(selection.read_text())['files']
 require(cue.get('authorized')is True and cue.get('source_native_application_requests_authorized')is True,'ExplicitRootCueRequired')
 require(cue.get('action')==ACTION and cue.get('cases')==['success','busy','clientloss'],'ExactActionCases')
 require(all(cue.get(k)is False for k in ['resource_sampling','performance_acceptance','full_M4_acceptance']),'FunctionalOnly')
 for name in ['issued_epoch','expires_epoch'] :require(type(cue.get(name))in[int,float]and math.isfinite(cue[name]),'FiniteEpoch')
 require(cue['issued_epoch']<=now<cue['expires_epoch']and 0<cue['expires_epoch']-cue['issued_epoch']<=1800,'FreshOnceCue')
 for name in ['issued','expires']:
  text=cue.get(name+'_utc');require(type(text)is str and text.endswith('Z'),'UTCString')
  require(abs(datetime.datetime.fromisoformat(text[:-1]+'+00:00').timestamp()-cue[name+'_epoch'])<=.001,'UTCEpoch')
 require(cue.get('boot_id')==Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'SameBoot')
 for key,value in [('selection_sha256',digest(selection)),('plan_sha256',digest(here/'execution-plan.json')),('policy_sha256',digest(here/'policy.json')),('binding_sha256',digest(here/'candidate-binding.json'))]:require(cue.get(key)==value,'CueSeal '+key)
 for key in ['build_manifest_sha256','candidate_tag','candidate_image','git_revision','source_tree_sha256','source_archive_sha256','Root_reviewed_Rust_input_count','Root_reviewed_input_map_sha256']:require(cue.get(key)==binding[key],'CueCandidate '+key)
 require(type(cue.get('Root_reviewed_Rust_input_count'))is int and cue['Root_reviewed_Rust_input_count']==794,'Strict794')
 require(cue.get('new_output')==plan['new_output'],'ExactNewOutput')
 preflight=repo/plan['Root_quiet_preflight'];require(preflight.is_file()and cue.get('Root_quiet_preflight_sha256')==digest(preflight),'ExactRootQuietPreflight')
 quiet=json.loads(preflight.read_text());require(quiet.get('passed')is True and quiet.get('boot_id')==cue['boot_id']and type(quiet.get('checked_epoch'))in[int,float]and cue['issued_epoch']-300<=quiet['checked_epoch']<=cue['issued_epoch'],'FreshQuietPreflight')
 require(len(rows)==plan['sealed_file_count']and len({r['path']for r in rows})==len(rows),'ExactInputSet')
 for r in rows:
  rel=Path(r['path']);require(not rel.is_absolute()and'..'not in rel.parts,'InputPath');file=here/rel;require(file.is_file()and not file.is_symlink(),'RegularInput')
  raw=file.read_bytes()
  if r['path']=='launch_once.py':
   require(raw.count(b'\nEXECUTION_BLOCKED=False\n')<=1,'OnlyFlagActivation');raw=raw.replace(b'\nEXECUTION_BLOCKED=False\n',b'\nEXECUTION_BLOCKED=True\n')
  require(hashlib.sha256(raw).hexdigest()==r['sha256'],'ExactInput '+r['path'])
 out=repo/plan['new_output'];require(not out.exists()and not out.is_symlink(),'FreshOutput')
 require(digest(repo/binding['build_manifest'])==binding['build_manifest_sha256'],'ExactBuildManifest')
 proof=guard_candidate(here,repo,binding)
 for key,left in [('image_id','candidate_image'),('git_revision','git_revision'),('source_tree_sha256','source_tree_sha256')]:require(proof[key]==binding[left],'RuntimeProof '+key)
 return plan,binding,proof

def dispatch(repo,here,plan,binding):
 spec=importlib.util.spec_from_file_location('image55_main_protocol',here/'tools/run.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 args=argparse.Namespace(repo=repo,candidate_image=binding['candidate_image'],build_manifest=repo/binding['build_manifest'],source_archive=repo/binding['source_archive'],output=repo/plan['new_output'])
 return m.execute(args,repo,args.output)
def main():
 if EXECUTION_BLOCKED:raise RuntimeError('IMAGE55_MAIN4S_ROOT_ACTIVATION_REQUIRED')
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--repo',type=Path,required=True);args=parser.parse_args();repo=args.repo.resolve();plan=json.loads((HERE/'execution-plan.json').read_text());cue_path=repo/plan['cue_path'];raw=cue_path.read_bytes();cue=json.loads(raw)
 plan,binding,proof=validate(repo,HERE,cue,time.time());require(cue['issued_epoch']<=time.time()<cue['expires_epoch'],'ExpiredBeforeClaim')
 claim=cue_path.with_name(cue_path.stem+'.claimed.json');body={'no_retry':True,'cue_sha256':hashlib.sha256(raw).hexdigest(),'selection_sha256':cue['selection_sha256'],'boot_id':cue['boot_id'],'claimed_epoch':time.time(),'pid':os.getpid(),'new_output':plan['new_output']}
 try:fd=os.open(claim,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
 except FileExistsError:raise ValueError('AlreadyClaimed')from None
 with os.fdopen(fd,'w')as f:f.write(json.dumps(body,indent=2)+'\n');f.flush();os.fsync(f.fileno())
 directory=os.open(claim.parent,os.O_RDONLY|os.O_DIRECTORY)
 try:os.fsync(directory)
 finally:os.close(directory)
 try:return dispatch(repo,HERE,plan,binding)
 finally:
  out=repo/plan['new_output']
  if out.is_dir():shutil.copyfile(cue_path,out/'execution-cue.json');shutil.copyfile(claim,out/'execution-claim.json')
if __name__=='__main__':raise SystemExit(main())
