#!/usr/bin/env python3
"""Offline SDK044f receipts only: no Docker, compiler, SDK/app or network execution.
Original Dispatcharr/dependency authors retain application and algorithm credit.
"""
import argparse,ast,hashlib,io,json,os,re,shlex,subprocess,tarfile
from pathlib import Path,PurePosixPath
REF='044f3ad0fb3ee68ffb17b23bbb796ccdae954298'
QA792='7acb3cf3c8777286cbfab1a8e338920ede7151d9ef173db5b259f77e3b6e802f'
QA797='46d649d3503e6ee202f1393c3981871fe74da75f4a554ae275247e84915090b9'
ARCHIVE='f077c33a799f18a0b8b68854e9f8af47cb6b71c01ce533cad4765f7a026c7af7'
def sha(data):return hashlib.sha256(data).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def archive_git(repo):
 env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1')
 return subprocess.check_output(['git','-C',str(repo),'-c','tar.umask=0002','-c','core.attributesFile=/dev/null','archive','--format=tar',REF],env=env,timeout=60)
def validate(root,repo=None):
 root=Path(root);manifest=json.loads((root/'raw-manifest.json').read_text());require(len(manifest)==54,'Exact54RawAndInputMembers')
 for name,item in manifest.items():
  p=PurePosixPath(name);require(not p.is_absolute()and'..'not in p.parts,'SafePacketMember');path=root/name;require(not path.is_symlink(),'RegularEvidence');data=path.read_bytes();require(len(data)==item['bytes']and sha(data)==item['sha256'],'ExactPacketMember:'+name)
 def load(name):return json.loads((root/'raw'/name).read_text())
 plan=load('plan.json');report=load('verification.json');actual=load('actual-process-exit.json');before=load('all-source-inputs.json');after=load('all-source-after.json');selected=load('verifier-selected-inputs.json')
 require(plan['ref']==report['revision']==REF and plan['archive_sha256']==ARCHIVE,'ExactFrozenCommitArchive')
 require(report['scope']=='core-verification-not-application-parity'and report['passed']is True and actual['returncode']==0 and actual['wait_completed']is True and report['cleanup_exit_code']==0 and report['resources_retained']is False,'ActualSDKExitAndScope')
 require(report['worktree_status']==''and before==after and len(before)==7231,'All7231FrozenSourceBytes')
 verifier=(root/'inputs/scripts/verify-rust-compose.py').read_bytes();require(sha(verifier)=='4af10beaab69f2933237a9ccd6364275fd1dacc58c69bce5eb059ca9a011c218','UnchangedVerifierCode')
 tree=ast.parse(verifier);prefixes=next(ast.literal_eval(n.args[0])for n in ast.walk(tree)if isinstance(n,ast.Call)and isinstance(n.func,ast.Attribute)and n.func.attr=='startswith'and isinstance(n.func.value,ast.Name)and n.func.value.id=='path')
 derived={k:v for k,v in before.items()if k.startswith(prefixes)};require(len(derived)==6012 and derived==selected==report['source_sha256'],'ExactDerivedVerifierSelection')
 require(any(isinstance(n,ast.Call)and any(isinstance(a,ast.Constant)and a.value=='--timeout'for a in n.args)and any(k.arg=='default'and isinstance(k.value,ast.Constant)and k.value.value==1800 for k in n.keywords)for n in ast.walk(tree)),'Unchanged1800DefaultTimeout')
 require('--timeout'not in plan['command']and'--keep'not in plan['command'],'NoSDKTimeoutOrRetentionOverride')
 rust={k:v for k,v in before.items()if k.startswith('rust/')};qa=load('QA2-rust792-map.json');coupled=load('QA2-coupled5-map.json');full=load('QA2-full797-map.json')
 require(len(rust)==792 and rust==load('rust-inputs792.json')==qa and sha((root/'raw/QA2-rust792-map.json').read_bytes())==QA792,'ExactQA2Rust792DictionaryAndReferenceSerialization')
 require(len(coupled)==5 and coupled==load('coupled5-inputs.json')and all(before[k]==v for k,v in coupled.items()),'ExactCoupled5')
 require({**rust,**coupled}==full==load('full797-inputs.json')and len(full)==797 and sha((root/'raw/QA2-full797-map.json').read_bytes())==QA797,'Exact792Plus5Dictionary')
 for p in(root/'inputs').rglob('*'):
  if p.is_file():require(sha(p.read_bytes())==before[str(p.relative_to(root/'inputs'))],'ExactCommittedControl:'+str(p))
 copy={}
 for line in(root/'inputs/rust/Dockerfile').read_text().splitlines():
  if not line.startswith('COPY '):continue
  tokens=shlex.split(line);require(len(tokens)==3,'ExplicitCOPYAuthority');name=tokens[1].rstrip('/')
  if name in before:copy[name]=before[name]
  else:
   part={k:v for k,v in before.items()if k.startswith(name+'/')};require(bool(part),'ExistingCOPYDirectory');copy.update(part)
 host=load('host-only-controls.json');require(host=={k:before[k]for k in ['docker/compose.rust.yml','scripts/verify-rust-compose.py']}and len(host)==2 and not set(host)&set(copy),'Exactly2HostOnlyControls')
 aux=load('independent-image-copy-hashes.json');require(aux['passed']is True and aux['count']==1506 and len(copy)==1506 and aux['files']==copy==load('SDK-copy-closure.json')and aux['missing']==[],'Actual1506COPYHashes')
 context=load('private-git-context-final.json');env=load('private-git-environment.json');require(context['HEAD']==REF and context['status']==''and context['alternate_file_mode']=='0o444'and context['private_environment']==env,'PrivateDetachedGitContext')
 require(env['GIT_INDEX_FILE']==env['GIT_DIR']+'/index'and env['GIT_OBJECT_DIRECTORY']==env['GIT_DIR']+'/objects'and env['GIT_WORK_TREE'].endswith('/sdk-044f-qualified-1/source')and env['GIT_OPTIONAL_LOCKS']=='0'and env['GIT_CONFIG_GLOBAL']=='/dev/null'and env['GIT_CONFIG_NOSYSTEM']=='1','PrivateGitWriteDestinations')
 container=load('independent-container-inspect.json');rawcontainer=load('actual-container-full-inspect.json')[0];image=load('actual-SDK-image-full-inspect.json')[0]
 require(container['container_id']==rawcontainer['Id']and container['image_id']==image['Id']==rawcontainer['Image'],'ActualSDKImageIdentity')
 require(container['network_mode']==rawcontainer['HostConfig']['NetworkMode']=='none'and container['command']==rawcontainer['Config']['Cmd']==['sh','scripts/rust-verify.sh'],'NetworkNoneExactSDKCommand')
 require('CARGO_TARGET_DIR=/tmp/dispatcharr-target'in image['Config']['Env']and image['Config']['WorkingDir']=='/workspace','ImageInternalCargoTarget')
 require(image['Id'].split(':',1)[1].startswith(report['image_ids'][0].split(':',1)[-1]),'VerifierActualImageID')
 events=[json.loads(s)for s in(root/'raw/Docker-events.jsonl').read_text().splitlines()];cid=container['container_id'];dies=[v for v in events if v.get('Type')=='container'and v.get('Action')=='die'and v.get('Actor',{}).get('ID')==cid];require(len(dies)==1 and dies[0]['Actor']['Attributes']['exitCode']=='0','ActualContainerDieExit0')
 require(any(v.get('Type')=='container'and v.get('Action')=='destroy'and v.get('Actor',{}).get('ID')==cid for v in events),'ActualContainerDestroy')
 monitor=load('independent-monitor.json');require(monitor['monitor_completed']is True and monitor['errors']==[]and type(monitor['event_process_exit'])is int,'AuxCollectorSettled')
 cleanup=load('independent-cleanup-query-receipt.json');require(all(cleanup[k]is True for k in ['all_project_resources_absent','exact_CID_absent','SDK_host_process_waited_exit0'])and cleanup['SDK_host_PID_proc_present']is False,'IndependentOwnedCleanupAndHostSettlement')
 require(cleanup['queries'][0]['argv'][2:4]==['ls','--all']and all(q['exit']==0 and q['stdout']==''for q in cleanup['queries'][:4]),'StoppedAndRunningProjectCIDInventoriesEmpty')
 gone=cleanup['queries'][4];require(gone['argv']==['docker','inspect',cid]and gone['exit']==1 and json.loads(gone['stdout'])==[]and'no such object:'in gone['stderr'],'ActualLowercaseDockerAbsenceDiagnostic')
 require(load('outer-owned-cleanup.json')['exit']==0,'ExactOuterCleanupAcknowledged')
 review=load('Root-review/Root-review.json');negative=load('Root-cleanup-negative-description.json');require(review['passed']is True and review['revision']==REF and negative['negative_description']==review['cleanup_review_negative']and'case-sensitive'in negative['negative_description']and'No such'in negative['negative_description']and'no such object'in negative['negative_description']and negative['SDK_workload_retry']is False,'RootNegativeReviewPreservedNotRelabeled')
 text='\n'.join(s.split('|',1)[1].strip()for s in(root/'raw/compose.log').read_text().splitlines()if'|'in s);rows=[tuple(map(int,x))for x in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',text)];totals=[sum(v[i]for v in rows)for i in range(3)];require(totals==[584,0,706]and len(rows)==184,'ActualRust584706')
 py=[int(x)for x in re.findall(r'Ran (\d+) tests? in',text)];skips=sum(map(int,re.findall(r'^OK \(skipped=(\d+)\)$',text,re.M)));require(py==[15,6,302,17,2]and sum(py)==342 and skips==1 and'FAILED ('not in text,'ActualPython342341Plus1')
 require('PASS: 1806 byte-exact Python/Rust cases; seed=20261109'in text,'ActualTSHelper1806');helper=load('ts-helper-comparison.json');require(helper['scope']=='ts-discontinuity-helper-only'and bool(helper['samples']),'HelperOnlyArtifactScope')
 require(len(report['commands'])==2 and all(v['exit_code']==0 for v in report['commands'])and'--abort-on-container-exit'in report['commands'][1]['argv']and'--exit-code-from'in report['commands'][1]['argv'],'ActualComposeExits')
 gitcheck=None
 if repo:
  raw=archive_git(repo);files={}
  with tarfile.open(fileobj=io.BytesIO(raw))as archive:
   for member in archive.getmembers():
    path=PurePosixPath(member.name);require(not path.is_absolute()and'..'not in path.parts,'SafeGitArchivePath')
    if member.isfile():files[member.name]=sha(archive.extractfile(member).read())
  require(files==before and sha(raw)==plan['archive_sha256'],'ExactGitArchiveReconstruction');gitcheck={'source_files':len(files),'all_bytes_exact':True,'archive_sha256':sha(raw),'archive_bytes':len(raw)}
 return {'passed':True,'ref':REF,'scope':'Offline exact core SDK receipts only; no new SDK/app/resource run or application/performance acceptance','Rust':{'passed':584,'failed':0,'ignored':706,'result_blocks':184},'Python':{'total':342,'passed':341,'skipped':1,'groups':[15,6,302,17,2],'contract_subset_total':325,'contract_subset_passed':324,'contract_subset_skipped':1},'TS_helper_byte_exact_cases':1806,'helper_seed':20261109,'source_files':7231,'selected_inputs':6012,'actual_COPY_inputs':1506,'host_only_controls':2,'current_aux_checker_passed':True,'Root_case_sensitive_review_negative_preserved':True,'container_exit':0,'actual_SDK_host_wait_exit':0,'project_and_CID_absent':True,'SDK_image_id':image['Id'],'QA2_792_reference_sha256':QA792,'QA2_797_reference_sha256':QA797,'dictionary_equality_distinct_from_serialized_map_hashes':True,'Original_Dispatcharr_and_dependency_credit_retained':True,'standard_SDK_helper_artifacts_do_not_transfer_performance_acceptance':True,'optional_Git_source_reconstruction':gitcheck}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=Path(__file__).parent);p.add_argument('--repo',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();v=validate(a.root,a.repo);text=json.dumps(v,indent=2)+'\n'
 if a.output:a.output.write_text(text)
 print(text,end='')
