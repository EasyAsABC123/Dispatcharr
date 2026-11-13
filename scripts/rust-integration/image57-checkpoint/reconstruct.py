"""Git-only historical input restoration. Never replay a consumed cue or run an app."""
import argparse,hashlib,io,json,os,shutil,subprocess,tarfile
from pathlib import Path
import verify as V
HERE=Path(__file__).resolve().parent
APP='d77f25fa94ee128c79ade9176d8eb4a9d23bf498';SUPPORT='f0bfc2f200dcfd1f64ae32a9855ea250731ce67f'
def fresh(path):
 p=Path(path).absolute();V.need(not p.exists()and not p.is_symlink()and not any(q.is_symlink()for q in p.parents),'FreshUnaliasedOutput');return p
def selected():
 V.need(V.sha(HERE/'authority.json')==V.AUTHORITY_SHA,'ExactAuthority');a=V.read(HERE/'authority.json')
 for p,h in a['files'].items():V.need(V.sha(HERE/p)==h,'ExactPayload:'+p)
def tools(name,output,historical=False):
 selected();out=fresh(output);V.need(name in ['main4s','header','notifications'],'DeclaredToolClosure');meta=V.read(HERE/'tool-closures.json')[name];root=HERE/'tools'/name;selection=root/'selection.json';V.need(V.sha(selection)==meta['selection_sha256'],'ExactToolSelection');rows=V.read(selection)['files'];V.need(len(rows)==meta['payload_count']and len({x['path']for x in rows})==len(rows),'AllToolInputs')
 for r in rows:V.need(V.sha(root/'blocked'/r['path'])==r['sha256'],'ToolSHA')
 launcher=root/'historical-executed-launcher.py';V.need(V.sha(launcher)==meta['executed_launcher_sha256']and launcher.read_bytes().replace(b'\nEXECUTION_BLOCKED=False\n',b'\nEXECUTION_BLOCKED=True\n')==(root/'blocked/launch_once.py').read_bytes(),'SingleFlagOnly')
 out.mkdir(parents=True)
 for r in rows:p=out/r['path'];p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(root/'blocked'/r['path'],p)
 shutil.copyfile(selection,out/'selection.json')
 if historical:shutil.copyfile(launcher,out/'launch_once.py')
 return {'tool_payloads':len(rows),'blocked':not historical,'historical_only':True,'new_authorization_granted':False,'cue_or_claim_installed':False,'app_executed':False}
def application(repo,output):
 selected();out=fresh(output);a=V.read(HERE/'application.json');V.need(a['git_revision']==APP and len(a['source_sha256'])==1336 and a['runtime_inputs']==839,'ExactApplication');env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};rows={}
 process=subprocess.Popen(['git','-C',str(Path(repo).resolve()),'cat-file','--batch'],env=env,stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
 try:
  for name,wanted in sorted(a['source_sha256'].items()):
   rel=Path(name);V.need(not rel.is_absolute()and'..'not in rel.parts,'SafeBlobPath');rev=a['Original_revision']if name.startswith('frontend/')or name=='LICENSE'else APP;process.stdin.write((rev+':'+name+'\n').encode());process.stdin.flush();head=process.stdout.readline(1024).split();V.need(len(head)==3 and head[1]==b'blob','RequiredGitBlob');size=int(head[2]);V.need(0<=size<=67108864,'BoundedBlob');raw=process.stdout.read(size);V.need(len(raw)==size and process.stdout.read(1)==b'\n'and hashlib.sha256(raw).hexdigest()==wanted,'ExactImmutableBlob:'+name);rows[name]=raw
 finally:
  process.stdin.close();process.stdout.close();V.need(process.wait(timeout=10)==0,'GitReaderSettled')
 out.mkdir(parents=True)
 for name,raw in rows.items():p=out/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(raw)
 with tarfile.open(out/'source.tar','w')as tar:
  for name,raw in sorted(rows.items()):i=tarfile.TarInfo(name);i.size=len(raw);i.mode=0o644;i.mtime=0;tar.addfile(i,io.BytesIO(raw))
 V.need(V.sha(out/'source.tar')==a['source_archive_sha256'],'ExactNormalizedBuilderArchive')
 return {'restored_application_inputs':len(rows),'Rust_inputs':839,'archive_sha256':V.sha(out/'source.tar'),'app_executed':False,'new_authorization_granted':False}
def support(repo,output):
 selected();out=fresh(output);m=V.read(HERE/'package-definition/support-inputs.json');V.need(m['revision']==SUPPORT and len(m['files'])==m['file_count']==6148,'ExactSupport6148');env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};raw=subprocess.check_output(['git','-C',str(Path(repo).resolve()),'archive','--format=tar',SUPPORT],env=env);V.need(hashlib.sha256(raw).hexdigest()==m['support_archive_sha256'],'ExactFullSupportArchive');out.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  V.need(all(not Path(v.name).is_absolute()and'..'not in Path(v.name).parts and(v.isdir()or v.isfile())for v in tar),'SafeSupportMembers');tar.extractall(out,filter='data')
 actual={str(p.relative_to(out)):V.sha(p)for p in out.rglob('*')if p.is_file()};V.need(actual==m['files'],'AllSupportSHA')
 git=out/'.git';subprocess.run(['git','init','--bare','--quiet',str(git)],check=True,env=env);objects=Path(subprocess.check_output(['git','-C',str(Path(repo).resolve()),'rev-parse','--git-path','objects'],env=env,text=True).strip());objects=objects if objects.is_absolute()else Path(repo).resolve()/objects;(git/'objects/info/alternates').write_text(str(objects.resolve())+'\n');(git/'HEAD').write_text(SUPPORT+'\n');env.update(GIT_DIR=str(git),GIT_WORK_TREE=str(out),GIT_OPTIONAL_LOCKS='0',GIT_INDEX_FILE=str(git/'index'));subprocess.run(['git','read-tree',SUPPORT],env=env,check=True);V.need(subprocess.check_output(['git','rev-parse','HEAD'],env=env,text=True).strip()==SUPPORT,'PinnedNestedHEAD');V.need(not subprocess.check_output(['git','status','--porcelain','--untracked-files=no'],env=env,cwd=out,text=True),'CleanTrackedSupport')
 return {'support_revision':SUPPORT,'support_files':6148,'archive_sha256':m['support_archive_sha256'],'isolated_nested_HEAD':SUPPORT,'actual_app_commands_dispatched':0}
def sdk(repo,output):
 selected();out=fresh(output);m=V.read(HERE/'evidence/SDK/all-source-inputs.json');V.need(len(m)==12176,'ExactSDKFullGitCount');env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};raw=subprocess.check_output(['git','-C',str(Path(repo).resolve()),'archive','--format=tar',APP],env=env);review=V.read(HERE/'evidence/SDK/offline-exact-review-corrected-label.json');V.need(hashlib.sha256(raw).hexdigest()==review['archive_sha256'],'ExactSDKArchive');out.mkdir(parents=True)
 with tarfile.open(fileobj=io.BytesIO(raw))as tar:
  V.need(all(not Path(v.name).is_absolute()and'..'not in Path(v.name).parts and(v.isdir()or v.isfile())for v in tar),'SafeSDKMembers');tar.extractall(out,filter='data')
 actual={str(q.relative_to(out)):V.sha(q)for q in out.rglob('*')if q.is_file()};V.need(actual==m,'All12176SDKInputSHA');return {'restored_Git_inputs':len(actual),'revision':APP,'archive_sha256':review['archive_sha256'],'application_executed':False,'SDK_reexecuted':False}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--kind',choices=['sdk','application','support','main4s','header','notifications'],required=True);p.add_argument('--repo',type=Path);p.add_argument('--output',type=Path,required=True);p.add_argument('--historical-executed',action='store_true');a=p.parse_args()
 if a.kind in ['sdk','application','support']:
  V.need(a.repo is not None,'ExplicitObjectRepository');result=({'application':application,'support':support,'sdk':sdk}[a.kind])(a.repo,a.output)
 else:result=tools(a.kind,a.output,a.historical_executed)
 print(json.dumps(result,indent=2))
