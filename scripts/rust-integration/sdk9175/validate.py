"""Offline SDK evidence checks; no Docker, compiler, application or network.
Original Dispatcharr/dependency authors retain application/algorithm credit.
"""
import argparse,hashlib,io,json,re,shlex,subprocess,tarfile
from pathlib import Path,PurePosixPath
REF='9175d2c56aff107174444e8518bc31700e756a6b'
def sha(data):return hashlib.sha256(data).hexdigest()
def validate(root,repo=None):
 root=Path(root);manifest=json.loads((root/'raw-manifest.json').read_text())
 for name,item in manifest.items():
  p=PurePosixPath(name)
  if p.is_absolute()or'..'in p.parts:raise ValueError('Unsafe packet member')
  path=root/name;data=path.read_bytes()
  if len(data)!=item['bytes']or sha(data)!=item['sha256']:raise ValueError('Changed raw packet member '+name)
 def load(name):return json.loads((root/'raw'/name).read_text())
 plan=load('plan.json');report=load('verification.json');review=load('independent-review.json');actual=load('actual-process-exit.json');before=load('all-source-inputs.json');after=load('all-source-after.json');selected=load('verifier-selected-inputs.json');parent=load('Root-parent-rust-inputs791.json')
 assert plan['ref']==report['revision']==review['ref']==REF
 assert report['scope']=='core-verification-not-application-parity'and report['passed']is True and actual['returncode']==0 and report['cleanup_exit_code']==0 and report['resources_retained']is False
 assert report['worktree_status']==''and before==after and len(before)==6158
 assert len(selected)==4939 and selected==report['source_sha256']and all(before[k]==v for k,v in selected.items())
 rust={k:v for k,v in before.items()if k.startswith('rust/')};assert len(rust)==791 and rust==load('rust-inputs791.json')==parent
 assert sha((root/'raw/Root-parent-rust-inputs791.json').read_bytes())=='a729127b8f57f261433152d958b60695fb9e25ce824b65678b614378ac6bc8c5'
 for p in(root/'inputs').rglob('*'):
  if p.is_file():assert sha(p.read_bytes())==before[str(p.relative_to(root/'inputs'))]
 copy={}
 for line in(root/'inputs/rust/Dockerfile').read_text().splitlines():
  if not line.startswith('COPY '):continue
  t=shlex.split(line);assert len(t)==3;name=t[1].rstrip('/')
  if name in before:copy[name]=before[name]
  else:
   part={k:v for k,v in before.items()if k.startswith(name+'/')};assert part;copy.update(part)
 aux=load('independent-image-copy-hashes.json');host=['docker/compose.rust.yml','scripts/verify-rust-compose.py'];assert aux['passed']is False and len(copy)==1159 and aux['files']==copy and set(aux['missing'])==set(host)
 assert set(load('SDK-copy-closure.json'))==set(copy)|set(host)
 for name in host:assert sha((root/'inputs'/name).read_bytes())==before[name]==selected[name]
 offline=load('offline-exact-COPY-proof.json');assert offline['passed']and offline['original_auxiliary_passed']is False and offline['copied_inputs']==copy
 image=load('independent-image-configuration.json');container=load('independent-container-inspect.json');assert image['image_id']==container['image_id']==review['image_id']and image['runtime_network_mode']==container['network_mode']=='none'and image['Cargo_target']=='/tmp/dispatcharr-target'
 assert image['runtime_command']==['sh','scripts/rust-verify.sh'];assert image['image_id'].split(':',1)[1].startswith(report['image_ids'][0].split(':',1)[-1])
 events=[json.loads(s)for s in(root/'raw/Docker-events.jsonl').read_text().splitlines()];cid=container['container_id'];dies=[v for v in events if v.get('Type')=='container'and v.get('Action')=='die'and v.get('Actor',{}).get('ID')==cid];assert len(dies)==1 and dies[0]['Actor']['Attributes']['exitCode']=='0';assert any(v.get('Type')=='container'and v.get('Action')=='destroy'and v.get('Actor',{}).get('ID')==cid for v in events)
 assert review['actual_project_inventories']=={'containers':[],'networks':[],'volumes':[]}and review['exact_container_absent']is True
 prior=load('independent-review-attempt1.json');assert prior['inspect_exit']==1 and json.loads(prior['inspect_stdout'])==[]and 'no such object'in prior['inspect_stderr'].casefold()
 log=(root/'raw/compose.log').read_text();text='\n'.join(s.split('|',1)[1].strip()for s in log.splitlines()if '|'in s);rows=[tuple(map(int,x))for x in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;',text)];totals=[sum(v[i]for v in rows)for i in range(3)];assert totals==[578,0,705]and len(rows)==183
 py=[int(x)for x in re.findall(r'Ran (\d+) tests? in',text)];assert py==[15,6,277,17,2];skip=sum(map(int,re.findall(r'^OK \(skipped=(\d+)\)$',text,re.M)));assert skip==1 and 'FAILED ('not in text
 assert 'PASS: 1806 byte-exact Python/Rust cases; seed=20261109'in text
 helper=load('ts-helper-comparison.json');assert helper['scope']=='ts-discontinuity-helper-only'and helper['samples']
 assert len(report['commands'])==2 and all(v['exit_code']==0 for v in report['commands']);assert '--abort-on-container-exit'in report['commands'][1]['argv']and '--exit-code-from'in report['commands'][1]['argv']
 gitcheck=None
 if repo:
  raw=subprocess.check_output(['git','-C',str(repo),'archive','--format=tar',REF]);files={}
  with tarfile.open(fileobj=io.BytesIO(raw))as archive:
   for member in archive.getmembers():
    if member.isfile():files[member.name]=sha(archive.extractfile(member).read())
  assert files==before;gitcheck={'source_bytes_exact':True,'file_count':len(files),'archive_sha256':sha(raw),'recorded_archive_bytes_exact':sha(raw)==plan['archive_sha256']}
 return {'passed':True,'ref':REF,'scope':'Offline exact SDK evidence only; no application/milestone/resource gains','Rust':{'passed':578,'ignored':705},'Python':{'passed':316,'skipped':1,'contract_subset_passed':299,'contract_subset_skipped':1},'Source_files':6158,'selected_inputs':4939,'actual_COPY1159_and_host_controls2':True,'auxiliary_negative_preserved':True,'review_negative_preserved':True,'container_exit':0,'owned_cleanup_empty':True,'optional_Git_source_reconstruction':gitcheck}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=Path(__file__).parent);p.add_argument('--repo',type=Path);p.add_argument('--output',type=Path);a=p.parse_args();v=validate(a.root,a.repo);text=json.dumps(v,indent=2)+'\n'
 if a.output:a.output.write_text(text)
 print(text,end='')
