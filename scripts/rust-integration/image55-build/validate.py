#!/usr/bin/env python3
"""Offline image55 build receipts; Original authors retain application credit."""
import argparse,hashlib,io,json,os,subprocess,tarfile
from pathlib import Path,PurePosixPath
REF='aca6d900ad13700ab86b2898134a61e4dba57bc1'
IMAGE='sha256:120df2f930a7f4d2738a04fbd793c16b230b7a80114f4d6b03f670244940a014'
TREE='6457d75bad17874f039eaed58941e3a047fb124a841637ff1a28392db2472e5c'
ARCHIVE='8addffe734f0326b99f9faeb7fac0af670129a482ecb591df2a63638446c74af'
sha=lambda b:hashlib.sha256(b).hexdigest()
def require(ok,label):
 if not ok:raise ValueError(label)
def validate(root,repo=None):
 root=Path(root);manifest=json.loads((root/'manifest.json').read_text());require(len(manifest)==12,'ExactBuildReceiptCount')
 for name,item in manifest.items():
  p=PurePosixPath(name);require(not p.is_absolute()and len(p.parts)==1 and not(root/name).is_symlink(),'RegularLocalEvidence');v=(root/name).read_bytes();require(len(v)==item['bytes']and sha(v)==item['sha256'],'ExactEvidence:'+name)
 load=lambda n:json.loads((root/n).read_text());b=load('build.json');p=load('plan.json');a=load('actual-exit.json');review=load('Root-review.json');image=load('Root-image-inspect.json')[0];probe=load('Root-credit-frontend-probe.json')
 require(b['passed']is True and b['exit_code']==a['exit']==0 and a['wait_completed']is True,'ActualBuildExit')
 require(b['git_revision']==p['revision']==REF and b['source_input_mode']=='committed-revision-snapshot','ExactCommittedInputs')
 h=b['source_sha256'];require(h==p['source_sha256']and len(h)==1291 and len([k for k in h if k.startswith('rust/')])==794 and len([k for k in h if k.startswith('frontend/')])==495,'ExactInputDomains')
 require(sha(json.dumps(h,sort_keys=True,separators=(',',':')).encode())==b['source_tree_sha256']==p['tree_sha256']==TREE and b['source_archive_sha256']==p['expected_source_archive_sha256']==ARCHIVE,'ExactTreeAndArchive')
 require(b['image_id']==image['Id']==review['image_id']==IMAGE and image['Config']['Labels']['org.opencontainers.image.source-tree-sha256']==TREE,'ActualImageIdentity')
 require(b['runtime_packages_sha256']==sha((root/'runtime-packages.txt').read_bytes())and b['runtime_packages_frozen']is True,'FrozenRuntimePackages')
 require(b['candidate_overlay_sha256']==sha((root/'candidate-overlay.yml').read_bytes())and b['candidate_overlay_template_sha256']==sha((root/'candidate-overlay-template.yml').read_bytes()),'ExactOverlay')
 require(p['Original_frontend_LICENSE_byte_exact']is True and p['ATTRIBUTION_unchanged']is True and review['performance_gain_claimed']is False and review['scope'].endswith('no package/Main/performance acceptance'),'BoundedQualification')
 require(probe['exit']==0 and probe['argv'][probe['argv'].index('--entrypoint')+2]==IMAGE and '--read-only'in probe['argv']and probe['argv'][probe['argv'].index('--network')+1]=='none','IsolatedExactImageProbe')
 lines=probe['stdout'].splitlines();require(lines[0]=='65532','UnprivilegedImage');installed={v.split(None,1)[1]:v.split(None,1)[0]for v in lines[1:]};require(installed['/usr/share/doc/dispatcharr/LICENSE']==h['LICENSE']and installed['/usr/share/doc/dispatcharr/ATTRIBUTION.md']==h['ATTRIBUTION.md']and '/opt/dispatcharr-frontend/index.html'in installed,'InstalledOriginalCreditsAndFrontend')
 gone=load('Root-probe-absent.json');require(gone['exit']==0 and gone['stdout']==''and'--all'in gone['argv'],'OwnedProbeAbsent')
 reconstruction=None
 if repo:
  env={k:v for k,v in os.environ.items()if not k.startswith('GIT_')};env.update(GIT_CONFIG_GLOBAL='/dev/null',GIT_CONFIG_NOSYSTEM='1')
  def git(ref,paths):
   raw=subprocess.check_output(['git','-C',str(repo),'archive',ref,'--',*paths],env=env,timeout=60)
   with tarfile.open(fileobj=io.BytesIO(raw))as t:return {m.name:t.extractfile(m).read()for m in t if m.isfile()}
  files=dict(sorted(git(REF,['rust','frontend','LICENSE','ATTRIBUTION.md']).items()));require({k:sha(v)for k,v in files.items()}==h,'GitApplicationInputBytes');original=git('1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430',['frontend','LICENSE']);require(len(original)==496 and all(files[k]==v for k,v in original.items()),'OriginalReactAndLicenseExact')
  out=io.BytesIO()
  with tarfile.open(fileobj=out,mode='w')as t:
   for k,v in files.items():
    m=tarfile.TarInfo(k);m.size=len(v);m.mode=0o644;m.mtime=0;t.addfile(m,io.BytesIO(v))
  require(sha(out.getvalue())==ARCHIVE,'ReconstructedNormalizedApplicationArchive');reconstruction={'application_inputs':len(files),'Original_React_inputs':495,'archive_bytes':len(out.getvalue()),'all_bytes_exact':True}
 return {'passed':True,'ref':REF,'image_id':IMAGE,'scope':'Offline exact build receipts only; no package/Main/performance acceptance','optional_Git_reconstruction':reconstruction}
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--root',type=Path,default=Path(__file__).parent);p.add_argument('--repo',type=Path);a=p.parse_args();print(json.dumps(validate(a.root,a.repo),indent=2))
