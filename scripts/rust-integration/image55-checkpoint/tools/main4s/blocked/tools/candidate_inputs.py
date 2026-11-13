"""UNEXECUTED immutable Main input guard; no fallback to mutable Rust sources."""
import hashlib,json,tarfile
from pathlib import Path

def candidate(manifest_path,image,expected_path,source_archive):
 manifest_path=Path(manifest_path).resolve();raw=manifest_path.read_bytes();record=json.loads(raw);expected=json.loads(Path(expected_path).read_text())
 if record.get('passed')is not True or record.get('exit_code')!=0:raise ValueError('Successful immutable build required')
 if image not in[record['tag'],record['image_id']]:raise ValueError('CandidateIdentityBinding')
 for name,sha in expected.items():
  if record['source_sha256'].get(name)!=sha:raise ValueError('UnqualifiedCandidateRuntime '+name)
 archive=Path(source_archive).resolve(strict=True)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=record['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 name='rust/crates/coordination/src/scripts.rs'
 with tarfile.open(archive)as source:
  for runtime,sha in expected.items():
   if hashlib.sha256(source.extractfile(runtime).read()).hexdigest()!=sha:raise ValueError("CandidateRuntimeArchiveBinding "+runtime)
  data=source.extractfile(name).read()
 if hashlib.sha256(data).hexdigest()!=record['source_sha256'][name]:raise ValueError('CoordinatorScriptBinding')
 text=data.decode()
 def part(label):return text.split('const '+label+': &str = r#"',1)[1].split('"#;',1)[0]
 script=(part('REAP')+part('RESERVE_PROVIDER')).encode()
 return{'manifest_sha256':hashlib.sha256(raw).hexdigest(),'image_id':record['image_id'],'source_tree_sha256':record['source_tree_sha256'],'git_revision':record['git_revision'],'reserve_script_sha1':hashlib.sha1(script).hexdigest(),'coordinator_source_sha256':hashlib.sha256(data).hexdigest(),'runtime_sha256':expected}


def bound_candidate(manifest_path,image,expected_path,source_archive):
 """Exact immutable image55 794 runtime inputs, no mutable/extra Rust fallback."""
 manifest_raw=Path(manifest_path).read_bytes();record=json.loads(manifest_raw);expected_raw=Path(expected_path).read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(manifest_raw).hexdigest()!=MANIFEST_SHA:raise ValueError('ExactImage55ManifestRequired')
 if hashlib.sha256(expected_raw).hexdigest()!=EXPECTED_SHA or len(expected)!=794:raise ValueError('ExactReviewedUnion794Required')
 require_runtime_set(record,expected)
 for key,value in IDENTITY.items():
  if record.get(key)!=value:raise ValueError('ExactImage55IdentityRequired '+key)
 if image not in[IDENTITY['tag'],IDENTITY['image_id']]:raise ValueError('ExactImage55ImageRequired')
 archive=Path(source_archive)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=IDENTITY['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 with tarfile.open(archive)as source:
  names=[m.name for m in source.getmembers()if m.isfile()and m.name.startswith('rust/')]
  if len(names)!=794 or len(set(names))!=794 or set(names)!=set(expected):raise ValueError('ExactArchiveUnion794Required')
 result=candidate(manifest_path,image,expected_path,source_archive)
 if result['reserve_script_sha1']!='d757c8db4c0aa5a26a91e84d2dcbfb4047589be0':raise ValueError('ExactReserveScriptRequired')
 return result

MANIFEST_SHA='3e1cb5c613d280c9297d75e1a69998f22c858c8a3d158949d61377cd1ffd3e76'
EXPECTED_SHA='a65ca5bfb57097b387451a9cea6c0341c670823d64e541704a20f63a4969dc0d'
IDENTITY={'tag': 'dispatcharr-rust-app:union794-image55', 'image_id': 'sha256:120df2f930a7f4d2738a04fbd793c16b230b7a80114f4d6b03f670244940a014', 'git_revision': 'aca6d900ad13700ab86b2898134a61e4dba57bc1', 'source_tree_sha256': '6457d75bad17874f039eaed58941e3a047fb124a841637ff1a28392db2472e5c', 'source_archive_sha256': '8addffe734f0326b99f9faeb7fac0af670129a482ecb591df2a63638446c74af'}

def require_runtime_set(record,expected):
 actual={k:v for k,v in record.get('source_sha256',{}).items()if k.startswith('rust/')}
 if len(expected)!=794 or actual!=expected:raise ValueError('ExactReviewedUnion794Required')
 return True
