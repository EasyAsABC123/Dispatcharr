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
 """Exact immutable image54 792 runtime inputs, no mutable/extra Rust fallback."""
 manifest_raw=Path(manifest_path).read_bytes();record=json.loads(manifest_raw);expected_raw=Path(expected_path).read_bytes();expected=json.loads(expected_raw)
 if hashlib.sha256(manifest_raw).hexdigest()!=MANIFEST_SHA:raise ValueError('ExactImage53ManifestRequired')
 if hashlib.sha256(expected_raw).hexdigest()!=EXPECTED_SHA or len(expected)!=792:raise ValueError('ExactReviewedUnion792Required')
 require_runtime_set(record,expected)
 for key,value in IDENTITY.items():
  if record.get(key)!=value:raise ValueError('ExactImage53IdentityRequired '+key)
 if image not in[IDENTITY['tag'],IDENTITY['image_id']]:raise ValueError('ExactImage53ImageRequired')
 archive=Path(source_archive)
 if hashlib.sha256(archive.read_bytes()).hexdigest()!=IDENTITY['source_archive_sha256']:raise ValueError('CandidateArchiveBinding')
 with tarfile.open(archive)as source:
  names=[m.name for m in source.getmembers()if m.isfile()and m.name.startswith('rust/')]
  if len(names)!=792 or len(set(names))!=792 or set(names)!=set(expected):raise ValueError('ExactArchiveUnion792Required')
 result=candidate(manifest_path,image,expected_path,source_archive)
 if result['reserve_script_sha1']!='d757c8db4c0aa5a26a91e84d2dcbfb4047589be0':raise ValueError('ExactReserveScriptRequired')
 return result

MANIFEST_SHA='cac7bcdcc60dff6f26e233e78c2737b7f5d9450cf54042719b8d5cd61cf5ff8b'
EXPECTED_SHA='7acb3cf3c8777286cbfab1a8e338920ede7151d9ef173db5b259f77e3b6e802f'
IDENTITY={'tag': 'dispatcharr-rust-app:notification-mutations-image54', 'image_id': 'sha256:8e3c133c0639d60057d8d05a5be2fce66e0f0968c35e29f674c348495a1a9ca0', 'git_revision': '044f3ad0fb3ee68ffb17b23bbb796ccdae954298', 'source_tree_sha256': 'a8ab7ab0e400c1ebcdc1bc0e232434bb9408ba5165a048d1cbf2c0716f501256', 'source_archive_sha256': '86fb4e93032cc1565ada0d0935e21805dc6e0352897f5eeed06b7a0748b3c880'}

def require_runtime_set(record,expected):
 actual={k:v for k,v in record.get("source_sha256",{}).items()if k.startswith("rust/")}
 if len(expected)!=792 or actual!=expected:raise ValueError("ExactReviewedUnion792Required")
 return True
