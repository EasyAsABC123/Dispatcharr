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
 """New Main plan: exact reviewed Root772 set; no unreviewed Rust extras."""
 expected=json.loads(Path(expected_path).read_text());record=json.loads(Path(manifest_path).read_text())
 actual={k:v for k,v in record.get('source_sha256',{}).items()if k.startswith('rust/')}
 if len(expected)!=772 or actual!=expected:raise ValueError('ExactReviewedUnion772Required')
 return candidate(manifest_path,image,expected_path,source_archive)
