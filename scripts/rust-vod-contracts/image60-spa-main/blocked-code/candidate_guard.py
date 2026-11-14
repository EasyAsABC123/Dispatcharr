"""Exact immutable app/source/credit checks; no Docker or application work."""
import hashlib,json,tarfile
from pathlib import Path
ROOT=Path('/home/justin/github/Dispatcharr');HERE=Path(__file__).resolve().parent

def need(v,label):
 if not v:raise ValueError(label)
def digest(p):
 value=hashlib.sha256()
 with Path(p).open('rb')as f:
  for b in iter(lambda:f.read(1048576),b''):value.update(b)
 return value.hexdigest()
def validate(binding=None):
 b=json.loads((HERE/'candidate-binding.json').read_text())if binding is None else binding
 need(b['Git']=='2dd210cf37877d9e132c864a54889fc66d6dd3b1'and b['image_id']=='sha256:1a184fd3f62c794f1be5fb44f750606217a620bd07134a8a6e0b0ea63208259e'and b['tree']=='e5a88d2cbe20412819cdb6e049c3dea4004b57c5ad6b9693fdb176d85743e5d2','ExactImage60Identity')
 for p,want in [(b['manifest'],b['manifest_sha256']),(b['archive'],b['archive_sha256']),(b['qualified_full_input_map'],b['qualified_full_input_map_sha256'])]:need(digest(ROOT/p)==want,'ExactBoundCandidateInput:'+p)
 m=json.loads((ROOT/b['manifest']).read_text());qualified=json.loads((ROOT/b['qualified_full_input_map']).read_text());runtime={k:v for k,v in qualified.items()if k.startswith('rust/')}
 need(m['passed']is True and m['exit_code']==0 and m['image_id']==b['image_id']and m['git_revision']==b['Git']and m['source_tree_sha256']==b['tree'],'ActualSuccessfulBoundBuild')
 need(digest(HERE/'expected-runtime.json')==b['expected_runtime_map_sha256']and json.loads((HERE/'expected-runtime.json').read_text())==runtime,'ExactLocalExpectedRuntimeMap')
 need(len(qualified)==861 and len(runtime)==856 and runtime=={k:v for k,v in m['source_sha256'].items()if k.startswith('rust/')}and len(m['source_sha256'])==1353,'ExactQualified856AndApp1353')
 with tarfile.open(ROOT/b['archive'],mode='r:')as archive:
  actual={}
  for row in archive:
   p=Path(row.name);need(row.isfile()and not p.is_absolute()and'..'not in p.parts and row.name not in actual and row.size<=16777216,'SafeUniqueAppRegularFile')
   actual[row.name]=hashlib.sha256(archive.extractfile(row).read()).hexdigest()
  need(actual==m['source_sha256'],'All1353ArchiveBytes')
 for kind,row in b['Root_review_hashes'].items():
  f=ROOT/row['path'];need(digest(f)==row['sha256'],'ExactRootReview:'+kind);r=json.loads(f.read_text());need(r['passed']is True,'ActualRootReviewPassed:'+kind)
 review=json.loads((ROOT/b['Root_credit_review_required']).read_text());need(review['image_id']==b['image_id']and review['git_revision']==b['Git']and review['Rust_inputs']==856 and review['React_inputs']==495 and review['credit_inputs']==2 and review['probe_actual_UID']==65532 and review['installed_asset_credit_files']==114,'ActualRootUIDCreditInputReview')
 return {'image_id':b['image_id'],'Git':b['Git'],'tree':b['tree'],'Rust_inputs':856,'full_inputs':861,'app_inputs':1353,'Root_installed_and_credit_review_bound':True}
