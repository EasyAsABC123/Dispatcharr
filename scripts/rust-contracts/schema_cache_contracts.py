"""Exact retained Original schema-cache tests; no Django/fixture execution.
Original Dispatcharr contributors retain application and algorithm credit.
"""
import argparse,ast,hashlib,json
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
KIND='pinned_original_schema_cache_tests'
BASE='scripts/rust-contracts/schema-cache'
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
IDENT='route-926efc2599602af7af63:METHOD_UNRESOLVED'
ORIGINAL_FILES={'tests/test_openapi_schema.py': 'b653b53c0319b0f184225a3d030d7f97698b5c99dbdfc07ffd24ceae289db495', 'apps/api/schema_views.py': 'f6c9b86a70f542d82aea28ef20ec7ab3970d501c0be85d7879e4c7bcd0cf0df4', 'apps/api/urls.py': 'ee72fb6d564ac86a5a9fcea794a8039bdbc2b4d3d5cf7035dd084500e3d039c6'}
OBSERVATIONS_SHA='4d25325880dc81b0c7e66338f1b9c086f2b513988d5e0c9d7dc84caea84bcd9d'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def read(root,name):return json.loads((Path(root)/name).read_text())
def authority(root,raw_log=None):
 root=Path(root);path=root/BASE/'observations.json';raw=path.read_bytes();assert sha(raw)==OBSERVATIONS_SHA,'ExactObservedTests'
 obs=json.loads(raw);refpath=root/'scripts/rust-contracts/framework-auth/full-suite-reference.json';ref=read(root,str(refpath.relative_to(root)))
 assert sha(refpath.read_bytes())==obs['full_suite_reference_sha256'],'ExactFullSuiteReference'
 assert ref['source_commit']==REV and ref['exit_code']==0 and ref['tests_passed']==3362
 assert obs['full_suite_log_sha256']==ref['artifact_sha256']['tests.log']
 for name,digest in ORIGINAL_FILES.items():assert sha((root/BASE/'original'/name).read_bytes())==digest,'PinnedOriginalFile:'+name
 tree=ast.parse((root/BASE/'original/tests/test_openapi_schema.py').read_text());cls=next(n for n in tree.body if isinstance(n,ast.ClassDef)and n.name=='LockedSpectacularAPIViewTests')
 assert ast_fingerprint(cls)==obs['class_AST_sha256']
 methods=[n for n in cls.body if isinstance(n,ast.FunctionDef)and n.name.startswith('test_')];assert len(methods)==len(obs['observations'])==3
 for method,row in zip(methods,obs['observations']):
  label='tests.test_openapi_schema.LockedSpectacularAPIViewTests.'+method.name
  assert row['test_id']==label and row['method_ast_sha256']==ast_fingerprint(method)
  assert row['success_log_line']==method.name+' ('+label+') ... ok'
  assert row['schema_generator_mocked']is True and row['cache_backend']=='LocMemCache' and row['Native_or_TCP_acceptance']is False
 if raw_log is not None:
  content=Path(raw_log).read_bytes();assert sha(content)==obs['full_suite_log_sha256'],'ActualRawFullSuiteHash'
  lines=content.decode().splitlines()
  for row in obs['observations']:assert lines.count(row['success_log_line'])==1,'ActualNamedSuccess'
 return obs

def build(root):
 obs=authority(root);labels=[r['test_id']for r in obs['observations']];fixture='Original-schema-cache3'
 assertions=['Execute the three exact unmodified named Original tests with original class setup, teardown, helpers and mocks',
 'Retain exact whole-workflow assertion ASTs: cached generator reuse, explicit invalidation and eight-thread single-flight with mocked schema generation and LocMemCache',
 'RequestFactory direct-view anonymous requests are not real TCP, full OpenAPI schema generation, Redis/multi-process cache or complete role coverage',
 'Keep METHOD_UNRESOLVED and all existing route identities; no Native result or milestone acceptance is inferred']
 contract={'contract_id':IDENT,'pattern_exact':'api/schema/','source_handler':'apps.api.schema_views.LockedSpectacularAPIView','method':None,'source_action':None,'owning_milestone':'M0','fixture_id':fixture,'assertions':assertions,'observed_roles':['anonymous'],'source_role_specs':{'anonymous':{'transport':'Django RequestFactory direct view','schema_generator_mocked':True,'authorization_backend_equivalence_claimed':False}},'source_test_methods':labels,'result':{'state':'selected_named_Original_tests_passed_in_retained_full3362_suite','methods':3,'new_Source_runs':0,'new_Source_cases_this_task':0,'Native_or_TCP_acceptance':False,'complete_role_matrix':False}}
 return {'format_version':1,'binding_kind':KIND,'Original_revision':REV,'scope':'One partial Source schema-cache framework-test assignment; not Native, real schema, TCP or milestone acceptance','credit':'Original Dispatcharr and dependency authors retain all application/algorithm/license credit.','operation_count':1,'fully_verified_added':0,'Native_coverage_increased':False,'fixtures':{fixture:{'id':fixture,'baseline_revision':REV,'baseline_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','roles':{'anonymous':None},'setup':['Use pinned Original source and unchanged baseline test runner in a new disposable environment','Preserve SimpleTestCase, RequestFactory, LocMemCache override, counting/slow generators and threaded assertions','Run exact selected test labels; retain actual exits and clean up only owned resources'],'execution':{'entrypoint':'scripts/rust-baseline/run.py','argv':['--output','<fresh-empty-output>',*labels],'fresh_live_replay_qualified_this_task':False},'retained_full_suite_reference':'scripts/rust-contracts/framework-auth/full-suite-reference.json','observations':BASE+'/observations.json','no_Swagger_Redoc_alias_or_real_schema_generation_claim':True}},'contracts':[contract],'non_HTTP_contracts':[]}

def dependencies():
 return [BASE+'/bindings.json',BASE+'/observations.json','scripts/rust-contracts/framework-auth/full-suite-reference.json',*[BASE+'/original/'+n for n in ORIGINAL_FILES]]

def validate(root,value,bindings=None):
 assert value==build(root),'ExactSchemaCacheContractAndScope'
 return {'passed':True,'operations':1,'Original_named_successes':3,'new_Source_runs':0,'fully_verified_added':0}

def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--repo',type=Path,required=True);p.add_argument('--raw-log',type=Path);p.add_argument('--write-bindings',action='store_true');args=p.parse_args();authority(args.repo,args.raw_log);path=args.repo/BASE/'bindings.json'
 if args.write_bindings:path.write_text(json.dumps(build(args.repo),indent=2)+'\n')
 print(json.dumps(validate(args.repo,json.loads(path.read_text())),indent=2))
if __name__=='__main__':main()
