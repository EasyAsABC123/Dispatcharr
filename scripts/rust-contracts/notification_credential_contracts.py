"""Three partial Source20 credential-framework assignments; no Native promotion.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
"""
import contextlib,copy,hashlib,importlib.util,json,re,sys
from pathlib import Path
BASE='scripts/rust-contracts/notification-credentials-source20-native20'
INPUT='scripts/rust-contracts/notification-credential-bindings.json'
KIND='retained_original_notification_Source20_real_credentials_framework'
REV='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
RAW='f02c49f2c1a3b7b876611ab99b2f7976aeef8cedaf471fc263c3223ba86680a0'
PROJECTION='ee65bdd1b940e0fafc761ca7a9ac900fe76e83ca11e6dbc119999f19caea8dce'
ITEMS=[('route-06ad6f49ab1bfa0a7ea3:GET',2,'retrieve'),('route-1fa68c983b2e31923427:GET',3,'unread_count'),('route-867117da1350f792b651:POST',20,'dismiss_all')]
def read(root,name):return json.loads((Path(root)/name).read_text())
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def ref(root,name):return {'path':name,'sha256':sha(Path(root)/name)}
@contextlib.contextmanager
def packet(root):
 path=Path(root)/BASE;old=list(sys.path);names=['project','validate_source_projection','source_seed_projection','credential_projection','cache_binding'];saved={k:sys.modules.get(k)for k in names}
 for k in names:sys.modules.pop(k,None)
 sys.path.insert(0,str(path))
 try:
  spec=importlib.util.spec_from_file_location('_notification_Source20_packet',path/'verify.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);yield m
 finally:
  sys.path[:]=old
  for k in names:
   sys.modules.pop(k,None)
   if saved[k]is not None:sys.modules[k]=saved[k]
def generate(root,registry=None):
 root=Path(root);reference=read(root,BASE+'/reference.json');review=read(root,BASE+'/Source20-Root-review.json')
 assert reference['Source20']['original_sha256']==RAW and reference['Source20']['projection_sha256']==PROJECTION and sha(root/BASE/'Source20-projection.json')==PROJECTION
 assert review['passed']is True and review['actual_Source_requests']==20 and review['Source_report_sha256']==RAW
 with packet(root)as p:
  source=p.load_source_projection();plan=p.load_plan()
  from validate_source_projection import compare
  qualified=compare(source,plan)
 assert qualified['passed']is True and qualified['actual_Source_requests']==20 and source['capture_valid']is True
 assert source['force_authenticate_used']is False and source['Source_TCP_or_Native']is False and source['enforce_csrf_checks']is True
 registry=registry or read(root,'docs/rust-contract-register.json');index={o['contract_id']:(r,o)for r in registry['runtime_routes']for o in r['operations']};roles={r['id']:r for r in plan['roles']};fixture_id='retained-Original-notification-real-credentials20'
 fixture={'id':fixture_id,'baseline_revision':REV,'baseline_image':'ghcr.io/dispatcharr/dispatcharr@sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f','roles':copy.deepcopy(plan['roles']),'scope':'Actual Original resolver/APIClient normal JWT/API-key/CSRF middleware; no forceauth, TCP, Native or whole role matrix','setup':{'fixed_plan':ref(root,BASE+'/plan.json'),'fixture_executor_seal_sha256':reference['Source_executor_seal_sha256'],'Original_archive_sha256':'f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff','new_replay_requires_fresh_owned_fixture':True,'sequential_context':'All20 requests in fixed order; selected operations are not independent captures'},'assertions':{'Source_validator':ref(root,BASE+'/validate_source_projection.py'),'Source_seed_validator':ref(root,BASE+'/source_seed_projection.py'),'shared_effect_qualification':qualified},'result':{'Source_shell_and_controller_exit':0,'Root_review':ref(root,BASE+'/Source20-Root-review.json'),'actual_Source_requests':20,'new_Source_runs_this_mapping':0},'projection':{'reference':ref(root,BASE+'/reference.json'),'original_raw_sha256':RAW,'observed_projection':ref(root,BASE+'/Source20-projection.json'),'policy':'Explicit credential hashes/lengths preserve secret fields; projected bytes were not executed; original identity remains separate'}}
 contracts=[]
 for identifier,ordinal,action in ITEMS:
  route,op=index[identifier];row=source['requests'][ordinal-1];spec=plan['cases'][ordinal-1];role=roles[row['actual_auth_observation']['user_id']];alias=role['alias']
  assert row['ordinal']==ordinal and row['method']==op['method']==spec['method'] and op['source_action']==action and op['owning_milestone']=='M5'
  assert route['source_handler']==row['resolver']['class']=='core.api_views.SystemNotificationViewSet' and row['resolver']['action']==action and row['resolver']['actions'][op['method'].lower()]==action
  assert re.fullmatch(route['pattern_exact'].replace('/^','/'),row['path'].lstrip('/'))
  assert row['status']==200 and row['transport']=='http_response' and row['framework_exception']is None and row['capture_complete']is True
  assert row['actual_auth_observation']['backend_class']=='rest_framework_simplejwt.authentication.JWTAuthentication'
  contracts.append({'contract_id':identifier,'pattern_exact':route['pattern_exact'],'source_handler':route['source_handler'],'method':op['method'],'source_action':action,'owning_milestone':'M5','fixture_id':fixture_id,'observed_roles':[alias],'source_role_specs':{alias:{'seed_role':copy.deepcopy(role),'transport':'Original APIClient normal realJWT/CSRF middleware','force_authenticate':False,'actual_authenticated_user_id':role['id'],'observed_backend':row['actual_auth_observation']['backend_class']}},'cases':[{'source_ordinal':ordinal,'label':row['name'],'whole_source_fixture_observations':20,'observation_projection':copy.deepcopy(row),'original_raw_sha256':RAW,'json_pointer':'/requests/'+str(ordinal-1),'full_role_matrix':False}],'assertions':['Exact actual resolver/method/action/target, credential headers and successful authenticator are checked','Exact response JSON and raw body hash, own request clocks, all11 SQL/sequence fingerprints and user/event/cache effects use the unchanged Source20 validator','Preserve the whole20 fixed sequential setup and real failed-key history; three selected cases are not20 new executions each','Public credential projections retain original hashes/lengths and original capture identity; no Source TCP or Native result is inferred'],'result':{'state':'retained_Original_Source20_real_credentials_framework_passed','historical_captured_cases':1,'whole_fixture_observations':20,'Source_shell_exit':0,'Source_controller_exit':0,'new_Source_cases_this_task':0,'new_Source_runs_this_mapping':0,'complete_role_matrix':False,'Native_or_TCP_acceptance':False},'remaining':['Only this observed role/request/context is assigned','No alias/renderer/serializer/credential grammar or concurrency completion','No Native, performance or formal milestone promotion'],'fully_verified':False})
 return {'format_version':1,'binding_kind':KIND,'Original_revision':REV,'scope':'Three partial Original realcredential framework assignments, no Native verification','operation_count':3,'fully_verified_added':0,'Native_coverage_increased':False,'unique_fixture_Source_observations':20,'new_Source_runs':0,'fixtures':{fixture_id:fixture},'contracts':contracts,'non_HTTP_contracts':[]}
def validate(root,value,bindings=None):
 assert value==generate(root),'ExactSource20CredentialContractAndScope'
 return {'passed':True,'operations':3,'retained_Source_observations':20,'new_Source_runs':0,'fully_verified_added':0}
def dependencies():
 root=Path(__file__).resolve().parents[2]
 return [INPUT]+[str(p.relative_to(root))for p in sorted((root/BASE).rglob('*'))if p.is_file()and'__pycache__'not in p.parts]
if __name__=='__main__':
 root=Path(__file__).resolve().parents[2];value=generate(root);(root/INPUT).write_text(json.dumps(value,indent=2)+'\n');print(json.dumps({'operations':3,'Source_observations':20,'new_Source_runs':0}))
