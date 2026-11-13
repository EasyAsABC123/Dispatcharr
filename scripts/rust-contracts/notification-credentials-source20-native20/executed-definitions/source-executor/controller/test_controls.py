"""Synthetic controls only. No Django, SQL, Docker, sockets or JWT verification."""
import ast,base64,copy,datetime,hashlib,json,sys,unittest,os,types
from pathlib import Path
HERE=Path(__file__).resolve().parent;ROOT=HERE.parent;sys.path.insert(0,str(HERE))
import effects,seed_binding,source_fixture
from fixture_guards import TABLES,database
PLAN=json.loads((ROOT/'inputs/plan.json').read_text())
def clone(v):return copy.deepcopy(v)
def digest(v):return hashlib.sha256(json.dumps(v,sort_keys=True).encode()).hexdigest()
def token(actor):
 data=base64.urlsafe_b64encode(json.dumps({'user_id':actor,'token_type':'access'}).encode()).decode().rstrip('=');return 'synthetic.'+data+'.not-a-verified-signature'
def sample():
 nonce='0123456789';seedtime='2026-11-12T20:00:00+00:00';users=[];tokens={}
 for role in PLAN['roles']:
  alias=role['alias'];users.append({'id':role['id'],'username':'rust-probe-notifcred-'+nonce+'-'+alias,'password':'!','api_key':'fixture-key-'+nonce+'-'+alias,'is_active':role['is_active'],'user_level':role['user_level'],'custom_properties':role['custom_properties'],'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'stream_limit':0,'avatar_config':{},'date_joined':seedtime})
  if role['is_active']:
   raw=token(role['id']);tokens[alias]={'raw':raw,'sha256':hashlib.sha256(raw.encode()).hexdigest()}
 graph={'notifications':[{**r,'title':'Credential notice '+str(r['id']),'message':'Owned fixture only','notification_type':'info','priority':'normal','action_data':{},'expires_at':None}for r in PLAN['seed_notices']],'dismissals':[]};seq={n:{'last_value':1,'is_called':False}for n in TABLES};seq['accounts_user']={'last_value':6,'is_called':True};seq[effects.N]={'last_value':2,'is_called':True}
 def audit():
  tables={n:[]for n in TABLES};tables['accounts_user']=users;tables[effects.N]=graph['notifications'];tables[effects.D]=graph['dismissals'];return {'tables':{n:{'rows':len(v),'sha256':digest(v)}for n,v in tables.items()},'sequences':{'public.'+n+'_id_seq':clone(v)for n,v in seq.items()}}
 report={'actual_auth_configuration':clone(seed_binding.AUTH_CONFIGURATION),'database':'rust_probe_notifcred_'+nonce,'seed_started_utc':seedtime,'seed_completed_utc':seedtime,'seed_users':clone(users),'seed_notifications':clone(graph),'seed_audit':audit(),'tokens_private':tokens,'capture_valid':True,'actual_request_count':20,'force_authenticate_used':False,'real_default_JWT_and_APIKey_backends':True,'Source_TCP_or_Native':False,'requests':[]};history=None;actors={r['alias']:type('Actor',(),{'api_key':u['api_key']})()for r,u in zip(PLAN['roles'],users)}
 for spec in PLAN['cases']:
  number=spec['ordinal'];time='2026-11-12T20:01:'+str(number).zfill(2)+'+00:00';actor=seed_binding.expected_actor(spec,PLAN);action=seed_binding.ACTIONS[number-1];row={**{k:spec[k]for k in('ordinal','name','method','path')},'request_payload':spec['payload'],'auth_selectors':spec['auth'],'raw_headers_private':{k:v.encode().hex()for k,v in source_fixture.headers(spec['auth'],actors,{a:t['raw']for a,t in tokens.items()}).items()},'resolver':{'class':'core.api_views.SystemNotificationViewSet','action':action,'actions':{spec['method'].lower():action},'kwargs':{'pk':spec['path'].split('/')[4]}if action in('retrieve','partial_update','destroy','dismiss')else{}},'before_users':clone(users),'after_users':clone(users),'before_events':[],'after_events':[],'before_audit':audit(),'before_notifications':clone(graph),'before_cache':{'api_key_failed_auth_history':clone(history)},'began_utc':time,'completed_utc':time,'transport':'http_response','status':spec['predicted_status_UNMEASURED'],'capture_complete':True,'framework_exception':None,'actual_auth_observation':{'user_id':actor,'is_authenticated':actor is not None,'backend_class':None if actor is None else 'rest_framework_simplejwt.authentication.JWTAuthentication'if'bearer'in spec['auth']else'apps.accounts.authentication.ApiKeyAuthentication'}};body={'detail':'synthetic refusal'}
  if number in(4,18,20):
   targets=[1]if number==4 else[2]if number==18 else[1,2,3]
   for target in targets:
    s=seq[effects.D];s['last_value']+=int(s['is_called']);s['is_called']=True;graph['dismissals'].append({'id':s['last_value'],'user_id':actor,'notification_id':target,'dismissed_at':time,'action_taken':None})
   body={'success':True,'dismissed_count':len(targets)}if number==20 else{'success':True,'message':'Notification dismissed','notification_key':'credential.'+('A'if number==4 else'B')}
  elif number==9:
   seq[effects.N]={'last_value':3,'is_called':True};n={'id':3,'notification_key':'credential.created','title':'Hello','message':'World','notification_type':'info','priority':'normal','source':'system','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':time,'updated_at':time};graph['notifications'].append(n);body=effects.serialized(n,actor,graph['dismissals'])
  elif number==10:graph['notifications'][0]['updated_at']=time;body=effects.serialized(graph['notifications'][0],actor,graph['dismissals'])
  elif spec['method']=='GET'and row['status']==200:
   notices=sorted(graph['notifications'],key=lambda n:(n['priority'],effects.clock(n['created_at'])),reverse=True);unread=[n for n in notices if not any(d['user_id']==actor and d['notification_id']==n['id']for d in graph['dismissals'])]
   if action=='retrieve':body=effects.serialized(graph['notifications'][0],actor,graph['dismissals'])
   elif action=='unread_count':body={'unread_count':len(unread)}
   else:
    selected=notices if'include_dismissed=true'in spec['path']else unread;body={'notifications':[effects.serialized(n,actor,graph['dismissals'])for n in selected],'count':len(selected),'unread_count':len(selected)}
  if number in(15,16):history=[effects.clock(time).timestamp()]+([]if history is None else history)
  raw=json.dumps(body).encode();row.update(json=clone(body),body_hex=raw.hex(),body_bytes=len(raw),body_sha256=hashlib.sha256(raw).hexdigest(),after_audit=audit(),after_notifications=clone(graph),after_cache={'api_key_failed_auth_history':clone(history)});report['requests'].append(row)
 return report
class Controls(unittest.TestCase):
 def test_complete_synthetic20(self):self.assertTrue(effects.compare(sample(),PLAN)['passed'])
 def reject(self,mutate):
  v=sample();mutate(v)
  with self.assertRaises((ValueError,KeyError,AssertionError)):effects.compare(v,PLAN)
 def test_wrong_role(self):self.reject(lambda v:v['seed_users'][3].update(user_level=10))
 def test_auth_configuration_order(self):self.reject(lambda v:v['actual_auth_configuration']['default_authentication_classes'].reverse())
 def test_wrong_notice_seed(self):self.reject(lambda v:v['seed_notifications']['notifications'][0].update(source='developer'))
 def test_swapped_target(self):self.reject(lambda v:v['requests'][1]['resolver']['kwargs'].update(pk='2'))
 def test_wrong_resolved_action(self):self.reject(lambda v:v['requests'][2]['resolver'].update(action='list'))
 def test_wrong_method_binding(self):self.reject(lambda v:v['requests'][0]['resolver']['actions'].update(get='retrieve'))
 def test_wrong_actual_header(self):self.reject(lambda v:v['requests'][0]['raw_headers_private'].update(HTTP_AUTHORIZATION='00'))
 def test_wrong_actual_actor(self):self.reject(lambda v:v['requests'][4]['actual_auth_observation'].update(user_id=1))
 def test_wrong_auth_precedence(self):self.reject(lambda v:v['requests'][12]['actual_auth_observation'].update(backend_class='apps.accounts.authentication.ApiKeyAuthentication'))
 def test_forceauth_scope_rejected(self):self.reject(lambda v:v.update(force_authenticate_used=True))
 def test_missing_request(self):self.reject(lambda v:v['requests'].pop())
 def test_wrong_status(self):self.reject(lambda v:v['requests'][0].update(status=403))
 def test_body_corruption(self):self.reject(lambda v:v['requests'][0].update(body_hex='7b7d'))
 def test_network_denial_mutation(self):self.reject(lambda v:v['requests'][5]['after_notifications']['notifications'][0].update(title='wrong'))
 def test_budget_not_consumed(self):self.reject(lambda v:v['requests'][14]['after_cache'].update(api_key_failed_auth_history=None))
 def test_budget_reset(self):self.reject(lambda v:v['requests'][16]['before_cache'].update(api_key_failed_auth_history=None))
 def test_good_jwt_badkey_no_budget(self):self.reject(lambda v:v['requests'][12]['after_cache'].update(api_key_failed_auth_history=[1.0]))
 def test_foreign_dismissal(self):self.reject(lambda v:v['requests'][3]['after_notifications']['dismissals'][0].update(user_id=1))
 def test_skipped_sequence(self):self.reject(lambda v:v['requests'][3]['after_audit']['sequences']['public.core_notificationdismissal_id_seq'].update(last_value=10))
 def test_patch_no_advance(self):self.reject(lambda v:v['requests'][9]['after_notifications']['notifications'][0].update(updated_at='2020-01-01T00:00:00Z'))
 def test_credential_user_row_changed(self):self.reject(lambda v:v['requests'][0]['after_users'][0].update(api_key='changed'))
 def test_credential_event_changed(self):self.reject(lambda v:v['requests'][14]['after_events'].append({'id':1}))
 def test_owned_database_guard(self):
  with self.assertRaises(ValueError):database('dispatcharr')
 def test_no_app_import_on_host(self):
  for p in HERE.glob('*.py'):ast.parse(p.read_bytes(),filename=str(p))
 def test_literal_exception_capacity_is_explicit(self):
  from capture_common import exception_record
  try:raise RuntimeError('x'*5000)
  except RuntimeError:record=exception_record(sys.exc_info())
  self.assertFalse(record['complete']);self.assertTrue(record['message_evidence']['truncated']);self.assertEqual(record['message_evidence']['original_bytes'],5000);self.assertEqual(record['message_evidence']['original_sha256'],hashlib.sha256(b'x'*5000).hexdigest());json.dumps(record,allow_nan=False)
 def test_pinned_management_startup_function_semantics(self):
  # Execute only this dependency-free function AST with explicit worker
  # boundaries; no Original module import, Django setup or application run.
  path=ROOT/'inputs/Original/dispatcharr/app_initialization.py';tree=ast.parse(path.read_text());fn=next(n for n in tree.body if isinstance(n,ast.FunctionDef)and n.name=='should_skip_initialization');module=ast.Module(body=[fn],type_ignores=[]);namespace={'sys':types.SimpleNamespace(argv=['manage.py','shell','-c','fixture']),'os':os,'logger':types.SimpleNamespace(debug=lambda *a:None),'_uwsgi_worker_id':lambda:None,'_is_worker_process':lambda:False};exec(compile(module,str(path),'exec'),namespace);self.assertTrue(namespace['should_skip_initialization']());namespace['sys'].argv=['manage.py','migrate'];self.assertTrue(namespace['should_skip_initialization']());namespace['sys'].argv=['fixture.py'];self.assertFalse(namespace['should_skip_initialization']())
if __name__=='__main__':unittest.main()
