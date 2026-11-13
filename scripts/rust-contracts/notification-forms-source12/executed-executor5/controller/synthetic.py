"""Pure synthetic Source12-shaped corruption specimens. Never actual Source observations."""
import copy,datetime,hashlib,json
from pathlib import Path
from definition import ROOT,target
from effects import ACTIONS,CACHE_VERSION,CACHE_GROUPS,SCHEMA
from fixture_guards import TABLES
T=datetime.datetime(2026,11,13,tzinfo=datetime.timezone.utc)
def instant(seconds):return(T+datetime.timedelta(seconds=seconds)).isoformat()
def state(graph,users,sequences,warm=False):
 tables={name:[]for name in TABLES};tables['accounts_user']=copy.deepcopy(users);tables['core_systemnotification']=copy.deepcopy(graph['notifications']);tables['core_notificationdismissal']=copy.deepcopy(graph['dismissals']);tables['core_coresettings']=[{'id':1,'key':'network_access','name':'Network Access','value':{'UI':'127.0.0.0/8'}}];fingerprints={};used=0
 for name,rows in tables.items():
  text=[json.dumps(r,sort_keys=True)for r in rows];encoded=b'['+b','.join(x.encode()for x in text)+b']';used+=sum(len(x.encode())for x in text);fingerprints[name]={'rows':len(rows),'sha256':hashlib.sha256(encoded).hexdigest(),'raw_rows':rows,'raw_json_text':text}
 payload={'tables':fingerprints,'sequences':copy.deepcopy(sequences)};audit={'database':'rust_probe_notifforms_aaaaaaaaaa',**payload,'dataset_sha256':hashlib.sha256(json.dumps(payload,sort_keys=True).encode()).hexdigest(),'capture_bytes':used,'scope':'SYNTHETIC eleven-table specimen, not real PG output'}
 keys={CACHE_VERSION}|(CACHE_GROUPS if warm else set());cache={'rows':[{'key_hex':k.encode().hex(),'dump_hex':b'SYNTHETIC-NOT-REAL-REDIS-DUMP'.hex(),'type':'string','value_hex':b'SYNTHETIC-CACHE-VALUE'.hex(),'pttl':-1 if k==CACHE_VERSION else 299000}for k in sorted(keys)],'capture_bytes':100,'api_key_failed_auth_history':None,'scope':'SYNTHETIC no application/cache connection'}
 full={'tables':{name:{'count':0,'sha_like_md5':hashlib.md5(b'[]').hexdigest()}for name in SCHEMA['tables']},'sequences':{name:[1,False]for name in SCHEMA['sequences']},**{k:copy.deepcopy(SCHEMA[k])for k in('columns','constraints','indexes')}}
 for name,rows in tables.items():full['tables'][name]={'count':len(rows),'sha_like_md5':hashlib.md5(('['+', '.join(sorted(json.dumps(r,sort_keys=True)for r in rows))+']').encode()).hexdigest()}
 for name,row in sequences.items():full['sequences'][name.removeprefix('public.')]=[row['last_value'],row['is_called']]
 receiver='specific.rust_owned_forms_aaaaaaaaaa!receiver';channel={'group':'updates','receiver':receiver,'Redis_database':1,'group_key_hex':b'asgi:group:updates'.hex(),'channel_key_hex':b'asgispecific.rust_owned_forms_aaaaaaaaaa!'.hex(),'mailbox_type':'none','mailbox_pttl':-2,'frames':[],'receive_pop_or_ack_calls':0,'unknown_or_setup_frames_consumed':False,'scope':'SYNTHETIC no real Channels traffic'}
 raw=[{'database':i,'rows':[],'capture_bytes':0,'complete':True,'destructive_commands_or_consumption':False}for i in range(4)];raw[1]['rows']=copy.deepcopy(cache['rows'])+[{'key_hex':b'asgi:group:updates'.hex(),'type':'zset','dump_hex':b'SYNTHETIC-GROUP'.hex(),'pttl':86400000,'zset':[{'member_hex':receiver.encode().hex(),'score':1.0,'score_hex':(1.0).hex()}]}]
 return {'audit':audit,'public_manifest':full,'raw_Redis':raw,'Channels':channel,'notifications':copy.deepcopy(graph),'users':copy.deepcopy(users),'events':[],'cache':cache,'media':[]}
def metadata(number):
 empty={'present':True,'class':'builtins.type','class_value':'rest_framework.request.Empty'};result={'request_available':True,'request_class':'rest_framework.request.Request','forced_parse':False,'cached':{k:copy.deepcopy(empty)for k in('_data','_files','_full_data')}}
 if number not in(5,6,10,11,12):
  for key in('_data','_files','_full_data'):result['cached'][key]={'present':True,'class':'django.http.request.QueryDict'if key!='_files'else'django.utils.datastructures.MultiValueDict','fields':[]}
 if number==7:result['cached']['_files']['fields']=[{'name':'unknown_upload','values':[{'kind':'file_metadata','class':'django.core.files.uploadedfile.InMemoryUploadedFile','name':'owned-small.txt','size':20,'content_type':'text/plain','charset':None,'file_content_read':False}]}]
 if number==8:result['cached']['_data']['fields']=[{'name':'title','values':[{'kind':'literal','value':'first'},{'kind':'literal','value':'second'}]}]
 return result

def example():
 plan=json.loads((ROOT/'inputs/plan.json').read_text());users=[]
 for role in plan['roles']:
  a=role['alias'];users.append({'id':role['id'],'username':'rust-probe-notifforms-aaaaaaaaaa-'+a,'password':'!','api_key':'fixture-forms-key-aaaaaaaaaa-'+a,'is_active':role['is_active'],'user_level':role['user_level'],'custom_properties':role['custom_properties'],'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'stream_limit':0,'avatar_config':{},'date_joined':instant(.1)})
 graph={'notifications':copy.deepcopy(plan['seed_notifications']),'dismissals':copy.deepcopy(plan['seed_dismissals'])};sequences={'public.'+t+'_id_seq':{'last_value':1,'is_called':False}for t in TABLES}
 for t,v in plan['seed_sequences'].items():sequences['public.'+t+'_id_seq']=copy.deepcopy(v)
 report={'Channels_probe_context':{'registered':True,'group':'updates','Redis_database':1,'non_consuming':True,'Source_methods_replaced':False,'group_registration_is_fixture_context_not_Default_graph':True},'_synthetic_only':True,'database':'rust_probe_notifforms_aaaaaaaaaa','capture_valid':True,'actual_request_count':12,'completed_capture_count':12,'force_authenticate_used':False,'Source_TCP_or_Native':False,'seed_started_utc':instant(0),'seed_completed_utc':instant(.2),'seed':state(graph,users,sequences),'aliases':copy.deepcopy(plan['seed_aliases']),'requests':[],'connections_close_called':True};aliases=copy.deepcopy(plan['seed_aliases']);warm=False
 for spec in plan['cases']:
  n=spec['index'];row={k:copy.deepcopy(spec[k])for k in('index','method','actor','target_alias','body_sha256','body_bytes','content_type')};row['path']=target(spec,aliases);uid=next(r['id']for r in plan['roles']if r['alias']==spec['actor']);user=users[uid-1];row.update(headers={'Content-Type':'application/json'},resolver={'class':'core.api_views.SystemNotificationViewSet','action':ACTIONS[n-1],'actions':{spec['method'].lower():ACTIONS[n-1]},'kwargs':{}if ACTIONS[n-1]=='create'else{'pk':str(aliases[spec['target_alias']])},'url_name':'systemnotification-list'if ACTIONS[n-1]=='create'else'systemnotification-detail'},before=state(graph,users,sequences,warm),raw_headers_private={'HTTP_X_API_KEY':user['api_key'].encode().hex()},began_utc=instant(n),completed_utc=instant(n+.2),transport='http_response',capture_complete=True,status=spec['predicted_status_UNMEASURED'],framework_exception=None,actual_auth_observation={'user_id':uid,'is_authenticated':True,'backend_class':'apps.accounts.authentication.ApiKeyAuthentication'},parsed_request_metadata=metadata(n));warm=True;response={'detail':'No SystemNotification matches the given query.'}
  if n in(1,7):
   ident=12 if n==1 else 13;alias='FormA'if n==1 else'FormB';aliases[alias]=ident;created={'id':ident,'notification_key':'owned_forms_a'if n==1 else'owned_forms_b','title':'Form A'if n==1 else'Form B','message':'First form notification'if n==1 else'Multipart notification','notification_type':'info','priority':'normal','source':'system','action_data':{},'is_active':True,'admin_only':False,'expires_at':None,'created_at':instant(n+.1),'updated_at':instant(n+.1)};graph['notifications'].append(created);graph['notifications'].sort(key=lambda r:r['id']);sequences['public.core_systemnotification_id_seq']={'last_value':ident,'is_called':True};response={k:v for k,v in created.items()if k!='updated_at'};response['is_dismissed']=False
  elif n in(2,4,8):
   current=next(r for r in graph['notifications']if r['id']==aliases[spec['target_alias']]);current['updated_at']=instant(n+.1)
   if n==2:current['action_data']={'flag':True,'count':1,'nested':[False,0]}
   if n==4:current.update(title='Form A replaced',message='Boolean fields omitted',is_active=False,admin_only=False)
   if n==8:current.update(title='second',priority='high',action_data={'flag':True,'count':1})
   response={k:v for k,v in current.items()if k!='updated_at'};response['is_dismissed']=False
  elif n in(3,9):response={'action_data':['Value must be valid JSON.']}
  elif n==10:response={'detail':'You do not have permission to perform this action.'}
  elif n==11:response={'detail':'Unsupported media type "text/plain" in request.'}
  elif n==12:
   graph['notifications']=[r for r in graph['notifications']if r['id']!=10];graph['dismissals']=[r for r in graph['dismissals']if r['notification_id']!=10];response=None
  raw=b''if n==12 else json.dumps(response,separators=(',',':')).encode();row.update(json=copy.deepcopy(response),response_body_hex=raw.hex(),response_body_bytes=len(raw),response_body_sha256=hashlib.sha256(raw).hexdigest(),after=state(graph,users,sequences,warm));report['requests'].append(row)
 report['aliases']=aliases;report['final_before_probe_cleanup']=copy.deepcopy(report['requests'][-1]['after']);report['Channels_probe_cleanup']={'own_membership_discarded':True,'frames_consumed':0,'mailbox_deleted_by_fixture':False};return report,plan
