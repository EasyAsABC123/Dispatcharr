"""Synthetic-only complete Source24 effect shape for socket-free corruption controls."""
import copy,datetime,hashlib,json
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
PLAN=json.loads((ROOT/'inputs/plan.json').read_text());SCHEMA=json.loads((ROOT/'inputs/schema62-authority.json').read_text())
def stamp(second):return(datetime.datetime(2026,11,13,tzinfo=datetime.timezone.utc)+datetime.timedelta(seconds=second)).isoformat()
def build():
 db='rust_probe_pluginrepo_'+'a'*10;nonce='a'*10;official={'id':1,'name':'Dispatcharr Official','url':'https://dispatcharr.github.io/Plugins/manifest.json','is_official':True,'enabled':True,'cached_manifest':{},'public_key':'','signature_verified':None,'last_fetched':None,'last_fetch_status':'','created_at':stamp(-100),'updated_at':stamp(-100)};repos=[official]
 for spec in PLAN['seed']['custom_repos']:repos.append({**copy.deepcopy(spec),'last_fetched':None,'last_fetch_status':'','created_at':stamp(.2),'updated_at':stamp(.21)})
 users=[]
 for spec in PLAN['roles']:users.append({**copy.deepcopy(spec),'username':'owned-pluginrepo-'+nonce+'-'+spec['alias'],'api_key':'owned-pluginrepo-api-'+nonce+'-'+spec['alias'],'password':'!','is_active':True,'first_name':'','last_name':'','email':'','is_staff':False,'is_superuser':False,'last_login':None,'avatar_config':{},'stream_limit':0,'date_joined':stamp(.3)})
 for user in users:user.pop('alias')
 configs=[]
 for spec in PLAN['seed']['managed_config_pairs']:configs.append({**copy.deepcopy(spec),'name':spec['key'],'slug':spec['key'],'version':'','description':'','settings':{},'owned_tasks':[],'suspended_schedules':[],'installed_version_is_prerelease':False,'deprecated':False,'created_at':stamp(.4),'updated_at':stamp(.41)})
 manifest={k:copy.deepcopy(SCHEMA[k])for k in ['columns','constraints','indexes']};manifest['tables']={n:{'count':0,'sha_like_md5':'0'*32}for n in SCHEMA['tables']};manifest['sequences']={n:[1,False]for n in SCHEMA['sequences']}
 for name,last in PLAN['seed']['sequence_last_values_called'].items():manifest['sequences'][name]=[last,True]
 manifest['tables']['plugins_pluginrepo']['count']=3;manifest['tables']['plugins_pluginconfig']['count']=2
 epoch=int(datetime.datetime(2026,11,13,tzinfo=datetime.timezone.utc).timestamp())*1000000000+500000000;cache=[]
 for group in ['network_access','system_settings']:
  raw=str(epoch).encode();cache.append({'key_hex':(':1:coresettings:groupver:'+group).encode().hex(),'type':'string','dump_hex':'00','value_hex':raw.hex(),'pttl':-1})
 channel={'group':'updates','receiver':'specific.rust_owned_forms_'+nonce+'!receiver','Redis_database':1,'receive_pop_or_ack_calls':0,'unknown_or_setup_frames_consumed':False,'frames':[]}
 state={'public_manifest':manifest,'repos':repos,'configs':configs,'settings':[{'id':i+1,**copy.deepcopy(s)}for i,s in enumerate(PLAN['seed']['CoreSettings'])],'system_notifications':[],'notification_dismissals':[],'users':users,'events':[],'cache':{'rows':cache,'api_key_failed_auth_history':None},'raw_Redis':[{'database':n,'rows':copy.deepcopy(cache)if n==1 else[],'capture_bytes':0,'complete':True,'destructive_commands_or_consumption':False}for n in range(4)],'Channels':channel,'owned_log_and_media_files':[]}
 report={'_synthetic_only':True,'capture_complete':True,'passed':False,'capture_only':True,'actual_request_count':24,'completed_capture_count':24,'Source_framework_APIClient_only':True,'Source_TCP_or_Native':False,'force_authenticate_used':False,'client_raise_request_exception':True,'database':db,'definition_sha256':hashlib.sha256((ROOT/'inputs/plan.json').read_bytes()).hexdigest(),'seed_started_utc':stamp(0),'seed_completed_utc':stamp(1),'phase_started_utc':stamp(2),'phase_completed_utc':stamp(29),'phase_elapsed_seconds':27.,'seed':copy.deepcopy(state),'before_owned_seed':{'repos':[copy.deepcopy(official)]},'requests':[],'connections_close_called':True,'probe_cleanup':{'own_membership_discarded':True,'frames_consumed':0,'mailbox_deleted_by_fixture':False}}
 def body_repo(row):
  result={k:copy.deepcopy(v)for k,v in row.items()if k!='cached_manifest'};m=row['cached_manifest'].get('manifest',row['cached_manifest']);result['registry_url']=m.get('registry_url','')or'';result['plugin_count']=len(m.get('plugins',[]))if type(m.get('plugins',[]))is list else 0;return result
 for spec in PLAN['cases']:
  row={'index':spec['index'],'name':spec['name'],'actor':spec['actor'],'method':spec['method'],'target_alias':spec['target'],'path':PLAN['method_route_bindings'][spec['target']],'body_hex':spec['body_hex'],'content_type':spec['content_type'],'body_bytes':spec['body_bytes'],'body_sha256':spec['body_sha256'],'raw_headers_private':{}if spec['actor']=='anonymous'else{'HTTP_X_API_KEY':('owned-pluginrepo-api-'+nonce+'-'+spec['actor']).encode().hex()},'resolver':{'class':'apps.plugins.api_views.'+('PluginRepoListCreateAPIView'if spec['target']=='collection'else'PluginRepoDetailAPIView'),'kwargs':{}if spec['target']=='collection'else{'pk':int(PLAN['method_route_bindings'][spec['target']].rstrip('/').split('/')[-1])}},'actual_request_invoked':True,'capture_complete':True,'began_utc':stamp(spec['index']+2),'completed_utc':stamp(spec['index']+2.5),'before':copy.deepcopy(state),'parsed_request_metadata':{'request_available':True,'forced_parse':False,'cached':{k:{'present':True,'class':'builtins.type','class_value':'rest_framework.request.Empty'}for k in ['_data','_files','_full_data']}}}
  status=spec['predicted_status'];body={}
  if status is None:
   row.update(transport='framework_exception',framework_exception={'type':'builtins.AttributeError','complete':True,'truncated':False})
   for field,data in [('message',b'synthetic list shape'),('traceback',b'synthetic complete traceback')]:row['framework_exception'].update({field+'_hex':data.hex(),field+'_bytes':len(data),field+'_sha256':hashlib.sha256(data).hexdigest(),field+'_original_bytes':len(data),field+'_original_sha256':hashlib.sha256(data).hexdigest(),field+'_truncated':False})
  else:
   if row['method']=='PUT'and status==200:
    target=next(r for r in state['repos']if r['id']==801);data=json.loads(bytes.fromhex(spec['body_hex']))
    if 'enabled'in data:target['enabled']=data['enabled']
    if 'public_key'in data:target['public_key']=data['public_key'].strip()
    target['updated_at']=stamp(spec['index']+2.2);body=body_repo(target)
   elif row['method']=='DELETE'and status==200:
    state['repos']=[r for r in state['repos']if r['id']!=801];state['public_manifest']['tables']['plugins_pluginrepo']['count']=2
    for config in state['configs']:
     if config['source_repo_id']==801:config['source_repo_id']=None
    body={'success':True}
   elif row['method']=='GET'and status==200:body=[body_repo(r)for r in sorted(state['repos'],key=lambda r:(not r['is_official'],r['name']))]
   if spec['actor']=='anonymous':auth={'user_id':None,'is_authenticated':False,'backend_class':None}
   else:auth={'user_id':next(r['id']for r in PLAN['roles']if r['alias']==spec['actor']),'is_authenticated':True,'backend_class':'apps.accounts.authentication.ApiKeyAuthentication'}
   raw=json.dumps(body,separators=(',',':')).encode();row.update(transport='http_response',status=status,headers={'Content-Type':'application/json'},json=body,response_body_hex=raw.hex(),response_body_bytes=len(raw),response_body_sha256=hashlib.sha256(raw).hexdigest(),actual_auth_observation=auth)
  row['after']=copy.deepcopy(state);report['requests'].append(row)
 report['final_before_probe_cleanup']=copy.deepcopy(state)
 return report
