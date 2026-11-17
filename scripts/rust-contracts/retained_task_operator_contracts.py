"""Seven exact retained Original task/operator Source assignments, no workloads.
Original Dispatcharr and dependency authors retain all application/algorithm credit.
"""
import ast,base64,hashlib,json,uuid
from pathlib import Path
from framework_auth_contracts import ast_fingerprint
KIND='retained_original_task_operator7_observations'
BASE='scripts/rust-contracts/retained-task-operator7/'
ORIGINAL='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'
ARCHIVE='f2323ac705be2af5d1c264f26b1d297cb2bede53874e6bceccee4bf6603fa7ff'
# Exact reviewed input hashes/identities are populated with the new binding.
AUTHORITIES=[{'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-3c2ec1e38e1aabe048cd.py', 'sha256': '8224bfb27e58792e08207e31560b1e4ea50ad770e710247c9d50d2b2e925de6a'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-4f7dabcaec2ccab989d7.py', 'sha256': 'f592ad7717a11fade830600a702da2b8f4814ad828bfc222b1184127829d5fc7'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-62ce30c3036ba0be3f4f.py', 'sha256': '56e84af9ac791bf05e8fd718366dd830c09e712399ede0c8e2a660eec361e29a'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-7ab936e6e461607a8252.py', 'sha256': '377dfdadd2b3bb510c31d853276da0c7ddb5efa7b615cf06134f2d1b2bd70a3e'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-adb99cdd2131aca35047.py', 'sha256': 'df7f7f9c37f613c3b44e320c4a2009ad8fbe6d3c5e9795ea188e433a9ab2dd6c'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-be4caac220021ac7dd32.py', 'sha256': 'be95ebeaf76392b96252637485ae16a6e2a10312f7b74aa643b34f369f058e99'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-d3f0c1a5a06af4e695a5.py', 'sha256': '5e9237b7b85a1edbf23cee6ef2d63df952390bdf1768a3bf4ac2cf1c9017cb4c'}, {'path': 'scripts/rust-contracts/retained-task-operator7/definitions.json', 'sha256': '8a203dd59a37bd3a57423aca07f7b623b4fbb4d7501f76425d5e976bf32c4b22'}, {'path': 'scripts/rust-contracts/retained-task-operator7/fixture-authorities.json', 'sha256': 'ef96be983d2fa77f3f2749e70c22f8f5efda01824e5578b55d76d9b802a7c7ac'}, {'path': 'scripts/rust-contracts/retained-task-operator7/observations.json', 'sha256': 'd97bd4ccc33de958560392907f4771630be4c7ab369c5d10ac7f865728e2cadc'}, {'path': 'scripts/rust-contracts/retained-task-operator7/operator-Original-source.json', 'sha256': '1043832c1cfa47bec05bf0a23500aa7d1e7b7c80c98ceefb1868097c9fe10615'}, {'path': 'scripts/rust-contracts/retained-task-operator7/private-provenance.json', 'sha256': '84f28e75d4e804a5ed76c29329d50d5dcf445624d54873da6d1eac6669aea011'}]
IDENTITIES=[{'candidate_id': 'candidate-4f7dabcaec2ccab989d7', 'source': {'kind': 'task_candidate', 'path': 'apps/vod/tasks.py', 'line': 96, 'name': 'refresh_vod_content', 'callable': 'celery.shared_task'}, 'original_file_sha256': '8bed587b6a113caf26039f1ee3e133a0c3a4d51f4909b4e7282956a6b0e5681d', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-4f7dabcaec2ccab989d7.py', 'sha256': 'f592ad7717a11fade830600a702da2b8f4814ad828bfc222b1184127829d5fc7'}, 'start_line': 96, 'end_line': 182}, {'candidate_id': 'candidate-3c2ec1e38e1aabe048cd', 'source': {'kind': 'task_candidate', 'path': 'apps/channels/tasks.py', 'line': 349, 'name': 'match_selected_channels_epg', 'callable': 'celery.shared_task'}, 'original_file_sha256': '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-3c2ec1e38e1aabe048cd.py', 'sha256': '8224bfb27e58792e08207e31560b1e4ea50ad770e710247c9d50d2b2e925de6a'}, 'start_line': 349, 'end_line': 474}, {'candidate_id': 'candidate-7ab936e6e461607a8252', 'source': {'kind': 'task_candidate', 'path': 'apps/channels/tasks.py', 'line': 477, 'name': 'match_single_channel_epg', 'callable': 'celery.shared_task'}, 'original_file_sha256': '92fd879189109310b58b9cdd684b4e00cb5e1d62b6931e975ab41f68ab8a6279', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-7ab936e6e461607a8252.py', 'sha256': '377dfdadd2b3bb510c31d853276da0c7ddb5efa7b615cf06134f2d1b2bd70a3e'}, 'start_line': 477, 'end_line': 484}, {'candidate_id': 'candidate-be4caac220021ac7dd32', 'source': {'kind': 'task_candidate', 'path': 'apps/epg/sd_tasks.py', 'line': 562, 'name': 'fetch_schedules_direct_stations', 'callable': 'celery.shared_task'}, 'original_file_sha256': '98909ed0e5ecd7bbeabb27d3de993e0f5323d5198288911c1406c510762aa60d', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-be4caac220021ac7dd32.py', 'sha256': 'be95ebeaf76392b96252637485ae16a6e2a10312f7b74aa643b34f369f058e99'}, 'start_line': 562, 'end_line': 574}, {'candidate_id': 'candidate-adb99cdd2131aca35047', 'source': {'kind': 'task_candidate', 'path': 'apps/m3u/tasks.py', 'line': 724, 'name': 'refresh_m3u_accounts', 'callable': 'celery.shared_task'}, 'original_file_sha256': '921e0fea7273c0ce7d1b1380a4f675718fbc5cdb3bb0d38005cdae2b73cce981', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-adb99cdd2131aca35047.py', 'sha256': 'df7f7f9c37f613c3b44e320c4a2009ad8fbe6d3c5e9795ea188e433a9ab2dd6c'}, 'start_line': 724, 'end_line': 735}, {'candidate_id': 'candidate-d3f0c1a5a06af4e695a5', 'source': {'kind': 'management_command', 'path': 'core/management/commands/reset_network_access.py', 'line': 7, 'name': 'Command'}, 'original_file_sha256': '942c8005aa5450a358b055ab21c232b6606ef5614eab76a9ec403189938b9df0', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-d3f0c1a5a06af4e695a5.py', 'sha256': '5e9237b7b85a1edbf23cee6ef2d63df952390bdf1768a3bf4ac2cf1c9017cb4c'}, 'start_line': 7, 'end_line': 13}, {'candidate_id': 'candidate-62ce30c3036ba0be3f4f', 'source': {'kind': 'management_command', 'path': 'core/management/commands/reset_user_network.py', 'line': 5, 'name': 'Command'}, 'original_file_sha256': '684d699f6ae0eb78d02ae1c5d945c98062092791b09cdc3f4ae7b7185bde41c0', 'slice': {'path': 'scripts/rust-contracts/retained-task-operator7/definitions/candidate-62ce30c3036ba0be3f4f.py', 'sha256': '56e84af9ac791bf05e8fd718366dd830c09e712399ede0c8e2a660eec361e29a'}, 'start_line': 5, 'end_line': 22}]
BINDING_PATH=BASE+'bindings.json'
def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()).hexdigest()
def wire_digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True).encode()).hexdigest()
def checked(root,ref):
 p=Path(root)/ref['path'];assert p.is_file(),'retained Original authority absent'
 assert hashlib.sha256(p.read_bytes()).hexdigest()==ref['sha256'],'exact retained Original authority bytes'
 return json.loads(p.read_text())if p.suffix=='.json'else p.read_text()
def dependencies():return [BINDING_PATH]+[r['path']for r in AUTHORITIES]
def uuid4(value):assert isinstance(value,str)and str(uuid.UUID(value))==value and uuid.UUID(value).version==4,'actual UUIDv4'
def receipt(value):assert value['actual_exit']==0 and value['failure']is None and value['stdout_EOF']is True and value['stderr_EOF']is True and value['process_group_settled']is True,'actual Source terminal'
def unpack(pack):
 groups={}
 for group,rows in pack.items():
  groups[group]={}
  for key,ref in rows.items():
   raw=base64.b64decode(ref['bytes_base64'],validate=True);assert hashlib.sha256(raw).hexdigest()==ref['sha256'],'exact copied Source authority'
   groups[group][key]=json.loads(raw)if ref['original_path'].endswith('.json')else raw.decode()
 return groups

def validate_task_observation(row,name):
 f=row['frame'];b=row['backend'];h=f['headers'];uuid4(h['id'])
 assert h['task']==name and h['id']==h['root_id']==b['task_id']and h['parent_id']is None,'actual task/root/backend identity'
 assert f['content-type']=='application/json'and f['content-encoding']=='utf-8'and f['properties']['correlation_id']==h['id']
 assert h['timelimit']==[None,None]and h['ignore_result']is False and type(h['retries'])is int and h['retries']==0
 body=json.loads(base64.b64decode(f['body'],validate=True));assert type(body)is list and len(body)==3 and type(body[0])is list and type(body[1])is dict and type(body[2])is dict
 assert set(body[2])=={'callbacks','errbacks','chain','chord'}and all(v is None for v in body[2].values())
 assert b['status']in ['SUCCESS','FAILURE']and isinstance(b['date_done'],str)
 if b['status']=='FAILURE':assert isinstance(b['traceback'],str)and b['traceback'],'actual observed failure, not synthetic SUCCESS'
 else:assert b['traceback']is None
 return {'task_id':h['id'],'actual_frame':f,'actual_backend':b,'decoded_arguments':body[0],'decoded_kwargs':body[1],'received_signal_retained':False,'raw_backend_wire_retained':False,'task_name':name,'full_metadata_and_result_preserved':True}

def derive(root):
 root=Path(root);values={r['path']:checked(root,r)for r in AUTHORITIES};observed=values[BASE+'observations.json'];definitions=values[BASE+'definitions.json'];pack=values[BASE+'fixture-authorities.json'];a=unpack(pack);provenance=values[BASE+'private-provenance.json'];operator=values[BASE+'operator-Original-source.json']
 assert [r['candidate_id']for r in definitions]==[r['candidate_id']for r in IDENTITIES]
 for d,expected in zip(definitions,IDENTITIES):
  assert d==expected,'exact seven original source identities/definitions'
  text=values[d['slice']['path']];tree=ast.parse(text);node=next(x for x in tree.body if isinstance(x,(ast.FunctionDef,ast.ClassDef))and x.name==d['source']['name']);assert min([node.lineno]+[x.lineno for x in getattr(node,'decorator_list',[])])==d['source']['line']and node.end_lineno==d['end_line']
  if d['source']['kind']=='task_candidate':assert any('shared_task'in ast.unparse(x)for x in node.decorator_list)
  else:assert d['source']['kind']=='management_command'and any(ast.unparse(x)=='BaseCommand'for x in node.bases)
 assert a['language']['qualification']['Source_category_language_capture_qualified']and a['language']['qualification']['actual_controller_exit']==0 and a['language']['qualification']['actual_Source_cases_passed']==11
 assert a['language']['identity']['Original_git_revision']==ORIGINAL and a['language']['identity']['Original_archive_sha256']==ARCHIVE and a['language']['identity']['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'
 assert a['language']['cleanup']['passed']and a['language']['provenance']['Source_task_bodies_invoked_by_observer']is False
 assert a['matching']['qualification']['actual_exit']==0 and a['matching']['qualification']['actual_cleanup_passed']and a['matching']['qualification']['natural_HTTP_cases']==14 and a['matching']['qualification']['actual_model_inference_qualified']is False
 assert a['stations']['identity']['Original_revision']==ORIGINAL and a['stations']['report']['Source_natural_HTTP_qualified']and a['stations']['report']['status']=='passed'
 assert a['aggregate']['qualification']['Original_revision']==ORIGINAL and a['aggregate']['qualification']['actual_exit']==0 and a['aggregate']['qualification']['actual_cleanup_passed']and a['aggregate']['qualification']['actual_Source_phases_passed']==4
 for group in ['language','matching','stations','aggregate']:receipt(a[group]['execution'])
 old=a['operator']['existing_public_proof'];integration=a['operator']['actual_Source_integration'];assert old['ordinary_source']['sha256']==pack['operator']['actual_Source_integration']['sha256']and old['ordinary_source']['cases']==17
 assert integration['passed']and integration['original_revision']==ORIGINAL and integration['original_archive_sha256']==ARCHIVE and integration['default_database_unchanged']and integration['cleanup']=='project removed'and integration['resources_retained']is False
 assert integration['source_report_sha256']==next(x['sha256']for x in AUTHORITIES if x['path']==BASE+'operator-Original-source.json')
 for key in ['scenario_setup','entrypoint']:assert integration['tool_source_sha256'][pack['operator'][key]['original_path']]==pack['operator'][key]['sha256']
 assert operator['passed']and operator['native_verified']is False and operator['performance_verified']is False and operator['destructive_commands_executed']is False and len(operator['cases'])==17
 fixtures={}
 for group in ['language','matching','stations','aggregate','operator']:
  fixtures[group]={'id':group,'baseline_revision':ORIGINAL,'baseline_archive_sha256':ARCHIVE,'execution_kind':'historical_actual_Original_capture_offline_assertion_replay','fixture_authorities':{k:{'original_path':v['original_path'],'sha256':v['sha256']}for k,v in pack[group].items()},'Source_replayed_for_mapping':False,'ownership':'Existing isolated Compose copied database and owned Redis/provider/Channels namespaces; actual cleanup and any default-database audit retained in exact Source authority. No production data/credentials.','observed_actor':({'kind':'owned_fixture_command_process_database_owner','HTTP_role_not_applicable':True}if group=='operator'else{'producer':'actual authenticated fixture admin HTTP','executor':'actual Original Celery worker in owned fixture','complete_role_matrix':False,'login_flow_qualified':False}),'setup_assertions':'Exact named case presets/raw frames/backend outputs below plus existing qualified Source setup, not a module-level test association.'}
 contracts=[];seen=set()
 for d in definitions:
  kind=d['source']['kind'];name=d['source']['name'];full=d['source']['path'].removesuffix('.py').replace('/','.')+'.'+name
  node=next(x for x in ast.parse(values[d['slice']['path']]).body if isinstance(x,(ast.FunctionDef,ast.ClassDef)))
  explicit=[kw.value.value for dec in getattr(node,'decorator_list',[])if isinstance(dec,ast.Call)for kw in dec.keywords if kw.arg=='name'and isinstance(kw.value,ast.Constant)and isinstance(kw.value.value,str)];assert len(explicit)<=1
  if explicit:full=explicit[0]
  group={'refresh_vod_content':'language','match_selected_channels_epg':'matching','match_single_channel_epg':'matching','fetch_schedules_direct_stations':'stations','refresh_m3u_accounts':'aggregate'}.get(name,'operator')
  if kind=='task_candidate':
   rows=[x for x in observed[group]if x['frame']['headers']['task']==full];assert len(rows)=={'language':11,'stations':4,'aggregate':4}.get(group,5 if name=='match_selected_channels_epg'else 7)
   cases=[]
   for x in rows:
    case=validate_task_observation(x,full);assert case['task_id']not in seen;seen.add(case['task_id']);case.update(case_name=x['case_name'],actual_Source_case=x['actual_Source_case'],retained_projection_sha256=digest(x))
    if group=='language':
     actual=x['actual_Source_case'];assert actual['status']=='passed'and actual['actual_root']==case['task_id']and actual['actual_queued_children']==0;assert x['producer_HTTP']['method']=='POST'and x['producer_HTTP']['credential_role']=='admin'and x['producer_HTTP']['status']==202;assert x['producer_HTTP']['path'].endswith('/refresh-vod/');assert len(case['decoded_arguments'])==1 and type(case['decoded_arguments'][0])is int and case['decoded_kwargs']=={};assert x['backend']['status']=='SUCCESS'and x['backend']['children']==[]
     for key in ['frame','backend','HTTP']:assert x['raw_member_sha256'][key]==a['language']['provenance']['private_capture_files'][x['case_name']+'/'+key+'.json']
    elif group=='matching':
     actual=a['matching']['cases']['cases'][x['source_index']];assert x['actual_Source_case']==actual and actual['root_task']==full and actual['status']=='passed'and actual['actual_HTTP_status']==202
     assert wire_digest(x['frame'])==actual['root_frame_sha256']and wire_digest(x['backend']['result'])==actual['backend_result_sha256']and x['backend']['status']==actual['backend_status']
     case['setup']=x['setup'];case['actual_HTTP']=x['producer_HTTP'];case['scope_limit']='Original DISABLE_ML_DOWNLOADS=true/no MiniLM cache; supplied vector decisions are not actual model inference. Single error result versus bulk FAILURE preserved.'
    elif group=='stations':
     item=x['actual_Source_case'];assert item['actual_root_task']==full and item['backend_status']==x['backend']['status']=='SUCCESS'and item['backend_result']is x['backend']['result']is None;assert item['source_dataset']=='Source53'and item['actual_ETA_present']==(x['frame']['headers']['eta']is not None);assert x['backend']['children']==[]
    else:
     actual=a['aggregate']['cases']['cases'][x['source_index']];assert x['actual_Source_case']==actual and actual['status']=='passed'and actual['actual_HTTP_status']==202 and x['actual_root']==case['task_id'];assert case['decoded_arguments']==[]and case['decoded_kwargs']=={}
     assert x['backend']['status']==actual['actual_parent_backend_status']=='SUCCESS'and x['backend']['result']==actual['actual_parent_result']and len(x['actual_direct_child_ids'])==actual['actual_direct_children']and actual['actual_root_parent_and_child_lineage_assertions_passed']
    cases.append(case)
   assertions=['Exact actual broker task/name/UUID/root/correlation and backend task identity, decoded arguments and declared producer role join','Exact retained observed SUCCESS/FAILURE/result/traceback/date/children values; no raw backend wire or received-signal invention','Existing scoped case and setup/HTTP/effect authority hashes agree; local/caught-error/empty/ETA branches stay distinct']
   result={'state':'partial_historical_Original_task_observations','actual_worker_observations':len(cases),'received_signals':0,'raw_backend_wire_asserted':False,'new_Source_cases_this_task':0,'Native_coverage_increased':False,'complete_role_matrix':False,'fully_verified':False}
  else:
   cases=[x for x in operator['cases']if x['arguments'][0]==Path(d['source']['path']).stem];assert len(cases)==(6 if name=='Command'and'reset_network_access'in d['source']['path']else 11)
   for x in cases:
    assert type(x['exit_code'])is int and x['exit_code']in [0,1,2]
    for key in ['stdout','stderr']:assert hashlib.sha256(x[key].encode()).hexdigest()==x[key+'_sha256']
    assert set(x['before'])==set(x['after'])=={'notification_sequence','network_identity','network','users','notifications','user_timestamps','cache','cache_ttls'}
   assertions=['Actual Original manage.py argv and exact stdout/stderr SHA/exit including expected exits1/2, not native comparison cases','Exact retained before/after network/user/date/cache/TTL/notification/sequence fields and ordered Channels startup/command effects','Source copied database and two-safe-command guard; destructive commands absent; Django global grammar and unobserved flags stay out of scope']
   result={'state':'partial_historical_Original_management_subprocess_observations','actual_command_subprocesses':len(cases),'actual_case_exit_counts':{str(code):sum(x['exit_code']==code for x in cases)for code in [0,1,2]},'new_Source_cases_this_task':0,'Native_coverage_increased':False,'HTTP_roles_not_applicable':True,'fully_verified':False}
  contracts.append({'candidate_id':d['candidate_id'],'source':d['source'],'owning_milestone':'M6'if kind=='task_candidate'else'M8','definition':{'path':d['slice']['path'],'original_file_sha256':d['original_file_sha256'],'slice_sha256':d['slice']['sha256'],'start_line':d['start_line'],'end_line':d['end_line'],'ast_sha256':ast_fingerprint(next(x for x in ast.parse(values[d['slice']['path']]).body if isinstance(x,(ast.FunctionDef,ast.ClassDef))))},'task_name':full if kind=='task_candidate'else None,'fixture_ids':[group],'observed_actor':fixtures[group]['observed_actor'],'observations':cases,'assertions':assertions,'result':result,'fully_verified':False,'remaining':['Only exact retained Source cases/roles/input domain, not a complete role or grammar matrix','No Native acceptance, HTTP format alias, generic ACK/expiry/revocation/concurrency or full milestone/performance claim','Inline decorated helpers are not inferred to have executed as queued children; received signals/raw backend wire absent where not captured']})
 return {'format_version':1,'kind':KIND,'Original_revision':ORIGINAL,'Original_archive_sha256':ARCHIVE,'credit':'Original Dispatcharr and dependency authors retain all application/protocol/algorithm credit and AGPL.','authorities':AUTHORITIES,'contracts':[],'non_HTTP_contracts':contracts,'fixtures':fixtures,'actual_Original_worker_observations':31,'actual_Original_command_subprocesses':17,'new_Source_execution':False,'Native_coverage_increased':False,'fully_verified_added':0}

def validate(root,value,bindings=None):
 assert digest(value)==digest(derive(root)),'exact seven retained Original task/operator definitions, fixtures, roles, assertions and observations'
 return {'nonHTTP_assignments':7,'actual_Original_worker_observations':31,'actual_Original_command_subprocesses':17,'new_Source_execution':False,'fully_verified_added':0}
