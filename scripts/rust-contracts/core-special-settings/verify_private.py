"""Strict finite Source51/Native51 privacy projection, private curator only.
No app, Source call or mutable fallback. Actual secrets are inventoried first;
public API body bytes are retained only when all actual credentials/nonce
plain/hex/base64 representations are absent. Opaque predicates are disclosed.
"""
import base64,hashlib,json
from pathlib import Path
HERE=Path(__file__).resolve().parent
RAW_ROOT=None
GROUPS=['system_settings','network_access','reverse_proxy_auth','proxy_settings','stream_settings','dvr_settings']
EPOCHS={'Source51':'m5-core-special-source-1','Native-first-HOST1':'m5-core-special-native-1','Native-corrected':'m5-core-special-native-2'}
sha=lambda b:hashlib.sha256(b).hexdigest()
SCHEMAS={}
def opaque(value):
 raw=json.dumps(value,sort_keys=True,separators=(',',':')).encode();return {'private_opaque_sha256':sha(raw),'public_recomputation':False}
def local(value):return {k:value[k]for k in ['display_zone','process_proxy_cache','owned_log_files']}
def tasks(snapshot):
 result=[]
 for row in snapshot['broker']['frames']:
  body=row['decoded_body'];kwargs=dict(body[1]);token=kwargs.pop('preacquired_lock_token',None)
  if token is not None:kwargs['preacquired_lock_token']={'private_token_sha256':sha(token.encode()),'literal_omitted':True}
  result.append({'raw_frame_sha256':row['raw_frame_sha256'],'database':row['database'],'queue_key_hex':row['queue_key_hex'],'task':row['headers']['task'],'args':body[0],'kwargs':kwargs,'raw_private_frame_not_publicly_decoded':True})
 return result
def channels(snapshot):
 result=[]
 for row in snapshot['Channels']['frames']:
  value={k:v for k,v in row['actual_layer_decoded'].items()if k!='__asgi_channel__'}
  result.append({'raw_frame_sha256':row['raw_sha256'],'decoded_safe_envelope':value,'receiver_digests':[sha(s.encode())for s in row['actual_layer_decoded'].get('__asgi_channel__',[])],'private_receiver_names_and_raw_frame_omitted':True})
 return result
def raw_effects(snapshot):
 locks=[];conditions=[];ua=[]
 for db in snapshot['raw_Redis']:
  for row in db['rows']:
   key=bytes.fromhex(row['key_hex'])
   if key.startswith(b'task_lock_refresh_single_m3u_account_'):
    locks.append({'database':db['database'],'key':key.decode(),'type':row['type'],'private_value_sha256':sha(bytes.fromhex(row['value_hex'])),'pttl':row['pttl']})
   elif key.startswith(b':1:dev_notif_condition_'):
    conditions.append({'database':db['database'],'actual_key_sha256':sha(key),'owned_key_template':':1:dev_notif_condition_owned_special_{PRIVATE_NONCE}','type':row['type'],'value_hex':row['value_hex'],'pttl':row['pttl'],'actual_key_private':True})
   elif key==b':1:coresettings:default_user_agent':
    ua.append({'database':db['database'],'key':key.decode(),'type':row['type'],'value_hex':row['value_hex'],'pttl':row['pttl']})
 return {'locks':locks,'conditions':conditions,'default_UA':ua}
def snapshot(v):
 full=v['SQL']['full6259'];schema={k:full[k]for k in ['columns','indexes','constraints']};digest=sha(json.dumps(schema,sort_keys=True,separators=(',',':')).encode());SCHEMAS[digest]=schema
 return {'SQL':{'full6259':{'tables':full['tables'],'sequences':full['sequences'],'schema_sha256':digest},'settings':v['SQL']['settings'],'notices':v['SQL']['notices'],'events':v['SQL']['events'],'user_rows_private':opaque(v['SQL']['users'])},'named_group_cache':v['named_group_cache'],'local':local(v['local']),'broker_frames':tasks(v),'Channels_frames':channels(v),'raw_effects':raw_effects(v)}
def case(v):
 fields=['id','name','group','role','method','path','request_body_hex','body_sha256','status','response_body_hex','response_body_bytes','response_body_sha256','headers','began_utc','completed_utc','began_epoch','completed_epoch']
 out={k:v[k]for k in fields if k in v};out['source_ip']=v.get('actual_TCP_source_ip',v.get('REMOTE_ADDR'));out['duration_seconds']=v['completed_monotonic']-v['began_monotonic'];out['before']=snapshot(v['before']);out['after']=snapshot(v['after']);out['private_headers_and_actor_SQL_rows_omitted']=True
 if'physical_close'in v:out['physical_close']={k:v['physical_close'][k]for k in ['fd_after','original_inode_absent','timer_joined']}
 return out
def control(v):
 fields=['path','status','actual_request_attempted','response_body_hex','response_body_bytes','response_body_sha256','json']
 out={k:v[k]for k in fields};out['physical_close']={k:v['physical_close'][k]for k in ['fd_after','original_inode_absent','timer_joined']}
 out['exception']=v.get('exception');out['close_exception']=v.get('close_exception');return out
def project():
 secrets=set();nonces=set();loaded={};origins=[]
 for epoch,folder in EPOCHS.items():
  loaded[epoch]=[]
  for group in GROUPS:
   path=RAW_ROOT/folder/(group+('-source-private.json'if epoch=='Source51'else'-native-private.json'));raw=path.read_bytes();value=json.loads(raw);nonce=value['database'].split('_')[2];nonces.add(nonce.encode())
   for row in value['requests']:
    for key,hex_value in row.get('private_header_hex',{}).items():
     if'api'in key.lower()or'authorization'in key.lower():secrets.add(bytes.fromhex(hex_value))
    for user in row['before']['SQL']['users']:
     if user.get('api_key'):secrets.add(user['api_key'].encode())
   loaded[epoch].append(value);origins.append({'epoch':epoch,'group':group,'private_raw_sha256':sha(raw),'bytes':len(raw)})
 needles=set()
 for value in secrets|nonces:
  needles.update([value,value.hex().encode(),base64.b64encode(value)])
 output={'scope':'Projected normal Source51/Native51 component evidence only; full3520 raw predicates require private verification','groups':GROUPS,'origins':origins,'epochs':{},'opaque_boundaries':['Raw user/header/nonce/receiver/parent cue/argv/PID content omitted','Raw Redis broker and Channels frames omitted; semantic frames/hash/equality relationships retained','Full SQL table digests retained, actual table rows not public','No public full3520 raw codec/auth/process recomputation claim']}
 for epoch,cohorts in loaded.items():
  output['epochs'][epoch]=[]
  for value in cohorts:
   out={'group':value['group'],'raw_recorder_passed':value['passed'],'capture_valid':value['capture_valid'],'cases':[case(c)for c in value['requests']]}
   if epoch!='Source51':out['controls']=[control(v)for v in value['control_requests']]
   output['epochs'][epoch].append(out)
 output['schema_catalog']=SCHEMAS
 raw=(json.dumps(output,indent=2,ensure_ascii=False)+'\n').encode()
 found=[sha(value)for value in needles if value and value in raw]
 if found:raise ValueError('ActualCredentialOrNonceRepresentationInProjection:'+str(len(found)))
 return output,{'actual_credential_values':len(secrets),'nonce_values':len(nonces),'representations_checked':len(needles),'matches':0,'scope':'Exact inventoried Source51/Native1/Native2 APIkey/nonce representations; not universal secret detection'}
if __name__=='__main__':
 import argparse
 parser=argparse.ArgumentParser();parser.add_argument('--raw-root',type=Path,required=True);args=parser.parse_args();RAW_ROOT=args.raw_root.resolve()
 value,privacy=project();expected=json.loads((HERE/'retained.json').read_text())
 if value!=expected:raise ValueError('PrivateProjectionMismatch')
 if privacy!=json.loads((HERE/'privacy-inventory.json').read_text()):raise ValueError('PrivacyInventoryMismatch')
 print(json.dumps({'exact_private_projection':True,'raw_cohorts':18,'requests':153,'privacy':privacy}))
