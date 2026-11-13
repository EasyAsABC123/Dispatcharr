"""Explicit Source32 privacy projection; never re-executes Source or substitutes raw data."""
import copy,hashlib,json
from pathlib import Path

def evidence(raw):return {'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}
def canon(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def project(raw):
 d=copy.deepcopy(raw);rules=[]
 def hide(value,kind,path):
  e=evidence(canon(value));rules.append({'path':path,'kind':kind,'original_JSON':e});return {'projection':kind,**e}
 def user(row,path):
  out=copy.deepcopy(row)
  for key in ['api_key','username','password']:
   if key in out:
    original=out[key];out[key]=hide(original,'credential-or-derivation-field',path+'.'+key)
    if key=='api_key':out[key]['UTF8']=evidence(original.encode())
  return out
 def redis(value,path):
  out={k:copy.deepcopy(v)for k,v in value.items()if k!='rows'};out['projection']='raw Redis content evidence only; not executable Redis bytes';out['rows']=[]
  for i,row in enumerate(value['rows']):
   q={'key_sha256':hashlib.sha256(bytes.fromhex(row['key_hex'])).hexdigest(),'type':row['type'],'pttl':row['pttl'],'original_record':evidence(canon({k:v for k,v in row.items()if k!='pttl'}))}
   key=bytes.fromhex(row['key_hex'])
   if key.startswith(b':1:coresettings:')or key==b'asgi:group:updates':q['key_literal_UTF8']=key.decode()
   rules.append({'path':path+'.rows['+str(i)+']','kind':'opaque Redis serialized key/value evidence','original_JSON':evidence(canon(row))});out['rows'].append(q)
  return out
 def walk(value,path='$'):
  if isinstance(value,list):return[walk(v,path+'['+str(i)+']')for i,v in enumerate(value)]
  if not isinstance(value,dict):return value
  if {'audit','public_manifest','settings','users','named_cache','raw_Redis','Channels'}<=set(value):
   out=copy.deepcopy(value);out['users']=[user(v,path+'.users['+str(i)+']')for i,v in enumerate(value['users'])];out['cache']=redis(value['cache'],path+'.cache');out['raw_Redis']=[redis(v,path+'.raw_Redis['+str(i)+']')for i,v in enumerate(value['raw_Redis'])]
   channels=value['Channels'];out['Channels']={'projection':'non-consuming mailbox coverage/evidence only; no delivery acceptance','Redis_database':channels['Redis_database'],'mailbox_type':channels['mailbox_type'],'mailbox_pttl':channels['mailbox_pttl'],'frame_count':len(channels['frames']),'frames':[{'raw':evidence(bytes.fromhex(r['raw_hex'])),'score':r['score'],'score_hex':r['score_hex'],'decoded_semantic_fields':{k:v for k,v in r['actual_layer_decoded'].items()if k!='__asgi_channel__'},'omitted_receiver_field':hide(r['actual_layer_decoded'].get('__asgi_channel__'),'owned receiver transport identity',path+'.Channels.frames['+str(i)+'].__asgi_channel__')}for i,r in enumerate(channels['frames'])],'receive_pop_or_ack_calls':channels['receive_pop_or_ack_calls'],'unknown_or_setup_frames_consumed':channels['unknown_or_setup_frames_consumed'],'owned_receiver':hide(channels['receiver'],'owned receiver derivation material',path+'.Channels.receiver')}
   audit=copy.deepcopy(value['audit']);audit['database']=hide(audit['database'],'owned database identity',path+'.audit.database')
   for table,row in audit['tables'].items():
    for key in ['raw_rows','raw_json_text']:
     if table=='accounts_user'and key in row:row[key]=hide(row[key],'SQL rows evidence; original fingerprints retained',path+'.audit.tables.'+table+'.'+key)
   out['audit']=audit;assert value['events']==[]
   out['events']=[];return out
  out={}
  for key,item in value.items():
   loc=path+'.'+key
   if key=='raw_headers_private':out[key]={k:{'projection':'authentication header byte evidence',**evidence(bytes.fromhex(v))}for k,v in item.items()}
   elif key=='database':out[key]=hide(item,'owned derivation identity',loc)
   elif key=='receiver_fixture_context':out[key]={k:(hide(v,'receiver transport derivation material',loc+'.'+k)if k in ['receiver','channel_key_hex']else v)for k,v in item.items()}
   elif key in ['initial_Redis_before_receiver','final_Redis_after_receiver_cleanup']:out[key]=[redis(v,loc+'['+str(i)+']')for i,v in enumerate(item)]
   else:out[key]=walk(item,loc)
  return out
 d=walk(d);d['public_projection']={'mode':'strict finite structural projection; not executed','original_canonical_JSON':evidence(canon(raw)),'SQL_fingerprints':'original fingerprints unchanged; accounts_user raw SQL text/rows omitted; harmless other named rows retained','credentials':'API-key/header/username/password/database/receiver and serialized cache/Redis/event content omitted to evidence objects'}
 return d,rules
