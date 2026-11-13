"""Synthetic-only corruption inputs from public historical projection. Never app evidence."""
import copy,json
from pathlib import Path
import validate_native as V
ROOT=Path(__file__).resolve().parents[1]
def native():
 d=json.loads((ROOT/'inputs/historical-Native19-projection.json').read_text());d['_synthetic_only']=True
 nonce=d['database'].removeprefix('rust_probe_notifforms_native_');roles={r['id']:r['alias']for r in V.PLAN['roles']}
 def credentials(x):
  if type(x)is list:
   for v in x:credentials(v)
  elif type(x)is dict:
   if 'api_key'in x:x['api_key']='native-forms-api-'+nonce+'-'+roles[x['id']]
   if 'raw_headers_private'in x:x['raw_headers_private']={}if x['actor']=='anonymous'else{'HTTP_X_API_KEY':('native-forms-api-'+nonce+'-'+x['actor']).encode().hex()}
   for v in x.values():credentials(v)
 credentials(d)
 def cache(warm):
  rows=copy.deepcopy(V.SOURCE['requests'][0]['after']['cache']['rows']if warm else V.SOURCE['seed']['cache']['rows']);named={k:{'raw_GET_hex':None,'decoded':None}for k in(V.GROUP,V.SYS,V.VERSION)}
  for row in rows:
   key=bytes.fromhex(row['key_hex']).decode();raw=bytes.fromhex(row['value_hex']);named[key]={'raw_GET_hex':raw.hex(),'decoded':int(raw)if key==V.VERSION else V.literal_pickle(raw)}
  return {'named_decoded':named,'rows':[{k:v for k,v in r.items()if k in('key_hex','dump_hex','pttl')}for r in rows],'api_key_failed_auth_history':None,'api_key_failed_auth_raw_hex':None}
 def attach(row,side,warm):
  row[side+'_cache']=cache(warm);raw=row[side+'_raw_Redis'];source_rows=V.SOURCE['requests'][0]['after']['cache']['rows']if warm else V.SOURCE['seed']['cache']['rows'];raw[1]['rows']=copy.deepcopy(source_rows)
 d['natural_cache_before_explicit_import']=copy.deepcopy(d['cache_after_seed']);d['imported_Source_existing_state']=copy.deepcopy(V.SOURCE['seed']['cache']['rows']);d['Native_network_save_signal_executed']=False;d['cache_after_seed']=cache(False)
 for i,row in enumerate(d['requests']):attach(row,'before',i!=0);attach(row,'after',True)
 for row in d['Native_transport_diagnostics']:attach(row,'before',True);attach(row,'after',True)
 d['final_cache']=cache(True);d['final_raw_Redis'][1]['rows']=copy.deepcopy(V.SOURCE['requests'][0]['after']['cache']['rows'])
 return d
