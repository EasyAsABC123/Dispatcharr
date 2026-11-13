"""Pure exact Source pre-request cache context and measured request1 delta."""
BASE={':1:coresettings:groupver:network_access','core:version_check_dispatch_lock','dvr:recover_dispatch_lock'}
ADDED={':1:coresettings:group:network_access',':1:coresettings:group:system_settings'}
def require(v,label):
 if not v:raise ValueError(label)
def strings(state):
 scope=state['redis'][0];require(scope['database']==1,'Cache1');rows={bytes.fromhex(x['key_hex']).decode():x for x in scope['rows']};require(len(rows)==len(scope['rows'])and all(x['type']=='string'for x in rows.values()),'ExactUniqueStrings');return rows
def validate_source(source):
 seed=strings(source['seed_snapshot']);before=strings(source['requests'][0]['before']);after=strings(source['requests'][0]['after']);require(set(seed)==set(before)==BASE and set(after)==BASE|ADDED,'MeasuredThreeBeforeFiveAfterNotPrefilledFive')
 for name,row in seed.items():
  require(row['value_hex']==before[name]['value_hex']==after[name]['value_hex']and row['dump_hex']==before[name]['dump_hex']==after[name]['dump_hex'],'ExactInitialContextValue')
  if name.endswith('groupver:network_access'):require(bytes.fromhex(row['value_hex']).decode().isdigit()and int(bytes.fromhex(row['value_hex']))>0 and row['pttl']==before[name]['pttl']==after[name]['pttl']==-1,'PersistentTimeNSVersionTTL')
  else:require(row['value_hex']=='31'and all(0<x<=300000 for x in(row['pttl'],before[name]['pttl'],after[name]['pttl'])),'FiniteLockTTL')
 return {'passed':True,'before':3,'after_request1':5,'persistent':1,'finite':2,'prefill_after_effects':False}
