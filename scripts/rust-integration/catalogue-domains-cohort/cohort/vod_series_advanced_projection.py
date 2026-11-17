"""Safe full-row Source4 projection; original Dispatcharr authors retain credit.

Only fixture credentials/clocks, causal generated clocks/new Series UUID and
root duration/receiver identifiers are abstracted. All11 sequences remain exact.
"""
import copy,datetime,json,re,uuid
from pathlib import Path
PHASES=['sparse-list-preserves-detail-and-Episodes','advanced-catalogue-repeat','prune-cascades-detail-Episodes-and-streams','yearless-list-reassigns-and-cascades-old-details']
CLOCKS={'created_at','updated_at','last_seen','last_advanced_refresh','last_episode_refresh','timestamp'}
def instant(value):return datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
def duration(value):return re.sub(r'\d+\.\d{2} seconds','<duration> seconds',value)if isinstance(value,str)else value
def safe_source_capture(value):
 """Public raw Source rows retain clocks/IDs; remove synthetic login values."""
 result=copy.deepcopy(value)
 for row in result.get('m3u_m3uaccount',[]):
  for key in ['username','password']:
   if row.get(key)is not None:row[key]='<fixture-credential>'
 return result
def tables(value):return {**{k:v for k,v in value.items()if isinstance(v,list)},**value['support']}
def temporal_effects(before,after):
 result={}
 old=tables(before);new=tables(after)
 for name,rows in new.items():
  prior={r['id']:r for r in old[name]}
  for row in rows:
   for key in CLOCKS&row.keys():
    value=row[key]
    if row['id']not in prior:effect='created'
    elif value==prior[row['id']].get(key):effect='unchanged'
    else:effect='changed'
    result[name+'/'+str(row['id'])+'/'+key]=effect
 return result
def phase_projection(directory,label):
 p=Path(directory)/label;before=json.loads((p/'SQL-before.json').read_text());after=json.loads((p/'SQL-after.json').read_text())
 replacements={}
 # Native and Source install exactly the same old rows from retained205.
 # Only the yearless replacement Series gets a new UUID/creation clocks.
 new_series={r['id']:r for r in after['vod_series']if r['id']not in {r['id']for r in before['vod_series']}}
 for row in new_series.values():
  assert uuid.UUID(row['uuid']).version==4
  replacements[row['uuid']]='<new-Series-UUIDv4>'
  for key in ['created_at','updated_at']:
   assert instant(before['observed_at'])<=instant(row[key])<=instant(after['observed_at'])
   replacements[row[key]]='<causal-created-clock>';replacements[row[key].replace('+00:00','Z')]='<causal-created-clock>'
 # Every changed existing support clock is independently causal; unchanged
 # support clocks must remain unchanged as encoded in temporal_effects.
 for table in ['vod_vodcategory','vod_m3uvodcategoryrelation']:
  old={r['id']:r for r in before['support'][table]}
  for row in after['support'][table]:
   for key in ['created_at','updated_at']:
    if row['id']not in old or row[key]!=old[row['id']][key]:
     assert instant(before['observed_at'])<=instant(row[key])<=instant(after['observed_at'])
 def clean(value):
  if isinstance(value,str):return replacements.get(value,value)
  if isinstance(value,list):return[clean(v)for v in value]
  if isinstance(value,dict):return{k:clean(v)for k,v in value.items()}
  return value
 def sql(value):
  value=safe_source_capture(value);value.pop('observed_at')
  for row in value['m3u_m3uaccount']:
   for key in ['created_at','updated_at']:
    if row[key]is not None:row[key]='<fixture-account-clock>'
  for table in ['vod_vodcategory','vod_m3uvodcategoryrelation']:
   for row in value['support'][table]:
    for key in ['created_at','updated_at']:
     if row[key]is not None:row[key]='<fixture-or-causal-support-clock>'
  for row in value['vod_m3useriesrelation']:
   if row['last_seen']is not None:row['last_seen']='<seed-or-causal-last-seen>'
  return clean(value)
 trace=json.loads((p/'Origin.json').read_text());requests=[]
 for row in trace:
  if 'path'not in row:continue
  row={k:v for k,v in row.items()if k!='epoch'}
  for key in ['username','password']:
   if key in row.get('query_sha256',{}):row['query_sha256'][key]=['<owned-credential-sha256>']
  requests.append(row)
 ws=json.loads((p/'WS.json').read_text());receiver=ws['actual_receiver'];replacements[receiver]='<owned-update-receiver>'
 backend=json.loads((p/'backend.json').read_text())
 events=copy.deepcopy(ws['events'])
 for event in events:
  if isinstance(event.get('data'),dict)and isinstance(event['data'].get('message'),str)and event['data']['message'].startswith('VOD refresh completed in '):event['data']['message']=duration(event['data']['message'])
 return {'before':sql(before),'after':sql(after),'clock_effects':temporal_effects(before,after),'HTTP':clean(json.loads((p/'HTTP.json').read_text())),'catalogue':clean(json.loads((p/'catalogue.json').read_text())),'provider':requests,'provider_errors':[row for row in trace if row.get('kind')=='actual_transport_error'],'ordered_WS':clean(events),'backend':{'status':backend['status'],'result':duration(backend['result']),'children':backend['children']}}
def phase_chain(directory):
 snapshots=[]
 for label in PHASES:
  p=Path(directory)/label
  snapshots.append((json.loads((p/'SQL-before.json').read_text()),json.loads((p/'SQL-after.json').read_text())))
 def state(value):
  value=copy.deepcopy(value);value.pop('observed_at');return value
 assert state(snapshots[1][0])==state(snapshots[0][1]),'raw phase1 after to phase2 before stability'
 assert state(snapshots[2][0])==state(snapshots[1][1]),'raw phase2 after to phase3 before stability'
 # The fourth branch explicitly reinstalls retained205 rows. Only its four
 # selected entity tables and four selected sequences can be reset.
 left=state(snapshots[2][1]);right=state(snapshots[3][0])
 for table in ['vod_series','vod_episode','vod_m3useriesrelation','vod_m3uepisoderelation']:
  left.pop(table);right.pop(table)
  name='public.'+table+'_id_seq';left['sequences'].pop(name);right['sequences'].pop(name)
 assert left==right,'independent branch preserves accounts/support/other sequence state'
 return {'phase1_to_phase2':'exact_raw_state','phase2_to_phase3':'exact_raw_state','phase3_to_phase4':'only_declared_four_tables_and_sequences_reseeded'}
def project(directory):
 chain=phase_chain(directory)
 return {'phase_chain':chain,'phases':{label:phase_projection(directory,label)for label in PHASES}}

