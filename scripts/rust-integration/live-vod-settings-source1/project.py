"""Typed projection of observed Source API12 across literal controller1 + controller0.
Initial clocks and login entropy normalize only after exact raw stability checks.
"""
import base64,copy,datetime,hashlib,json,re,uuid
from pathlib import Path
CLOCKS={'created_at','updated_at','last_seen','timestamp'}
OWNED={'Owned VOD ingest','Owned VOD disabled','Owned VOD non-XC'}
def load(p):return json.loads(Path(p).read_text())
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def instant(v):assert isinstance(v,str);return datetime.datetime.fromisoformat(v.replace('Z','+00:00'))
def tables(v):return {**{k:r for k,r in v.items()if k not in('support','live','observed_at','live_observed_at','sequences')},**v['support'],**v['live']}
def state(v):return {k:r for k,r in v.items()if k not in('observed_at','live_observed_at')}
class State:
 def __init__(self,first):self.first=tables(first)
 def clock(self,table,row,key,value):
  original=next(r for r in self.first[table]if r['id']==row['id']);assert value==original[key],(table,row['id'],key,'Initial raw clock must remain stable')
  if value is None:return None
  instant(value);return {'stable_setup_clock':[table,row['id'],key]}
 def credential(self,row,key,value):
  initial=next(r for r in self.first['m3u_m3uaccount']if r['id']==row['id']);assert value==initial[key],('Raw setup credential stable',row['id'],key)
  if value is None:return None
  assert isinstance(value,str)and len(value)<=255
  return {'owned_fixture_credential':[row['name'],key]}if row['name']in OWNED else{'fixture_login_sha256':hashlib.sha256(value.encode()).hexdigest()}
 def canonical(self,value):
  result=copy.deepcopy(state(value));mapping=tables(result);assert set(mapping)==set(self.first)
  for table,rows in mapping.items():
   assert [r['id']for r in rows]==[r['id']for r in self.first[table]],(table,'No persistent row creation/deletion in API12')
   for row in rows:
    for key in CLOCKS&row.keys():row[key]=self.clock(table,row,key,row[key])
    if table=='m3u_m3uaccount':
     for key in('username','password'):row[key]=self.credential(row,key,row[key])
  return result
 def get(self,envelope,after):
  result=copy.deepcopy(envelope);body=result['body'];account=next(r for r in after['m3u_m3uaccount']if r['id']==body['id'])
  for key in('username','password'):assert body[key]==account[key];body[key]=self.credential(account,key,body[key])
  for key in('created_at','updated_at'):
   assert body[key]is None and account[key]is None or instant(body[key])==instant(account[key]);body[key]=self.clock('m3u_m3uaccount',account,key,account[key])
  for group in body['channel_groups']:
   row=next(r for r in after['live']['dispatcharr_channels_channelgroupm3uaccount']if r['channel_group_id']==group['channel_group']and r['m3u_account_id']==body['id'])
   assert instant(group['last_seen'])==instant(row['last_seen']);group['last_seen']=self.clock('dispatcharr_channels_channelgroupm3uaccount',row,'last_seen',row['last_seen'])
  return result

_RECEIVER_CACHE={}
def receiver_proof(fixture,cell):
 name=load(cell/'WS.json')['actual_receiver'];match=re.fullmatch(r'specific\.aggregate_([0-9a-f]{32})!receiver',name);assert match and uuid.UUID(hex=match[1]).version==4
 if fixture not in _RECEIVER_CACHE:
  ready_rows=[]
  for path in fixture.glob('command-*.json'):
   receipt=load(path);stdout=Path(receipt['stdout'])
   if stdout.stat().st_size>4096:continue
   try:ready=json.loads(stdout.read_bytes())
   except(ValueError,UnicodeError):continue
   if isinstance(ready,dict)and set(ready)=={'actual_group_add_completed','receiver'}:ready_rows.append((path,receipt,stdout,ready))
  _RECEIVER_CACHE[fixture]=ready_rows
 matches=[row for row in _RECEIVER_CACHE[fixture]if row[3]=={'actual_group_add_completed':True,'receiver':name}]
 assert len(matches)==1;path,r,stdout,ready=matches[0];assert r['actual_exit']==0 and r['failure']is None and r['stdout_EOF']and r['stderr_EOF']and r['process_group_settled']and r['output_bytes_discarded']==0
 assert path.parent==fixture and stdout.parent==fixture/'private'and path.stem==stdout.stem
 return {'group':'updates','receiver':name,'returned':ready,'safe_receipt':{k:r[k]for k in('actual_exit','failure','stdout_EOF','stderr_EOF','process_group_settled','output_bytes_discarded','argv_sha256','stdout_sha256','stderr_sha256')},'raw_stdout_base64':base64.b64encode(stdout.read_bytes()).decode(),'actual_receipt':{'path':str(path),'sha256':sha(path)},'actual_stdout':{'path':str(stdout),'sha256':sha(stdout)}}

def project(first_fixture,second_fixture):
 first=Path(first_fixture);second=Path(second_fixture);C=first/'live-vod-group-settings/private/combined';S=second/'live-vod-group-settings/private';initial=load(C/'SQL-before.json');codec=State(initial);setup=load(first/'declared-live-vod-fixture-setup.json');phases={};previous=None;proofs={};receivers=set()
 labels=['combined','repeat','partial-category','explicit-null','profiles','bad-start','bad-end','reversed-range','bad-category','duplicate-live','missing-live-FK','missing-category-FK']
 assert state(load(S/'declared-combined-state-SQL-after.json'))==state(load(C/'SQL-after.json')),'Explicit continuation seed is exact old combined state'
 for label in labels:
  fixture=first if label=='combined'else second;p=C if label=='combined'else S/label;before=load(p/'SQL-before.json');after=load(p/'SQL-after.json')
  if previous is not None:assert state(before)==state(previous),'Raw phase chain across known combined and explicit seed'
  request=load(p/'request.json');response=load(p/'HTTP.json');detail=load(p/'GET.json');ws=load(p/'WS.json');journal=load(p/'journal.json');assert set(ws)=={'actual_receiver','events','raw_events_base64'}and not ws['events']and not ws['raw_events_base64']and not journal['publications'];proof=receiver_proof(fixture,p);assert proof['receiver']not in receivers;receivers.add(proof['receiver']);proofs[label]=proof
  assert load(p/'cache-before.json')==load(p/'cache-after.json')and load(p/'broker-before.json')==load(p/'broker-after.json')
  phases[label]={'request':request,'HTTP':response,'GET':codec.get(detail,after),'before':codec.canonical(before),'after':codec.canonical(after),'WS':{'actual_receiver':{'owned_aggregate_receiver_uuid4':label},'events':[],'raw_events_base64':[]},'producer_publications':journal['publications'],'cache_unchanged':True,'broker_unchanged':True};previous=after
 return {'Source_controller_outcomes':[1,0],'single_whole12_exit0':False,'manual_API_cells':12,'known_combined_replayed':False,'same_fixture_namespace':setup,'initial_clock_and_credential_stability_checked_before_typed_projection':True,'all15sequence_values_PK_business_and_error_bodies_literal':True,'phases':phases},proofs
