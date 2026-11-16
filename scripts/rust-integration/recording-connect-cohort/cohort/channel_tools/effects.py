"""Exact Source9 row-delta comparison with explicit setup/generated-ID bindings."""
import copy,datetime,uuid
CHANNEL='dispatcharr_channels_channel';LOGO='dispatcharr_channels_logo';LINK='dispatcharr_channels_channelstream';MEMBER='dispatcharr_channels_channelprofilemembership'
def delta(before,after):
 result={}
 for table,old in before.items():
  a={r['id']:r for r in old};b={r['id']:r for r in after[table]};result[table]={'inserted':[v for k,v in b.items()if k not in a],'removed':[v for k,v in a.items()if k not in b],'changed':[{'before':a[k],'after':v}for k,v in b.items()if k in a and a[k]!=v]}
 return result

def compare(row,reference,seed,bindings):
 expected=copy.deepcopy(reference['SQL_changes']);actual=delta(row['before']['tables'],row['after']['tables']);checks={};record=[]
 channels=bindings.setdefault('channels',{});logos=bindings.setdefault('logos',{});streams=bindings.setdefault('streams',{});epgs=bindings.setdefault('epgs',{});links=bindings.setdefault('links',{})
 # Foreign-key identities originate from actual task arguments / actual unique
 # inserted rows. No arbitrary field normalization or fixture authority minting.
 for source_id,native_id in bindings.get('seed_channel_pairs',[]):channels[int(source_id)]=native_id
 for table in expected:
  checks[table+'-delta-cardinalities']=all(len(expected[table][k])==len(actual[table][k])for k in ['inserted','removed','changed'])
 for old in expected[LOGO]['inserted']:
  matches=[r for r in actual[LOGO]['inserted']if r['url']==old['url']]
  if len(matches)==1:logos[old['id']]=matches[0]['id'];record.append({'table':LOGO,'Source_id':old['id'],'Native_id':matches[0]['id'],'basis':'actual unique inserted URL'})
 if len(expected[CHANNEL]['inserted'])==len(actual[CHANNEL]['inserted'])==1:
  old=expected[CHANNEL]['inserted'][0];new=actual[CHANNEL]['inserted'][0];channels[old['id']]=new['id'];epgs[old['epg_data_id']]=seed['epg_id'];record.append({'table':CHANNEL,'Source_id':old['id'],'Native_id':new['id'],'basis':'actual one new channel of the actual bulk task'})
 if len(expected[LINK]['inserted'])==len(actual[LINK]['inserted'])==1:
  old=expected[LINK]['inserted'][0];new=actual[LINK]['inserted'][0];links[old['id']]=new['id'];streams[old['stream_id']]=seed['stream_id'];record.append({'table':LINK,'Source_id':old['id'],'Native_id':new['id'],'basis':'actual one new channel-stream association'})
 before_channels={r['id']:r for r in row['before']['tables'][CHANNEL]}
 seed_values={}
 for change in expected[CHANNEL]['changed']:
  old=change['before'];native=before_channels.get(channels.get(old['id']))
  if native:
   # These values belong to setup, not a task mutation. Source AFTER must still
   # equal its original setup value; replacing after-only mutation cannot pass.
   for field in ['uuid','created_at','updated_at']:
    seed_values[(old['id'],field,old[field])]=native[field]
 def rewrite(table,value,insert=False):
  value=copy.deepcopy(value);old_id=value['id'];mapping={CHANNEL:channels,LOGO:logos,LINK:links}.get(table,{})
  if old_id in mapping:value['id']=mapping[old_id]
  if table==CHANNEL:
   if value.get('epg_data_id')in epgs:value['epg_data_id']=epgs[value['epg_data_id']]
   if value.get('logo_id')in logos:value['logo_id']=logos[value['logo_id']]
   for field in ['uuid','created_at','updated_at']:
    key=(old_id,field,value[field])
    if key in seed_values:value[field]=seed_values[key]
   if insert:
    # Generated values are tested against real UUID/time predicates in callback;
    # they are the ONLY inserted field values excluded from epoch byte equality.
    for field in ['uuid','created_at','updated_at']:value[field]='ACTUAL_GENERATED_'+field
  if table in [LINK,MEMBER]:
   if value['channel_id']in channels:value['channel_id']=channels[value['channel_id']]
  if table==LINK and value['stream_id']in streams:value['stream_id']=streams[value['stream_id']]
  return value
 for table,d in expected.items():
  for kind in ['inserted','removed']:
   want=[rewrite(table,r,kind=='inserted'and table==CHANNEL)for r in d[kind]];got=copy.deepcopy(actual[table][kind])
   if kind=='inserted'and table==CHANNEL:
    for item in got:
     for field in ['uuid','created_at','updated_at']:item[field]='ACTUAL_GENERATED_'+field
   checks[table+'-'+kind+'-all-fields']=sorted(want,key=lambda r:r['id'])==sorted(got,key=lambda r:r['id'])
  want=[{'before':rewrite(table,c['before']),'after':rewrite(table,c['after'])}for c in d['changed']]
  checks[table+'-changed-all-fields']=sorted(want,key=lambda r:r['before']['id'])==sorted(actual[table]['changed'],key=lambda r:r['before']['id'])
 return {'passed':all(checks.values()),'checks':checks,'actual_deltas':actual,'explicit_new_bindings':record}
