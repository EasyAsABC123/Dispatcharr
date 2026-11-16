"""Real ordered stream-name XMLTV/XC/grid reads in the SAME existing guide context.
No application or task body executes on import. Original Dispatcharr authors credited.
"""
import base64,datetime,hashlib,json,secrets,traceback,urllib.request,urllib.error,urllib.parse,xml.etree.ElementTree as ET
from pathlib import Path
CASE='schedules-direct'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 script="from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership,Stream,ChannelStream;from apps.accounts.models import User;specs=[('selected-second',2),('missing-index',99),('bool-index',True),('negative-float',0.5),('out-of-range-float',99.5),('no-streams',2.0),('false-index',False),('default-index','omit'),('large-integer',9223372036854775808)];sources=[];channels=[];proof=[];p=ChannelProfile.objects.create(name='Owned stream-name guide output');ChannelProfileMembership.objects.filter(channel_profile=p).update(enabled=False);\nfor i,(label,index)in enumerate(specs):\n props={'name_source':'stream','title_pattern':'','fallback_description_template':'Owned selected stream description'};\n if index!='omit':props['stream_index']=index\n source=EPGSource.objects.bulk_create([EPGSource(name='Owned stream-name source '+label,source_type='dummy',is_active=False,custom_properties=props)])[0];epg=EPGData.objects.bulk_create([EPGData(epg_source=source,tvg_id='owned-stream-name-'+str(i),name='Owned stream-name '+label)])[0];c=Channel.objects.bulk_create([Channel(name='Owned display '+label,channel_number=91600+i,epg_data=epg,tvg_id='owned-stream-name-'+str(i))])[0];ChannelProfileMembership.objects.update_or_create(channel_profile=p,channel=c,defaults={'enabled':True});ordered=[];\n if label!='no-streams':\n  streams=Stream.objects.bulk_create([Stream(name='Owned join-order second',url='http://127.0.0.1:18097/unrequested-stream-name/second'),Stream(name='Owned join-order first',url='http://127.0.0.1:18097/unrequested-stream-name/first')]);ChannelStream.objects.bulk_create([ChannelStream(channel=c,stream=streams[0],order=20),ChannelStream(channel=c,stream=streams[1],order=10)]);ordered=list(ChannelStream.objects.filter(channel=c).order_by('order').values('stream_id','order','stream__name'))\n proof.append({'id':c.pk,'uuid':str(c.uuid),'name':c.name,'tvg_id':c.tvg_id,'label':label,'expected_parse_name':c.name if label=='no-streams' else 'Owned join-order second' if label=='selected-second' else 'Owned join-order first','actual_join_order':ordered})\nactual_members=list(ChannelProfileMembership.objects.filter(channel_profile=p,enabled=True).order_by('channel_id').values_list('channel_id',flat=True));assert actual_members==sorted(row['id']for row in proof),actual_members\nu=User.objects.get(username='owned-dummy-fallback-viewer');u.channel_profiles.add(p);print(json.dumps({'profile':{'id':p.pk,'name':p.name},'channels':proof,'xc_username':u.username}))"
 setup=fixture.sql(script,secret=True);setup['xc_password']=fixture._dummy_fallback['xc_password'];fixture._stream_names=setup
 save(Path(fixture.output)/'declared-stream-name-inputs.json',{'profile':setup['profile'],'channels':setup['channels'],'source_type':'dummy','new_paired_observations_required':True,'scenario':'actual join order differs from stream insertion order; index2/bool/default/large integer/out-of-range float/no-stream fallback','tie_choice_parity_claimed':False,'xc_secret':'private fixture-only; excluded','new_application_or_task_body_replay':False})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'stream-name-guide';out.mkdir();private=out/'private';private.mkdir(mode=0o700);setup=fixture._stream_names;cells=[]
 def request(path,label,auth=True):
  headers={'Authorization':'Bearer '+fixture.admin}if auth else{}
  r=urllib.request.Request(fixture.base_url+path,headers=headers);opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  try:response=opener.open(r,timeout=fixture.remaining(30))
  except urllib.error.HTTPError as error:response=error
  with response:raw=response.read(262145);status=response.status;kind=response.headers.get('Content-Type','')
  (private/(label+'.bin')).write_bytes(raw);save(private/(label+'.json'),{'status':status,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'content_type':kind,'credential_role':'owned XC viewer'if not auth else'fixture admin; credentials excluded'})
  assert status==200 and len(raw)<=262144;return raw
 def check(index,title,description):
  assert title==setup['channels'][index]['expected_parse_name']
  assert description=='Owned selected stream description'
 def xml():
  path='/output/epg/'+urllib.parse.quote(setup['profile']['name'],safe='')+'/?days=1&prev_days=0&tvg_id_source=tvg_id';root=ET.fromstring(request(path,'XMLTV'));assert root.tag=='tv'and len(root.findall('channel'))==9
  programmes=root.findall('programme');assert programmes
  counts=[]
  for i,ch in enumerate(setup['channels']):
   selected=[p for p in programmes if p.attrib['channel']==ch['tvg_id']];assert selected;counts.append(len(selected))
   for p in selected:check(i,p.findtext('title'),p.findtext('desc'))
  assert [c.findtext('display-name')for c in root.findall('channel')]==[c['name']for c in setup['channels']]
  return {'channels':9,'programme_counts':counts,'display_names_unchanged_parse_names_used_for_programmes':True,'source_byte_clock_parity_claimed':False}
 def grid():
  now=datetime.datetime.now(datetime.timezone.utc);end=now+datetime.timedelta(hours=8)
  params=urllib.parse.urlencode({'channel_profile_id':setup['profile']['id'],'start':now.isoformat(),'end':end.isoformat()});value=json.loads(request('/api/epg/grid/?'+params,'grid'));rows=value['data'];assert rows
  counts=[]
  for i,ch in enumerate(setup['channels']):
   selected=[p for p in rows if p['tvg_id']==ch['uuid']];assert selected;counts.append(len(selected))
   for p in selected:
    check(i,p['title'],p['description']);assert p['sub_title']is None and p['custom_properties']is None and not p['is_live']and not p['is_new']
  return {'dummy_rows_by_channel':counts,'existing_dummy_custom_identity_prefix_preserved':all(p['id'].startswith('dummy-custom-')for p in rows)}
 def xc():
  counts=[]
  for i,ch in enumerate(setup['channels']):
   query=urllib.parse.urlencode({'username':setup['xc_username'],'password':setup['xc_password'],'action':'get_short_epg','stream_id':ch['id'],'limit':2,'days':1,'prev_days':0});value=json.loads(request('/player_api.php?'+query,'XC-'+str(i),False));rows=value['epg_listings'];assert rows and len(rows)<=2;counts.append(len(rows))
   for p in rows:check(i,base64.b64decode(p['title'],validate=True).decode(),base64.b64decode(p['description'],validate=True).decode())
  return {'actual_XC_short_rows_by_channel':counts,'credential_logs_public':False}
 for name,body in [('actual-stream-name-XMLTV',xml),('actual-stream-name-grid',grid),('actual-stream-name-XC',xc)]:
  row={'name':name,'status':'running'};cells.append(row)
  try:row.update(status='passed',actual=body())
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'full_M7_or_custom_dummy_parity':False,'performance_verified':False,'unqualified':['actual regex/time/date pattern generation','tie choice for equal-order distinct stream names','custom logo/poster/category/live/new fields','malformed Source error-body parity','physical guide cancellation/large scaling']};save(out/'report.json',report);return report
