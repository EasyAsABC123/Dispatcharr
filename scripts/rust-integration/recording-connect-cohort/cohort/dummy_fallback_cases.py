"""Real dummy fallback XMLTV/XC/grid reads in the SAME existing guide context.
No application or task body executes on import. Original Dispatcharr authors credited.
"""
import base64,datetime,hashlib,json,secrets,traceback,urllib.request,urllib.error,urllib.parse,xml.etree.ElementTree as ET
from pathlib import Path
CASE='schedules-direct'
_STANDARD=['Late Night with {channel} - Where insomniacs unite!', "The 'Why Am I Still Awake?' Show on {channel}", 'Counting Sheep - A {channel} production for the sleepless', 'Dawn Patrol - Rise and shine with {channel}!', 'Early Bird Special - Coffee not included', 'Morning Zombies - Before coffee viewing on {channel}', "Mid-Morning Meetings - Pretend you're paying attention while watching {channel}", "The 'I Should Be Working' Hour on {channel}", "Productivity Killer - {channel}'s daytime programming", 'Lunchtime Laziness with {channel}', 'The Afternoon Slump - Brought to you by {channel}', 'Post-Lunch Food Coma Theater on {channel}', "Rush Hour - {channel}'s alternative to traffic", "The 'What's For Dinner?' Debate on {channel}", "Evening Escapism - {channel}'s remedy for reality", "Prime Time Placeholder - {channel}'s finest not-programming", "The 'Netflix Was Too Complicated' Show on {channel}", 'Family Argument Avoider - Courtesy of {channel}']
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 password=secrets.token_urlsafe(24)
 script="from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership;from apps.accounts.models import User;specs=[{'title_pattern':'','time_pattern':'(?invalid','timezone':'Not/AZone','program_duration':0},{'title_pattern':False,'fallback_title_template':'\\x1c Literal {channel} <&> \\x1f'},{'fallback_description_template':'\\u0085Literal description {title}\\u3000'},{'fallback_title_template':' \\x1c\\u3000','fallback_description_template':'\\x1f '}];ss=EPGSource.objects.bulk_create([EPGSource(name='Owned fallback source '+str(i),source_type='dummy',is_active=False,custom_properties=p)for i,p in enumerate(specs)]);es=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-fallback-'+str(i),name='Owned fallback '+str(i))for i,s in enumerate(ss)]);cs=Channel.objects.bulk_create([Channel(name='Owned fallback channel '+str(i),channel_number=91400+i,epg_data=e,tvg_id='owned-fallback-'+str(i))for i,e in enumerate(es)]);p=ChannelProfile.objects.create(name='Owned dummy fallback output');ChannelProfileMembership.objects.filter(channel_profile=p).exclude(channel_id__in=[c.pk for c in cs]).update(enabled=False)\nfor c in cs:\n ChannelProfileMembership.objects.update_or_create(channel_profile=p,channel=c,defaults={'enabled':True})\nactual_members=list(ChannelProfileMembership.objects.filter(channel_profile=p,enabled=True).order_by('channel_id').values_list('channel_id',flat=True));assert actual_members==sorted(c.pk for c in cs),actual_members\nu=User.objects.create(username='owned-dummy-fallback-viewer',user_level=1,is_active=True,custom_properties={'xc_password':"+repr(password)+",'epg_days':1,'epg_prev_days':0});u.channel_profiles.add(p);print(json.dumps({'profile':{'id':p.pk,'name':p.name},'channels':[{'id':c.pk,'uuid':str(c.uuid),'name':c.name,'tvg_id':c.tvg_id}for c in cs],'xc_username':u.username}))"
 setup=fixture.sql(script,secret=True);setup['xc_password']=password;fixture._dummy_fallback=setup
 save(Path(fixture.output)/'declared-dummy-fallback-inputs.json',{'profile':setup['profile'],'channels':setup['channels'],'source_type':'dummy','new_paired_observations_required':True,'scenario':'no title pattern genuinely returns None before invalid time/timezone/duration use; static fallbacks remain literal','xc_secret':'private fixture-only; excluded','new_application_or_task_body_replay':False})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'dummy-fallback';out.mkdir();private=out/'private';private.mkdir(mode=0o700);setup=fixture._dummy_fallback;cells=[]
 def request(path,label,auth=True):
  headers={'Authorization':'Bearer '+fixture.admin}if auth else{}
  r=urllib.request.Request(fixture.base_url+path,headers=headers);opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  try:response=opener.open(r,timeout=fixture.remaining(30))
  except urllib.error.HTTPError as error:response=error
  with response:raw=response.read(262145);status=response.status;kind=response.headers.get('Content-Type','')
  (private/(label+'.bin')).write_bytes(raw);save(private/(label+'.json'),{'status':status,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'content_type':kind,'credential_role':'owned XC viewer'if not auth else'fixture admin; credentials excluded'})
  assert status==200 and len(raw)<=262144;return raw
 def expected(index):
  name=setup['channels'][index]['name'];return ('Literal {channel} <&>','EPG information is currently unavailable for '+name)if index==1 else(name,'Literal description {title}')if index==2 else(name,None)
 def check(index,title,description):
  t,d=expected(index);assert title==t
  if d is not None:assert description==d
  else:assert description in [text.replace('{channel}',setup['channels'][index]['name'])for text in _STANDARD]
 def xml():
  path='/output/epg/'+urllib.parse.quote(setup['profile']['name'],safe='')+'/?days=1&prev_days=0&tvg_id_source=tvg_id';root=ET.fromstring(request(path,'XMLTV'));assert root.tag=='tv'and len(root.findall('channel'))==4
  programmes=root.findall('programme');assert programmes
  counts=[]
  for i,ch in enumerate(setup['channels']):
   selected=[p for p in programmes if p.attrib['channel']==ch['tvg_id']];assert selected;counts.append(len(selected))
   for p in selected:check(i,p.findtext('title'),p.findtext('desc'))
  return {'channels':4,'programme_counts':counts,'literal_braces_not_interpolated':True,'source_byte_clock_parity_claimed':False}
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
 for name,body in [('actual-dummy-fallback-XMLTV',xml),('actual-dummy-fallback-grid',grid),('actual-dummy-fallback-XC',xc)]:
  row={'name':name,'status':'running'};cells.append(row)
  try:row.update(status='passed',actual=body())
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'full_M7_or_custom_dummy_parity':False,'performance_verified':False,'unqualified':['actual regex/time/date pattern generation','stream-name ordered lookup','custom logo/poster/category/live/new fields','malformed Source error-body parity','physical guide cancellation/large scaling']};save(out/'report.json',report);return report
