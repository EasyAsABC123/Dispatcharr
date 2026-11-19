"""Source/Native real no-time custom guide output in existing SD context.
Pure setup/import; no Source backend task body or extra app/Origin/worker.
Original Dispatcharr/regex/CPython authors retain application/algorithm credit.
"""
import base64,datetime,hashlib,json,threading,time,traceback,urllib.request,urllib.error,urllib.parse,xml.etree.ElementTree as ET
from pathlib import Path
CASE='schedules-direct';HELD_CASE='custom-guide-held';ORIGIN='http://127.0.0.1:18097'
_STANDARD=['Late Night with {channel} - Where insomniacs unite!', "The 'Why Am I Still Awake?' Show on {channel}", 'Counting Sheep - A {channel} production for the sleepless', 'Dawn Patrol - Rise and shine with {channel}!', 'Early Bird Special - Coffee not included', 'Morning Zombies - Before coffee viewing on {channel}', "Mid-Morning Meetings - Pretend you're paying attention while watching {channel}", "The 'I Should Be Working' Hour on {channel}", "Productivity Killer - {channel}'s daytime programming", 'Lunchtime Laziness with {channel}', 'The Afternoon Slump - Brought to you by {channel}', 'Post-Lunch Food Coma Theater on {channel}', "Rush Hour - {channel}'s alternative to traffic", "The 'What's For Dinner?' Debate on {channel}", "Evening Escapism - {channel}'s remedy for reality", "Prime Time Placeholder - {channel}'s finest not-programming", "The 'Netflix Was Too Complicated' Show on {channel}", 'Family Argument Avoider - Courtesy of {channel}']
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def date_specs():
    shared = {
        "title_pattern": r"^(?<month>[A-Za-z]+) (?<title>.+?) @",
        "date_pattern": r"@ (?<month>[A-Za-z]+) (?<day>\d+)",
        "program_duration": 180,
        "timezone": "UTC",
        "title_template": "{title} [{month}/{day}/{month_normalize}]",
        "subtitle_template": "day {day}",
        "description_template": "captured {month} date {day}",
        "channel_logo_url": ORIGIN + "/date-logo/{month}/{day}.png",
        "program_poster_url": ORIGIN + "/date-poster/{month}/{day}.png",
    }
    return [
        ("January Owned Bowl @ Oct 17", shared,
         ("Owned Bowl [Oct/17/oct]", "day 17", "captured Oct date 17"),
         "January/{day}", "Oct/17", 8, 180),
        ("January Unsupplied @ soon", shared,
         ("Unsupplied [January/{day}/january]", "day {day}", "captured January date {day}"),
         "January/{day}", "January/{day}", 8, 180),
        ("January Broken @ Oct 21", {**shared, "date_pattern": "(?<bad"},
         ("Broken [January/{day}/january]", "day {day}", "captured January date {day}"),
         "January/{day}", "January/{day}", 8, 180),
        ("Owned-fallback no-title @ Oct 17",
         {**shared, "fallback_title_template": "Owned date profile fallback",
          "fallback_description_template": "Title did not match"},
         ("Owned date profile fallback", None, "Title did not match"), None, None, 6, 240),
    ]


def prepare_date_context(fixture, account):
    seeds = [(item[0], item[1]) for item in date_specs()]
    return prepare_pattern_context(fixture, account, seeds, "Owned date-only custom guide", "owned-date-guide-", 91850)


def prepare_pattern_context(fixture, account, seeds, profile_name, tvg_prefix, base_number):
    script = """
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelProfile, ChannelProfileMembership
from apps.epg.models import EPGSource, EPGData
profile = ChannelProfile.objects.create(name=PROFILE_NAME)
ChannelProfileMembership.objects.filter(channel_profile=profile).update(enabled=False)
channels = []
for index, (name, custom) in enumerate(SEEDS):
    tvg_id = TVG_PREFIX + str(index)
    source = EPGSource.objects.bulk_create([EPGSource(
        name=tvg_id, source_type='dummy', is_active=False, custom_properties=custom,
    )])[0]
    epg = EPGData.objects.bulk_create([EPGData(epg_source=source, tvg_id=tvg_id, name=name)])[0]
    channel = Channel.objects.bulk_create([Channel(
        name=name, channel_number=BASE_NUMBER+index, epg_data=epg, tvg_id=tvg_id,
    )])[0]
    ChannelProfileMembership.objects.update_or_create(
        channel_profile=profile, channel=channel, defaults={'enabled': True},
    )
    channels.append({'id': channel.pk, 'uuid': str(channel.uuid), 'tvg_id': tvg_id})
actual = ChannelProfileMembership.objects.filter(channel_profile=profile, enabled=True)
assert set(actual.values_list('channel_id', flat=True)) == {row['id'] for row in channels}
User.objects.get(username=ACCOUNT).channel_profiles.add(profile)
print(json.dumps({'profile': {'id': profile.pk, 'name': profile.name}, 'channels': channels}))
"""
    setup = {"SEEDS": seeds, "ACCOUNT": account, "PROFILE_NAME": profile_name,
             "TVG_PREFIX": tvg_prefix, "BASE_NUMBER": base_number}
    script = "".join(f"{key} = {value!r}\n" for key, value in setup.items()) + script
    return fixture.sql(script, secret=True)


def check_date_guides(fixture, setup, private):
    cases = date_specs()
    case = setup["date"]
    query = "?days=1&prev_days=0&tvg_id_source=tvg_id"
    profile = urllib.parse.quote(case["profile"]["name"], safe="")
    root = ET.fromstring(request(fixture, "/output/epg/" + profile + "/" + query, "date-XML", private))
    channels = root.findall("channel")
    assert [row.findtext("display-name") for row in channels] == [row[0] for row in cases]

    def observe(index, row):
        _, _, expected, _, _, _, period = cases[index]
        assert (row["title"], row["subtitle"], row["description"]) == expected
        assert (row["stop"] - row["start"]).total_seconds() == 60 * period

    counts = []
    for index, (channel, identity) in enumerate(zip(channels, case["channels"])):
        _, _, _, logo, poster, count, _ = cases[index]
        expected_logo = "" if logo is None else ORIGIN + "/date-logo/" + logo + ".png"
        assert channel.find("icon").attrib["src"] == expected_logo
        rows = [row for row in root.findall("programme") if row.attrib["channel"] == identity["tvg_id"]]
        assert len(rows) == count
        counts.append(count)
        previous_stop = None
        for row in rows:
            start = datetime.datetime.strptime(row.attrib["start"], "%Y%m%d%H%M%S %z")
            stop = datetime.datetime.strptime(row.attrib["stop"], "%Y%m%d%H%M%S %z")
            observe(index, {"title": row.findtext("title"), "subtitle": row.findtext("sub-title"),
                            "description": row.findtext("desc"), "start": start, "stop": stop})
            assert previous_stop is None or start == previous_stop
            previous_stop = stop
            if poster is None:
                assert row.find("icon") is None and row.find("date") is None
            else:
                assert row.find("icon").attrib["src"] == ORIGIN + "/date-poster/" + poster + ".png"
                assert row.findtext("date") == start.date().isoformat()

    now = datetime.datetime.now(datetime.timezone.utc)
    query = urllib.parse.urlencode({"channel_profile_id": case["profile"]["id"], "start": now.isoformat(),
                                    "end": (now + datetime.timedelta(hours=7)).isoformat()})
    grid = json.loads(request(fixture, "/api/epg/grid/?" + query, "date-grid", private))
    grid_counts = []
    xc_counts = []
    for index, identity in enumerate(case["channels"]):
        rows = [row for row in grid["data"] if row["tvg_id"] == identity["uuid"]]
        assert rows
        for row in rows:
            start = datetime.datetime.fromisoformat(row["start_time"])
            stop = datetime.datetime.fromisoformat(row["end_time"])
            observe(index, {"title": row["title"], "subtitle": row["sub_title"],
                            "description": row["description"], "start": start, "stop": stop})
        grid_counts.append(len(rows))
        params = urllib.parse.urlencode({"username": setup["xc_username"], "password": setup["xc_password"],
                                        "action": "get_short_epg", "stream_id": identity["id"],
                                        "limit": 2, "days": 1, "prev_days": 0})
        xc = json.loads(request(fixture, "/player_api.php?" + params, "date-XC-" + str(index),
                                private, False))["epg_listings"]
        assert len(xc) == 2
        for row in xc:
            title = base64.b64decode(row["title"], validate=True).decode()
            desc = base64.b64decode(row["description"], validate=True).decode()
            assert (title, desc) == (cases[index][2][0], cases[index][2][2])
        xc_counts.append(len(xc))
    return {"xml_counts": counts, "grid_counts": grid_counts, "xc_counts": xc_counts,
            "fixed_durations_minutes": [row[-1] for row in cases],
            "title_channel_logo_and_date_programme_poster": True,
            "date_nomatch_invalid_and_title_fallback": True}


def prepare_context(fixture):
 if fixture.case!=CASE:return
 props=[{'title_pattern':r'^(?<league>Owned) (?P<team1>.+) vs (?P<team2>[^!]+)(?:!(?P<empty>x))?$','program_duration':181,'timezone':'UTC','title_template':'{league}: {team1} vs {team2} [{team1_normalize}/{team2_normalize}]','subtitle_template':'{empty}','description_template':'Literal {empty}|{unknown}','channel_logo_url':ORIGIN+'/unrequested-logo/{team1}/{team2}/{league_normalize}.png','program_poster_url':ORIGIN+'/unrequested-poster/{team1_normalize}.jpg','category':' Sport , Live ,, Sport ','include_live':True,'include_new':True}, {'title_pattern':'^(?<title>No such name)$','timezone':'UTC','fallback_title_template':' Owned unmatched fallback ','fallback_description_template':' Preserved four-hour blocks ','program_poster_url':ORIGIN+'/should-not-appear.jpg'}, {'title_pattern':'(?<broken','timezone':'UTC'}, {'title_pattern':'^Owned no groups$','timezone':'UTC','channel_logo_url':ORIGIN+'/no-groups-logo-must-not-appear.png','program_poster_url':ORIGIN+'/unrequested-poster/no-groups.jpg'}]
 names=['Owned Älpha &/B vs Beta+Team','Owned mismatch','Owned invalid pattern','Owned no groups']
 script="from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership;from apps.accounts.models import User;specs="+repr(props)+";names="+repr(names)+";p=ChannelProfile.objects.create(name='Owned custom no-time guide');ChannelProfileMembership.objects.filter(channel_profile=p).update(enabled=False);rows=[];\nfor i,(props,name)in enumerate(zip(specs,names)):\n s=EPGSource.objects.bulk_create([EPGSource(name='Owned custom guide '+str(i),source_type='dummy',is_active=False,custom_properties=props)])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-custom-'+str(i),name=name)])[0];c=Channel.objects.bulk_create([Channel(name=name,channel_number=91800+i,epg_data=e,tvg_id='owned-custom-'+str(i))])[0];ChannelProfileMembership.objects.bulk_create([ChannelProfileMembership(channel_profile=p,channel=c,enabled=True)]);rows.append({'id':c.pk,'uuid':str(c.uuid),'name':c.name,'tvg_id':c.tvg_id})\nu=User.objects.get(username='owned-dummy-fallback-viewer');u.channel_profiles.add(p);held=EPGSource.objects.bulk_create([EPGSource(name='Owned custom held search',source_type='dummy',is_active=False,custom_properties={'title_pattern':'(?:a|aa)+$','timezone':'UTC'})])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=held,tvg_id='owned-custom-held',name='Owned held')])[0];c=Channel.objects.bulk_create([Channel(name='a'*511+'!',channel_number=91890,epg_data=e,tvg_id='owned-custom-held')])[0];hp=ChannelProfile.objects.create(name='Owned custom held guide');ChannelProfileMembership.objects.filter(channel_profile=hp).exclude(channel_id=c.pk).update(enabled=False);ChannelProfileMembership.objects.update_or_create(channel_profile=hp,channel=c,defaults={'enabled':True});assert set(ChannelProfileMembership.objects.filter(channel_profile=p,enabled=True).values_list('channel_id',flat=True))=={r['id']for r in rows};assert list(ChannelProfileMembership.objects.filter(channel_profile=hp,enabled=True).values_list('channel_id',flat=True))==[c.pk];print(json.dumps({'profile':{'id':p.pk,'name':p.name},'channels':rows,'xc_username':u.username,'held_profile':hp.name}))"
 setup=fixture.sql(script,secret=True);setup['xc_password']=fixture._dummy_fallback['xc_password'];fixture._custom_guide=setup
 if fixture.binding.get('custom_guide_date_acceptance'):
  setup['date']=prepare_date_context(fixture,setup['xc_username'])
 if fixture.binding.get('custom_guide_time_acceptance'):
  import custom_guide_time_cases as timed
  seeds=[(case[0],timed.CONFIG)for case in timed.CASES]
  setup['time']=prepare_pattern_context(fixture,setup['xc_username'],seeds,'Owned UTC time-only guide','owned-time-guide-',91860)
 fixture._plugin_environment={**getattr(fixture,'_plugin_environment',{}),'RUST_EPG_REGEX_DIAGNOSTICS':'1'}
 save(Path(fixture.output)/'declared-custom-guide-inputs.json',{'profile':setup['profile'],'channels':setup['channels'],'new_Source_and_Native_required':True,'regex_engine':'actual pinned dependency; no per-search timeout','timezone_scope':'UTC only','timelines':'positive integral no-time duration;181minute non-divisor plus default180','new_Origin_or_workers':False,'diagnostic_bound':'64 retirement records per view registry; metadata only','held_gate':'optional Native-only final controlledMain stop, physically held search; no Source shutdown-equivalence claim','unqualified':['time patterns','non-UTC/fractional durations','EPG-download Connect/plugin sideeffects','large expansion/guide stress','Source held HTTP cold-stop counterpart']})
def request(fixture,path,label,out,auth=True):
 headers={'Authorization':'Bearer '+fixture.admin}if auth else{}
 r=urllib.request.Request(fixture.base_url+path,headers=headers);opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
 try:response=opener.open(r,timeout=fixture.remaining(30))
 except urllib.error.HTTPError as error:response=error
 with response:raw=response.read(524289);status=response.status;kind=response.headers.get('Content-Type','')
 (out/(label+'.bin')).write_bytes(raw);save(out/(label+'.json'),{'status':status,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'content_type':kind,'credential_role':'owned XC viewer'if not auth else'fixture admin; excluded'})
 assert status==200 and len(raw)<=524288;return raw
def retirements(fixture):
 if fixture.backend!='Native':return []
 fixture.command([fixture.docker,'logs','--tail','256',fixture.native],seconds=10,secret=True)
 receipt=fixture.cli[-1];raw=Path(receipt['stdout']).read_bytes()+Path(receipt['stderr']).read_bytes();assert len(raw)<=2*1024*1024
 rows=[]
 for line in raw.decode('utf8',errors='replace').splitlines():
  if 'epg_regex_retirement 'in line:rows.append(json.loads(line.split('epg_regex_retirement ',1)[1]))
 return rows
def process_snapshot(fixture,expected=None):
 code="""import json,os
from pathlib import Path
if os.geteuid()==0:os.setgid(65532);os.setuid(65532)
rows=[]
for p in Path('/proc').glob('[0-9]*'):
 try:
  if int(p.name)==os.getpid():continue
  cmd=(p/'cmdline').read_bytes()
  if EXPECTED is not None:
   if int(p.name)not in EXPECTED:continue
  elif b'/bridge/regex_bootstrap.py'not in cmd:continue
  stat=(p/'stat').read_text().rsplit(')',1)[1].split()
  if EXPECTED is not None and stat[19]!=EXPECTED[int(p.name)]:continue
  rows.append({'pid':int(p.name),'generation':stat[19],'cpu_ticks':int(stat[11])+int(stat[12]),'pgrp':int(stat[2]),'namespace':os.readlink(p/'ns/pid')})
 except(FileNotFoundError,ProcessLookupError,PermissionError):continue
print(json.dumps(rows))
""".replace('EXPECTED',repr(None if expected is None else dict(expected)))
 return json.loads(fixture.command([fixture.docker,'exec','--user','65532:65532',fixture.native,'/usr/local/bin/dispatcharr-python','-I','-c',code],seconds=10,secret=True))
def run(fixture):
 if fixture.case!=CASE:raise ValueError('ExistingOwnedGuideContextRequired')
 setup=fixture._custom_guide;out=Path(fixture.output)/'custom-guide';out.mkdir();private=out/'private';private.mkdir(mode=0o700);cells=[]
 prior={(r['pid'],r['generation'])for r in retirements(fixture)}
 def expected(index):return ('Owned: Älpha &/B vs Beta+Team [lphab/betateam]','Literal |{unknown}')if index==0 else('Owned unmatched fallback','Preserved four-hour blocks')if index==1 else(setup['channels'][index]['name'],None if index==2 else setup['channels'][index]['name'])
 def check(index,title,description):
  wanted,desc=expected(index);assert title==wanted
  if desc is None:assert description in[t.replace('{channel}',setup['channels'][index]['name'])for t in _STANDARD]
  else:assert description==desc
 def xml():
  path='/output/epg/'+urllib.parse.quote(setup['profile']['name'],safe='')+'/?days=1&prev_days=0&tvg_id_source=tvg_id';root=ET.fromstring(request(fixture,path,'XML',private));headers=root.findall('channel');assert [c.findtext('display-name')for c in headers]==[c['name']for c in setup['channels']]
  assert headers[0].find('icon').attrib['src']==ORIGIN+'/unrequested-logo/%C3%84lpha%20%26%2FB/Beta%2BTeam/owned.png';assert all(c.find('icon').attrib['src']==''for c in headers[1:])
  counts=[]
  for i,ch in enumerate(setup['channels']):
   rows=[r for r in root.findall('programme')if r.attrib['channel']==ch['tvg_id']];counts.append(len(rows));assert len(rows)==[7,6,6,8][i]
   for row in rows:
    check(i,row.findtext('title'),row.findtext('desc'));start=datetime.datetime.strptime(row.attrib['start'],'%Y%m%d%H%M%S %z');stop=datetime.datetime.strptime(row.attrib['stop'],'%Y%m%d%H%M%S %z');assert(stop-start).total_seconds()==([181,240,240,180][i]*60)
    if i==0:assert [v.text for v in row.findall('category')]==['Sport','Live','Sport']and row.find('live')is not None and row.find('new')is not None and row.find('sub-title')is None
    if i in(0,3):assert row.findtext('date')==start.date().isoformat()and row.find('icon').attrib['src']==ORIGIN+('/unrequested-poster/lphab.jpg'if i==0 else'/unrequested-poster/no-groups.jpg')
    else:assert row.find('date')is None and row.find('icon')is None
  return {'channels':4,'actual_programme_counts':counts,'nativeTimeline_and_literal_unknown_placeholders':True,'matched_empty_group_not_no_match':True}
 def grid():
  now=datetime.datetime.now(datetime.timezone.utc);end=now+datetime.timedelta(hours=8);query=urllib.parse.urlencode({'channel_profile_id':setup['profile']['id'],'start':now.isoformat(),'end':end.isoformat()});value=json.loads(request(fixture,'/api/epg/grid/?'+query,'grid',private));counts=[]
  for i,ch in enumerate(setup['channels']):
   rows=[r for r in value['data']if r['tvg_id']==ch['uuid']];assert rows;counts.append(len(rows))
   for row in rows:
    check(i,row['title'],row['description']);start=datetime.datetime.fromisoformat(row['start_time']);stop=datetime.datetime.fromisoformat(row['end_time']);assert(stop-start).total_seconds()==[181,240,240,180][i]*60
    if i==0:assert row['sub_title']==''and row['is_new']and row['is_live']and row['custom_properties']['categories']==['Sport','Live','Sport']
    if i in(0,3):assert row['custom_properties']['date']==start.date().isoformat()
    else:assert row['custom_properties']is None
   if i==0:assert datetime.datetime.fromisoformat(rows[0]['start_time'])==now and datetime.datetime.fromisoformat(rows[-1]['end_time'])>end
  return {'actual_grid_counts':counts,'explicit_microsecond_start_preserved':True,'Source_unclipped_last_custom_block':True}
 def xc():
  counts=[]
  for i,ch in enumerate(setup['channels']):
   query=urllib.parse.urlencode({'username':setup['xc_username'],'password':setup['xc_password'],'action':'get_short_epg','stream_id':ch['id'],'limit':2,'days':1,'prev_days':0});rows=json.loads(request(fixture,'/player_api.php?'+query,'XC-'+str(i),private,False))['epg_listings'];assert len(rows)==2;counts.append(len(rows))
   for row in rows:check(i,base64.b64decode(row['title'],validate=True).decode(),base64.b64decode(row['description'],validate=True).decode())
  return {'actual_short_rows':counts,'secret_in_public_capture':False}
 for name,body in [('actual-named-groups-UTC-custom-XMLTV',xml),('actual-named-groups-custom-grid',grid),('actual-named-groups-custom-XC',xc)]:
  row={'name':name,'status':'running'};cells.append(row)
  try:row.update(status='passed',actual=body())
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/(name+'-failure.txt');p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
  save(out/'report.json',{'status':'running','cases':cells})
 if fixture.backend=='Native':
  row={'name':'actual-native-regex-worker-normal-retirement','status':'running'};cells.append(row)
  try:
   def done():
    rows=[r for r in retirements(fixture)if(r['pid'],r['generation'])not in prior];return rows if len(rows)>=6 else False
   rows=fixture.wait(done,15,'actual-custom-guide-regex-retirement-receipts');save(private/'actual-retirements.json',rows);assert len(rows)==6 and all(r['actual_exit']==0 and r['wait_observed']and r['stdout_eof']and r['stderr_eof']and r['group_absent']and r['callbacks_settled']and not r['forced']and not r['operation_fault']for r in rows);owned={(r['pid'],r['generation'])for r in rows};after=process_snapshot(fixture,owned);save(private/'actual-after-processes.json',after);assert not [p for p in after if(p['pid'],p['generation'])in owned];row.update(status='passed',actual={'actual_exit0_wait_EOF_group_absence_receipts':6,'actual_remaining_owned_regex_children':0,'other_regex_children':len(after)})
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__);p=private/'retirement-failure.txt';p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
 if 'date' in setup:
  row={'name':'actual-date-only-XMLTV-grid-XC','status':'running'};cells.append(row)
  try:
   date_prior={(r['pid'],r['generation'])for r in retirements(fixture)}
   actual=check_date_guides(fixture,setup,private)
   if fixture.backend=='Native':
    def date_retired():
     rows=[r for r in retirements(fixture)if(r['pid'],r['generation'])not in date_prior]
     return rows if len(rows)>=6 else False
    rows=fixture.wait(date_retired,15,'actual-date-guide-regex-retirement-receipts')
    save(private/'actual-date-retirements.json',rows)
    assert len(rows)==6 and all(r['actual_exit']==0 and r['wait_observed']and r['stdout_eof']and r['stderr_eof']and r['group_absent']and r['callbacks_settled']and not r['forced']and not r['operation_fault']for r in rows)
    owned={(r['pid'],r['generation'])for r in rows}
    assert not process_snapshot(fixture,owned)
    actual['actual_exit0_wait_EOF_group_absence_receipts']=len(rows)
   row.update(status='passed',actual=actual)
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__)
   file=private/'date-guide-failure.txt';file.write_text(traceback.format_exc()[-8192:]);file.chmod(0o600)
 if 'time' in setup:
  import custom_guide_time_cases as timed
  row={'name':'actual-UTC-time-only-XMLTV-grid-XC','status':'running'};cells.append(row)
  try:
   time_prior={(r['pid'],r['generation'])for r in retirements(fixture)}
   actual=timed.run(fixture,setup,private,request)
   if fixture.backend=='Native':
    def time_retired():
     rows=[r for r in retirements(fixture)if(r['pid'],r['generation'])not in time_prior]
     return rows if len(rows)>=4 else False
    rows=fixture.wait(time_retired,15,'actual-time-guide-regex-retirement-receipts')
    save(private/'actual-time-retirements.json',rows)
    assert len(rows)==4 and all(r['actual_exit']==0 and r['wait_observed']and r['stdout_eof']and r['stderr_eof']and r['group_absent']and r['callbacks_settled']and not r['forced']and not r['operation_fault']for r in rows)
    assert not process_snapshot(fixture,{(r['pid'],r['generation'])for r in rows})
    actual['actual_exit0_wait_EOF_group_absence_receipts']=len(rows)
   row.update(status='passed',actual=actual)
  except Exception as error:
   row.update(status='failed',error_type=type(error).__name__)
   file=private/'time-guide-failure.txt';file.write_text(traceback.format_exc()[-8192:]);file.chmod(0o600)
 report={'status':'passed'if all(c['status']=='passed'for c in cells)else'failed','cases':cells,'full_M7_or_resource_gain':False,'unqualified':['timed date schedules, non-UTC timed settings and fractional durations','metadata/template beyond explicit budget','malformed Source rendering','EPG-download integration sideeffects','Source held HTTP cold termination counterpart','held Native shutdown unless separately invoked LAST']};save(out/'report.json',report);return report

def prepare_held_context(fixture):
 if fixture.case!=HELD_CASE or fixture.backend!='Native':raise ValueError('DedicatedFinalNativeGuideContextRequired')
 # DATA only: real profile post_save runs; inherited non-target memberships
 # are disabled explicitly. No backend task body, provider, or XC login.
 script="from apps.epg.models import EPGSource,EPGData;from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership;s=EPGSource.objects.bulk_create([EPGSource(name='Owned custom held search',source_type='dummy',is_active=False,custom_properties={'title_pattern':'(?:a|aa)+$','timezone':'UTC'})])[0];e=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-custom-held',name='Owned held')])[0];c=Channel.objects.bulk_create([Channel(name='a'*511+'!',channel_number=91890,epg_data=e,tvg_id='owned-custom-held')])[0];p=ChannelProfile.objects.create(name='Owned custom held guide');ChannelProfileMembership.objects.filter(channel_profile=p).exclude(channel_id=c.pk).update(enabled=False);ChannelProfileMembership.objects.update_or_create(channel_profile=p,channel=c,defaults={'enabled':True});members=list(ChannelProfileMembership.objects.filter(channel_profile=p,enabled=True).values_list('channel_id',flat=True));assert members==[c.pk];print(json.dumps({'held_profile':p.name,'channel_id':c.pk,'channel_uuid':str(c.uuid),'actual_enabled_members':members}))"
 setup=fixture.sql(script,secret=True);fixture._custom_guide=setup
 fixture._plugin_environment={**getattr(fixture,'_plugin_environment',{}),'RUST_EPG_REGEX_DIAGNOSTICS':'1'}
 save(Path(fixture.output)/'declared-custom-held-inputs.json',{'case':HELD_CASE,'backend':'Native','source_shutdown_counterpart':'unmeasured','data_setup':setup,'actual_Main_stop_required':True,'normal_context_reused':False})
def run_held_native(fixture):
 """FINAL standalone Native context, after all other business contexts.
 Actual Main/HTTP request/CPU/generation/TERM/worker wait+EOF, not a fake handler.
 Source shutdown-equivalence is explicitly NOT claimed by this diagnostic.
 """
 if fixture.case!=HELD_CASE or fixture.backend!='Native':raise ValueError('FinalOwnedNativeGuideContextRequired')
 out=Path(fixture.output)/'custom-guide-held';out.mkdir();private=out/'private';private.mkdir(mode=0o700);report={'status':'running','normal0claim':False,'Source_shutdown_counterpart':'unmeasured','performance_verified':False};done=threading.Event();result={};thread=None
 try:
  setup=fixture._custom_guide;path='/output/epg/'+urllib.parse.quote(setup['held_profile'],safe='')+'/?days=1&prev_days=0';prior={(r['pid'],r['generation'])for r in retirements(fixture)}|{(r['pid'],r['generation'])for r in process_snapshot(fixture)}
  info=json.loads(fixture.command([fixture.docker,'inspect',fixture.native],seconds=5,secret=True))[0];env=dict(v.split('=',1)for v in info['Config']['Env']if'='in v);seconds=int(env['RUST_SHUTDOWN_SECONDS']);assert 0<seconds<=120;assert env.get('RUST_EPG_REGEX_DIAGNOSTICS')=='1'
  def reader():
   raw=b''
   try:
    req=urllib.request.Request(fixture.base_url+path,headers={'Authorization':'Bearer '+fixture.admin});opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(req,timeout=fixture.remaining(seconds+15))as response:
     result['actual_HTTP_status']=response.status
     try:raw=response.read(524289)
     except Exception as error:raw=getattr(error,'partial',b'');result['error_type']=type(error).__name__
    result['raw']=raw
   except urllib.error.HTTPError as error:result.update(error_type=type(error).__name__,actual_HTTP_status=error.code)
   except Exception as error:result['error_type']=type(error).__name__
   finally:
    (private/'held-response.bin').write_bytes(raw);save(private/'held-response.json',{'status':result.get('actual_HTTP_status'),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'error_type':result.get('error_type')});done.set()
  thread=threading.Thread(target=reader,name='owned-custom-guide-held-observer',daemon=False);thread.start();first={}
  def active():
   if done.is_set()and result.get('actual_HTTP_status')!=200:raise AssertionError('Held guide prerequisite HTTP failed: '+str(result.get('actual_HTTP_status')))
   now=time.monotonic()
   for row in process_snapshot(fixture):
    key=(row['pid'],row['generation'])
    if key in prior:continue
    if key not in first:first[key]=(row,now)
    elif now-first[key][1]>=.25 and row['cpu_ticks']>first[key][0]['cpu_ticks']and not done.is_set()and result.get('actual_HTTP_status')==200:return {'before':first[key][0],'after':row,'interval_seconds':now-first[key][1]}
   return False
  proof=fixture.wait(active,15,'actual-held-guide-search-CPU-generation');report['actual_held_cpu']=proof;save(out/'report.json',report)
  # Begin the real existing Main stop; preserve its actual exit instead of
  # inventing a clean outcome for the unknown/cancelled regex operation.
  report['actual_Main_stop']=fixture.stop_native(expected=None,seconds=seconds+10);assert report['actual_Main_stop']['actual_exit']==1 and not report['actual_Main_stop']['oom_killed']
  thread.join(10);assert not thread.is_alive()and done.is_set();report['request_result']={'error_type':result.get('error_type'),'bytes':len(result.get('raw',b''))}
  rows=[]
  # stop_native's original docker-log stdout/stderr receipts remain private.
  for receipt in fixture.cli:
   for kind in('stdout','stderr'):
    file=Path(receipt[kind]);raw=file.read_bytes()
    if len(raw)>1048576:raise AssertionError('DeclaredHeldReceiptBudget')
    for line in raw.decode('utf8',errors='replace').splitlines():
     if 'epg_regex_retirement 'in line:
      try:row=json.loads(line.split('epg_regex_retirement ',1)[1])
      except json.JSONDecodeError:continue
      if(row.get('pid'),row.get('generation'))==(proof['after']['pid'],proof['after']['generation']):rows.append(row)
  save(private/'actual-held-retirement.json',rows);assert rows and all(r['wait_observed']and r['stdout_eof']and r['stderr_eof']and r['group_absent']and r['callbacks_settled']and r['forced']for r in rows)
  complete=False
  try:ET.fromstring(result.get('raw',b''));complete=True
  except ET.ParseError:pass
  assert not complete;report.update(status='passed',actual_worker_retired=True,unknown_search_not_no_match=True,actual_nonzero_Main=True,context_exhausted_after_this_cell=True)
 except Exception as error:
  report.update(status='failed',error_type=type(error).__name__);p=private/'failure.txt';p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
 finally:
  if thread and thread.is_alive():
   if fixture.native:report['failure_cleanup_stop']=fixture.stop_native(expected=None,seconds=20)
   thread.join(10)
   if thread.is_alive():report.update(status='failed',unsettled_observer=True)
  save(out/'report.json',report)
 return report
