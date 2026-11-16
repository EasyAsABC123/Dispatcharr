"""Actual XMLTV output of ordinary configured XML/SD source properties.
Add prepare/run to the SAME schedules-direct context. No app on import.
Original Dispatcharr authors retain all application/formatting algorithm credit.
"""
import hashlib,json,traceback,urllib.request,urllib.error,urllib.parse,xml.etree.ElementTree as ET
from pathlib import Path
CASE='schedules-direct'
def save(path,value):path.write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')
def prepare_context(fixture):
 if fixture.case!=CASE:return
 fixture._xmltv_properties_output=fixture.sql("from apps.epg.models import EPGSource,EPGData,ProgramData;from apps.channels.models import Channel,ChannelProfile,ChannelProfileMembership;from django.utils import timezone;from datetime import timedelta;sources=EPGSource.objects.bulk_create([EPGSource(name='Owned output XML properties',source_type='xmltv',is_active=False,custom_properties={'auto_apply_epg_logos':False}),EPGSource(name='Owned output SD properties',source_type='schedules_direct',is_active=False,custom_properties={'lineup':'owned-output-input','auto_apply_epg_logos':False})]);epgs=EPGData.objects.bulk_create([EPGData(epg_source=s,tvg_id='owned-output-'+str(i),name='Owned ordinary output '+str(i))for i,s in enumerate(sources)]);channels=Channel.objects.bulk_create([Channel(name='Owned configured guide '+str(i),channel_number=90200+i,epg_data=e,tvg_id='owned-output-'+str(i))for i,e in enumerate(epgs)]);profile=ChannelProfile.objects.create(name='Owned ordinary source output');ChannelProfileMembership.objects.filter(channel_profile=profile).exclude(channel_id__in=[c.pk for c in channels]).update(enabled=False)\nfor c in channels:\n ChannelProfileMembership.objects.update_or_create(channel_profile=profile,channel=c,defaults={'enabled':True})\nactual_members=list(ChannelProfileMembership.objects.filter(channel_profile=profile,enabled=True).order_by('channel_id').values_list('channel_id',flat=True));assert actual_members==sorted(c.pk for c in channels),actual_members\nstart=timezone.now()+timedelta(hours=1);ProgramData.objects.bulk_create([ProgramData(epg=e,start_time=start,end_time=start+timedelta(minutes=30),title='Owned configured programme '+str(i),description='Stored guide output; provider config is not a dummy regex',tvg_id=e.tvg_id)for i,e in enumerate(epgs)]);print(json.dumps({'profile':profile.name,'sources':[s.pk for s in sources],'channels':[c.pk for c in channels]}))",secret=True)
 save(Path(fixture.output)/'declared-ordinary-XMLTV-properties-output.json',{'profile':fixture._xmltv_properties_output['profile'],'source_types':['xmltv','schedules_direct'],'properties_are_not_custom_dummy_configuration':True,'Source_and_Native_new_output_observation_required':True})
def run(fixture):
 if fixture.case!=CASE:raise ValueError('OwnedSDContextRequired')
 out=Path(fixture.output)/'xmltv-source-properties-output';out.mkdir();private=out/'private';private.mkdir(mode=0o700);row={'name':'actual-generated-XMLTV-stored-XML-and-SD-properties','status':'running'}
 try:
  path='/output/epg/'+urllib.parse.quote(fixture._xmltv_properties_output['profile'],safe='')+'/'
  # Keep the existing fixture API JSON method unchanged. This one protocol
  # request reads a declared64KiB XML entity, with actual HTTP status/body first.
  request=urllib.request.Request(fixture.base_url+path,headers={'Authorization':'Bearer '+fixture.admin})
  opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
  try:response=opener.open(request,timeout=fixture.remaining(30))
  except urllib.error.HTTPError as error:response=error
  with response:
   raw=response.read(65537);status=response.status;content_type=response.headers.get('Content-Type','')
  (private/'actual-response.bin').write_bytes(raw);save(private/'actual-response.json',{'status':status,'content_type':content_type,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'request_path':path,'credential_role':'fixture-admin; token excluded'})
  assert status==200 and len(raw)<=65536
  root=ET.fromstring(raw);assert root.tag=='tv'
  channels=root.findall('channel');programmes=root.findall('programme')
  assert len(channels)==2 and sorted(c.findtext('display-name')for c in channels)==['Owned configured guide 0','Owned configured guide 1']
  assert len(programmes)==2 and sorted(p.findtext('title')for p in programmes)==['Owned configured programme 0','Owned configured programme 1']
  assert all(p.findtext('desc')=='Stored guide output; provider config is not a dummy regex'for p in programmes)
  row.update(status='passed',actual={'HTTP_status':status,'channel_count':2,'stored_programme_count':2,'private_entity_sha256':hashlib.sha256(raw).hexdigest(),'Source_byte_parity_claimed':False})
 except Exception as error:
  row.update(status='failed',error_type=type(error).__name__);p=private/'failure.txt';p.write_text(traceback.format_exc()[-8192:]);p.chmod(0o600)
 report={'status':row['status'],'cases':[row],'full_M7_parity':False,'performance_verified':False,'unqualified':['genuine custom dummy regex/template generation','stream-name lookup properties','truthy malformed JSON source-properties error rendering']};save(out/'report.json',report);return report
