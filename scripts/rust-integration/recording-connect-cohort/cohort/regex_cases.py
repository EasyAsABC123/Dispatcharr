"""Additive regex acceptance in the SAME owned auto-sync fixture/Origin.
Original Dispatcharr/regex/dependency authors retain all algorithm credit.
No runtime on import; no Source task-function replay or diagnostic app route.
"""
import json,time,traceback
from pathlib import Path

def save(path,value):Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n')

def prepare_context(fixture):
 if fixture.case!='auto-sync':return
 setup=fixture.sql('''
from apps.m3u.models import M3UAccount,M3UAccountProfile
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,ChannelProfile
account=M3UAccount.objects.bulk_create([M3UAccount(name='Owned Regex',account_type='STD',server_url='http://127.0.0.1:18098/regex.m3u',stale_stream_days=0,custom_properties={'hash_key':'url,m3u_id','auto_enable_new_groups_live':True,'orphan_channel_cleanup':'never'})])[0]
M3UAccountProfile.objects.bulk_create([M3UAccountProfile(m3u_account=account,name='Default',is_default=True,is_active=True)])
group=ChannelGroup.objects.create(name='Owned Regex')
profile=ChannelProfile.objects.create(name='Owned Regex profile')
relation=ChannelGroupM3UAccount.objects.create(channel_group=group,m3u_account=account,enabled=True,auto_channel_sync=True,auto_sync_channel_start=30000,custom_properties={'channel_numbering_mode':'fixed','channel_sort_order':'name','channel_profile_ids':[profile.pk]})
print(json.dumps({'account':account.pk,'group':group.pk,'relation':relation.pk,'profile':profile.pk}))
''',secret=True)
 fixture._regex_setup=setup;save(Path(fixture.output)/'regex-inputs.json',setup)

class Cases:
 def __init__(self,fixture):
  self.f=fixture;self.setup=fixture._regex_setup
  self.out=Path(fixture.output)/fixture.case/'regex';self.out.mkdir(parents=True,exist_ok=False)
  self.rows=[];self.source='new Source authority; no previously qualified task replay'
  self.prefix=fixture._auto_backend_prefix
 def backend(self,task):
  raw=self.f.redis(2,"local n=redis.call('STRLEN',ARGV[1]);if n>1048576 then return redis.error_reply('regex result capture capacity')end;return redis.call('GET',ARGV[1])",self.prefix+task)
  return None if raw is None else json.loads(raw)
 def snapshot(self):
  s=self.setup
  return self.f.sql("from apps.channels.models import Channel,ChannelStream,ChannelProfileMembership,Stream;from apps.m3u.models import M3UAccount;a=M3UAccount.objects.get(pk="+str(s['account'])+");rows=list(Channel.objects.filter(channel_group_id="+str(s['group'])+").order_by('channel_number','id').values('id','uuid','name','channel_number'));print(json.dumps({'account_status':a.status,'last_message':a.last_message,'rows':rows,'account_streams':list(Stream.objects.filter(m3u_account_id="+str(s['account'])+").order_by('id').values('id','m3u_account_id','stream_hash','name','channel_group_id')),'stream_links':ChannelStream.objects.filter(channel_id__in=[r['id']for r in rows]).count(),'memberships':ChannelProfileMembership.objects.filter(channel_id__in=[r['id']for r in rows],channel_profile_id="+str(s['profile'])+",enabled=True).count()},default=str))",secret=True)
 def configure(self,config):
  self.f.sql("from apps.channels.models import ChannelGroupM3UAccount;r=ChannelGroupM3UAccount.objects.get(pk="+str(self.setup['relation'])+");p={'channel_numbering_mode':'fixed','channel_sort_order':'name','channel_profile_ids':"+repr([self.setup['profile']])+"};p.update("+repr(config)+");r.custom_properties=p;r.save(update_fields=['custom_properties']);print(json.dumps({'actual_configuration_written':True}))",secret=True)
 def publish(self,label,config,wait=True):
  self.configure(config);before=self.snapshot();save(self.out/(label+'-before.json'),before)
  journal=self.f.case_data/'auto-publications.jsonl';offset=journal.stat().st_size
  response=self.f.api('POST','/api/m3u/refresh/'+str(self.setup['account'])+'/',{});save(self.out/(label+'-API.json'),response)
  if response['status']!=202:raise AssertionError('ActualRegexRefreshAPI')
  found=[]
  def actual_parent():
   nonlocal offset
   with journal.open('rb')as stream:stream.seek(offset);raw=stream.read(262145)
   if len(raw)>262144:raise RuntimeError('RegexOwnJournalSuffixBudget')
   end=raw.rfind(b'\n')+1;offset+=end
   for line in raw[:end].splitlines():
    row=json.loads(line);h=row.get('headers')or{}
    if h.get('task')=='apps.m3u.tasks.refresh_single_m3u_account':found.append(row)
   if len(found)>1:raise RuntimeError('AmbiguousRegexProducerIdentity')
   return found[0]if found else False
  frame=self.f.wait(actual_parent,20,'actual-regex-API-publication');save(self.out/(label+'-publication.json'),frame);task=frame['headers']['id']
  if not wait:return task
  def terminal():
   value=self.backend(task)
   return value if value and value.get('status')in('SUCCESS','FAILURE','REVOKED')else False
  result=self.f.wait(terminal,60,'actual-regex-parent-terminal');save(self.out/(label+'-backend.json'),result)
  if result.get('status')not in('SUCCESS','FAILURE','REVOKED'):raise AssertionError('RegexTerminalStateRequired')
  after=self.snapshot();save(self.out/(label+'-after.json'),after)
  return {'parent':task,'backend':result,'before':before,'after':after}
 def cell(self,name,work):
  row={'name':name,'status':'running'};self.rows.append(row);self.report()
  try:row.update(actual=work(),status='passed')
  except BaseException as error:
   row.update(status='failed',error_type=type(error).__name__);(self.out/(name+'-failure.txt')).write_text(traceback.format_exc()[-8192:])
  self.report()
 def report(self,finished=False):
  result={'status':('passed'if all(r['status']=='passed'for r in self.rows)else'failed')if finished else'running','cases':self.rows,'Source_authority':self.source,'full_regex_parity':False,'performance_verified':False,'startup_handshake_drain':'separate component scope, not naturally injectable through Main','required_unmeasured':['exact full progress/Channels failure-detail payloads','held search physical cancellation diagnostic','component startup/drain','complete arbitrary regex grammar coverage']}
  save(self.out/'report.json',result);return result
 def normal(self):
  def grammar():
   value=self.publish('grammar',{'name_match_regex':r'(?V1)^(?:Auto (?P<n>\d{5})|Straße)$','name_match_exclude_regex':r'(?V1)^(?=Auto)Auto 00002$','name_regex_pattern':r'(?V1)^(?|Auto (\d{5})|(Straße))$','name_replace_pattern':'Selected $1'})
   after=value['after'];assert len(after['account_streams'])==5 and all(r['m3u_account_id']==self.setup['account']for r in after['account_streams']);assert sorted(r['name']for r in after['rows'])==['Selected 00001','Selected Straße']
   assert after['stream_links']==2 and after['memberships']==2
   return value
  def invalid():
   value=self.publish('invalid-filter',{'name_match_regex':'('});assert len(value['after']['rows'])==5;return value
  def timeout():
   value=self.publish('rename-timeout',{'name_match_regex':'^a','name_regex_pattern':r'(?:a|aa)+$','name_replace_pattern':'changed'})
   expected={'Auto 00001','Auto 00002','a'*511+'b'};assert set(r['name']for r in value['after']['rows'])==expected
   before={(r['id'],r['uuid'],r['name'])for r in value['before']['rows']if r['name']in expected};after={(r['id'],r['uuid'],r['name'])for r in value['after']['rows']};assert before==after and len(after)==3
   value['actual_Source_ignorecase_selection']=3;value['caught_timeout_exception_kind_observed']=False;value['Source_rename_timeout_seconds_from_code']=0.1;return value
  self.cell('actual-full-grammar-search-include-exclude-branchreset-rename',grammar)
  self.cell('actual-invalid-filter-skipped-preserves-source-list-distinction',invalid)
  # These are NEW error characterizations. Do not invent their containing task
  # status/SQL result; the paired read-only qualifier compares reached effects.
  def filter_typeerror():
   value=self.publish('filter-TypeError',{'name_match_regex':12});assert value['before']['rows']==value['after']['rows'];assert 'Auto-sync error:'in value['after']['last_message'];assert value['backend']['status']=='SUCCESS'and value['after']['account_status']=='success';return value
  self.cell('actual-nonstring-filter-error-boundary',filter_typeerror)
  self.cell('actual-rename100ms-timeout-fallback-final512',timeout)
  def replacement_typeerror():
   value=self.publish('rename-TypeError',{'name_match_regex':'^Auto','name_regex_pattern':'.','name_replace_pattern':12});assert '2 failed'in value['after']['last_message'];assert value['backend']['status']=='SUCCESS'and value['after']['account_status']=='success'
   expected={(r['id'],r['uuid'],r['name'])for r in value['before']['rows']if r['name']in('Auto 00001','Auto 00002')};after={(r['id'],r['uuid'],r['name'])for r in value['after']['rows']};assert len(expected)==2 and after==expected
   value['existing_selected_ID_UUID_names_preserved']=True;value['renumber_fields_allowed_to_change_before_rename']=True;return value
  self.cell('actual-nonstring-replacement-perstream-error-boundary',replacement_typeerror)
  return self.report(True)

def run(fixture):
 cases=Cases(fixture);fixture._regex_cases=cases;return cases.normal()
