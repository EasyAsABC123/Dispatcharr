"""Native combined React live/VOD PATCH compatibility; no task-body calls.
Original Dispatcharr/dependency authors retain all application credit.
"""
import copy,datetime,gzip,hashlib,importlib.util,json,traceback,uuid
from pathlib import Path
from workflow import save
import dvr_domain_cases as journal
import m3u_refresh_all_cases as aggregate
import vod_series_cases as base
from vod_series_advanced_cases import snapshot as catalogue_snapshot
from vod_series_info_cases import MODEL_IMPORT
CASE='live-vod-group-settings'
STAGES=('combined','repeat','partial-category','explicit-null','profiles','bad-start','bad-end','reversed-range','bad-category','duplicate-live','missing-live-FK','missing-category-FK')

def _prepare_declared_context(f):
 assert f.backend=='Native'and f.case==CASE
 f._vod_login={'username':uuid.uuid4().hex,'password':uuid.uuid4().hex}
 base._prepare_rows(f)
 code="""
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,ChannelProfile
from apps.vod.models import VODCategory,M3UVODCategoryRelation
from django_celery_beat.models import PeriodicTask,PeriodicTasks
from core.models import CoreSettings
PeriodicTask.objects.all().update(enabled=False);PeriodicTasks.update_changed()
s,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});s.value={**(s.value or {}),'schedule_enabled':False};s.save()
a=ACCOUNT;b=OTHER
p=ChannelProfile.objects.create(name='Owned group settings profile')
groups=ChannelGroup.objects.bulk_create([ChannelGroup(name='Owned combined live one'),ChannelGroup(name='Owned combined live two')])
for g in groups:
 ChannelGroupM3UAccount.objects.create(channel_group=g,m3u_account_id=a,enabled=True,auto_channel_sync=False,auto_sync_channel_start=1,custom_properties={'baseline':'live','channel_profile_ids':[p.id]})
ChannelGroupM3UAccount.objects.create(channel_group=groups[0],m3u_account_id=b,custom_properties={'unrelated':'must stay exact'})
categories=VODCategory.objects.bulk_create([VODCategory(name='Owned combined Movie',category_type='movie'),VODCategory(name='Owned combined Series',category_type='series')])
for c in categories:
 M3UVODCategoryRelation.objects.create(m3u_account_id=a,category=c,enabled=True,custom_properties={'language':'en','quality':'SD','baseline':'category'})
M3UVODCategoryRelation.objects.create(m3u_account_id=b,category=categories[0],enabled=False,custom_properties={'unrelated':'must stay exact'})
assert not ChannelGroup.objects.filter(pk=2147483001).exists()and not VODCategory.objects.filter(pk=2147483002).exists()
print(json.dumps({'account':a,'other':b,'groups':[g.id for g in groups],'categories':[c.id for c in categories],'profile':p.id,'missing_group':2147483001,'missing_category':2147483002}))
""".replace('ACCOUNT',str(f._vod_setup['account'])).replace('OTHER',str(f._vod_setup['disabled']))
 f._live_setup=f.sql(code,secret=True)
 save(f.output/'declared-live-vod-fixture-setup.json',f._live_setup)

LIVE_SNAPSHOT="""
from apps.channels.models import ChannelGroup,ChannelGroupM3UAccount,ChannelProfile,ChannelProfileMembership
extra=[ChannelGroup,ChannelGroupM3UAccount,ChannelProfile,ChannelProfileMembership]
assert all(m.objects.count()<=256 for m in extra)
result={'tables':{m._meta.db_table:list(m.objects.order_by('id').values())for m in extra},'sequences':{},'observed_at':__import__('django.utils.timezone',fromlist=['now']).now()}
with connection.cursor()as cursor:
 for model in extra:
  cursor.execute("SELECT pg_get_serial_sequence(%s,'id')",[model._meta.db_table]);name=cursor.fetchone()[0]
  if name:
   assert name=='public.'+model._meta.db_table+'_id_seq'
   cursor.execute('SELECT last_value,is_called FROM '+name);value=cursor.fetchone();result['sequences'][name]={'last_value':value[0],'is_called':value[1]}
print(json.dumps(result,cls=SourceExactEncoder))
"""
def snapshot(f):
 value=catalogue_snapshot(f);extra=f.sql(MODEL_IMPORT+LIVE_SNAPSHOT,secret=True)
 assert not set(extra['sequences'])&set(value['sequences'])
 value['live']=extra['tables'];value['sequences'].update(extra['sequences']);value['live_observed_at']=extra['observed_at']
 return value

def payloads(setup):
 g1,g2=setup['groups'];c1,c2=setup['categories'];profile=setup['profile']
 live=[{'channel_group':g1,'enabled':True,'auto_channel_sync':True,'auto_sync_channel_start':10.5,'auto_sync_channel_end':30.5,'custom_properties':{'channel_profile_ids':[str(profile),'bad',profile],'custom':'replace-me'}},{'channel_group':g2,'enabled':False,'auto_channel_sync':False,'auto_sync_channel_start':1.0,'auto_sync_channel_end':None,'custom_properties':{'second':True}}]
 combined={'group_settings':live,'category_settings':[{'id':c1,'enabled':True,'custom_properties':{'language':'FR'}},{'id':c2,'enabled':False,'custom_properties':{'quality':'1080p'}}]}
 rows={name:copy.deepcopy(combined)for name in STAGES}
 rows['partial-category']['category_settings']=[{'id':c1,'enabled':True,'custom_properties':{'quality':'720p'}}]
 rows['partial-category']['group_settings'][0]['custom_properties']={'channel_profile_ids':str(profile),'replacement':True}
 rows['explicit-null']['category_settings']=[{'id':c1,'enabled':False,'custom_properties':{'language':None,'quality':None}}]
 rows['explicit-null']['group_settings'][0].pop('custom_properties');rows['explicit-null']['group_settings'][0]['auto_sync_channel_start']=None;rows['explicit-null']['group_settings'][0]['auto_sync_channel_end']=None;rows['explicit-null']['group_settings'][1]['custom_properties']=None
 rows['profiles']['group_settings'][0]['custom_properties']={'channel_profile_ids':[str(profile),None,'bad',False,2.9],'preserve':'sent'}
 rows['profiles']['group_settings'][1]['custom_properties']=json.dumps({'channel_profile_ids':[],'legacy':'encoded object'})
 rows['bad-start']['group_settings'][0]['auto_sync_channel_start']=0.5
 rows['bad-end']['group_settings'][0]['auto_sync_channel_end']=0.5
 rows['reversed-range']['group_settings'][0]['auto_sync_channel_end']=9.5
 rows['bad-category']['category_settings'][0]['custom_properties']={'language':'eng'}
 rows['duplicate-live']['group_settings'].append(copy.deepcopy(live[0]))
 rows['missing-live-FK']['group_settings'][1]['channel_group']=setup['missing_group']
 rows['missing-category-FK']['category_settings'][1]['id']=setup['missing_category']
 return rows

def raw_state(value):return {k:v for k,v in value.items()if k not in('observed_at','live_observed_at','sequences')}
def live_props(raw):
 if isinstance(raw,str):
  try:raw=json.loads(raw)
  except ValueError:raw={}
 value=dict(raw)if isinstance(raw,dict)else{}
 if 'channel_profile_ids'in value:
  items=value['channel_profile_ids']
  if not isinstance(items,list):items=[]if items in(None,'')else[items]
  ids=[]
  for item in items:
   try:ids.append(int(item))
   except(TypeError,ValueError):pass
  value['channel_profile_ids']=ids
 return value

def check(setup,label,request,before,after,response):
 failed=label in STAGES[5:];expected_status=400 if failed else 200
 assert response=={'context':CASE,'method':'PATCH','path':'/api/m3u/accounts/'+str(setup['account'])+'/group-settings/','credential_role':'admin','status':expected_status,'body':response['body']}
 if failed:
  assert isinstance(response['body'],dict)and isinstance(response['body'].get('error'),str)and response['body']['error']
  assert raw_state(before)==raw_state(after),'Caught invalid save must not partly commit business/support/live rows'
  # Sequence effects are retained literally; transaction rollback cannot rewind nextval.
  return
 assert response['body']=={'message':'Group settings updated successfully'}
 expected=copy.deepcopy(before)
 for value in request['group_settings']:
  target=next(r for r in expected['live']['dispatcharr_channels_channelgroupm3uaccount']if r['m3u_account_id']==setup['account']and r['channel_group_id']==value['channel_group'])
  target.update(enabled=value.get('enabled',True),auto_channel_sync=value.get('auto_channel_sync',False),auto_sync_channel_start=value.get('auto_sync_channel_start'),auto_sync_channel_end=value.get('auto_sync_channel_end'),custom_properties=live_props(value.get('custom_properties')))
 for value in request['category_settings']:
  target=next(r for r in expected['support']['vod_m3uvodcategoryrelation']if r['m3u_account_id']==setup['account']and r['category_id']==value['id'])
  props=dict(value.get('custom_properties')or{})
  if props.get('language')is not None:props['language']=props['language'].lower()
  target.update(enabled=value.get('enabled',True),custom_properties={**(target['custom_properties']or{}),**props})
 assert raw_state(expected)==raw_state(after),'Exact Source update fields, replacements/merge, immutable clocks/IDs and unrelated rows'


def run(f):
 assert f.backend=='Native'and f.case==CASE
 out=f.output/CASE;out.mkdir();private=out/'private';private.mkdir(mode=0o700)
 status,raw=f.get(f.base_url+'/health/ready');health=json.loads(raw);save(private/'actual-Native-health.json',health);assert status==200 and health['selected_workers']['ready']and health['selected_workers']['clocked_scheduler_enabled']is False;f.wait(lambda:not any(aggregate.broker(f).values()),25,'group-settings-ready-idle-drain')
 report={'status':'running','Source_characterization':False,'Native_qualified':False,'cases':[],'full_milestone_passed':False,'performance_verified':False};save(out/'report.json',report)
 for label,request in payloads(f._live_setup).items():
  cell={'name':label,'status':'running','observations_complete':False};report['cases'].append(cell);save(out/'report.json',report);p=private/label;p.mkdir(mode=0o700)
  try:
   f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-prior-drain');before=snapshot(f);save(p/'SQL-before.json',before);assert_initial_clocks(f,before);cache_before=aggregate.cache_state(f);broker_before=aggregate.broker(f)
   receiver=capture_receiver(f,label,p);observer=journal.observer_start(f,label,database=1);response=None;detail=None
   try:
    response=f.api('PATCH','/api/m3u/accounts/'+str(f._live_setup['account'])+'/group-settings/',request)
    # React API.updateM3UGroupSettings immediately fetches the account detail.
    detail=f.api('GET','/api/m3u/accounts/'+str(f._live_setup['account'])+'/',None)
    f.wait(lambda:not any(aggregate.broker(f).values()),20,label+'-post-request-drain')
   finally:
    publications=journal.producer_observer_stop(f,observer);after=snapshot(f);events=aggregate.receiver_read(f,receiver);cache_after=aggregate.cache_state(f);broker_after=aggregate.broker(f)
    for name,value in [('request',request),('HTTP',response),('GET',detail),('SQL-before',before),('SQL-after',after),('journal',publications),('WS',events),('cache-before',cache_before),('cache-after',cache_after),('broker-before',broker_before),('broker-after',broker_after)]:save(p/(name+'.json'),value)
   cell['observations_complete']=True
   try:
    check(f._live_setup,label,request,before,after,response)
    assert detail['context']==CASE and detail['method']=='GET'and detail['credential_role']=='admin'and detail['status']==200 and detail['path']=='/api/m3u/accounts/'+str(f._live_setup['account'])+'/'
    assert not publications['publications']and not events['events'],'No publications/events may be silently filtered'
    assert cache_before==cache_after and broker_before==broker_after,'Retain actual cache/broker effects without filtering'
    cell['status']='passed'
   except AssertionError:
    cell.update(status='expectation_failed',failure_type='AssertionError');(p/'expectation-failure.txt').write_text(traceback.format_exc()[-16000:])
  except BaseException as error:
   cell.update(status='failed',failure_type=type(error).__name__);(p/'failure.txt').write_text(traceback.format_exc()[-16000:]);report['status']='failed';save(out/'report.json',report);break
  save(out/'report.json',report)
 report['all_twelve_observations_complete']=len(report['cases'])==12 and all(c['observations_complete']for c in report['cases']);report['status']='passed'if report['all_twelve_observations_complete']and all(c['status']=='passed'for c in report['cases'])else'failed';save(out/'report.json',report)
 if report['status']=='passed':
  try:
   compare_public_Source(f,private);report['Native_qualified']=True
  except BaseException as error:
   report.update(status='failed',failure_type=type(error).__name__);(private/'Source-comparison-failure.txt').write_text(traceback.format_exc()[-16000:])
  save(out/'report.json',report)
 return report

def digest(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def source_authority(f):
 if hasattr(f,'_live_source_authority'):return f._live_source_authority
 refs=f.binding['retained_source_live_vod_settings'];public=Path(refs['index']['path']).parent
 for ref in refs.values():assert digest(ref['path'])==ref['sha256']
 index=json.loads(Path(refs['index']['path']).read_text())
 for row in index['files']:
  path=public/row['path'];assert path.resolve().is_relative_to(public.resolve())and digest(path)==row['sha256']and path.stat().st_size==row['bytes']
 q=json.loads(Path(refs['qualification']['path']).read_text());identity=json.loads(Path(refs['identity']['path']).read_text());assert q['Source_twelve_manual_cells_qualified']and q['single_whole_twelve_cell_controller_exit0']is False and q['first_controller_exit']==1 and q['remaining_controller_exit']==0 and q['remaining_new_manual_cells_passed']==11 and q['known_combined_business_replayed']is False and q['declared_combined_state_import_selected_tables']==q['declared_combined_state_import_exact_sequences']==15
 assert identity['Original_git_revision']=='1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430'and identity['actual_Source_image_id']=='sha256:34f9011514edd31ef0ff111b5f256d2e37614fcb08a6ee1ed9eccec9251a856f'and identity['actual_Native_contexts']==0
 for label,code in(('Source1',1),('Source2',0)):
  execution=json.loads(Path(refs[label+'_execution']['path']).read_text());assert execution['actual_exit']==code and execution['stdout_EOF']and execution['stderr_EOF']and execution['process_group_settled']and json.loads(Path(refs[label+'_Root_cleanup']['path']).read_text())['passed']
 raw=gzip.decompress(Path(refs['projection']['path']).read_bytes());assert len(raw)==q['semantic_projection']['uncompressed_bytes']<=2*1024*1024 and hashlib.sha256(raw).hexdigest()==q['semantic_projection']['uncompressed_canonical_sha256'];oracle=json.loads(raw)
 spec=importlib.util.spec_from_file_location('public_live_settings_projection_'+digest(refs['project']['path'])[:12],refs['project']['path']);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
 api=gzip.decompress(Path(refs['API']['path']).read_bytes());assert len(api)<=2*1024*1024;observations=json.loads(api);proofs=json.loads(Path(refs['receiver_proofs']['path']).read_text());assert proofs['schema']=='ActualSourceReact12GroupAdd1'and proofs['group']=='updates'and proofs['matching_guest_sha256']==digest(Path(__file__).with_name('matching_guest.py'))
 f._live_source_authority=(refs,oracle,module,observations,proofs);return f._live_source_authority

def prepare_context(f):
 assert f.backend=='Native'and f.case==CASE
 refs,oracle,module,_,_=source_authority(f)
 before=snapshot(f);start=f.sql("from django.utils import timezone;print(json.dumps(timezone.now().isoformat()))")
 _prepare_declared_context(f)
 end=f.sql("from django.utils import timezone;print(json.dumps(timezone.now().isoformat()))");after=snapshot(f)
 assert f._live_setup==oracle['same_fixture_namespace'],'Observed exact fixture-account/group/category/profile IDs required'
 lo=module.instant(start);hi=module.instant(end);assert lo<=hi
 old=module.tables(before);new=module.tables(after);facts=[]
 for table,rows in new.items():
  prior={r['id']:r for r in old[table]}
  for row in rows:
   for key in module.CLOCKS&row.keys():
    value=row[key]
    if row['id']in prior:
     assert value==prior[row['id']][key],(table,row['id'],key,'Existing setup clock stable')
    elif value is not None:assert lo<=module.instant(value)<=hi,(table,row['id'],key,'New fixture clock causal within actual preparation')
    facts.append({'table':table,'id':row['id'],'field':key,'value':value,'prior_row':row['id']in prior})
 f._live_initial_clock_facts=facts
 f._live_credential_hashes={row['id']:{key:None if row[key]is None else hashlib.sha256(row[key].encode()).hexdigest()for key in('username','password')}for row in after['m3u_m3uaccount']}
 save(f.output/'live-vod-settings-initial-credential-digests.json',f._live_credential_hashes)
 save(f.output/'live-vod-settings-initial-clock-proof.json',{'actual_setup_start':start,'actual_setup_end':end,'same_observed_fixture_namespace':True,'all_initial_clock_facts':facts,'future_API_clock_drift_not_normalized':True})

def assert_initial_clocks(f,snapshot):
 _,_,module,_,_=source_authority(f);values=module.tables(snapshot)
 for fact in f._live_initial_clock_facts:
  row=next(r for r in values[fact['table']]if r['id']==fact['id']);assert row[fact['field']]==fact['value'],(fact['table'],fact['id'],fact['field'],'No startup/API clock drift')
 for row in snapshot['m3u_m3uaccount']:
  for key in('username','password'):
   digest_value=None if row[key]is None else hashlib.sha256(row[key].encode()).hexdigest();assert digest_value==f._live_credential_hashes[row['id']][key],(row['id'],key,'No startup/API raw credential drift')

def capture_receiver(f,label,path):
 from vod_category_language_compare import capture_receiver as capture
 receiver=aggregate.receiver_begin(f,label);proof=capture(f,receiver);save(path/'receiver-begin-proof.json',proof);return receiver

def compare_public_Source(f,directory):
 refs,oracle,module,source_api,source_receivers=source_authority(f);directory=Path(directory)
 first=json.loads((directory/STAGES[0]/'SQL-before.json').read_text());codec=module.State(first);phases={};previous=None;native_names=set();source_names=set()
 from vod_dense_list_compare import exact
 from vod_category_language_compare import validate_receiver_proof
 for label in STAGES:
  p=directory/label;before=json.loads((p/'SQL-before.json').read_text());after=json.loads((p/'SQL-after.json').read_text())
  if previous is not None:exact(module.state(before),module.state(previous),'Contiguous Native API12 raw state chain; no Source continuation seed replay')
  assert_initial_clocks(f,before);assert_initial_clocks(f,after)
  ws=json.loads((p/'WS.json').read_text());exact(set(ws),{'actual_receiver','events','raw_events_base64'},'Exact current receiver shape');exact(ws['events'],[],'No filtered events');exact(ws['raw_events_base64'],[],'No filtered raw events')
  source_ws=source_api[label]['WS'];exact(set(source_ws),{'actual_receiver','events','raw_events_base64'},'Exact retained Source receiver shape');exact(source_ws['events'],[],'Source empty events');exact(source_ws['raw_events_base64'],[],'Source empty raw events')
  sname=validate_receiver_proof(source_receivers['proofs'][label],source_ws['actual_receiver']);nproof=json.loads((p/'receiver-begin-proof.json').read_text());nname=validate_receiver_proof(nproof,ws['actual_receiver'],True,f.output)
  assert sname not in source_names and nname not in native_names and nname not in{proof['receiver']for proof in source_receivers['proofs'].values()},'Fresh distinct actual completed receiver UUIDs'
  source_names.add(sname);native_names.add(nname)
  journal_value=json.loads((p/'journal.json').read_text());exact(journal_value['publications'],[],'No filtered task publications')
  exact(json.loads((p/'cache-before.json').read_text()),json.loads((p/'cache-after.json').read_text()),'Selected settings cache stable');exact(json.loads((p/'broker-before.json').read_text()),json.loads((p/'broker-after.json').read_text()),'Broker state stable')
  phases[label]={'request':json.loads((p/'request.json').read_text()),'HTTP':json.loads((p/'HTTP.json').read_text()),'GET':codec.get(json.loads((p/'GET.json').read_text()),after),'before':codec.canonical(before),'after':codec.canonical(after),'WS':{'actual_receiver':{'owned_aggregate_receiver_uuid4':label},'events':[],'raw_events_base64':[]},'producer_publications':journal_value['publications'],'cache_unchanged':True,'broker_unchanged':True};previous=after
 exact(oracle['phases'],phases,'Full public Source API12 rows/PK/all15seq/HTTP/GET/errors/CP/ranges/profile semantics')
 save(directory.parent/'Source-comparison.json',{'status':'passed','actual_Source_manual_cells':12,'actual_Native_manual_cells':12,'Source_controllers_literal_exits':[1,0],'Source_single_whole12_controller0':False,'known_Source_combined_replayed':False,'Native_twelve_contiguous_requests':True,'no_Native_continuation_seed_imported':True,'all_business_rows_PK_all15sequences_exact':True,'exact_source_PG_error_DETAIL_HINT_text':True,'initial_clocks_causal_and_raw_stable_before_projection':True,'raw_owned_credentials_initial_stable':True,'actual_completed_owned_receiver_proofs':12,'public_safe_Source_authority_only':True,'private_Source_capture_files_opened':False,'Source_public_index_sha256':refs['index']['sha256'],'performance_verified':False,'full_milestone_accepted':False})
