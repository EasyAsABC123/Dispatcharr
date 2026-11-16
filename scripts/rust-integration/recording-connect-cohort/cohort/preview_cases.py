"""Direct profile preview cells inside the existing redirect-common fixture.
No startup/fixture on import. Original Dispatcharr authors retain full credit.
"""
import hashlib,json,urllib.request,urllib.error,uuid
from pathlib import Path
from output_profile_reader import Reader

def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
class NoRedirect(urllib.request.HTTPRedirectHandler):
 def redirect_request(self,*args,**kwargs):return None

def run(f):
 out=f.output/'preview-capture';out.mkdir(mode=0o755);report={'status':'running','cells':[],'full_M3_passed':False,'performance_verified':False,'scope':'direct TS HTTP preview; no HLS cross-request epoch claim'}
 seed=f._redirect_seed;hash_id='owned-preview-'+uuid.uuid4().hex
 original=f.sql("from apps.channels.models import Stream;s=Stream.objects.get(pk="+str(seed['stream'])+");print(json.dumps({'stream_profile':s.stream_profile_id,'hash':s.stream_hash,'url':s.url}))")
 f.sql("from apps.channels.models import Stream;from core.models import StreamProfile;Stream.objects.filter(pk="+str(seed['stream'])+").update(stream_profile=StreamProfile.objects.get(name='Proxy',locked=True),stream_hash="+repr(hash_id)+",url="+repr(seed['media_url'])+");print(json.dumps({'actual_owned_preview_row_seeded':True}))")
 def persist():save(out/'report.json',report)
 def cell(name,operation):
  row={'name':name,'status':'running'};report['cells'].append(row);persist()
  try:row['observation']=operation();row['status']='passed'
  except Exception as error:row.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:160])
  persist()
 def snap(worker,scoped):
  # Fixture.redis uses zero KEYS by contract; key is a bounded literal argument.
  key='stream_profile:'+str(seed['stream'])+(':p'+str(seed['profile'])if scoped else':preview')
  result=f.redis(0,"return {redis.call('GET',ARGV[1])or'',redis.call('SCARD','live:channel:'..ARGV[2]..':clients'),redis.call('GET','profile_connections:'..ARGV[3])or'0',redis.call('GET','channel_stream:'..ARGV[4])or'',redis.call('GET','stream_profile:'..ARGV[5])or'',redis.call('GET','live:channel:'..ARGV[2]..':owner')or''}",key,worker,str(seed['profile']),str(f.channel_id),str(seed['stream']))
  return {'preview_alias':result[0],'clients':result[1],'provider_count':int(result[2]),'bare_channel_alias':result[3],'bare_stream_alias':result[4],'owner':result[5]}
 def headers(path):
  request=urllib.request.Request(f.base_url+path,headers={'Authorization':'Bearer '+f.actual_admin_token()})
  try:response=urllib.request.build_opener(NoRedirect()).open(request,timeout=f.remaining(12))
  except urllib.error.HTTPError as error:response=error
  with response:return {'status':response.status,'location_present':bool(response.headers.get('Location'))}
 def decode(path,label):
  local='/evidence/'+str(path.relative_to(f.output));probe=json.loads(f.command(f.compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json',local],30))
  progress=f.command(f.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-threads','1','-filter_threads','1','-filter_complex_threads','1','-progress','pipe:1','-nostats','-i',local,'-t','3','-map','0:v:0','-map','0:a:0','-threads:v','1','-threads:a','1','-f','null','-'],30,allow=True)
  actual=f.cli[-1];stderr=Path(actual['stderr']).read_bytes();frames=max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0)
  observed={'actual_exit':actual['actual_exit'],'stdout_EOF':actual['stdout_EOF'],'stderr_EOF':actual['stderr_EOF'],'process_group_settled':actual['process_group_settled'],'stderr_bytes':len(stderr),'stderr_sha256':hashlib.sha256(stderr).hexdigest(),'video_frames':frames,'video_and_audio':{'video','audio'}<={s['codec_type']for s in probe['streams']},'file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'file_bytes':path.stat().st_size,'scope':'existing strict3s A/V prefix; not full-window performance useful-work'};save(out/(label+'-decoder.json'),observed)
  if not(observed['actual_exit']==0 and observed['stdout_EOF']and observed['stderr_EOF']and observed['process_group_settled']and not stderr and frames>=70 and observed['video_and_audio']):raise AssertionError('ActualPreviewThreeSecondAVRequired')
  return observed
 def viewers(scoped):
  worker=hash_id+('.p'+str(seed['profile'])if scoped else'');label='scoped'if scoped else'default';readers=[];before=json.loads(f.get(f.origin+'/stats')[1]);proof={}
  try:
   url=f.base_url+'/proxy/ts/stream/'+(f.channel_uuid+'?skip_redirect=TRUE'if scoped else hash_id)
   for n in range(2):
    reader=Reader(url,out/(label+'-'+str(n)+'.ts'),f.actual_admin_token());readers.append(reader);f.wait(lambda:reader.snapshot()['media_bytes']>=750000 and not reader.snapshot()['errors'],35,'actual-preview-useful-TS')
   actual=snap(worker,scoped);origin=json.loads(f.get(f.origin+'/stats')[1]);proof={'actual':actual,'origin_new_connections':origin['connections']-before['connections'],'origin_active':origin['active']};save(out/(label+'-active.json'),proof)
   if actual['preview_alias']!=str(seed['profile'])or actual['clients']!=2 or actual['provider_count']!=1 or not actual['owner']or actual['bare_channel_alias']or actual['bare_stream_alias']or proof['origin_new_connections']!=1 or proof['origin_active']!=1:raise AssertionError('ActualPreviewScopeSingleSlotAndUpstream')
   if scoped:
    direct=headers('/proxy/ts/stream/'+worker);plain=headers('/proxy/ts/stream/'+f.channel_uuid);proof['direct_minted_id']=direct;proof['ordinary_Redirect_at_capacity']=plain;save(out/(label+'-active.json'),proof)
    if direct['status']!=404 or plain['status']!=503 or snap(worker,True)['clients']!=2:raise AssertionError('OrdinaryRedirectCannotAttachToPreview')
   readers[0].close();save(out/(label+'-first-reader.json'),readers[0].snapshot());f.wait(lambda:snap(worker,scoped)['clients']==1,10,'actual-selective-preview-reader-retirement');start=readers[1].snapshot()['media_bytes'];f.wait(lambda:readers[1].snapshot()['media_bytes']>=start+188000 and not readers[1].snapshot()['errors'],10,'actual-preview-sibling-survives')
  except BaseException as error:save(out/(label+'-primary-failure.json'),{'error_type':type(error).__name__,'error_label':str(error)[:160]});raise
  finally:
   for n,reader in enumerate(readers):reader.close();save(out/(label+'-reader-final-'+str(n)+'.json'),reader.snapshot())
   stopped=f.api('POST','/proxy/ts/stop/'+worker,{},'admin');save(out/(label+'-stop.json'),stopped)
   f.wait(lambda:json.loads(f.get(f.origin+'/stats')[1])['active']==0 and not snap(worker,scoped)['owner']and not snap(worker,scoped)['preview_alias']and snap(worker,scoped)['provider_count']==0,30,'actual-preview-process-slot-retirement')
  proof['decoders']=[decode(out/(label+'-'+str(n)+'.ts'),label+'-'+str(n))for n in range(2)];return proof
 try:
  cell('privileged-scoped-Redirect-preview-sharing-policy-and-retirement',lambda:viewers(True))
  if json.loads(f.get(f.origin+'/stats')[1])['active']==0:
   cell('default-Stream-preview-sharing-isolated-from-bare-Channel-aliases',lambda:viewers(False))
  else:report['cells'].append({'name':'default-Stream-preview-sharing-isolated-from-bare-Channel-aliases','status':'blocked','reason':'prior preview upstream did not retire'});persist()
  def fallback():
   proxy=f.sql("from core.models import StreamProfile;p=StreamProfile.objects.get(name='Proxy',locked=True);changed=StreamProfile.objects.filter(pk=p.pk,name='Proxy',locked=True).update(name='owned-absent-proxy-template');print(json.dumps({'id':p.pk,'actual_fixture_only_template_rows_changed':changed}))")
   try:
    save(out/'missing-Proxy-fixture-input.json',proxy)
    if proxy['actual_fixture_only_template_rows_changed']!=1:raise AssertionError('ActualOneOwnedLockedProxyTemplateFixtureMutation')
    answer=headers('/proxy/ts/stream/'+f.channel_uuid+'?skip_redirect=true');save(out/'missing-Proxy-fallback.json',answer)
    if answer['status']!=302 or not answer['location_present']:raise AssertionError('MissingLockedProxyMustFallBackToChannelRedirect')
    return answer
   finally:f.sql("from core.models import StreamProfile;StreamProfile.objects.filter(pk="+str(proxy['id'])+").update(name='Proxy');print(json.dumps({'actual_proxy_template_restored':True}))")
  if json.loads(f.get(f.origin+'/stats')[1])['active']==0:
   cell('missing-locked-Proxy-Channel-skip-falls-back-to-real-Redirect',fallback)
  else:report['cells'].append({'name':'missing-locked-Proxy-Channel-skip-falls-back-to-real-Redirect','status':'blocked','reason':'prior preview upstream did not retire'});persist()
 finally:
  if not snap(hash_id,False)['owner']and not snap(hash_id+'.p'+str(seed['profile']),True)['owner']:
   f.sql("from apps.channels.models import Stream;Stream.objects.filter(pk="+str(seed['stream'])+").update(stream_profile_id="+repr(original['stream_profile'])+",stream_hash="+repr(original['hash'])+",url="+repr(original['url'])+");print(json.dumps({'actual_owned_stream_restored':True}))")
  else:report['row_restore_blocked_by_actual_live_preview_owner']=True;persist()
 report['status']='passed'if len(report['cells'])==3 and all(c['status']=='passed'for c in report['cells'])else'failed';persist();return report
