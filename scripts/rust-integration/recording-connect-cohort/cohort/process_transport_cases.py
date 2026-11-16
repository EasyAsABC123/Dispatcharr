"""Actual UDP Proxy auto-promotion, shared demand and retirement.
Joins the existing Fixture; no app/container/bootstrap on import.
"""
import hashlib,json,uuid,time
from certified_media import certify
from complete_units import Index
from window_offsets import offsets
from pathlib import Path
from output_profile_reader import Reader

def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def prepare_context(f):
 # A real owned ephemeral UDP port in this fixture's shared network namespace.
 port=json.loads(f.python("import json,socket;s=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);s.bind(('127.0.0.1',0));print(json.dumps(s.getsockname()[1]));s.close()"))
 if not 1024<=port<=65535:raise AssertionError('OwnedEphemeralUDPPort')
 uri='udp://127.0.0.1:'+str(port)+'?overrun_nonfatal=1&fifo_size=28672'
 seed=f.sql("from apps.channels.models import Channel,ChannelStream,Stream;from core.models import StreamProfile;c=Channel.objects.get(pk="+str(f.channel_id)+");s=c.streams.order_by('channelstream__order').first();ChannelStream.objects.filter(channel=c).exclude(stream=s).delete();c.stream_profile=StreamProfile.objects.get(name='Proxy',locked=True);c.save(update_fields=['stream_profile']);s.url="+repr(uri)+";s.save(update_fields=['url']);print(json.dumps({'stream':s.pk,'only_assigned_stream':list(c.streams.values_list('pk',flat=True)),'input_profile':'locked Proxy','URI_sha256':__import__('hashlib').sha256(s.url.encode()).hexdigest()}))")
 if seed['only_assigned_stream']!=[seed['stream']]:raise AssertionError('NoHTTPFallbackCanQualifyUDP')
 f._transport_udp={'port':port,'URI':uri,'seed':seed};save(f.output/'process-transport-seed.json',{'port':port,'seed':seed,'scope':'owned shared namespace only; no host port publication'})

def run(f):
 out=f.output/'process-transport-capture';out.mkdir(mode=0o755);report={'status':'running','scope':'new actual Source/Native UDP Proxy auto-promotion; RTSP/RTP media remain unmeasured','full_M3_passed':False,'performance_verified':False};readers=[];state='/shared/'+f.case+'/udp-origin';config=f._transport_udp
 def persist():save(out/'report.json',report)
 def metadata():
  value=f.redis(0,"return {redis.call('SCARD','live:channel:'..ARGV[1]..':clients'),redis.call('GET','profile_connections:'..ARGV[2])or'0',redis.call('GET','live:channel:'..ARGV[1]..':owner')or'',redis.call('HGET','live:channel:'..ARGV[1]..':metadata','url')or''}",f.channel_uuid,str(f.provider))
  return {'clients':value[0],'provider':int(value[1]),'owner_present':bool(value[2]),'selected_URI_sha256':hashlib.sha256(value[3].encode()).hexdigest()}
 def receivers():
  cid=f.native if f.backend=='Native'else f.command(f.compose+['ps','-q','baseline']).strip();text=f.command([f.docker,'top',cid,'-eo','pid,args'])
  return [line for line in text.splitlines()[1:]if'ffmpeg'in line and'-i '+config['URI']in line]
 try:
  reader=Reader(f.base_url+'/proxy/ts/stream/'+f.channel_uuid,out/'owner.ts',f.actual_admin_token());readers.append(reader)
  def bound():
   return json.loads(f.python("from pathlib import Path;import json;rows=[v.split()for v in Path('/proc/net/udp').read_text().splitlines()[1:]];print(json.dumps([{'local':v[1],'uid':int(v[7])}for v in rows if int(v[1].split(':')[1],16)=="+str(config['port'])+"]))"))
  bound_socket=f.wait(bound,8,'actual-process-backed-UDP-socket');report['actual_receiver_socket']=bound_socket;persist()
  if len(bound_socket)!=1 or bound_socket[0]['uid']!=65532:raise AssertionError('ActualSourceNativeInputUIDSocket')
  f.python("import os;from pathlib import Path;p=Path("+repr(state)+");p.mkdir(mode=0o755);os.chown(p,65532,65532)")
  f.role('promotion-origin',['/dispatcharrpy/bin/python','/normal-dvr-tools/promotion_origin.py'],extra={'PROMOTION_ROOT':state,'PROMOTION_TRANSPORT':'udp','PROMOTION_UDP_PORT':str(config['port'])})
  report['actual_publisher_role']=f.roles['promotion-origin'];persist()
  for n in range(2):
   if n:reader=Reader(f.base_url+'/proxy/ts/stream/'+f.channel_uuid,out/'follower.ts',f.actual_admin_token());readers.append(reader)
   f.wait(lambda:reader.snapshot()['media_bytes']>=750000 and not reader.snapshot()['errors'],35,'actual-UDP-promoted-media')
  # Freeze a new window only AFTER both real readers have nonnull media.
  # Keep all prefix bytes. Five wall seconds allow >=3 complete A/V seconds;
  # the certificate still rejects any insufficient/missing/internal units.
  starts=[r.snapshot()['media_bytes']for r in readers];window_start=time.monotonic()
  report['certified_window']={'start_monotonic':window_start,'start_offsets':starts,'measurement_seconds':5,'maximum_post_window_drain_seconds':5};persist()
  f.wait(lambda:time.monotonic()-window_start>=5 and all(r.snapshot()['media_bytes']>n for r,n in zip(readers,starts)),8,'actual-UDP-fresh-five-second-window')
  ends=[r.snapshot()['media_bytes']for r in readers];window_end=window_start+5
  report['certified_window'].update(end_monotonic=window_end,end_observation_monotonic=time.monotonic(),end_offsets=ends);persist()
  # Fixed bounded drain, not an adaptive attempt to find a passing boundary.
  f.wait(lambda:time.monotonic()-window_end>=5,6,'actual-UDP-fixed-five-second-drain')
  report['certified_window']['drain_offsets']=[r.snapshot()['media_bytes']for r in readers];report['certified_window']['drain_end_monotonic']=time.monotonic();persist()
  report['actual']=metadata();report['actual_receiver_processes']=receivers();persist()
  if report['actual']['clients']!=2 or report['actual']['provider']!=1 or not report['actual']['owner_present']or report['actual']['selected_URI_sha256']!=config['seed']['URI_sha256']or len(report['actual_receiver_processes'])!=1:raise AssertionError('ActualUDPInputSharedOneSlotAndReceiver')
  if any('user_agent'in row or'user-agent'in row for row in report['actual_receiver_processes']):raise AssertionError('SourceUDPMustRemoveHTTPUserAgent')
  readers[0].close();save(out/'owner-retired.json',readers[0].snapshot());f.wait(lambda:metadata()['clients']==1,10,'actual-UDP-selective-viewer-retirement');before=readers[1].snapshot()['media_bytes'];f.wait(lambda:readers[1].snapshot()['media_bytes']>=before+188000 and not readers[1].snapshot()['errors'],10,'actual-UDP-sibling-continues')
  report['business_checks_passed']=True
 except BaseException as error:report['primary_failure']={'type':type(error).__name__,'label':str(error)[:180]};persist()
 finally:
  for n,reader in enumerate(readers):reader.close();save(out/('reader-final-'+str(n)+'.json'),reader.snapshot())
  try:
   report['actual_stop']=f.api('POST','/proxy/ts/stop/'+f.channel_uuid,{},'admin');persist();f.wait(lambda:not receivers()and not metadata()['owner_present']and metadata()['provider']==0,30,'actual-UDP-input-slot-process-retirement')
   report['actual_publisher_stop']=f.role('promotion-origin',[],stop=True)
   report['publisher_final']=json.loads(f.python("from pathlib import Path;print(Path("+repr(state+'/final.json')+").read_text())"));persist()
   # Retained exact producer bytes and existing complete-unit parser reference;
   # no new container, source download, remux or acceptance threshold.
   origin=f.case_data/'udp-origin'/'fixture.ts'
   reference=Index().read(origin);save(out/'origin-complete-unit-reference.json',reference)
   report['origin_reference']={'path':str(origin),'sha256':reference['raw_sha256'],'bytes':reference['raw_bytes'],'index_path':str(out/'origin-complete-unit-reference.json'),'index_sha256':hashlib.sha256((out/'origin-complete-unit-reference.json').read_bytes()).hexdigest(),'looped':False,'finite_seconds':40}
   if reference['raw_sha256']!=report['publisher_final']['fixture_sha256']or reference['raw_bytes']!=report['publisher_final']['fixture_bytes']or reference['error_count']or set(reference['pid_kinds'].values())!={'video','audio'}or report['publisher_final']['looped']is not False:raise AssertionError('ExactFiniteOriginMediaReferenceRequired')
   persist()
   if not(report['publisher_final']['actual_exit']==0 and not report['publisher_final']['forced']and report['publisher_final']['producer_generation_absent']):raise AssertionError('ActualUDPFixturePublisherRetirement')
   report['physical_retirement_passed']=True
  except Exception as error:report['cleanup_failure']={'type':type(error).__name__,'label':str(error)[:180]};persist()
 # Retain strict existing3-second A/V checks separately from ownership effects.
 report['decoders']=[]
 if report.get('business_checks_passed')and report.get('physical_retirement_passed'):
  for name in ['owner','follower']:
   path=out/(name+'.ts');local='/evidence/'+str(path.relative_to(f.output));probe=json.loads(f.command(f.compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json',local],30));progress=f.command(f.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-v','error','-xerror','-progress','pipe:1','-nostats','-threads','1','-filter_threads','1','-filter_complex_threads','1','-i',local,'-t','3','-map','0:v:0','-map','0:a:0','-threads:v','1','-threads:a','1','-f','null','-'],30,allow=True);actual=f.cli[-1];error=Path(actual['stderr']).read_bytes();cell={'name':name,'actual_exit':actual['actual_exit'],'stderr_bytes':len(error),'stderr_sha256':hashlib.sha256(error).hexdigest(),'stdout_EOF':actual['stdout_EOF'],'stderr_EOF':actual['stderr_EOF'],'process_group_settled':actual['process_group_settled'],'video_frames':max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0),'video_and_audio':{'video','audio'}<={v['codec_type']for v in probe['streams']},'media_sha256':hashlib.sha256(path.read_bytes()).hexdigest()};cell['passed']=cell['actual_exit']==0 and not error and cell['stdout_EOF']and cell['stderr_EOF']and cell['process_group_settled']and cell['video_and_audio']and cell['video_frames']>=70;report['decoders'].append(cell);persist()
 # Raw prefix criterion remains literal. This NEW protocol additionally
 # certifies untouched fixed-window bytes using the existing generic parser.
 report['raw_prefix_strict_passed']=len(report['decoders'])==2 and all(c['passed']for c in report['decoders'])
 report['certified_decoders']=[]
 if report.get('business_checks_passed')and report.get('physical_retirement_passed')and 'drain_offsets'in report.get('certified_window',{}):
  for ordinal,name in enumerate(['owner','follower']):
   cell={'name':name,'passed':False,'scope':'new complete-unit certified full-file decode; >=3 useful A/V seconds inside predeclared fresh window; not retroactive raw-prefix qualification'};report['certified_decoders'].append(cell);persist()
   try:
    raw=out/(name+'.ts');derivative=out/(name+'-certified.ts');w=report['certified_window'];ledger=[json.loads(line)for line in raw.with_suffix('.arrival.jsonl').read_text().splitlines()];start,end=offsets(ledger,w['start_monotonic'],w['end_monotonic']);drain=max([v['end_offset']for v in ledger if v['end_monotonic']<=w['end_monotonic']+5],default=0)
    # Scheduler lateness never authorizes >5s of closure bytes.
    if drain<end:raise AssertionError('ActualBoundedDrainMustCoverWindow')
    cell['actual_ledger_window']={'start':start,'end':end,'drain':drain,'fixed_drain_deadline_monotonic':w['end_monotonic']+5};persist()
    certificate=certify(raw,derivative,start,end,drain);save(out/(name+'-certificate.json'),certificate)
    spans={}
    for kind,step in [('video',3600),('audio',1920)]:
     units=[u['pts90k']for g in certificate['window']['complete_groups']if g.get('kind')==kind for u in g['units']]
     if not units or any(b-a!=step for a,b in zip(units,units[1:])):raise AssertionError('MissingDuplicateOrReorderedWindowUnit')
     spans[kind]=(units[-1]+step-units[0])/90000
     if spans[kind]<3:raise AssertionError('AtLeastThreeActualCompleteSecondsRequired')
    cell['useful_seconds']=spans;persist()
    local='/evidence/'+str(derivative.relative_to(f.output));progress=f.command(f.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-v','error','-xerror','-progress','pipe:1','-nostats','-threads','1','-filter_threads','1','-filter_complex_threads','1','-i',local,'-map','0:v:0','-map','0:a:0','-threads:v','1','-threads:a','1','-f','null','-'],30,allow=True);actual=f.cli[-1];error=Path(actual['stderr']).read_bytes();frames=max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0)
    cell.update(actual_exit=actual['actual_exit'],stderr_bytes=len(error),stderr_sha256=hashlib.sha256(error).hexdigest(),video_frames=frames,stdout_EOF=actual['stdout_EOF'],stderr_EOF=actual['stderr_EOF'],process_group_settled=actual['process_group_settled'],derivative_sha256=certificate['derivative_sha256'],raw_sha256=certificate['raw_sha256']);persist()
    if actual['actual_exit']!=0 or error or frames<70 or not(actual['stdout_EOF']and actual['stderr_EOF']and actual['process_group_settled']):raise AssertionError('CertifiedFullDecodeZeroStderrAndAtLeast70FramesRequired')
    cell['passed']=True
   except Exception as error:cell.update(error_type=type(error).__name__,error_label=str(error)[:180])
   persist()
 report['status']='passed'if report.get('business_checks_passed')and report.get('physical_retirement_passed')and len(report['certified_decoders'])==2 and all(c['passed']for c in report['certified_decoders'])else'failed';persist();return report
