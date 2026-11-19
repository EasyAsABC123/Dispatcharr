"""One new Source/Native media context, same existing Fixture/bootstrap/limits.
Actual pre-delivery OutputProfile is shared by TS, HLS and fMP4 consumers.
Original Dispatcharr authors retain full credit. No app starts on import.
"""
import hashlib,json,re,urllib.request,urllib.error,urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from output_profile_reader import Reader
from budget import bounded_read
MARKER='owned-output-profile-marker'
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def prepare_context(fixture):
    value=fixture.sql("from core.models import OutputProfile;p=OutputProfile.objects.create(name='owned-output-profile-shared',command='ffmpeg',parameters='-hide_banner -loglevel error -threads 1 -filter_threads 1 -i pipe:0 -map 0:v:0 -map 0:a:0 -c copy -metadata title="+MARKER+" -f mpegts pipe:1',is_active=True);print(json.dumps({'profile_id':p.pk,'command':p.command,'parameters':p.parameters}))")
    fixture._actual_output_profile_id=value['profile_id'];save(fixture.output/'output-profile-seed.json',value)
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):return None
def run(fixture):
    out=fixture.output/'output-profile-capture';out.mkdir(mode=0o755)
    report={'status':'running','scope':'one actual raw upstream, one output-profile process shared by TS/HLS/fMP4 clients; no active DVR or performance claim','cells':[],'full_M3_passed':False,'performance_verified':False}
    readers=[];hls_token=None
    def persist():save(out/'report.json',report)
    def cell(name,operation):
        item={'name':name,'status':'running'};report['cells'].append(item);persist()
        try:item['observation']=operation();item['status']='passed'
        except Exception as error:item.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:160])
        persist();return item['status']=='passed'
    def request(path,method='GET',limit=4*1024*1024,follow=True,agent=None):
        url=urllib.parse.urljoin(fixture.base_url,path)
        if urllib.parse.urlsplit(url).netloc!=urllib.parse.urlsplit(fixture.base_url).netloc:raise RuntimeError('OwnedMainOnly')
        headers={'Authorization':'Bearer '+fixture.actual_admin_token()}
        if agent is not None:headers['User-Agent']=agent
        req=urllib.request.Request(url,headers=headers,method=method)
        opener=urllib.request.build_opener()if follow else urllib.request.build_opener(NoRedirect())
        try:response=opener.open(req,timeout=fixture.remaining(15))
        except urllib.error.HTTPError as error:response=error
        with response:
            body=bounded_read(response,limit,fixture.remaining)
            return response.status,dict(response.headers),body
    def processes():
        if fixture.backend=='Native'and fixture.native is None:return [] # stop_native already retained actual exit/removal receipt
        cid=fixture.native if fixture.backend=='Native'else fixture.command(fixture.compose+['ps','-q','baseline']).strip()
        output=fixture.command([fixture.docker,'top',cid,'-eo','pid,args'])
        return [line.split(None,1)[0]for line in output.splitlines()[1:]if 'ffmpeg'in line and MARKER in line and 'pipe:0'in line]
    def advance(reader):
        state=reader.snapshot()
        if state['errors']:raise RuntimeError(('ActualReaderError',state['errors']))
        return state['bytes']>=750000
    def decode(name,path):
        local='/evidence/'+str(path.relative_to(fixture.output))
        probe=json.loads(fixture.command(fixture.compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json',local],30))
        progress=fixture.command(fixture.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-threads','1','-filter_threads','1','-filter_complex_threads','1','-i',local,'-t','3','-map','0:v:0','-map','0:a:0','-threads:v','1','-threads:a','1','-f','null','-'],30,allow=True)
        actual=fixture.cli[-1];stderr=Path(actual['stderr']).read_bytes();frames=max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0)
        observed={'viewer':name,'actual_exit':actual['actual_exit'],'stdout_EOF':actual['stdout_EOF'],'stderr_EOF':actual['stderr_EOF'],'process_group_settled':actual['process_group_settled'],'stderr_bytes':len(stderr),'stderr_sha256':hashlib.sha256(stderr).hexdigest(),'video_frames':frames,'video_and_audio':{'video','audio'}<={s['codec_type']for s in probe['streams']},'file_bytes':path.stat().st_size,'file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'scope':'3-second positive A/V prefix; whole-file tail and full M3 remain distinct'}
        save(out/(name+'-decoder.json'),observed)
        if not(actual['actual_exit']==0 and actual['stdout_EOF']and actual['stderr_EOF']and actual['process_group_settled']and not stderr and frames>=70 and observed['video_and_audio']):raise AssertionError('ActualThreeSecondAVDecode')
        return observed
    if fixture.binding.get('native_profiled_hls_two_viewers') is True:
        if fixture.backend != 'Native':
            raise AssertionError('Focused typed HLS case requires Rust Main')
        return profiled_hls_two_viewers(fixture, out, request, processes, decode)
    persist()
    try:
        profile=fixture._actual_output_profile_id
        # Origin is a shared infrastructure role across contexts. Its counters
        # are monotonic; a prior promotion failover is not this context's input.
        origin_before=json.loads(fixture.get(fixture.origin+'/stats')[1])
        report['actual_origin_baseline']=origin_before;persist()
        if origin_before['active']!=0:raise AssertionError('OutputProfileRequiresRetiredPreviousOrigin')
        stream='/proxy/ts/stream/'+fixture.channel_uuid
        raw=Reader(fixture.base_url+stream,out/'raw.ts',fixture.actual_admin_token());readers.append(('raw',raw))
        fixture.wait(lambda:advance(raw),30,'actual-output-profile-raw-first')
        for name in ['profile-one','profile-two']:
            reader=Reader(fixture.base_url+stream+'?output_profile='+str(profile),out/(name+'.ts'),fixture.actual_admin_token());readers.append((name,reader))
            fixture.wait(lambda reader=reader:advance(reader),30,'actual-output-'+name)
        def shared():
            pids=processes();origin=json.loads(fixture.get(fixture.origin+'/stats')[1])
            metadata=fixture.redis(0,"local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('GET',p..':output:mpegts:p'..ARGV[2]..':state')or'',redis.call('GET',p..':output:mpegts:p'..ARGV[2]..':buffer:index')or''}",fixture.channel_uuid,str(profile))
            observed={'actual_output_process_pids':pids,'actual_origin_connections':origin['connections'],'actual_origin_connection_delta':origin['connections']-origin_before['connections'],'actual_origin_active':origin['active'],'actual_clients':metadata[0],'actual_profile_state':metadata[1],'actual_profile_index':metadata[2]}
            save(out/'actual-TS-sharing.json',observed)
            if not(len(pids)==1 and observed['actual_origin_connection_delta']==1 and origin['active']==1 and metadata[0]==3 and metadata[1]=='active'and int(metadata[2])>0):raise AssertionError('ActualSharedRawAndProfileTS')
            return observed
        cell('raw-plus-two-profile-TS-one-input',shared)
        def hls():
            nonlocal hls_token
            status,headers,body=request(stream+'?output_profile='+str(profile)+'&output_format=hls',follow=False)
            if status!=302:raise AssertionError(('ActualProfileHLSMint',status))
            location=headers.get('Location')or headers.get('location');hls_token=headers.get('X-Dispatcharr-Session-Token')or headers.get('x-dispatcharr-session-token')
            if not location or not hls_token:raise AssertionError('ActualHLSCapabilityRequired')
            state={}
            def ready():
                code,_,body=request(location);state.update(status=code,body=body)
                return code==200 and b'#EXTINF:'in body
            fixture.wait(ready,30,'actual-output-profile-HLS-playlist')
            playlist=state['body'].decode();uris=[line for line in playlist.splitlines()if line and not line.startswith('#')]
            if len(uris)<1:raise AssertionError('ActualHLSSegmentRequired')
            capture=out/'hls.ts';total=0
            with capture.open('wb')as target:
                for uri in uris[-2:]:
                    code,_,body=request(urllib.parse.urljoin(location,uri))
                    if code!=200 or not body or len(body)%188 or body[0]!=0x47:raise AssertionError('ActualProfileHLSTS')
                    target.write(body);total+=len(body)
            if len(processes())!=1:raise AssertionError('HLSDuplicatedOutputProfileProcess')
            return {'playlist_sha256':hashlib.sha256(state['body']).hexdigest(),'actual_segments':min(2,len(uris)),'actual_bytes':total,'decoded':decode('hls',capture),'profile_processes':len(processes())}
        cell('same-profile-HLS-output-and-decoder',hls)
        mp4=Reader(fixture.base_url+stream+'?output_profile='+str(profile)+'&output_format=fmp4',out/'fmp4.mp4',fixture.actual_admin_token(),transport='fmp4');readers.append(('fmp4',mp4))
        def fmp4():
            fixture.wait(lambda:advance(mp4),40,'actual-output-profile-fMP4')
            mp4.close()
            if mp4.snapshot().get('content_type')!='video/mp4':raise AssertionError('ActualMP4ContentType')
            origin=json.loads(fixture.get(fixture.origin+'/stats')[1]);pids=processes()
            ownership={'actual_process_pids':pids,'actual_origin_connections':origin['connections'],'actual_origin_connection_delta':origin['connections']-origin_before['connections']}
            save(out/'actual-fMP4-sharing.json',ownership)
            # Useful-media decoding is independent of sharing observation; keep
            # it even if an ownership/count predicate later fails.
            decoded=decode('fmp4',out/'fmp4.mp4')
            if len(pids)!=1 or ownership['actual_origin_connection_delta']!=1:raise AssertionError('FMP4DuplicatedProfileOrUpstream')
            return {'decoded':decoded,'ownership':ownership}
        cell('same-profile-fMP4-output-and-decoder',fmp4)
        for name,reader in readers:
            if name=='raw':continue
            reader.close()
            if name.startswith('profile-'):cell(name+'-decoder',lambda name=name:decode(name,out/(name+'.ts')))
        if hls_token:
            status,_,body=request('/api/proxy/hls/sessions/'+hls_token+'/',method='DELETE')
            save(out/'hls-stop.json',{'status':status,'body_bytes':len(body),'body_sha256':hashlib.sha256(body).hexdigest()})
            if status not in(200,204):raise AssertionError(('ActualHLSStop',status))
            hls_token=None
        def retired():
            return not processes()and fixture.redis(0,"return redis.call('EXISTS','live:channel:'..ARGV[1]..':output:mpegts:p'..ARGV[2]..':owner')",fixture.channel_uuid,str(profile))==0
        def selective():
            if fixture.backend=='Native':fixture.wait(retired,45,'actual-profile-retirement-only')
            # Baseline is observed AFTER retirement/Source retention observation,
            # so already accumulated data cannot impersonate a surviving viewer.
            before=raw.snapshot()['media_bytes']
            fixture.wait(lambda:raw.snapshot()['media_bytes']>=before+188000 and not raw.snapshot()['errors']and not raw.snapshot()['ended'],10,'actual-raw-byte-advance-after-profile-retirement')
            active=advance(raw);connections=json.loads(fixture.get(fixture.origin+'/stats')[1])['connections'];delta=connections-origin_before['connections']
            observed={'Source_or_Native_retired_observed':retired(),'raw_viewer_continues':active,'raw_nonnull_bytes_before':before,'raw_nonnull_bytes_after':raw.snapshot()['media_bytes'],'actual_origin_connections':connections,'actual_origin_connection_delta':delta,'timing_parity_claimed':False}
            save(out/'actual-selective-retirement.json',observed)
            if not active or delta!=1:raise AssertionError('OrdinaryRawViewerMustSurviveProfileRetirement')
            return observed
        cell('profile-retirement-preserves-ordinary-raw-viewer',selective)
    except Exception as error:report.update(error_type=type(error).__name__,error_label=str(error)[:160])
    finally:
        for name,reader in readers:
            try:reader.close()
            except Exception as error:report.setdefault('reader_cleanup_errors',[]).append({'viewer':name,'error_type':type(error).__name__})
        if hls_token:
            try:request('/api/proxy/hls/sessions/'+hls_token+'/',method='DELETE')
            except Exception as error:report['HLS_cleanup_error']=type(error).__name__
        report['readers']=[{'viewer':name,**reader.snapshot()}for name,reader in readers]
        report['actual_all_reader_HTTP_worker_settlement_errors_and_TS_sync']=bool(readers)and all(
            item.get('http_status')==200 and item.get('ended')is True and not item.get('errors')and
            (item.get('transport')=='fmp4' or item.get('sync_errors')==0)
            for item in report['readers'])
        try:
            if fixture.backend=='Native':report['actual_Main_stop']=fixture.stop_native(expected=0)
            else:fixture.stop_roles();report['actual_Source_roles_stopped']=not fixture.roles
            report['actual_origin_and_profile_retired']=fixture.wait(lambda:not processes()and json.loads(fixture.get(fixture.origin+'/stats')[1]).get('active')==0,20,'actual-output-profile-final-retirement')
        except Exception as error:report['shutdown_error']=type(error).__name__
        report['status']='passed'if len(report['cells'])==6 and all(cell['status']=='passed'for cell in report['cells'])and report['actual_all_reader_HTTP_worker_settlement_errors_and_TS_sync']and not any(k.endswith('_error')or k.endswith('_errors')or k=='error_type'for k in report)else'failed'
        persist()
    return report


def profiled_hls_two_viewers(fixture, out, request, processes, decode):
    """Two real Main capabilities share one explicitly profiled HLS output."""
    profile = str(fixture._actual_output_profile_id)
    channel = fixture.channel_uuid
    stream = f'/proxy/ts/stream/{channel}?output_profile={profile}&output_format=hls'
    origin_before = json.loads(fixture.get(fixture.origin + '/stats')[1])
    report = {
        'status': 'running',
        'scope': 'Native Main, two distinct typed HLS capabilities, one origin and transform',
        'actual_origin_baseline': origin_before,
        'full_M3_passed': False,
        'performance_verified': False,
    }
    viewers = {}

    def persist():
        save(out / 'report.json', report)

    def stop(token):
        status, _, _ = request('/api/proxy/hls/sessions/' + token + '/', method='DELETE')
        if status not in (200, 204):
            raise AssertionError(('ActualHLSStop', status))

    def mint(name):
        status, headers, _ = request(stream, follow=False, agent='owned-profiled-hls-' + name)
        lower = {key.lower(): value for key, value in headers.items()}
        token = lower.get('x-dispatcharr-session-token')
        if token and re.fullmatch(r'[a-zA-Z0-9_-]{1,128}', token):
            viewers[name] = token
        else:
            raise AssertionError(('ActualProfileHLSMint', status))
        location = lower.get('location')
        if status != 302 or location != f'/proxy/hls/{token}/index.m3u8':
            raise AssertionError(('ActualProfileHLSMint', status))
        return location

    def playlist(location):
        status, _, body = request(location)
        if status != 200 or b'#EXTINF:' not in body:
            return {}
        result = {}
        for line in body.decode().splitlines():
            if line and not line.startswith('#'):
                name = urllib.parse.urlsplit(line).path.rsplit('/', 1)[-1]
                if not re.fullmatch(r'[0-9]+\.ts', name):
                    raise AssertionError('Unexpected profiled HLS segment URI')
                absolute = urllib.parse.urljoin(location, line)
                if absolute != location.rsplit('/', 1)[0] + '/' + name:
                    raise AssertionError('Segment lacks its viewer capability')
                result[int(name[:-3])] = absolute
        return result

    def sharing(first, second=None):
        # Redis and saved CLI have no raw capabilities. Use exact token hashes
        # only to prove association to the independently counted client rows.
        hashes = [hashlib.sha1(token.encode(), usedforsecurity=False).hexdigest()
                  for token in (first, second) if token is not None]
        stores = fixture.redis(0, """
local p = 'live:channel:'..ARGV[1]
local k = p..':output:hls:p'..ARGV[2]
local owner = redis.call('GET',k..':owner')
local matched=0
for _,id in ipairs(redis.call('SMEMBERS',p..':clients')) do
 local fields=redis.call('HMGET',p..':clients:'..id,'output_format','output_profile_id','hls_token')
 if fields[1]=='hls' and fields[2]==ARGV[2] and fields[3] then
  local fingerprint=redis.sha1hex(fields[3])
  if fingerprint==ARGV[3] or fingerprint==ARGV[4] then matched=matched+1 end
 end
end
return {owner and redis.sha1hex(owner) or '',redis.call('GET',k..':state') or '',redis.call('SCARD',p..':clients'),matched}
""", channel, profile, *hashes)
        origin = json.loads(fixture.get(fixture.origin + '/stats')[1])
        return {
            'hls_owner_fingerprint': stores[0],
            'hls_output_state': stores[1],
            'channel_clients': stores[2],
            'matching_profiled_HLS_clients': stores[3],
            'transform_pids': processes(),
            'origin_connection_delta': origin['connections'] - origin_before['connections'],
            'origin_active': origin['active'],
        }

    def shared(observed, count):
        return (
            bool(observed['hls_owner_fingerprint'])
            and observed['hls_output_state'] == 'active'
            and observed['channel_clients'] == count
            and observed['matching_profiled_HLS_clients'] == count
            and len(observed['transform_pids']) == 1
            and observed['origin_connection_delta'] == 1
            and observed['origin_active'] == 1
        )

    persist()
    try:
        if origin_before['active'] != 0:
            raise AssertionError('OutputProfileRequiresRetiredPreviousOrigin')
        names = ('one', 'two')
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = {name: pool.submit(mint, name) for name in names}
            locations = {name: future.result() for name, future in futures.items()}
        if len(set(viewers.values())) != 2:
            raise AssertionError('DistinctHLSCapabilitiesRequired')
        report['distinct_playback_capabilities'] = True
        playlists = {}

        def ready():
            playlists.update({name: playlist(locations[name]) for name in names})
            return len(set(playlists['one']) & set(playlists['two'])) >= 2

        fixture.wait(ready, 30, 'actual-two-viewer-profiled-HLS-common-segments')
        common = sorted(set(playlists['one']) & set(playlists['two']))[-2:]
        if common[1] != common[0] + 1:
            raise AssertionError('ConsecutiveSharedHLSSegmentsRequired')
        report['simultaneous'] = sharing(viewers['one'], viewers['two'])
        persist()
        if not shared(report['simultaneous'], 2):
            raise AssertionError('TwoProfiledHLSViewersRequireOneOwnerTransformAndOrigin')
        signatures = {}
        for name in names:
            capture = out / f'hls-{name}.ts'
            signatures[name] = []
            with capture.open('wb') as target:
                for sequence in common:
                    status, _, body = request(playlists[name][sequence])
                    if status != 200 or not body or len(body) % 188 or body[0] != 0x47:
                        raise AssertionError(('UsableProfiledHLSSegment', status))
                    signatures[name].append({'sequence': sequence, 'bytes': len(body),
                                             'sha256': hashlib.sha256(body).hexdigest()})
                    target.write(body)
        report['segment_signatures'] = signatures
        persist()
        if signatures['one'] != signatures['two']:
            raise AssertionError('SameProfiledHLSOwnerMustReturnIdenticalSegments')
        stop(viewers['one'])
        del viewers['one']

        def survives():
            observed = sharing(viewers['two'])
            if not shared(observed, 1):
                return False
            original = report['simultaneous']
            if observed['hls_owner_fingerprint'] != original['hls_owner_fingerprint'] or observed['transform_pids'] != original['transform_pids']:
                raise AssertionError('SurvivingHLSViewerMustKeepTheSameOwnerAndTransform')
            segments = playlist(locations['two'])
            if not segments or max(segments) <= common[-1]:
                return False
            status, _, body = request(segments[max(segments)])
            if status != 200 or not body or len(body) % 188 or body[0] != 0x47:
                return False
            report['after_first_stop'] = dict(observed, later_segment_sequence=max(segments))
            return True

        fixture.wait(survives, 25, 'actual-second-profiled-HLS-viewer-survives')
        stop(viewers['two'])
        del viewers['two']
        report['decoded'] = {
            name: decode(f'hls-{name}', out / f'hls-{name}.ts') for name in names
        }
        report['status'] = 'passed'
    except Exception as error:
        # Exceptions can contain a signed media URL. Diagnostic labels are
        # emitted only by explicit assertions with public statuses.
        report['status'] = 'failed'
        report['error_type'] = type(error).__name__
        if isinstance(error, AssertionError):
            report['assertion'] = str(error)[:160]
    finally:
        for token in viewers.values():
            try:
                stop(token)
            except Exception as error:
                report.setdefault('cleanup_errors', []).append(type(error).__name__)
        if report.get('cleanup_errors'):
            report['status'] = 'failed'
        persist()
    return report
