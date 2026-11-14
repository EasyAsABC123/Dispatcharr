"""Off-window independent fixture index and full-capture useful-content gate."""
import hashlib,json
from fractions import Fraction
from pathlib import Path
BATCH=188*1361
FIXTURE_SHA='304385e14b16549a96074fd8b5d851a73be60b17c99bae58021bd9f8b49f538c'

def frame_rows(text):
    bases={};rows=[]
    for raw in text.splitlines():
        if raw.startswith('#tb '):
            key,value=raw[4:].split(':',1);bases[int(key)]=Fraction(value.strip())
        elif raw and not raw.startswith('#'):
            parts=[s.strip() for s in raw.split(',')]
            if len(parts)!=6:raise ValueError('ExactFramehashSchemaRequired')
            stream,dts,pts,duration,size,digest=parts;sid=int(stream)
            rows.append({'stream':sid,'pts':float(int(pts)*bases[sid]),'duration':float(int(duration)*bases[sid]),'size':int(size),'hash':digest})
    if set(bases)!={0,1} or not all(any(r['stream']==s for r in rows)for s in(0,1)):raise ValueError('AudioAndVideoFramehashRequired')
    return rows

def contiguous_match(reference,observed,expected_first_pts=None,tolerance=0):
    if not observed:raise ValueError('EmptyUsefulContent')
    candidates=[i for i,r in enumerate(reference)if r['hash']==observed[0]['hash']and r['size']==observed[0]['size']]
    if expected_first_pts is not None:candidates=[i for i in candidates if abs(reference[i]['pts']-expected_first_pts)<=tolerance]
    matches=[]
    for start in candidates:
        if start+len(observed)<=len(reference)and all(a['hash']==b['hash']and a['size']==b['size']for a,b in zip(reference[start:start+len(observed)],observed)):matches.append(start)
    if len(matches)!=1:raise ValueError('MissingOrAmbiguousContiguousContentAuthority')
    return matches[0]

def bound_from_index(packets):
    # Encoded elementary-packet bytes exclude input CBR null stuffing. A real
    # B-byte TS batch cannot contain more than B encoded content bytes.
    # Preserve mux/demux order. B encoded bytes conservatively overestimate
    # useful content in one B-byte TS batch (headers/stuffing omitted). This
    # is an upper bound, never an exact transport-time characterization.
    rows=[p for p in packets if 'pts_time'in p and 'size'in p];right=0;used=0;largest=0
    if any(int(p['size'])>BATCH for p in rows):raise ValueError('AccessUnitLargerThanBatchUnsupported')
    for left,row in enumerate(rows):
        while right<len(rows)and used+int(rows[right]['size'])<=BATCH:
            used+=int(rows[right]['size']);right+=1
        if right>left:
            tail=max(float(p['pts_time'])+float(p.get('duration_time',0))for p in rows[left:right]);largest=max(largest,tail-min(float(p['pts_time'])for p in rows[left:right]))
        if right==left:right+=1
        else:used-=int(row['size'])
    if largest<=0:raise ValueError('IndependentBatchPTSBoundMissing')
    return largest

def media_index(command,compose,output):
    # Index where the verified immutable clip actually lives. This avoids a
    # Docker archive/cp transport dependency and duplicate75MB retained files.
    verification=command(compose+['exec','-T','origin','/dispatcharrpy/bin/python','-c',"import hashlib,json,os;from pathlib import Path;p=Path('/tmp/fixture.ts');h=hashlib.sha256();f=p.open('rb');[h.update(b)for b in iter(lambda:f.read(1048576),b'')];print(json.dumps({'path':str(p),'bytes':p.stat().st_size,'sha256':h.hexdigest()}))"],timeout=30)
    info=json.loads(verification)
    if info['path']!='/tmp/fixture.ts' or info['sha256']!=FIXTURE_SHA:raise ValueError('ExactIndependentFixtureRequired')
    versions={tool:command(compose+['exec','-T','origin',tool,'-version'],timeout=15).splitlines()[0]for tool in ('ffmpeg','ffprobe')}
    exact_help=command(compose+['exec','-T','origin','ffmpeg','-hide_banner','-h','full'],timeout=15)
    if '-fps_mode'not in exact_help:raise ValueError('PinnedPassthroughTimingOptionUnavailable')
    result=probe(command,compose,output/'reference-fixture.ts',output,reference=True,service='origin',local_path='/tmp/fixture.ts',media_digest=info['sha256'])
    result['verified_Origin_fixture']=info;result['actual_Origin_media_versions']=versions
    (output/'reference-Origin-authority.json').write_text(json.dumps({'fixture':info,'media_tools':versions,'one_batch_PTS_upper_bound_seconds':result['one_batch_max_PTS_seconds']},indent=2)+'\n')
    return result

def probe(command,compose,path,output,reference=False,service='baseline',local_path=None,media_digest=None):
    local=local_path or '/evidence/'+str(path.relative_to(output));prefix=['exec','-T',service]
    packets_raw=command(compose+prefix+['ffprobe','-v','error','-show_entries','stream=index,codec_type,codec_name:packet=stream_index,pts_time,duration_time,pos,size','-of','json',local],timeout=60)
    if len(packets_raw)>32*1024*1024:raise ValueError('BoundedPacketIndexExceeded')
    packets=json.loads(packets_raw)
    frame_raw=command(compose+prefix+['ffmpeg','-hide_banner','-loglevel','error','-copyts','-i',local,'-map','0:v:0','-map','0:a:0','-fps_mode:v','passthrough','-c:v','rawvideo','-pix_fmt','yuv420p','-c:a','pcm_s16le','-f','framehash','-hash','sha256','pipe:1'],timeout=120 if reference else 60)
    if len(frame_raw)>16*1024*1024:raise ValueError('BoundedFrameIndexExceeded')
    rows=frame_rows(frame_raw);target=path.with_suffix('.framehash');target.write_text(frame_raw)
    result={'fixture_reference':reference,'media_sha256':media_digest or hashlib.sha256(path.read_bytes()).hexdigest(),'frames':rows,'packets':packets['packets'],'streams':packets['streams'],'framehash_sha256':hashlib.sha256(frame_raw.encode()).hexdigest()}
    if reference:result['one_batch_max_PTS_seconds']=bound_from_index(packets['packets'])
    path.with_suffix('.media-index.json').write_text(json.dumps(result,indent=2)+'\n')
    return result

def qualify(command,compose,output,report,reference):
    result={'protocol':'v2 independently indexed whole-window useful media','passed':False,'readers':[],'one_batch_bytes':BATCH,'one_batch_PTS_seconds':reference['one_batch_max_PTS_seconds'],'extra_wall_or_equivalence_quantization_allowance_seconds':0,'gain_acceptance':False}
    samplepath=output/'resource-samples.json';samples=json.loads(samplepath.read_text())if samplepath.exists()else{'samples':[]};ticks=samples.get('samples',[]);elapsed=samples.get('elapsed_seconds',(ticks[-1]['monotonic_start']-ticks[0]['monotonic_start'])if len(ticks)>=2 else 0);bound=reference['one_batch_max_PTS_seconds']
    result['elapsed_seconds']=elapsed;result['live_state_passed']=samples.get('live_state_passed',False);errors=[]
    for i,label in enumerate(('owner','follower')):
        path=output/'candidate-shared-demand'/(label+'.ts')
        if not path.exists():path=output/'baseline-shared-demand'/(label+'.ts')
        row={'reader':label,'passed':False};result['readers'].append(row)
        try:
            # Decode the full retained capture even if live/resource checks failed.
            decoded=probe(command,compose,path,output)
            first,last=samples['samples'][0]['readers'][i],samples['samples'][-1]['readers'][i];start,end=first['bytes'],last['bytes'];row['window_byte_offsets']=[start,end];row['window_bytes']=end-start;row['final_reader_state']=last
            if last['sync_errors']or last['errors']or last['ended']or last.get('http_status')!=200:raise ValueError('ActualLiveReaderFailed')
            kinds={s['codec_type']for s in decoded['streams']}
            if not {'video','audio'}<=kinds:raise ValueError('DecodedAudioVideoMissing')
            row['streams']={}
            for sid in(0,1):
                authority=[r for r in reference['frames']if r['stream']==sid];observed=[r for r in decoded['frames']if r['stream']==sid]
                if sid==0:
                    offset=contiguous_match(authority,observed);video_reference_start=authority[offset]['pts'];video_observed_start=observed[0]['pts']
                else:
                    expected=video_reference_start+observed[0]['pts']-video_observed_start;offset=contiguous_match(authority,observed,expected,1/25+1024/48000)
                # Full-frame content sequence is independent of remux timestamps.
                # Assign N/A packet positions to their known PES group. Only
                # a following same-stream PES start proves the previous group
                # fully arrived by the measurement boundary. Exclude boundary
                # groups lacking that proof instead of borrowing later bytes.
                groups=[]
                for packet in decoded['packets']:
                    if packet['stream_index']!=sid:continue
                    pos=packet.get('pos')
                    if pos is not None:
                        pos=int(pos)
                        if not groups or groups[-1]['pos']!=pos:groups.append({'pos':pos,'packets':[]})
                    if groups:groups[-1]['packets'].append(packet)
                positions=[]
                for group,nextgroup in zip(groups,groups[1:]):
                    if start<=group['pos'] and nextgroup['pos']<=end:positions.extend(p for p in group['packets']if 'pts_time'in p)
                if not positions:raise ValueError('WindowPacketPositionsMissing')
                pts_start=min(float(p['pts_time'])for p in positions);pts_end=max(float(p['pts_time'])+float(p.get('duration_time',0))for p in positions)
                window=[(n,r)for n,r in enumerate(observed)if pts_start<=r['pts']<pts_end]
                if not window:raise ValueError('WindowDecodedFramesMissing')
                first_n,first_frame=window[0];last_n,last_frame=window[-1];ref_begin=authority[offset+first_n]['pts'];ref_end=authority[offset+last_n]['pts']+authority[offset+last_n]['duration'];span=ref_end-ref_begin
                row['streams'][str(sid)]={'decoded_full_capture_frames':len(observed),'decoded_window_frames':len(window),'reference_index_start':offset+first_n,'reference_PTS_start':ref_begin,'reference_PTS_end':ref_end,'useful_duration_seconds':span,'difference_from_wall_seconds':span-elapsed,'contiguous_content_match':True}
                if abs(span-elapsed)>bound:raise ValueError('BeyondSingleBatchBoundaryMediaWork')
            row['passed']=True
        except Exception as error:row['error']={'type':type(error).__name__,'message':str(error)};errors.append(label)
    result['passed']=result['live_state_passed']and not errors and len(result['readers'])==2
    (output/'useful-media-v2.json').write_text(json.dumps(result,indent=2)+'\n')
    return result
