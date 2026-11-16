"""New normal DVR/profile context in the existing Source/Native Fixture.
Original Dispatcharr authors retain application/algorithm credit. No starts on import.
"""
import datetime,json,re
from pathlib import Path
from workflow import Workflow,utc
from output_profile_reader import Reader
MARKER='owned-dvr-output-profile-marker'
def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')
def prepare_context(fixture):
    value=fixture.sql("from core.models import OutputProfile,CoreSettings;p=OutputProfile.objects.create(name='owned-dvr-output-profile',command='ffmpeg',parameters='-hide_banner -loglevel error -threads 1 -filter_threads 1 -i pipe:0 -map 0:v:0 -map 0:a:0 -c copy -metadata title="+MARKER+" -f mpegts pipe:1',is_active=True);d=dict(CoreSettings.get_dvr_settings());d.update(output_profile_id=' +000'+str(p.pk)+' ',comskip_enabled=False);s,_=CoreSettings.objects.get_or_create(key='dvr_settings',defaults={'value':d});s.value=d;s.save();print(json.dumps({'profile_id':p.pk,'getter_id':CoreSettings.get_dvr_output_profile_id()}))")
    if value['getter_id']!=value['profile_id']:raise AssertionError('ActualCachedDVRGetter')
    fixture._recording_profile_id=value['profile_id'];save(fixture.output/'recording-profile-seed.json',value)
def run(fixture):
    flow=Workflow(fixture,fixture.output/'recording-profile-capture');report=flow.report;raw=None
    def cell(name,operation):
        row={'name':name,'status':'running'};report['cases'].append(row);flow.flush()
        try:row['observation']=operation();row['status']='passed'
        except Exception as error:row.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:160])
        flow.flush()
    def advance(reader,before=0):
        state=reader.snapshot()
        return state['media_bytes']>=before+188000 and not state['errors']and not state['ended']
    ids=[]
    try:
        origin=json.loads(fixture.get(fixture.origin+'/stats')[1]);save(flow.out/'origin-before.json',origin)
        if origin['active']!=0:raise AssertionError('OwnedOriginInitiallyIdle')
        raw=Reader(fixture.base_url+'/proxy/ts/stream/'+fixture.channel_uuid,flow.out/'ordinary-raw.ts',fixture.actual_admin_token())
        fixture.wait(lambda:advance(raw),25,'recording-profile-ordinary-raw')
        start=utc()+datetime.timedelta(seconds=3)
        ids=[flow.book(start+datetime.timedelta(seconds=i),start+datetime.timedelta(seconds=30+i),'DVR profile overlap')for i in range(2)]
        flow.wait(lambda:flow.active(ids),25,'recording-profile-both-active')
        fixture.wait_for_hls_segments(ids,2,25)
        def shared():
            observed=flow.snapshot('recording-profile-active');processes=observed['actual_processes']
            args=[p.get('argv_text','')for p in processes]
            recorders=[a for a in args if 'ffmpeg'in a and re.search(r'Dispatcharr-DVR/recording-(?:'+ '|'.join(map(str,ids))+r')(?:\s|$)',a)]
            profiles=[p for p in processes if MARKER in p.get('argv_text','')and 'pipe:0'in p.get('argv_text','')]
            query='?output_profile='+str(fixture._recording_profile_id)
            proof={'recorder_count':len(recorders),'each_actual_Main_URL_uses_profile':len(recorders)==2 and all('/proxy/ts/stream/'+fixture.channel_uuid+query in a for a in recorders),'output_process_count':len(profiles),'origin_connection_delta':observed['actual_origin']['connections']-origin['connections'],'origin_active':observed['actual_origin']['active'],'provider_slots':observed['actual_provider_slots'],'ordinary_raw_advances':advance(raw)}
            save(flow.out/'shared-profile-proof.json',proof)
            if not(proof['each_actual_Main_URL_uses_profile']and len(profiles)==1 and proof['origin_connection_delta']==1 and proof['origin_active']==1 and proof['provider_slots']==1 and proof['ordinary_raw_advances']):raise AssertionError('ActualDVRSharedProfileAndRawInput')
            return proof
        cell('two-recordings-use-one-shared-profile-and-raw-upstream',shared)
        for ident in ids:
            cell('recording-terminal-and-decoder-'+str(ident),lambda ident=ident:(flow.wait(lambda:flow.terminal(ident,'completed'),75,'recording-profile-terminal-'+str(ident)),flow.decode(ident))and {'recording_id':ident,'actual_terminal_and_complete_file_decode':True})
        def invalid():
            fixture.sql("from core.models import CoreSettings;d=dict(CoreSettings.get_dvr_settings());d['output_profile_id']={'invalid':True};s=CoreSettings.objects.get(key='dvr_settings');s.value=d;s.save();assert CoreSettings.get_dvr_output_profile_id() is None;print(json.dumps({'actual_getter_none':True}))")
            now=utc()+datetime.timedelta(seconds=2);ident=flow.book(now,now+datetime.timedelta(seconds=12),'Invalid profile falls back to raw')
            flow.wait(lambda:flow.active([ident]),20,'recording-invalid-profile-active')
            observed=flow.snapshot('recording-invalid-profile-active');args=[p.get('argv_text','')for p in observed['actual_processes']if re.search(r'Dispatcharr-DVR/recording-'+str(ident)+r'(?:\s|$)',p.get('argv_text',''))and 'ffmpeg'in p.get('argv_text','')]
            proof={'recording_id':ident,'one_actual_recorder':len(args)==1,'no_output_profile_query':bool(args)and all('?output_profile='not in a for a in args)};save(flow.out/'invalid-profile-proof.json',proof)
            flow.wait(lambda:flow.terminal(ident,'completed'),55,'recording-invalid-profile-terminal');flow.decode(ident)
            if not(proof['one_actual_recorder']and proof['no_output_profile_query']):raise AssertionError('ActualInvalidGetterRawURL')
            return proof
        cell('invalid-cached-getter-produces-raw-recording',invalid)
        before=raw.snapshot()['media_bytes'];fixture.wait(lambda:advance(raw,before),10,'ordinary-raw-survives-DVR-profile-end')
        report['ordinary_raw_survives']=True
    except Exception as error:report.update(error_type=type(error).__name__,error_label=str(error)[:160])
    finally:
        if raw:
            raw.close();report['ordinary_raw_final']=raw.snapshot()
        report['status']='passed'if len(report['cases'])==4 and all(c['status']=='passed'for c in report['cases'])and report.get('ordinary_raw_survives')and not report.get('error_type')and raw and raw.snapshot()['ended']and not raw.snapshot()['errors']else'failed'
        report['scope']='normal cached DVR profile getter, actual capture URL, overlapping media and ordinary raw demand; comskip/whole M3/performance remain unqualified'
        flow.flush()
    return report
