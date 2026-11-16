"""Existing Main/profile shared-demand functional slice; no resource sampling.
Original Dispatcharr/dependency authors retain full credit. No Source18 replay.
"""
import hashlib,json,time
from pathlib import Path
from live_reader import Reader
from startup_receipts import collect


def save(path,value):Path(path).write_text(json.dumps(value,indent=2)+'\n')


def prepare_context(fixture):
    script=Path(__file__).with_name('idle_profile_prepare.py')
    observed=json.loads(fixture.python(script.read_text()))
    save(fixture.output/'idle-viewing-profile-prepare.json',observed)
    fixture.sql("from core.models import CoreSettings;from django_celery_beat.models import PeriodicTask,PeriodicTasks;s,_=CoreSettings.objects.get_or_create(key='backup_settings',defaults={'name':'Backup Settings','value':{}});s.value={**(s.value if isinstance(s.value,dict)else{}),'schedule_enabled':False};s.save();n=PeriodicTask.objects.update(enabled=False);PeriodicTasks.update_changed();print(json.dumps({'backup_schedule_enabled':False,'disabled_actual_rows':n}))")


def run(fixture):
    out=fixture.output/'idle-viewing-capture';out.mkdir(mode=0o755)
    report={'status':'running','scope':'two authenticated idle live viewers, custom Source-template FFmpeg input; no active Recording rows or performance measurement','profile_template':'actual locked FFmpeg template; Source18 model methods not called','readers':[],'decoders':[],'performance_verified':False,'full_M3_passed':False}
    readers=[]
    def persist():save(out/'report.json',report)
    def processes():
        cid=fixture.native if fixture.backend=='Native'else fixture.command(fixture.compose+['ps','-q','baseline']).strip()
        text=fixture.command([fixture.docker,'top',cid,'-eo','pid,args'])
        return [line for line in text.splitlines()[1:]if 'ffmpeg'in line and 'http://origin:8080/live/'in line]
    persist()
    try:
        if fixture.backend=='Native':report['startup_roots']=collect(fixture)
        else:
            if set(fixture.roles)!={'web','celery','dvr','beat'}:raise AssertionError('ActualFourSourceRolesRequired')
            report['actual_ready_Source_roles']=sorted(fixture.roles)
        if fixture.read_rows():raise AssertionError('IdleViewingMustHaveNoRecordingRows')
        path='/health/ready'if fixture.backend=='Native'else'/api/channels/recordings/'
        code,body=fixture.get(fixture.base_url+path);report['actual_Main_readiness_status']=code
        if code not in([200]if fixture.backend=='Native'else[401,403]):raise AssertionError('ActualLoadedGraphReadyRequired')
        before=json.loads(fixture.get(fixture.origin+'/stats')[1])
        url=fixture.base_url+'/proxy/ts/stream/'+fixture.channel_uuid
        for name in ['owner','follower']:
            reader=Reader(url,out/(name+'.ts'),fixture.actual_admin_token());readers.append(reader)
            def advanced(reader=reader):
                state=reader.snapshot()
                if state['errors']:raise AssertionError(('ActualIdleReaderFailure',state))
                return state['bytes']>=750000
            fixture.wait(advanced,30,'actual-idle-viewer-media-'+name)
        live=fixture.snapshot('idle-viewing-active');report['active']={k:live[k]for k in ['actual_upstream_connections','actual_provider_slots','actual_recording_ffmpeg_count']};report['actual_input_process_count']=len(processes())
        current=json.loads(fixture.get(fixture.origin+'/stats')[1]);report['actual_origin_connection_delta']=current['connections']-before['connections']
        if report['active']!={'actual_upstream_connections':1,'actual_provider_slots':1,'actual_recording_ffmpeg_count':0}or report['actual_input_process_count']!=1 or report['actual_origin_connection_delta']!=1:raise AssertionError('ActualOneInputAndUpstreamForTwoViewersRequired')
        metadata=fixture.redis(0,"local p='live:channel:'..ARGV[1];return {redis.call('SCARD',p..':clients'),redis.call('HLEN',p..':metadata'),redis.call('HGET',p..':metadata','state')or''}",fixture.channel_uuid)
        report['actual_client_count']=metadata[0]
        if metadata[0]!=2:raise AssertionError('ActualTwoClientRegistryRequired')
        for reader in readers:reader.close()
        report['readers']=[r.snapshot()for r in readers]
        if any(v['errors']or not v['ended']or v['http_status']!=200 or v['sync_errors']for v in report['readers']):raise AssertionError('ActualReaderSettlementRequired')
        for name in ['owner','follower']:
            path=out/(name+'.ts');local='/evidence/'+str(path.relative_to(fixture.output))
            probe=json.loads(fixture.command(fixture.compose+['exec','-T','baseline','ffprobe','-v','error','-show_entries','stream=codec_type,codec_name','-of','json',local],30))
            progress=fixture.command(fixture.compose+['exec','-T','baseline','ffmpeg','-hide_banner','-loglevel','error','-xerror','-progress','pipe:1','-nostats','-i',local,'-t','3','-map','0:v:0','-map','0:a:0','-f','null','-'],30,allow=True)
            actual=fixture.cli[-1];stderr=Path(actual['stderr']).read_bytes();frames=max([int(line.split('=')[1])for line in progress.splitlines()if line.startswith('frame=')],default=0);kinds={s['codec_type']for s in probe['streams']}
            decoded={'viewer':name,'actual_exit':actual['actual_exit'],'stdout_EOF':actual['stdout_EOF'],'stderr_EOF':actual['stderr_EOF'],'process_group_settled':actual['process_group_settled'],'stderr_bytes':len(stderr),'stderr_sha256':hashlib.sha256(stderr).hexdigest(),'video_frames':frames,'video_and_audio':{'video','audio'}<=kinds,'file_bytes':path.stat().st_size,'file_sha256':hashlib.sha256(path.read_bytes()).hexdigest(),'scope':'existing qualified3s positive A/V prefix decoder; not full-window resource useful-work acceptance'};report['decoders'].append(decoded);persist()
            if not(decoded['actual_exit']==0 and decoded['stdout_EOF']and decoded['stderr_EOF']and decoded['process_group_settled']and not stderr and frames>=70 and decoded['video_and_audio']):raise AssertionError('ActualQualifiedThreeSecondAVDecodeRequired')
        def retired():
            origin=json.loads(fixture.get(fixture.origin+'/stats')[1]);return origin.get('active')==0 and not processes()
        if fixture.backend=='Native':
            report['actual_idle_upstream_and_process_retired']=fixture.wait(retired,45,'actual-idle-input-retirement')
            report['actual_Main_normal_stop']=fixture.stop_native(expected=0)
        else:
            report['Source_post_reader_close_retirement_observation']={'upstream_active':json.loads(fixture.get(fixture.origin+'/stats')[1]).get('active'),'input_process_count':len(processes()),'timing_parity_claimed':False}
            fixture.stop_roles();report['actual_Source_roles_normal_stop']=not fixture.roles
            report['actual_upstream_retired_after_Main_shutdown']=fixture.wait(lambda:json.loads(fixture.get(fixture.origin+'/stats')[1]).get('active')==0,20,'actual-Source-upstream-after-normal-shutdown')
        report['status']='passed'
    except Exception as error:
        report.update(status='failed',error_type=type(error).__name__,error_label=str(error)[:256])
    finally:
        for reader in readers:
            try:reader.close()
            except Exception as error:report.update(status='failed',reader_cleanup_error=type(error).__name__)
        report['readers']=[reader.snapshot()for reader in readers]
        persist()
    return report
