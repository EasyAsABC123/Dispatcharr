"""Typed per-record Comskip business comparison; complete raw evidence is separate.
Original Dispatcharr, Celery, Comskip and FFmpeg authors retain their work credit.
This never claims whole manual-journal or natural-media byte equivalence.
"""
import base64,copy,datetime,hashlib,json,math,re,uuid
from pathlib import Path
HERE=Path(__file__).resolve().parent
TASK='apps.channels.tasks.comskip_process_recording'
RECORD='apps.channels.tasks.run_recording'
ART='apps.channels.tasks.prefetch_recording_artwork'
CALLBACKS={'callbacks':None,'errbacks':None,'chain':None,'chord':None}
LABELS=('positive-mark','mark-repeat','positive-cut','cut-repeat','natural-automatic-mark','natural-automatic-cut')

def load(root,name):return json.loads((Path(root)/(name+'.json')).read_text())
def instant(value,naive=False):
    assert isinstance(value,str)
    parsed=datetime.datetime.fromisoformat(value.replace('Z','+00:00'))
    if naive:
        assert parsed.tzinfo is None
        parsed=parsed.replace(tzinfo=datetime.timezone.utc)
    assert parsed.tzinfo is not None
    return parsed.timestamp()
def v4(value):
    assert isinstance(value,str)and str(uuid.UUID(value))==value and uuid.UUID(value).version==4
    return value
def integer(value):assert type(value)is int;return value
def equal(actual,expected,path='business'):
    assert type(actual)is type(expected),(path,'type',type(actual).__name__,type(expected).__name__)
    if isinstance(actual,dict):
        assert set(actual)==set(expected),(path,'keys',sorted(actual),sorted(expected))
        for key in actual:equal(actual[key],expected[key],path+'.'+key)
    elif isinstance(actual,list):
        assert len(actual)==len(expected),(path,'length',len(actual),len(expected))
        for index,(a,b)in enumerate(zip(actual,expected)):equal(a,b,path+'['+str(index)+']')
    else:assert actual==expected,(path,actual,expected)

def receiver(root,ident,source_ident):
    ws=load(root,'WS');ready=load(root,'receiver-ready');final=load(root,'receiver-final')
    name=ws['actual_receiver'];match=re.fullmatch(r'specific\.positive_([0-9a-f]{32})!receiver',name)
    assert match and uuid.UUID(hex=match[1]).version==4
    assert ready['receiver']==name and ready['actual_group_add_completed']is True and ready['continuous_retention']is True and ready['actual_euid']==65532
    assert final['actual_receiver']==name and final['pass']is True and final['reader_closed']is True and final['group_discard_completed']is True and final['failure_type']is None
    assert integer(final['records'])==len(ws['events'])==len(ws['actual_records'])
    assert ws['continuous_retention']is True
    assert ws['raw_events_base64']==[r['raw_base64']for r in ws['actual_records']]
    statuses=[]
    for event,record in zip(ws['events'],ws['actual_records']):
        equal(event,record['actual'],'WS.actual_record')
        assert hashlib.sha256(base64.b64decode(record['raw_base64'],validate=True)).hexdigest()==record['sha256']
        assert event['__asgi_channel__']==[name]
        data=event['data']
        if data.get('type')=='comskip_status':
            assert event['type']=='update'and data['recording_id']==ident
            projected=copy.deepcopy(data);projected['recording_id']=source_ident;statuses.append(projected)
    return statuses

def clock_window(journal):
    assert journal['terminal']['thread_settled']and journal['terminal']['terminal_barrier_observed']and journal['terminal']['terminal_barrier_fsynced']and journal['terminal']['writer_closed']
    assert journal['terminal']['terminal_reader_error']is None
    lower=min(float(row['actual_redis_time'])for row in journal['publications'])
    upper=max(float(row['time'])for row in journal['queue_operations'])
    assert math.isfinite(lower)and math.isfinite(upper)and lower<=upper
    return lower,upper

def task(frame,kind,ident,source_ident,ids,role,queue,args,projected_args,parent):
    headers=frame['headers'];properties=frame['properties'];ident_task=v4(headers['id'])
    assert ident_task==ids[role]and headers['task']==kind
    assert headers['parent_id']==(None if parent is None else ids[parent])
    assert headers['root_id']==(ident_task if parent is None else ids[parent])
    assert properties['correlation_id']==ident_task and properties['delivery_info']['routing_key']==queue
    assert headers['retries']==0 and type(headers['retries'])is int and headers['timelimit']==[None,None]
    assert headers['ignore_result']is False and headers['kwargsrepr']=='{}'
    body=json.loads(base64.b64decode(frame['body'],validate=True));equal(body,[args,{},CALLBACKS],'task.body')
    assert headers['argsrepr']==repr(tuple(args)if kind==TASK else args)
    return {'task':kind,'queue':queue,'args':projected_args,'kwargs':{},'callbacks':CALLBACKS,'parent':parent,'root':role if parent is None else parent,'retries':0,'timelimit':[None,None],'ignore_result':False}

def backend(value,ids,role,window):
    assert value['task_id']==ids[role]and value['status']=='SUCCESS'and value['traceback']is None
    done=instant(value['date_done']);assert window[0]<=done<=window[1],('backend clock outside actual journal',role,done,window)
    def mapped(item):
        if isinstance(item,str)and item in ids.values():return '@task.'+next(k for k,v in ids.items()if v==item)
        if isinstance(item,list):return [mapped(v)for v in item]
        if isinstance(item,dict):return {k:mapped(v)for k,v in item.items()}
        return item
    out=mapped(copy.deepcopy(value));out['date_done']='@actual-backend-completion';return out

def cp_paths(row,recording_file,effective_ini,source_row,source_ident):
    result=copy.deepcopy(row);assert integer(result['id'])>0
    result['id']=source_ident;props=result['custom_properties'];assert props['file_path']==recording_file
    props['file_path']=source_row['custom_properties']['file_path']
    if 'comskip'in props:
        assert props['comskip']['ini_path']==effective_ini
        props['comskip']['ini_path']=source_row['custom_properties']['comskip']['ini_path']
    return result

def project(root,label,*,source_cell,recording_file,effective_ini,ini_role,recording_id_map,is_source=False):
    assert label in LABELS
    automatic=label.startswith('natural-');repeat=label.endswith('repeat');mode='cut'if 'cut'in label else'mark'
    expected_role='default-packaged-ini'if label in('positive-cut','cut-repeat')else'custom-positive-ini'
    assert ini_role==expected_role
    raw_before=load(root,'SQL-before');raw_after=load(root,'SQL-after');source_after=load(source_cell,'SQL-after')
    source_row=source_after['recordings'][0]if automatic else source_after
    source_ident=integer(source_row['id']);assert set(recording_id_map)=={source_ident}
    ident=integer(recording_id_map[source_ident]);assert ident>0
    journal=load(root,'journal');window=clock_window(journal);http=load(root,'HTTP')
    assert http['credential_role']=='admin'and http['method']=='POST'
    allowed_contexts={'comskip-positive-automatic'if automatic else'comskip-positive-manual'}
    if not is_source:allowed_contexts.add('comskip-manual')
    assert http['context']in allowed_contexts
    statuses=receiver(root,ident,source_ident)
    media=load(root,'media-after');edl=load(root,'actual-generated-EDL');assert load(root,'EDL-watch-errors')==[]
    assert media['file_path']==recording_file and media['exists']is True and integer(media['bytes'])>0
    assert re.fullmatch('[0-9a-f]{64}',media['sha256'])
    assert media['probe']['actual_exit']==0
    streams=media['probe']['actual']['streams'];equal(streams,[{'codec_name':'h264','codec_type':'video'},{'codec_name':'aac','codec_type':'audio'}],'media stream codecs')
    assert math.isfinite(float(media['probe']['actual']['format']['duration']))and float(media['probe']['actual']['format']['duration'])>0
    observations={'actual_recording_id':ident,'actual_file_path':recording_file,'actual_effective_ini':effective_ini,'media':media,'raw_SQL_before':raw_before,'raw_SQL_after':raw_after}
    if not automatic:
        before=raw_before;after=raw_after;assert before['id']==after['id']==ident
        assert http['path']==f'/api/channels/recordings/{ident}/comskip/'and type(http['status'])is int and http['status']==200
        equal(http['body'],{'success':True,'queued':True},'HTTP.body')
        source_before=load(source_cell,'SQL-before')
        before_projected=cp_paths(before,recording_file,effective_ini,source_before,source_ident)
        after_projected=cp_paths(after,recording_file,effective_ini,source_row,source_ident)
        frame=load(root,'frame');ids={'comskip':v4(frame['headers']['id'])}
        selected=[r for r in journal['publications']if r['task']==TASK]
        assert len(selected)==1 and selected[0]['id']==ids['comskip']
        equal(frame,selected[0]['raw_frame'],'captured manual frame vs fulljournal')
        if not is_source:assert len(journal['publications'])==1,'Native manual epoch must retain/validate complete journal without background publications'
        graph=[task(frame,TASK,ident,source_ident,ids,'comskip','celery',[ident],[source_ident],None)]
        result=backend(load(root,'backend'),ids,'comskip',window)
        before_media=load(root,'media-before');assert before_media['file_path']==recording_file and before_media['exists']is True
        if label!='cut-repeat':assert before_media['sha256']=='be04f525c8783b7077b99130d5e26cc5941ce8e21eb887682248e2d5d2c4c29b'
        if repeat:
            equal(before,after,'repeat complete row');assert before_media['sha256']==media['sha256']and before_media['EDL']==media['EDL']
            assert not statuses and result['result']=='already_processed'
        else:
            assert result['result']=='ok'and len(edl)==1
            if mode=='mark':assert load(root,'media-before')['sha256']==media['sha256']and media['EDL']['exists']is True
            else:
                assert load(root,'media-before')['sha256']!=media['sha256']and not media['EDL']['exists']and not media['fixed_cut_paths_remaining']
                proof=load(Path(source_cell).parents[1],'manual-cut-content')['framehash']if is_source else load(root,'manual-content-proof')
                if not is_source:assert proof['passed']is True and proof['full_decoded_all_stream_content_and_time_exact']is True and proof['framehash_sha256']=='500d1a5d5a76bcf2034a1e9e9ac59dd02c9e3e7196daa01da66fd173e6315a89'
        return {'business':{'label':label,'HTTP':{'method':'POST','path':f'/api/channels/recordings/{source_ident}/comskip/','credential_role':'admin','status':http['status'],'body':http['body']},'SQL_before':before_projected,'SQL_after':after_projected,'task_graph':graph,'backend':result,'Comskip_WS':statuses,'media_streams':streams,'EDL_after_exists':media['EDL']['exists'],'EDL_text':None if repeat else next(iter(edl.values()))['text']},'typed_runtime_observations':observations}
    rows=[r for r in raw_after['recordings']if r['id']==ident];assert len(rows)==1
    row=rows[0];assert all(r['id']!=ident for r in raw_before['recordings'])
    prior={r['id']:r for r in raw_before['recordings']};remaining={r['id']:r for r in raw_after['recordings']if r['id']!=ident};equal(remaining,prior,'unrelated owned recordings')
    assert http['path']=='/api/channels/recordings/'and type(http['status'])is int and http['status']==201
    body=copy.deepcopy(http['body']);assert body['id']==ident and body['channel']==row['channel_id']and body['task_id']==row['task_id']=='dvr-recording-'+str(ident)
    start=instant(row['start_time']);end=instant(row['end_time']);assert end-start==520
    assert instant(body['start_time'])==start and instant(body['end_time'])==end
    body.update(id=source_ident,start_time='@actual-booking.start',end_time='@actual-booking.end',task_id='dvr-recording-'+str(source_ident))
    cp=copy.deepcopy(row['custom_properties']);assert cp['file_path']==recording_file and cp['file_name']==Path(recording_file).name
    assert cp['file_name']==datetime.datetime.fromtimestamp(start,datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')+'.mkv'
    assert cp['file_url']==cp['output_file_url']==f'/api/channels/recordings/{ident}/file/'
    assert cp['status']=='completed'and cp['remux_success']is True and cp['fixture_case_label']==label
    started=instant(cp['started_at'],naive=True);ended=instant(cp['ended_at'],naive=True);assert start<=started<=end<=ended<=window[1]
    assert type(cp['bytes_written'])is int and cp['bytes_written']>0
    assert cp['comskip']['ini_path']==effective_ini and cp['comskip']['edl']==Path(recording_file).with_suffix('.edl').name
    cp.update(file_path='@owned-recording.file',file_name='@owned-recording.basename',file_url=f'/api/channels/recordings/{source_ident}/file/',output_file_url=f'/api/channels/recordings/{source_ident}/file/',started_at='@actual-recording.started',ended_at='@actual-recording.ended',bytes_written='@actual-recording.bytes')
    cp['comskip']['ini_path']='@'+ini_role;cp['comskip']['edl']='@owned-recording.edl'
    dvr=copy.deepcopy(raw_after['DVR']);equal(raw_before['DVR'],raw_after['DVR'],'DVR unchanged during task');assert dvr['comskip_custom_path']==effective_ini and dvr['comskip_mode']==mode and dvr['comskip_enabled']is True and dvr['comskip_hw_accel']=='none';dvr['comskip_custom_path']='@'+ini_role
    pubs=journal['publications'];assert [r['task']for r in pubs]==[ART,RECORD,TASK]and [r['queue']for r in pubs]==['celery','dvr','celery']
    ids={role:v4(pub['id'])for role,pub in zip(['artwork','recording','comskip'],pubs)};assert len(set(ids.values()))==3
    frames={role:pub['raw_frame']for role,pub in zip(['artwork','recording','comskip'],pubs)}
    for role in ['recording','comskip']:equal(load(root,role+'-frame'),frames[role],'captured automatic frame vs fulljournal')
    if (Path(root)/'artwork-frame.json').exists():equal(load(root,'artwork-frame'),frames['artwork'],'captured artwork frame vs fulljournal')
    else:assert is_source and label=='natural-automatic-mark'
    parent_args=json.loads(base64.b64decode(frames['recording']['body']))[0];assert parent_args[:2]==[ident,row['channel_id']]and len(parent_args)==4 and instant(parent_args[2])==start and instant(parent_args[3])==end
    assert frames['recording']['headers']['periodic_task_name']==row['task_id']
    eta=instant(frames['artwork']['headers']['eta']);assert 0<=eta-float(pubs[0]['actual_redis_time'])<=1 and eta<float(pubs[1]['actual_redis_time'])<float(pubs[2]['actual_redis_time'])
    graph=[task(frames['artwork'],ART,ident,source_ident,ids,'artwork','celery',[ident],[source_ident],None),task(frames['recording'],RECORD,ident,source_ident,ids,'recording','dvr',parent_args,[source_ident,row['channel_id'],'@actual-booking.start','@actual-booking.end'],None),task(frames['comskip'],TASK,ident,source_ident,ids,'comskip','celery',[ident],[source_ident],'recording')]
    results={role:backend(load(root,role+'-backend'),ids,role,window)for role in ['recording','comskip']}
    assert results['recording']['result']is None and results['comskip']['result']=='ok'
    if (Path(root)/'artwork-backend.json').exists():
        art=backend(load(root,'artwork-backend'),ids,'artwork',window);assert art['result']=='ok'and art['children']==[]
        if label=='natural-automatic-cut':results['artwork']=art
    else:assert is_source and label=='natural-automatic-mark'
    assert len(edl)==1
    return {'business':{'label':label,'HTTP':{'method':'POST','path':http['path'],'credential_role':'admin','status':http['status'],'body':body},'recording':{'id':source_ident,'channel_id':row['channel_id'],'duration_seconds':520,'task_id':'dvr-recording-'+str(source_ident),'custom_properties':cp},'DVR':dvr,'task_graph':graph,'backends':results,'Comskip_WS':statuses,'media_streams':streams,'EDL_after_exists':media['EDL']['exists'],'EDL_text':next(iter(edl.values()))['text'],'Source_MARK_artwork_backend_unobserved':label=='natural-automatic-mark'},'typed_runtime_observations':observations}

def compare_business_cell(source_root,label,native_cell_dir,*,recording_file,effective_ini,ini_role,recording_id_map):
    source=Path(source_root)/'cells'/label;raw=load(source,'SQL-after');row=raw['recordings'][0]if label.startswith('natural-')else raw
    source_file=row['custom_properties']['file_path'];source_ini=row['custom_properties']['comskip']['ini_path'];source_id=row['id']
    expected=project(source,label,source_cell=source,recording_file=source_file,effective_ini=source_ini,ini_role=ini_role,recording_id_map={source_id:source_id},is_source=True)
    actual=project(native_cell_dir,label,source_cell=source,recording_file=recording_file,effective_ini=effective_ini,ini_role=ini_role,recording_id_map=recording_id_map)
    equal(actual['business'],expected['business'])
    return {'passed':True,'label':label,'typed_business_parity':True,'full_raw_observations_retained':True,'whole_manual_producer_parity_claimed':False,'natural_whole_media_equivalence_claimed':False,'manual_CUT_content_comparison_required':label=='positive-cut','Source_MARK_artwork_backend_claimed_observed':False}
