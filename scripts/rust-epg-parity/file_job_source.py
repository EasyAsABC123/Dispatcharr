"""Actual original refresh_epg_data local-file task/lock/dispatch trace.
Original Dispatcharr authors retain all application and algorithm credit.
Child dispatch is observed, not executed; this is not native/app acceptance.
"""
import base64
import datetime as dt
import gzip
import json
import os
from pathlib import Path
import sys
import uuid
from unittest.mock import patch
from django.test import override_settings

if os.environ.get('POSTGRES_PASSWORD')!='playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell');os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGData,ProgramData,EPGSourceIndex
from apps.channels.models import Channel
from apps.channels import tasks as channel_tasks
from core import utils as core_utils
from core.models import SystemEvent
from django.db import connection
from django.utils import timezone

PREFIX='rust-epg-file-job-'
NOW=dt.datetime(2026,11,9,12,34,56,123456,tzinfo=dt.timezone.utc)
MEDIA='/tmp/rust-epg-file-job-media-'+uuid.uuid4().hex
if EPGSource.objects.filter(name__startswith=PREFIX).exists():raise RuntimeError('Fresh namespace required')
redis=core_utils.RedisClient.get_client()


def main():
    cases=[]
    names=['local-xml','empty-guide','no-mappings','local-gzip','local-incomplete-xml','local-incomplete-txt','missing-no-url','inactive','dummy','refresh-held','file-held','per-channel-active','defer-exhausted','bad-counter','missing-source','auto-logo-on','auto-logo-bad-properties']
    for ordinal,name in enumerate(names):
        suffix='.gz' if name=='local-gzip' else '.txt' if name=='local-incomplete-txt' else '.xml'
        path=Path('/tmp/'+PREFIX+name+suffix)
        body='<channel id="mapped"><display-name>Refreshed guide</display-name></channel><programme channel="mapped" start="20261110110000 +0000" stop="20261110120000 +0000"><title>Refreshed programme</title></programme>'
        if name=='auto-logo-on':body=body.replace('</display-name>','</display-name><icon src="https://fixture.invalid/rust-file-job-auto-logo/new"/>',1)
        payload=('<tv>'+('' if name=='empty-guide' else body)+('' if 'incomplete' in name else '</tv>')).encode()
        path.write_bytes(gzip.compress(payload,mtime=0) if name=='local-gzip' else payload)
        if name=='missing-no-url':path.unlink()
        if name=='missing-source':
            source_id=999999;source=None;before_rows=[]
        else:
            source=EPGSource(name=PREFIX+name,source_type='dummy' if name=='dummy' else 'xmltv',is_active=name!='inactive',file_path=str(path),custom_properties={'auto_apply_epg_logos':True} if name=='auto-logo-on' else [True] if name=='auto-logo-bad-properties' else {})
            EPGSource.objects.bulk_create([source]);source_id=source.pk
            epg=EPGData.objects.create(epg_source=source,tvg_id='mapped',name='Before guide')
            if name!='no-mappings':Channel.objects.bulk_create([Channel(name=PREFIX+name,channel_number=1_650_000+ordinal,epg_data=epg)])
            ProgramData.objects.create(epg=epg,title='Before programme',start_time=dt.datetime(2026,11,9,10,tzinfo=dt.timezone.utc),end_time=dt.datetime(2026,11,9,11,tzinfo=dt.timezone.utc),tvg_id='mapped')
            EPGSourceIndex.objects.create(source=source,data={'fixture':'old byte-offset index'})
            before_rows=list(EPGData.objects.filter(epg_source=source).values('id','tvg_id','name','icon_url'))
        refresh=f'task_lock_refresh_epg_data_{source_id}';file_key=f'task_lock_epg_source_file_{source_id}';counter=f'epg_source_tvg_parse_{source_id}'
        if name=='refresh-held':redis.set(refresh,'locked',ex=300)
        if name in ('file-held','defer-exhausted'):redis.set(file_key,'locked',ex=300)
        if name in ('per-channel-active','bad-counter'):redis.set(counter,'1' if name=='per-channel-active' else 'not-an-integer')
        effects=[]
        def state():
            row=EPGSource.objects.filter(pk=source_id).values('status','last_message','updated_at','file_path','extracted_file_path').first()
            if row is not None and row['updated_at'] is not None:row['updated_at']=row['updated_at'].isoformat()
            index=EPGSourceIndex.objects.filter(source_id=source_id).values_list('data',flat=True).first()
            generation=redis.get('epg_content_generation')
            return {'generation':generation.decode() if isinstance(generation,bytes) else generation,'source':row,'epg_rows':list(EPGData.objects.filter(epg_source_id=source_id).order_by('id').values('id','tvg_id','name','icon_url')),'programmes':list(ProgramData.objects.filter(epg__epg_source_id=source_id).order_by('id').values('epg_id','title','start_time','end_time')),'index':index,'refresh_held':bool(redis.exists(refresh)),'file_held':bool(redis.exists(file_key)),'base_logo_urls':list(Channel.objects.filter(epg_data__epg_source_id=source_id).order_by('id').values_list('logo__url',flat=True)),'audit_events':list(SystemEvent.objects.filter(details__source_name=PREFIX+name).values('event_type','details'))}
        def event(kind,**data):effects.append({'kind':kind,**data,'state':state()})
        def wire(group,message):event('publication',group=group,message=message)
        def index(*args,**kwargs):event('index_dispatch',args=args,kwargs=kwargs)
        def rules(*args,**kwargs):event('series_dispatch',args=args,kwargs=kwargs)
        def deferred(*args,**kwargs):event('refresh_defer_dispatch',args=args,kwargs=kwargs)
        try:
            before=state()
            before_channels=list(Channel.objects.filter(epg_data__epg_source_id=source_id).values('id','logo_id'))
            before_programmes=list(ProgramData.objects.filter(epg__epg_source_id=source_id).values())
            with override_settings(MEDIA_ROOT=MEDIA), \
                 patch.object(tasks.timezone,'now',return_value=NOW), \
                 patch.object(tasks,'_is_celery_worker_context',return_value=True), \
                 patch.object(core_utils,'_should_use_sync_websocket_send',return_value=True), \
                 patch.object(core_utils,'_gevent_ws_send',side_effect=wire), \
                 patch.object(core_utils,'_dispatch_system_event_integrations'), \
                 patch.object(tasks,'build_programme_index_task') as build_index, \
                 patch.object(channel_tasks,'evaluate_series_rules') as series, \
                 patch.object(tasks.refresh_epg_data,'apply_async',side_effect=deferred):
                build_index.delay.side_effect=index;series.delay.side_effect=rules
                result=tasks.refresh_epg_data.run(source_id,force=True,_file_defer_retry=480 if name=='defer-exhausted' else 0)
            cases.append({'name':name,'source_id':source_id,'before':before,'before_epg_rows':before_rows,'result':result,'after':state(),'effects':effects,'input_bytes':len(payload),'xml_base64':base64.b64encode(payload).decode(),'file_base64':base64.b64encode(gzip.compress(payload,mtime=0) if name=='local-gzip' else payload).decode(),'file_path':str(path),'source_name':PREFIX+name,'now':NOW.isoformat(),'before_programmes':before_programmes,'before_channels':before_channels})
        finally:
            redis.delete(refresh,file_key,counter)
            path.unlink(missing_ok=True)
            if source is not None:
                source.refresh_from_db()
                if source.extracted_file_path and source.extracted_file_path.startswith(MEDIA+'/cached_epg/'):
                    Path(source.extracted_file_path).unlink(missing_ok=True)
    print(json.dumps({'format_version':1,'cache_dir':MEDIA+'/cached_epg','scope':'original local-file refresh task locks/effects/child dispatch boundaries','cases':cases,'native_verified':False,'child_task_execution_verified':False,'http_dispatch_verified':False,'performance_verified':False},default=lambda value:value.isoformat() if hasattr(value,'isoformat') else str(value)))

main()
