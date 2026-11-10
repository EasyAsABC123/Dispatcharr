"""Original XMLTV channel pass on real disposable ORM rows.
Full application/algorithm credit belongs to original Dispatcharr contributors.
This characterizes the helper, not the complete refresh job or dispatch.
"""
import base64
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0,'/app');sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE','dispatcharr.settings')
import django
django.setup()
from apps.epg import tasks
from apps.epg.models import EPGSource,EPGData,ProgramData
from apps.channels.models import Channel,ChannelOverride,Logo
from apps.channels import utils as channel_utils
from core import utils as core_utils
from core.models import SystemEvent
from django.utils import timezone

PREFIX='rust-guide-channels-'
if EPGSource.objects.filter(name__startswith=PREFIX).exists():
    raise RuntimeError('Fresh fixture namespace required')


def channel(identifier,name='',icons=''):
    return '<channel id="'+identifier+'">'+name+icons+'</channel>'

def rows(source):
    return list(EPGData.objects.filter(epg_source=source).order_by('id').values('id','tvg_id','name','icon_url'))


def main():
    definitions=[
        ('basic',channel('new','<display-name>New guide</display-name>','<icon src="https://fixture.invalid/new.png"/>')+channel('existing','<display-name>Updated</display-name>')),
        ('duplicate-new',channel('new','<display-name>First</display-name>')+channel('new','<display-name>Last</display-name>')),
        ('duplicate-existing',channel('existing','<display-name>First</display-name>')+channel('existing','<display-name>Last</display-name>')),
        ('blank-identifiers','<channel/><channel id=" "/>'+channel('&#160;existing&#160;','<display-name>Trimmed</display-name>')),
        ('display-fallback',channel('existing','<display-name> </display-name><display-name>Later</display-name>')+channel('new','<display-name><b>Nested</b>tail</display-name><display-name>Direct</display-name>')),
        ('blank-name-icon-tail',channel('existing','<display-name> </display-name><display-name>Later</display-name>','<icon src="https://fixture.invalid/first"/><icon src="https://fixture.invalid/last"/>')),
        ('icon-order',channel('existing','<icon src=""/><icon src="https://fixture.invalid/first"/><display-name>A</display-name><icon src="https://fixture.invalid/ignored"/>')),
        ('unicode-length',channel('existing','<display-name>'+'é🦀'*300+'</display-name>','<icon src="'+'é'*500+'"/>')+channel('new','','<icon src="'+'x'*501+'"/>')),
        ('namespaces','<x:channel xmlns:x="urn:fixture" id="ignored"><display-name>Ignored</display-name></x:channel>'+channel('existing','<x:display-name xmlns:x="urn:fixture">Ignored</x:display-name>')),
        ('programmes-only','<programme channel="existing"><title>Ignored programme</title></programme>'),
        ('empty-complete',''),
        ('incomplete-helper',channel('new','<display-name>Recovered</display-name>')),
        ('create-flush-500',''.join(channel('new-'+str(i),'<display-name>Guide '+str(i)+'</display-name>') for i in range(501))),
        ('cache-clear-1000-first-update-wins','<channel/>'*999+channel('existing','<display-name>First</display-name>')+channel('existing','<display-name>Second</display-name>')),
        ('new-duplicate-after-flush',''.join(channel('new-'+str(i),'<display-name>First</display-name>') for i in range(500))+channel('new-0','<display-name>Second</display-name>')),
        ('create-error-at-500',''.join(channel('new-'+str(i),'<display-name>Valid</display-name>') for i in range(499))+channel('x'*256)),
        ('identifier-too-long',channel('x'*256,'<display-name>Too long</display-name>')),
        ('flush-before-error',''.join(channel('new-'+str(i),'<display-name>Guide</display-name>') for i in range(501))+channel('x'*256)),
        ('missing-file',''),
        ('auto-logo-on',channel('mapped-base','<display-name>Automatic icon</display-name>','<icon src="https://fixture.invalid/rust-auto-logo-on/new"/>')),
        ('auto-logo-bad-properties',channel('mapped-base','<display-name>Stored before property error</display-name>')),
    ]
    cases=[]
    for ordinal,(name,body) in enumerate(definitions):
        properties={'auto_apply_epg_logos':True} if name=='auto-logo-on' else [True] if name=='auto-logo-bad-properties' else {}
        source=EPGSource.objects.create(name=PREFIX+name,is_active=False,source_type='xmltv',custom_properties=properties)
        initial=[]
        for index,tvg in enumerate(['existing','stale','mapped-base','mapped-override',None,'']):
            initial.append(EPGData(tvg_id=tvg,name='Before '+str(index),icon_url='https://fixture.invalid/old',epg_source=source))
        EPGData.objects.bulk_create(initial)
        initial=list(EPGData.objects.filter(epg_source=source).order_by('id'))
        base=Channel.objects.create(name=PREFIX+name,channel_number=1_560_000+ordinal,epg_data=initial[2])
        override=ChannelOverride.objects.create(channel=base,epg_data=initial[3])
        for epg in initial:
            ProgramData.objects.create(epg=epg,title='Stored',start_time=timezone.now(),end_time=timezone.now(),tvg_id=epg.tvg_id)
        path=Path('/tmp/'+PREFIX+name+'.xml')
        payload=('<tv>'+body+('' if name=='incomplete-helper' else '</tv>')).encode()
        path.write_bytes(payload)
        if name=='missing-file':path.unlink()
        source.file_path=str(path);source.save(update_fields=['file_path'])
        base_logo_before=base.logo_id
        before=rows(source)
        before_programmes=list(ProgramData.objects.filter(epg__epg_source=source).values('id','epg_id','start_time','end_time','title','sub_title','description','tvg_id','program_id','custom_properties'))
        effects=[]
        def state():
            stored=EPGSource.objects.values('status','last_message','updated_at','file_path').get(pk=source.pk)
            stored['updated_at']=stored['updated_at'].isoformat() if stored['updated_at'] is not None else None
            stored['epg_rows']=EPGData.objects.filter(epg_source=source).count()
            stored['programmes']=ProgramData.objects.filter(epg__epg_source=source).count()
            stored['audit_events']=SystemEvent.objects.filter(details__source_name=source.name).count()
            stored['base_logo_url']=Channel.objects.filter(pk=base.pk).values_list('logo__url',flat=True).first()
            return stored
        def wire(group,message):
            effects.append({'kind':'publication','group':group,'message':message,'state':state()})
        def logos(value):
            effects.append({'kind':'logo_boundary','state':state()})
            # All fixtures disable auto-apply; invoke the real no-op policy.
            return original_logos(value)
        original_logos=channel_utils.maybe_auto_apply_epg_logos
        try:
            with patch.object(core_utils,'_should_use_sync_websocket_send',return_value=True), \
                 patch.object(core_utils,'_gevent_ws_send',side_effect=wire), \
                 patch.object(core_utils,'_dispatch_system_event_integrations'), \
                 patch.object(channel_utils,'maybe_auto_apply_epg_logos',side_effect=logos):
                before_state=state()
                passed=tasks.parse_channels_only(source)
                after_state=state()
            base.refresh_from_db();override.refresh_from_db()
            cases.append({'name':name,'source_id':source.pk,'xml_base64':base64.b64encode(payload).decode(),
                'file_exists':path.exists(),'before':before,'before_programmes':before_programmes,'before_state':before_state,'after':rows(source),
                'after_state':after_state,'passed':passed,'effects':effects,'base_epg_id':base.epg_data_id,
                'override_epg_id':override.epg_data_id,'base_id':base.pk,'override_id':override.pk,'source_properties':properties})
            # Restore only this isolated source for the native pass. Keep the
            # original rows/effects above immutable, including partial failures.
            ProgramData.objects.filter(epg__epg_source=source).delete()
            EPGData.objects.filter(epg_source=source).delete()
            EPGData.objects.bulk_create([EPGData(epg_source=source,**row) for row in before])
            ProgramData.objects.bulk_create([ProgramData(**row) for row in before_programmes])
            Channel.objects.filter(pk=base.pk).update(epg_data_id=before[2]['id'],logo_id=base_logo_before)
            Logo.objects.filter(url='https://fixture.invalid/rust-auto-logo-on/new').delete() if name=='auto-logo-on' else None
            ChannelOverride.objects.filter(pk=override.pk).update(epg_data_id=before[3]['id'])
            EPGSource.objects.filter(pk=source.pk).update(status=before_state['status'],last_message=before_state['last_message'],updated_at=before_state['updated_at'],file_path=before_state['file_path'])
            SystemEvent.objects.filter(details__source_name=source.name).delete()
        finally:
            path.unlink(missing_ok=True)
    print(json.dumps({'format_version':1,'scope':'original parse_channels_only helper rows/progress/stale cleanup',
                      'cases':cases,'native_verified':False,'full_refresh_job_verified':False,
                      'websocket_transport_verified':False,'auto_logo_enabled_variants_verified':False},default=str))

main()
