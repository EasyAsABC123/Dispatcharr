"""Original guide import producer/status ordering on a disposable migrated DB.
Original Dispatcharr authors retain all application and algorithm credit.
Publication envelopes are intercepted at the original wire sender; this is not
an end-to-end WebSocket delivery or external integration dispatch proof.
"""
import base64
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import sys
from unittest.mock import patch

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.epg import tasks
from apps.epg.models import EPGSource, EPGData, ProgramData
from apps.channels.models import Channel
from apps.output import streaming_chunk_cache as chunks
from core import utils as core_utils
from core.models import CoreSettings, EpgHistoryPolicy, SystemEvent
from django.core.cache import cache
from django.db import connection

NOW = dt.datetime(2026, 11, 9, 12, tzinfo=dt.timezone.utc)
PREFIX = 'rust-epg-reporter-'
if EPGSource.objects.filter(name__startswith=PREFIX).exists():
    raise RuntimeError('Fixture namespace must be fresh')

def programme(channel='mapped', start='20261109110000 +0000'):
    return f'<programme channel="{channel}" start="{start}" stop="20261109120000 +0000"><title>Fixture</title></programme>'

def main():
    definitions = [
        ('valid', programme()+programme('unmapped')+programme(start='bad')),
        ('empty-complete', ''),
        ('no-mappings-no-file', ''),
        ('mapped-null-empty', programme('')+programme('mapped')),
        ('missing-file', ''),
        ('incomplete', programme()),
        ('progress-5000', programme()*5001),
        ('invalidate-wrongtype', programme()),
        ('swap-error', programme()),
    ]
    cases = []
    redis = chunks._get_redis()
    policy = EpgHistoryPolicy(1, 30, 0, 0)
    for ordinal, (name, body) in enumerate(definitions):
        source = EPGSource.objects.create(name=PREFIX+name, source_type='xmltv', is_active=False,
                                         status='parsing', last_message='Before import')
        EPGSource.objects.filter(pk=source.pk).update(updated_at=NOW-dt.timedelta(days=1))
        source.refresh_from_db()
        epgs = [EPGData.objects.create(epg_source=source, tvg_id=tvg, name=PREFIX+str(ordinal)+'-'+str(i))
                for i,tvg in enumerate(['mapped', 'unmapped', None, ''])]
        if name != 'no-mappings-no-file':
            for index in ([2,3] if name == 'mapped-null-empty' else [0]):
                Channel.objects.create(name=PREFIX+name+str(index), channel_number=950000+ordinal*10+index,
                                       epg_data=epgs[index])
        ProgramData.objects.create(epg=epgs[0], title='Before import', start_time=NOW-dt.timedelta(hours=1),
                                   end_time=NOW, tvg_id='mapped')
        path = Path('/tmp/'+PREFIX+name+'.xml')
        payload = ('<tv>'+body+('' if name=='incomplete' else '</tv>')).encode()
        path.write_bytes(payload)
        if name in ('missing-file','no-mappings-no-file'):
            path.unlink()
        source.file_path = str(path)
        source.save(update_fields=['file_path'])
        redis.set(chunks.EPG_CACHE_GENERATION_KEY, 'not-an-integer' if name=='invalidate-wrongtype' else 10)
        effects = []
        def state():
            row = EPGSource.objects.values('status','last_message','updated_at').get(pk=source.pk)
            row['updated_at'] = row['updated_at'].isoformat()
            row['programmes'] = ProgramData.objects.filter(epg__epg_source=source).count()
            row['in_atomic_block'] = connection.in_atomic_block
            row['audit_events'] = SystemEvent.objects.filter(details__source_name=source.name).count()
            value = redis.get(chunks.EPG_CACHE_GENERATION_KEY)
            row['generation'] = value.decode() if isinstance(value,bytes) else value
            return row
        def effect(kind, **values):
            effects.append({'kind':kind, **values, 'state':state()})
        def wire(group, message):
            # Original send_websocket_update constructs this exact outer envelope.
            effect('publication', group=group, message=message)
        original_flush = tasks._flush_epg_program_staging_batch
        def flush(batch):
            original_flush(batch)
            effect('staged', rows=len(batch))
        original_swap = tasks._swap_staged_epg_programs
        def swap(*args, **kwargs):
            if name == 'swap-error':
                raise RuntimeError('fixture swap failure')
            result = original_swap(*args, **kwargs)
            effect('swapped_inside_transaction')
            return result
        original_invalidate = chunks.invalidate_epg_chunk_cache
        def invalidate():
            effect('invalidation_before')
            original_invalidate()
            effect('invalidation_after')
        original_audit = tasks.log_system_event
        def audit(*args, **kwargs):
            original_audit(*args, **kwargs)
            effect('audit_returned', arguments=kwargs,
                   stored=list(SystemEvent.objects.filter(details__source_name=source.name).values('event_type','channel_id','channel_name','details')))
        def late(*args):
            effect('late_mapping_dispatch_boundary', mapped_ids=sorted(args[1]))
        try:
            with patch.object(core_utils,'_should_use_sync_websocket_send',return_value=True), \
                 patch.object(core_utils,'_gevent_ws_send',side_effect=wire), \
                 patch.object(core_utils,'_dispatch_system_event_integrations'), \
                 patch.object(tasks,'_flush_epg_program_staging_batch',side_effect=flush), \
                 patch.object(tasks,'_swap_staged_epg_programs',side_effect=swap), \
                 patch.object(chunks,'invalidate_epg_chunk_cache',side_effect=invalidate), \
                 patch.object(tasks,'log_system_event',side_effect=audit), \
                 patch.object(tasks,'_dispatch_late_mapped_epg_parses',side_effect=late), \
                 patch.object(CoreSettings,'get_epg_history_policy',return_value=policy), \
                 patch.object(tasks.timezone,'now',return_value=NOW):
                before = state()
                before_rows=list(ProgramData.objects.filter(epg__epg_source=source).values('id','epg_id','start_time','end_time','title','sub_title','description','tvg_id','program_id','custom_properties'))
                passed = tasks.parse_programs_for_source(source)
                after = state()
            cases.append({'name':name,'source_id':source.pk,'source_name':source.name,
                          'xml_base64':base64.b64encode(payload).decode(),'file_exists':path.exists(),
                          'passed':passed,'before':before,'before_programmes':before_rows,'after':after,'effects':effects})
            # Preserve a pristine per-source state for the native importer; the
            # original response/effect trace above is never rewritten.
            ProgramData.objects.filter(epg__epg_source=source).delete()
            ProgramData.objects.bulk_create([ProgramData(**row) for row in before_rows])
            EPGSource.objects.filter(pk=source.pk).update(status=before['status'],last_message=before['last_message'],updated_at=before['updated_at'])
            SystemEvent.objects.filter(details__source_name=source.name).delete()
        finally:
            path.unlink(missing_ok=True)
    redis.delete(chunks.EPG_CACHE_GENERATION_KEY)
    system_settings=CoreSettings.get_system_settings()
    stored_system=CoreSettings.objects.get(key='system_settings')
    saved_system=stored_system.value
    CoreSettings.objects.filter(pk=stored_system.pk).update(value={})
    cache.delete(CoreSettings.group_cache_key('system_settings'))
    system_default_cache=CoreSettings.get_system_settings()
    CoreSettings.objects.filter(pk=stored_system.pk).update(value=saved_system)
    cache.delete(CoreSettings.group_cache_key('system_settings'))
    print(json.dumps({'format_version':1,'scope':'original parse_programs_for_source producer/status ordering',
                      'cases':cases,'system_settings':system_settings,'system_default_cache':system_default_cache,'websocket_transport_verified':False,'external_integrations_verified':False,
                      'native_verified':False,'performance_verified':False},default=str))

main()
