"""Original file-to-guide rows/skip/error/retention oracle on disposable Django.
Original Dispatcharr authors retain all application and algorithm credit.
This is a file import component proof, not scheduling/status/WS job migration.
"""
import base64
import contextlib
import datetime as dt
import json
import os
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch
import uuid
if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable original schema fixture required')
sys.path.insert(0, '/app')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from apps.epg import tasks, utils
from apps.epg.models import EPGSource, EPGData, ProgramData
from apps.channels.models import Channel, ChannelOverride, ChannelGroup
from core.models import EpgHistoryPolicy, CoreSettings
from django.core.cache import cache
NOW = dt.datetime(2026, 11, 9, tzinfo=dt.timezone.utc)
FIELDS = ['id', 'epg_id', 'start_time', 'end_time', 'title', 'sub_title', 'description', 'tvg_id', 'program_id', 'custom_properties']


def programme(channel='mapped', title='A programme', start='20261109110000 +0000', stop='20261109120000 +0000', extra=''):
    return f'<programme channel="{channel}" start="{start}" stop="{stop}"><title>{title}</title>{extra}</programme>'


def snapshot(ids):
    return list(ProgramData.objects.filter(epg_id__in=ids).order_by('id').values(*FIELDS))


def serialize(value):
    if isinstance(value, dt.datetime):
        return value.isoformat()
    raise TypeError(type(value).__name__)


def main():
    definitions = [
        ('valid', programme() + programme('override', 'Alternate guide')),
        ('unmapped-invalid', programme('unmapped') + programme(start='bad') + programme()),
        ('metadata-unicode-title', programme(title='é🦀' * 160, extra='<sub-title>subtitle</sub-title><desc>S2 E3 — Plot &amp; details</desc><episode-num system="xmltv_ns">1.2.</episode-num><category>News</category><icon src="https://fixture.invalid/poster.png"/>')),
        ('malformed-recovery', '<programme channel="mapped" start="20261109110000 +0000" stop="20261109120000 +0000"><title>Recovered</programme>'),
        ('invalid-all', programme(start='bad') + '<programme channel="mapped"/>'),
        ('retention-future-boundary', programme(start='20261107000000 +0000', stop='20261108000000 +0000') + programme(start='20261109000000 +0000', stop='20261109010000 +0000')),
        ('retention-catchup-override', programme('override', 'Catch-up retained', start='20261106000000 +0000', stop='20261106010000 +0000') + programme('mapped')),
        ('no-mappings-no-file', ''),
        ('mapped-null-empty', programme('') + programme('mapped')),
        ('incomplete', programme()),
        ('missing-file', ''),
        ('empty-complete', ''),
        ('many-batches', ''.join(programme(title=f'Programme {index}') for index in range(23))),
        ('policy-clock-change', ''.join(programme(title=f'Late cutoff {index}') for index in range(7))),
        ('cached-policy-clock-change', ''.join(programme(title=f'Cached late cutoff {index}') for index in range(7))),
    ]
    cases = []
    for ordinal, (name, body) in enumerate(definitions):
        label = 'rust-file-guide-' + uuid.uuid4().hex
        source = EPGSource.objects.create(name=label, source_type='xmltv', is_active=False)
        epgs = [EPGData.objects.create(name=label + str(i), tvg_id=tvg, epg_source=source)
                for i, tvg in enumerate(['mapped', 'override', 'unmapped'])]
        group = ChannelGroup.objects.create(name=label)
        channel = None
        if name != 'no-mappings-no-file':
            channel = Channel.objects.create(name=label, channel_number=800000 + ordinal,
                channel_group=group, epg_data=epgs[0], is_catchup=name == 'retention-catchup-override', catchup_days=5)
            ChannelOverride.objects.create(channel=channel, epg_data=epgs[1])
        if name == 'mapped-null-empty':
            epgs[0].tvg_id = None
            epgs[0].save(update_fields=['tvg_id'])
            epgs[1].tvg_id = ''
            epgs[1].save(update_fields=['tvg_id'])
        ids = [entry.pk for entry in epgs]
        for epg in epgs:
            for label, start, end in [('old', -48, -36), ('boundary', -26, -24), ('current', -2, 0), ('future', 0, 2)]:
                ProgramData.objects.create(epg=epg, start_time=NOW + dt.timedelta(hours=start),
                    end_time=NOW + dt.timedelta(hours=end), title=label, description='stored',
                    tvg_id=epg.tvg_id, program_id='legacy-' + label, custom_properties={'keep': True})
        before = snapshot(ids)
        payload = ('<?xml version="1.0"?><tv>' + body + ('' if name == 'incomplete' else '</tv>')).encode()
        descriptor, filename = tempfile.mkstemp(prefix='rust-file-guide-', suffix='.xml')
        os.close(descriptor)
        path = Path(filename)
        path.write_bytes(payload)
        if name in ('missing-file', 'no-mappings-no-file'):
            path.unlink()
        source.file_path = str(path)
        source.save(update_fields=['file_path'])
        published = []
        audits = []
        policy = EpgHistoryPolicy(1, 30, 0, 0)
        live = {'now': NOW, 'policy': policy, 'flushes': 0}
        if name == 'cached-policy-clock-change':
            CoreSettings.objects.update_or_create(key='epg_settings', defaults={'name': 'Cached import policy fixture', 'value': {'epg_history_min_days': 1, 'epg_history_max_days': 30}})
            cache.set(CoreSettings.group_cache_key('epg_settings'), {'epg_history_min_days': 1, 'epg_history_max_days': 30}, timeout=300)
        original_flush = tasks._flush_epg_program_staging_batch
        def flush(batch):
            original_flush(batch)
            live['flushes'] += 1
            if name in ('policy-clock-change', 'cached-policy-clock-change') and live['flushes'] == 1:
                live['now'] = NOW + dt.timedelta(days=1)
                live['policy'] = EpgHistoryPolicy(0, 0, 0, 0)
                if name == 'cached-policy-clock-change':
                    cache.set(CoreSettings.group_cache_key('epg_settings'), {'epg_history_min_days': 0, 'epg_history_max_days': 0}, timeout=300)
        policy_override = contextlib.nullcontext() if name == 'cached-policy-clock-change' else patch('core.models.CoreSettings.get_epg_history_policy', side_effect=lambda: live['policy'])
        try:
            with patch.object(tasks, 'send_epg_update', side_effect=lambda *args, **kw: published.append({'args': args, 'kwargs': kw})), \
                 patch.object(tasks, 'log_system_event', side_effect=lambda *args, **kw: audits.append(kw)), \
                 patch.object(tasks, '_dispatch_late_mapped_epg_parses'), \
                 patch.object(utils.dj_timezone, 'now', side_effect=lambda: live['now']), \
                 policy_override, \
                 patch.object(tasks, '_EPG_PARSE_BATCH_SIZE', 3), patch.object(tasks, '_EPG_SWAP_BATCH_SIZE', 2), \
                 patch.object(tasks, '_flush_epg_program_staging_batch', side_effect=flush):
                passed = tasks.parse_programs_for_source(source)
            source.refresh_from_db()
            expected = snapshot(ids)
            event = next((row for row in audits if row.get('event_type') == 'epg_refresh'), None)
            cases.append({'name': name, 'source_id': source.pk, 'epg_ids': ids,
                'now': NOW.isoformat(), 'policy': {'min_days': 1, 'max_days': 30, 'default_prev_days': 0, 'default_days': 0},
                'xml_base64': base64.b64encode(payload).decode(), 'file_exists': name not in ('missing-file', 'no-mappings-no-file'),
                'before': before, 'expected': expected, 'passed': passed,
                'parsed': event.get('programs') if event else None,
                'channels': event.get('channels') if event else None,
                'skipped_unmapped': event.get('skipped_programs') if event else None,
                'source_status': source.status, 'source_last_message': source.last_message,
                'late_swap_policy': {'now': live['now'].isoformat(), 'policy': {'min_days': 0, 'max_days': 0, 'default_prev_days': 0, 'default_days': 0}} if name in ('policy-clock-change', 'cached-policy-clock-change') else None,
                'flushes': live['flushes'],
                'original_publication_actions': [row['args'][1] for row in published if len(row['args']) > 1]})
        finally:
            path.unlink(missing_ok=True)
        # Restore actual stored row identities, ready for the native service.
        ProgramData.objects.filter(epg_id__in=ids).delete()
        for row in before:
            ProgramData.objects.create(**row)
    print(json.dumps({'format_version': 1, 'scope': 'Actual original parse_programs_for_source file-to-guide components',
                      'cases': cases, 'application_jobs_verified': False, 'performance_verified': False}, default=serialize))


if __name__ == '__main__':
    main()
