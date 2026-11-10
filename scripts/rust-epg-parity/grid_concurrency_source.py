"""Original grid statement/iterator schedules on disposable owned rows.
Original Dispatcharr authors retain all application and algorithm credit.
This is an original-only oracle, not native acceptance or a benchmark.
"""
import datetime as dt
import json
import os
import queue
import sys
import threading
import time
from unittest.mock import patch

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Disposable fixture required')
sys.path.insert(0, '/app')
sys.argv.append('shell')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'dispatcharr.settings')
import django
django.setup()
from django.db import connection, transaction, close_old_connections
from rest_framework.test import APIRequestFactory, force_authenticate
from apps.accounts.models import User
from apps.channels.models import Channel, ChannelProfile, ChannelProfileMembership
from apps.epg.models import EPGSource, EPGData, ProgramData
from apps.epg import api_grid

PREFIX = 'rust-grid-concurrency-'
NOW = dt.datetime(2026, 11, 10, 12, tzinfo=dt.timezone.utc)


def main():
    if User.objects.filter(username=PREFIX+'admin').exists():
        raise RuntimeError('Fresh owned namespace required')
    user = User.objects.create(username=PREFIX+'admin', password='!', user_level=10, is_active=True)
    source = EPGSource(name=PREFIX+'source', source_type='xmltv', is_active=False)
    EPGSource.objects.bulk_create([source])
    entry = EPGData.objects.create(name=PREFIX+'entry', tvg_id=PREFIX+'entry', epg_source=source)
    channel = Channel.objects.create(name=PREFIX+'channel', channel_number=99001, epg_data=entry)
    profile = ChannelProfile.objects.create(name=PREFIX+'profile')
    ChannelProfileMembership.objects.filter(channel_profile=profile).exclude(channel=channel).update(enabled=False)
    ChannelProfileMembership.objects.update_or_create(channel=channel, channel_profile=profile, defaults={'enabled': True})

    def programme(title):
        return ProgramData.objects.create(epg=entry, start_time=NOW-dt.timedelta(minutes=30),
            end_time=NOW+dt.timedelta(minutes=30), title=title, tvg_id=entry.tvg_id, custom_properties={})

    def request():
        value = APIRequestFactory().get('/api/epg/grid/', {'channel_profile_id': profile.pk})
        force_authenticate(value, user=user)
        return api_grid.EPGGridAPIView.as_view()(value)

    def capture(statements):
        def wrapper(execute, sql, params, many, context):
            value = execute(sql, params, many, context)
            statements.append({'sql': sql, 'params': params, 'many': many})
            return value
        return wrapper

    cases = []
    for name in ['replace-programmes-after-partition', 'hide-channel-after-partition', 'delete-programmes-after-opening-prefix']:
        Channel.objects.filter(pk=channel.pk).update(hidden_from_output=False)
        ProgramData.objects.filter(epg=entry).delete()
        original = programme('before-partition')
        statements = []
        with connection.execute_wrapper(capture(statements)), patch.object(api_grid.timezone, 'now', return_value=NOW):
            response = request()
            partition_queries = len(statements)
            iterator = iter(response.streaming_content)
            opening = next(iterator)
            if opening != b'{"data":[':
                raise RuntimeError('Unexpected original opening prefix')
            if name == 'replace-programmes-after-partition':
                ProgramData.objects.filter(pk=original.pk).delete()
                programme('after-partition')
            elif name == 'hide-channel-after-partition':
                Channel.objects.filter(pk=channel.pk).update(hidden_from_output=True)
            else:
                # Delete stored rows after publication without deleting the owned
                # EPG fixture used by later schedules.
                ProgramData.objects.filter(epg=entry).delete()
            body = opening+b''.join(iterator)
        decoded = json.loads(body)
        expected = ['after-partition'] if name.startswith('replace') else ['before-partition'] if name.startswith('hide') else []
        if [row['title'] for row in decoded['data']] != expected:
            raise RuntimeError('Unexpected original iterator schedule: '+name+' '+json.dumps({'body':decoded,'queries':statements},default=str))
        cases.append({'name': name, 'status': response.status_code, 'headers': dict(response.items()),
            'body': decoded, 'opening_prefix': opening.decode(), 'queries_before_iteration': partition_queries,
            'queries': statements, 'passed': True})

    Channel.objects.filter(pk=channel.pk).update(hidden_from_output=False)
    ProgramData.objects.filter(epg=entry).delete()
    programme('relation-lock-programme')
    finished = queue.Queue()
    started = threading.Event()

    def worker():
        close_old_connections()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET application_name='rust-grid-concurrency-worker'")
                cursor.execute("SET statement_timeout='8000ms'")
            started.set()
            statements = []
            with connection.execute_wrapper(capture(statements)), patch.object(api_grid.timezone, 'now', return_value=NOW):
                response = request()
                body = b''.join(response.streaming_content)
            finished.put({'status': response.status_code, 'body': json.loads(body),
                'queries': statements})
        except Exception as error:
            finished.put({'error_type': type(error).__name__, 'error': str(error)})
        finally:
            connection.close()

    thread = threading.Thread(target=worker, daemon=True)
    witness = None
    try:
        with transaction.atomic():
            with connection.cursor() as cursor:
                cursor.execute('LOCK TABLE epg_epgsource IN ACCESS EXCLUSIVE MODE')
            thread.start()
            if not started.wait(3):
                raise RuntimeError('Original worker did not start')
            deadline = time.monotonic()+4
            while time.monotonic() < deadline:
                with connection.cursor() as cursor:
                    cursor.execute('SELECT pg_stat_clear_snapshot()')
                    cursor.execute("SELECT pid,wait_event_type,wait_event,query FROM pg_stat_activity WHERE application_name='rust-grid-concurrency-worker' AND wait_event_type='Lock'")
                    row = cursor.fetchone()
                if row is not None:
                    witness = dict(zip(['pid', 'wait_event_type', 'wait_event', 'query'], row))
                    break
                time.sleep(.02)
            if witness is None or 'dispatcharr_channels' not in witness['query']:
                raise RuntimeError('Original metadata relation-lock witness missing')
            Channel.objects.filter(pk=channel.pk).update(hidden_from_output=True)
        thread.join(10)
        if thread.is_alive():
            raise RuntimeError('Original worker did not drain')
        result = finished.get(timeout=1)
        if result.get('status') != 200 or result.get('body') != {'data': []}:
            raise RuntimeError('Unexpected original visibility after relation lock: '+str(result))
        cases.append({'name': 'hide-channel-during-source-relation-lock', 'witness': witness, **result, 'passed': True})
    finally:
        if thread.ident is not None:
            thread.join(10)
        if thread.is_alive():
            raise RuntimeError('Original worker remains active after cleanup')
    print(json.dumps({'scope': 'Original grid partition/iterator and actual metadata relation-lock schedules',
        'cases': cases, 'clock': NOW.isoformat(), 'user_id': user.pk, 'profile_id': profile.pk,
        'channel_id': channel.pk, 'epg_id': entry.pk, 'source_id': source.pk,
        'native_verified': False, 'performance_verified': False,
        'credit': 'Original Dispatcharr authors and contributors retain all application and algorithm credit.'}, default=str))


main()
