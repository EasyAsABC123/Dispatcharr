"""Disposable recordings and exact stored playback metadata."""
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import shutil

from django.db import connection, transaction
from apps.channels.models import Channel, Recording
from core.utils import RedisClient

FILES = ('owned-completed.mkv', 'owned-index.m3u8', '000000.ts')


def dispatch(data):
    action = data['action']
    if action in ('recording_read_heartbeat', 'recording_read_reset_heartbeat'):
        recording = Recording.objects.get(pk=data['id'])
        if recording.custom_properties.get('_hls_dir') != '/data/recordings/owned-active':
            raise RuntimeError('Expected owned HLS recording heartbeat')
        key = f"dvr:hls_viewer:{data['id']}"
        cache = RedisClient.get_client()
        if action == 'recording_read_reset_heartbeat':
            cache.delete(key)
            return {'absent': cache.get(key) is None}
        return {'active': cache.get(key) in (b'1', '1'), 'ttl': cache.ttl(key)}
    with transaction.atomic():
        if action == 'recording_read_seed':
            if Recording.objects.exists():
                raise RuntimeError('Recording playback fixture requires a fresh database')
            directory = Path('/data/recordings')
            active_directory = directory / 'owned-active'
            active_directory.mkdir(exist_ok=True, parents=True)
            for name, dest in zip(FILES, (directory / FILES[0], active_directory / 'index.m3u8',
                                          active_directory / FILES[2]), strict=True):
                shutil.copyfile('/integration/recording_read_assets/' + name, dest)
                dest.chmod(0o644)
            channel = Channel.objects.get(name='Offline benchmark channel 0')
            start = datetime(2026, 11, 15, 12, 0, tzinfo=timezone.utc)
            end = datetime(2026, 11, 15, 12, 2, tzinfo=timezone.utc)
            completed, active = Recording.objects.bulk_create([
                Recording(channel=channel, start_time=start, end_time=end,
                          custom_properties={'status': 'completed', 'file_name': FILES[0],
                                             'file_path': str(directory / FILES[0]),
                                             'program': {'title': 'Owned completed recording'}}),
                Recording(channel=channel, start_time=start, end_time=end,
                          custom_properties={'status': 'recording', '_hls_dir': str(active_directory),
                                             'program': {'title': 'Owned active recording'}}),
            ])
            for row, suffix in ((completed, 'file/'), (active, 'hls/index.m3u8')):
                path = f'/api/channels/recordings/{row.pk}/{suffix}'
                props = {**row.custom_properties, 'file_url': path, 'output_file_url': path}
                Recording.objects.filter(pk=row.pk).update(custom_properties=props)
            return {'completed': completed.pk, 'active': active.pk,
                    'files': {name: hashlib.sha256(Path('/integration/recording_read_assets', name).read_bytes()).hexdigest()
                              for name in FILES}}
        if action != 'recording_read_snapshot':
            raise RuntimeError('Unknown recording playback fixture operation')
        rows = list(Recording.objects.order_by('id').values(
            'id', 'channel_id', 'start_time', 'end_time', 'task_id', 'custom_properties'))
        for row in rows:
            row['channel'] = row.pop('channel_id')
            for key in ('start_time', 'end_time'):
                row[key] = row[key].isoformat().replace('+00:00', 'Z')
        with connection.cursor() as cursor:
            cursor.execute('SELECT last_value,is_called FROM dispatcharr_channels_recording_id_seq')
            seq = list(cursor.fetchone())
        return {'rows': rows, 'sequence': seq}
